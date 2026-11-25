package dsp.ocr.local;

import dsp.ocr.common.AppInfo;
import dsp.ocr.common.Aws;
import dsp.ocr.common.Config;
import dsp.ocr.common.Ec2Launcher;
import dsp.ocr.common.Messages;
import dsp.ocr.common.Messages.Done;
import dsp.ocr.common.Messages.NewTask;
import dsp.ocr.common.Messages.Rejected;
import dsp.ocr.common.Messages.Terminate;
import dsp.ocr.common.Queues;
import dsp.ocr.common.SummaryLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Local Application (README section 5). One sequential flow that mostly waits:
 * upload the input → send NEW_TASK to a manager → wait for DONE (or REJECTED, then try again
 * elsewhere) → download the summary → write the HTML → optionally send TERMINATE → clean up.
 *
 * Usage: java -jar local-app.jar inputFileName outputFileName manager-clients-n worker-files-n [terminate]
 *
 * The manager is found on EC2 (or launched). For development against a manager running on the
 * laptop, -Dmanager.id=&lt;id&gt; skips discovery and uses that manager.
 */
public class LocalApp {
    private static final Logger log = LoggerFactory.getLogger(LocalApp.class);

    /** After this many failures (FAILED replies, lost managers), the client gives up (README section 13). */
    private static final int MAX_FAILURES = 3;
    private static final long WAIT_LOG_INTERVAL_MS = 60_000;
    /**
     * While no manager can be launched, discovery is retried after 15 s, then every 30 s. Short
     * enough to notice soon that capacity freed up; each retry is just one DescribeInstances call.
     */
    private static final long MAX_BACKOFF_MS = 30_000;

    private final Aws aws;
    private final Args args;
    private final Ec2Launcher launcher;
    private final String jobId = UUID.randomUUID().toString();
    private volatile String replyQueue;

    LocalApp(Aws aws, Args args) {
        this.aws = aws;
        this.args = args;
        this.launcher = new Ec2Launcher(aws);
    }

    public static void main(String[] argv) {
        Args args;
        try {
            args = Args.parse(argv);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(Args.USAGE);
            System.exit(2);
            return;
        }
        log.info(AppInfo.banner("LocalApp"));
        try (Aws aws = new Aws()) {
            System.exit(new LocalApp(aws, args).run());
        }
    }

    int run() {
        long start = System.currentTimeMillis();
        replyQueue = Queues.createClientQueue(aws, jobId);
        // Ctrl+C while waiting: still remove this run's reply queue.
        Thread cleanupHook = new Thread(this::deleteReplyQueue, "cleanup");
        Runtime.getRuntime().addShutdownHook(cleanupHook);
        try {
            String inputKey = Config.inputKey(jobId);
            aws.putFile(Config.BUCKET, inputKey, args.input());
            log.info("job {}: input uploaded to s3://{}/{}", jobId, Config.BUCKET, inputKey);

            // README section 5, steps 3-6: refusals mean "try the next manager" and are not limited;
            // failures (FAILED, or a lost manager) are, since they may repeat.
            Map<String, String> refusedBy = new LinkedHashMap<>();
            int failures = 0;
            while (true) {
                String managerId = nextManager(refusedBy);
                if (managerId == null) {
                    log.error("job {}: the manager given with -Dmanager.id refused the job: {}", jobId, refusedBy);
                    return 1;
                }
                Messages.Message reply;
                try {
                    reply = submitAndWait(managerId, inputKey);
                } catch (ManagerLost e) {
                    failures++;
                    log.warn("job {}: {} (failure {} of {}); resubmitting, the input is still in S3",
                            jobId, e.getMessage(), failures, MAX_FAILURES);
                    if (failures == MAX_FAILURES) {
                        log.error("job {}: giving up after {} failures", jobId, failures);
                        return 1;
                    }
                    refusedBy.put(managerId, "LOST");
                    continue;
                }
                if (reply instanceof Done done) {
                    writeOutput(done);
                    if (args.terminate()) sendTerminate(managerId);
                    log.info("job {}: finished in {}s; output written to {}",
                            jobId, (System.currentTimeMillis() - start) / 1000, args.output());
                    return 0;
                }
                if (reply instanceof Rejected r && r.reason() == Rejected.Reason.FAILED) {
                    failures++;
                    log.warn("job {}: manager {} could not process the job (failure {} of {})",
                            jobId, managerId, failures, MAX_FAILURES);
                    if (failures == MAX_FAILURES) {
                        log.error("job {}: giving up after {} failures", jobId, failures);
                        return 1;
                    }
                    continue;
                }
                String reason = reply instanceof Rejected r ? r.reason().name() : "GONE";
                refusedBy.put(managerId, reason);
                log.info("job {}: manager {} refused ({}); trying another", jobId, managerId, reason);
            }
        } catch (Exception e) {
            log.error("job {}: failed: {}", jobId, e.toString());
            return 1;
        } finally {
            deleteReplyQueue();
            deleteJobFiles(); // success or failure: this run's job id is never used again
            try {
                Runtime.getRuntime().removeShutdownHook(cleanupHook);
            } catch (IllegalStateException alreadyShuttingDown) {
                // Ctrl+C: the hook is running (or ran) already
            }
        }
    }

    /**
     * Picks the next manager to try (README section 5, step 3): a running manager that hasn't
     * refused this job, in random order; if there is none, launches a new one through the
     * instance guard; if the guard has no room, waits with backoff and looks again.
     *
     * Development override: with -Dmanager.id=&lt;id&gt; only that manager is used (null once it refused).
     */
    private String nextManager(Map<String, String> refusedBy) throws InterruptedException {
        String fixed = System.getProperty("manager.id");
        if (fixed != null) {
            return refusedBy.containsKey(fixed) ? null : fixed;
        }
        long backoffMs = 15_000;
        while (true) {
            List<String> candidates = new ArrayList<>(launcher.runningManagers());
            candidates.removeAll(refusedBy.keySet());
            if (!candidates.isEmpty()) {
                Collections.shuffle(candidates); // clients starting together spread over the managers
                log.info("job {}: {} manager(s) to try; trying {}", jobId, candidates.size(), candidates.get(0));
                return candidates.get(0);
            }
            String launched = launchManager();
            if (launched != null) return launched;
            log.info("job {}: no manager available and no room to launch one; looking again in {}s", jobId, backoffMs / 1000);
            Thread.sleep(backoffMs);
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            refusedBy.values().removeIf("FULL"::equals); // a full manager frees up when one of its jobs finishes
        }
    }

    /**
     * Launches a new manager with a fresh UUID. Its inbox is created first, so a client that
     * discovers the booting manager can already send to it; if the launch doesn't happen, the
     * inbox is deleted again. Returns the manager id, or null if the guard had no room.
     */
    private String launchManager() {
        String managerId = UUID.randomUUID().toString();
        String inbox = Queues.ensureInbox(aws, managerId);
        try {
            var instance = launcher.launchManager(managerId);
            if (instance.isPresent()) {
                log.info("job {}: no manager available; launched manager {} (instance {})", jobId, managerId, instance.get());
                return managerId;
            }
        } catch (Exception e) {
            log.warn("job {}: launching a manager failed: {}", jobId, e.toString());
        }
        aws.deleteQueue(inbox);
        return null;
    }

    /** The manager's instance disappeared while the client was waiting for it (found by the watchdog). */
    private static final class ManagerLost extends Exception {
        ManagerLost(String managerId) {
            super("manager " + managerId + " has no running instance anymore");
        }
    }

    /**
     * Sends NEW_TASK and waits for this job's DONE or REJECTED. Returns null if the manager's inbox is gone.
     * Every WATCHDOG_SECONDS it checks that the manager's instance still exists (README section 5,
     * step 5); not with -Dmanager.id, where the manager runs outside EC2.
     *
     * @throws ManagerLost if the manager's instance is gone
     */
    private Messages.Message submitAndWait(String managerId, String inputKey) throws ManagerLost {
        String inbox;
        try {
            inbox = aws.sqs.getQueueUrl(b -> b.queueName(Queues.inboxName(managerId))).queueUrl();
            aws.send(inbox, Messages.toJson(
                    new NewTask(jobId, inputKey, args.workerFilesN(), args.managerClientsN(), replyQueue)));
        } catch (QueueDoesNotExistException e) {
            return null; // README section 5, step 6: a missing inbox counts as a rejection
        }
        log.info("job {}: NEW_TASK sent to manager {}; waiting for the result", jobId, managerId);

        boolean watchdog = System.getProperty("manager.id") == null;
        long waitingSince = System.currentTimeMillis();
        long nextLog = waitingSince + WAIT_LOG_INTERVAL_MS;
        long nextCheck = waitingSince + Config.WATCHDOG_SECONDS * 1000L;
        while (true) {
            if (watchdog && System.currentTimeMillis() >= nextCheck) {
                if (!launcher.isManagerAlive(managerId)) throw new ManagerLost(managerId);
                nextCheck += Config.WATCHDOG_SECONDS * 1000L;
            }
            for (Message m : aws.receive(replyQueue, 1)) {
                aws.delete(replyQueue, m);
                Messages.Message reply;
                try {
                    reply = Messages.fromJson(m.body());
                } catch (IllegalArgumentException e) {
                    log.warn("ignoring malformed reply: {}", e.getMessage());
                    continue;
                }
                // Only replies for this job from the manager being waited on count (README section 3).
                if (reply instanceof Done d && d.jobId().equals(jobId) && managerId.equals(d.managerId())) return d;
                if (reply instanceof Rejected r && r.jobId().equals(jobId) && managerId.equals(r.managerId())) return r;
                log.warn("ignoring unexpected reply: {}", m.body());
            }
            if (System.currentTimeMillis() >= nextLog) {
                log.info("job {}: still waiting ({}s)", jobId, (System.currentTimeMillis() - waitingSince) / 1000);
                nextLog += WAIT_LOG_INTERVAL_MS;
            }
        }
    }

    /** Downloads the summary, orders it by index, and writes the HTML. */
    private void writeOutput(Done done) throws IOException {
        Path summary = Files.createTempFile("ocr-summary-", ".jsonl");
        try {
            aws.downloadToFile(Config.BUCKET, done.summaryKey(), summary);
            List<SummaryLine> lines;
            try (Stream<String> s = Files.lines(summary)) {
                lines = s.filter(l -> !l.isBlank()).map(SummaryLine::fromJson)
                        .sorted(Comparator.comparingInt(SummaryLine::index)).toList();
            }
            HtmlWriter.write(args.output(), lines, l -> l.textKey() == null ? l.text()
                    : aws.getString(Config.BUCKET, l.textKey()));
            long ok = lines.stream().filter(SummaryLine::ok).count();
            log.info("job {}: {} results ({} with text, {} errors)", jobId, lines.size(), ok, lines.size() - ok);
        } finally {
            Files.deleteIfExists(summary);
        }
    }

    private void sendTerminate(String managerId) {
        try {
            String inbox = aws.sqs.getQueueUrl(b -> b.queueName(Queues.inboxName(managerId))).queueUrl();
            aws.send(inbox, Messages.toJson(new Terminate(jobId)));
            log.info("job {}: TERMINATE sent to manager {}", jobId, managerId);
        } catch (QueueDoesNotExistException e) {
            log.info("job {}: manager {} is already gone, nothing to terminate", jobId, managerId);
        }
    }

    /** Removes jobs/&lt;jobId&gt;/ (input, summary, large texts) from S3. */
    private void deleteJobFiles() {
        try {
            aws.deletePrefix(Config.BUCKET, "jobs/" + jobId + "/");
        } catch (Exception e) {
            log.warn("could not delete s3://{}/jobs/{}/ (the cleanup command will): {}", Config.BUCKET, jobId, e.toString());
        }
    }

    private synchronized void deleteReplyQueue() {
        if (replyQueue == null) return;
        try {
            aws.deleteQueue(replyQueue);
        } catch (Exception e) {
            log.warn("could not delete reply queue {}: {}", replyQueue, e.toString());
        }
        replyQueue = null;
    }
}
