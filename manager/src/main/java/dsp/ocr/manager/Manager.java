package dsp.ocr.manager;

import dsp.ocr.common.AppInfo;
import dsp.ocr.common.Aws;
import dsp.ocr.common.Config;
import dsp.ocr.common.Ec2Launcher;
import dsp.ocr.common.Messages;
import dsp.ocr.common.Messages.Done;
import dsp.ocr.common.Messages.ImageTask;
import dsp.ocr.common.Messages.NewTask;
import dsp.ocr.common.Messages.OcrDone;
import dsp.ocr.common.Messages.Rejected;
import dsp.ocr.common.Messages.Terminate;
import dsp.ocr.common.Queues;
import dsp.ocr.common.Queues.ManagerQueues;
import dsp.ocr.common.SummaryLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manager (README section 6), core part: admission, splitting, collecting, summaries.
 * It shuts down on TERMINATE, or by itself after a while with no jobs (idle retirement).
 *
 * Threads (README section 6, "Threads"):
 *  - intake (1): reads the inbox, admits or rejects each NEW_TASK, hands admitted jobs to the job pool
 *  - job pool (4): downloads an input, counts its URLs, registers the job, sends IMAGE_TASKs in batches
 *  - collectors (2): read OCR_DONE results 10 at a time and record them into their jobs
 *  - dead-letter reader (1): records each task in the DLQ as a failed line
 *  - finisher pool (2): uploads a finished job's summary and sends DONE
 *  - scaler (1, scheduled): launches workers as the active jobs need them
 *
 * Usage: java -jar manager.jar <managerId>
 */
public class Manager {
    private static final Logger log = LoggerFactory.getLogger(Manager.class);

    private static final int JOB_THREADS = 4;
    private static final int COLLECTOR_THREADS = 2;
    private static final int FINISHER_THREADS = 2;
    private static final int TASK_BATCH = 10;

    private final Aws aws;
    private final String managerId;
    private final ManagerQueues queues;
    private final Path workDir;

    /** Jobs that are registered and not yet finished, by jobId. */
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    /** Every jobId ever admitted, so a duplicate NEW_TASK delivery is ignored. */
    private final Set<String> admitted = ConcurrentHashMap.newKeySet();
    /** Admitted jobs not yet finished, including ones still being split. Only intake increments it. */
    private final AtomicInteger activeJobs = new AtomicInteger();
    /** When the manager last became idle (no active jobs); drives idle retirement. */
    private final AtomicLong idleSince = new AtomicLong(System.currentTimeMillis());
    private volatile boolean accepting = true;
    /** Why the manager stopped accepting: TERMINATING or RETIRING (null while accepting). */
    private volatile Rejected.Reason shutdownReason;
    private final CountDownLatch shutdownRequested = new CountDownLatch(1);
    /** The receive loops run while this is true. */
    private volatile boolean running = true;

    private final ExecutorService jobPool = Executors.newFixedThreadPool(JOB_THREADS, named("job"));
    private final ExecutorService finisherPool = Executors.newFixedThreadPool(FINISHER_THREADS, named("finisher"));
    /** One thread, so two rebalances never run at the same time. */
    private final ScheduledExecutorService scaler = Executors.newSingleThreadScheduledExecutor(named("scaler"));
    private final Ec2Launcher launcher;
    /** False only for development runs on a laptop (-Dmanager.scale=false): then no workers are launched. */
    private final boolean scaling;

    Manager(Aws aws, String managerId, ManagerQueues queues, Path workDir, Ec2Launcher launcher, boolean scaling) {
        this.aws = aws;
        this.managerId = managerId;
        this.queues = queues;
        this.workDir = workDir;
        this.launcher = launcher;
        this.scaling = scaling;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: java -jar manager.jar <managerId>");
            System.exit(2);
        }
        log.info(AppInfo.banner("Manager"));
        Aws aws = new Aws();
        ManagerQueues queues = Queues.ensureManagerQueues(aws, args[0]);
        Path workDir = Files.createTempDirectory("ocr-manager-");
        boolean scaling = !"false".equals(System.getProperty("manager.scale"));
        new Manager(aws, args[0], queues, workDir, new Ec2Launcher(aws), scaling).run();
    }

    void run() throws InterruptedException {
        if (scaling) {
            scaler.scheduleWithFixedDelay(this::rebalance,
                    Config.SCALER_TICK_SECONDS, Config.SCALER_TICK_SECONDS, TimeUnit.SECONDS);
        } else {
            log.info("scaling disabled (-Dmanager.scale=false): start workers by hand");
        }
        List<Thread> loops = new ArrayList<>();
        loops.add(startLoop("intake", this::intakeOnce));
        for (int i = 1; i <= COLLECTOR_THREADS; i++) {
            loops.add(startLoop("collector-" + i, () -> collectOnce(queues.results(), false)));
        }
        loops.add(startLoop("dlq-reader", () -> collectOnce(queues.dlq(), true)));
        log.info("Manager {} ready. Inbox: {}", managerId, queues.inbox());
        log.info("Workers for this manager run as: java -jar worker.jar {} {}", queues.tasks(), queues.results());

        shutdownRequested.await();
        shutDown(loops);
    }

    // ---------------- shutdown (README section 6, "Termination" and "Idle retirement") ----------------

    /** Stops accepting new jobs and starts the shutdown sequence. Called on the intake thread only. */
    private void beginShutdown(Rejected.Reason reason) {
        if (!accepting) return; // already shutting down
        accepting = false;
        shutdownReason = reason;
        log.info("shutting down ({}): no longer accepting new jobs", reason);
        shutdownRequested.countDown();
    }

    /**
     * 1. wait until every active job has finished and got its DONE (intake keeps rejecting new tasks)
     * 2. when retiring, keep rejecting for a grace period, for clients that found this manager just now
     * 3. stop the receive loops (each ends after its current long poll)
     * 4. terminate this manager's workers, found by their ManagerId tag
     * 5. delete this manager's four queues
     * 6. exit; on EC2 the boot script uploads this log and the instance terminates
     */
    private void shutDown(List<Thread> loops) throws InterruptedException {
        long nextLog = 0;
        while (activeJobs.get() > 0) {
            if (System.currentTimeMillis() >= nextLog) {
                log.info("shutdown: waiting for {} active job(s) to finish", activeJobs.get());
                nextLog = System.currentTimeMillis() + 30_000;
            }
            Thread.sleep(2_000);
        }
        if (shutdownReason == Rejected.Reason.RETIRING) {
            log.info("shutdown: rejecting new tasks for a {}s grace period", Config.RETIRE_GRACE_SECONDS);
            Thread.sleep(Config.RETIRE_GRACE_SECONDS * 1000L);
        }

        running = false;
        for (Thread t : loops) t.join(30_000);
        scaler.shutdownNow();
        jobPool.shutdownNow();
        finisherPool.shutdownNow();

        try {
            List<String> workers = launcher.terminateWorkers(managerId);
            log.info("shutdown: terminated {} worker(s) {}", workers.size(), workers);
        } catch (Exception e) {
            log.warn("shutdown: could not terminate workers ({}); they exit by themselves when idle", e.toString());
        }
        for (String queue : List.of(queues.inbox(), queues.tasks(), queues.results(), queues.dlq())) {
            try {
                aws.deleteQueue(queue);
            } catch (Exception e) {
                log.warn("shutdown: could not delete {}: {}", queue, e.toString());
            }
        }
        log.info("shutdown: queues deleted; manager {} exiting", managerId);
        System.exit(0);
    }

    // ---------------- intake ----------------

    private void intakeOnce() {
        List<Message> messages = aws.receive(queues.inbox(), 10);
        for (Message m : messages) {
            handleInbox(m);
        }
        aws.deleteBatch(queues.inbox(), messages);

        // Idle retirement is decided here, on the intake thread, so it can't race with an admission.
        long idleMs = System.currentTimeMillis() - idleSince.get();
        if (accepting && activeJobs.get() == 0 && idleMs >= Config.managerIdleRetireSeconds() * 1000L) {
            log.info("no jobs for {}s", idleMs / 1000);
            beginShutdown(Rejected.Reason.RETIRING);
        }
    }

    private void handleInbox(Message m) {
        Messages.Message msg;
        try {
            msg = Messages.fromJson(m.body());
        } catch (IllegalArgumentException e) {
            log.warn("dropping malformed inbox message: {}", e.getMessage());
            return;
        }
        switch (msg) {
            case NewTask t -> admit(t);
            case Terminate t -> {
                log.info("TERMINATE from job {}", t.jobId());
                beginShutdown(Rejected.Reason.TERMINATING);
            }
            default -> log.warn("unexpected {} in the inbox, dropped", msg.getClass().getSimpleName());
        }
    }

    /**
     * Admission (README section 6). Runs only on the intake thread, and the active count grows
     * at the moment of admission, so clients arriving together can't overfill the manager.
     */
    private void admit(NewTask t) {
        if (admitted.contains(t.jobId())) {
            log.info("job {}: duplicate NEW_TASK ignored", t.jobId());
            return;
        }
        if (t.inputKey() == null || t.replyQueueUrl() == null || t.workerFilesN() < 1 || t.managerClientsN() < 1) {
            log.warn("job {}: invalid NEW_TASK {}", t.jobId(), t);
            reject(t, Rejected.Reason.FAILED);
            return;
        }
        if (!accepting) {
            log.info("job {}: rejected {} (this manager is shutting down)", t.jobId(), shutdownReason);
            reject(t, shutdownReason); // TERMINATING or RETIRING
            return;
        }
        int active = activeJobs.get();
        if (active >= t.managerClientsN()) {
            log.info("job {}: rejected FULL ({} active, client's limit is {})", t.jobId(), active, t.managerClientsN());
            reject(t, Rejected.Reason.FULL);
            return;
        }
        activeJobs.incrementAndGet();
        admitted.add(t.jobId());
        log.info("job {}: admitted ({} active, client's limit is {})", t.jobId(), active + 1, t.managerClientsN());
        jobPool.submit(() -> startJob(t));
    }

    // ---------------- job pool: split ----------------

    private void startJob(NewTask t) {
        String jobId = t.jobId();
        Path input = workDir.resolve(jobId + ".input.txt");
        Job job = null;
        try {
            aws.downloadToFile(Config.BUCKET, t.inputKey(), input);
            int total = countUrls(input);
            job = new Job(t, total, workDir.resolve(jobId + ".summary.jsonl"));
            jobs.put(jobId, job); // registered before any task is sent, so no result can arrive first
            log.info("job {}: {} URLs, {} images per worker", jobId, total, t.workerFilesN());
            if (job.tryFinish()) { // empty input
                Job empty = job;
                finisherPool.submit(() -> finish(empty));
                return;
            }
            triggerRebalance(); // workers start booting while the tasks are being sent
            sendTasks(job, input);
            log.info("job {}: all {} tasks sent", jobId, total);
        } catch (Exception e) {
            log.error("job {}: could not start: {}", jobId, e.toString());
            if (job != null) {
                jobs.remove(jobId);
                job.discard();
            }
            admitted.remove(jobId); // the client resubmits
            jobEnded();
            reject(t, Rejected.Reason.FAILED);
        } finally {
            deleteQuietly(input);
        }
    }

    /** Counts the input's URLs: every non-blank line. sendTasks skips the same lines. */
    private static int countUrls(Path input) throws IOException {
        try (BufferedReader r = reader(input)) {
            return (int) r.lines().filter(line -> !line.isBlank()).count();
        }
    }

    /** Streams the input from disk and sends one IMAGE_TASK per URL, in batches of 10. */
    private void sendTasks(Job job, Path input) throws IOException {
        List<String> batch = new ArrayList<>(TASK_BATCH);
        int index = 0;
        try (BufferedReader r = reader(input)) {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                if (line.isBlank()) continue;
                batch.add(Messages.toJson(new ImageTask(job.task.jobId(), index++, line.strip())));
                if (batch.size() == TASK_BATCH) {
                    aws.sendBatch(queues.tasks(), batch);
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) aws.sendBatch(queues.tasks(), batch);
    }

    /** UTF-8 reader that replaces undecodable bytes instead of failing on them. */
    private static BufferedReader reader(Path file) throws IOException {
        return new BufferedReader(new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8));
    }

    // ---------------- collectors and dead-letter reader ----------------

    private void collectOnce(String queueUrl, boolean fromDlq) {
        List<Message> messages = aws.receive(queueUrl, 10);
        List<Message> handled = new ArrayList<>(messages.size());
        for (Message m : messages) {
            try {
                record(m, fromDlq);
                handled.add(m);
            } catch (Exception e) {
                // Not deleted, so it comes back after its visibility timeout and is retried.
                log.warn("could not record a result, it will be retried: {}", e.toString());
            }
        }
        aws.deleteBatch(queueUrl, handled);
    }

    private void record(Message m, boolean fromDlq) throws IOException {
        Messages.Message msg;
        try {
            msg = Messages.fromJson(m.body());
        } catch (IllegalArgumentException e) {
            log.warn("dropping malformed result: {}", e.getMessage());
            return;
        }
        OcrDone result;
        if (fromDlq && msg instanceof ImageTask t) {
            log.warn("job {} line {}: task failed {} deliveries, recorded as failed ({})",
                    t.jobId(), t.index(), Config.MAX_RECEIVE_COUNT, t.url());
            result = OcrDone.failure(t, "failed after " + Config.MAX_RECEIVE_COUNT + " attempts");
        } else if (!fromDlq && msg instanceof OcrDone r) {
            result = r;
        } else {
            log.warn("unexpected {} dropped", msg.getClass().getSimpleName());
            return;
        }

        Job job = jobs.get(result.jobId());
        if (job == null) {
            log.info("result for unknown or finished job {} (line {}) dropped", result.jobId(), result.index());
            return;
        }
        switch (job.record(result.index(), SummaryLine.from(result).toJson())) {
            case COMPLETED -> finisherPool.submit(() -> finish(job));
            case DUPLICATE -> log.info("job {} line {}: duplicate result ignored", result.jobId(), result.index());
            case OUT_OF_RANGE -> log.warn("job {}: result index {} out of range, dropped", result.jobId(), result.index());
            case RECORDED -> logProgress(job);
        }
    }

    private static void logProgress(Job job) {
        int completed = job.completed();
        int step = Math.max(1, job.total / 10);
        if (completed % step == 0) {
            log.info("job {}: {}/{} results", job.task.jobId(), completed, job.total);
        }
    }

    // ---------------- scaler (README section 6, "Scaling workers") ----------------

    private void triggerRebalance() {
        if (scaling) scaler.execute(this::rebalance);
    }

    /**
     * Launches m - k workers, where m is what the active jobs call for and k is this manager's
     * live workers. Only adds workers: idle workers exit by themselves (README section 9).
     * Runs every SCALER_TICK_SECONDS and whenever a job is registered; the periodic tick also
     * replaces workers that died and retries launches the guard deferred.
     */
    private void rebalance() {
        try {
            int wanted = desiredWorkers(jobs.values());
            if (wanted == 0) return;
            int live = launcher.liveWorkers(managerId).size();
            if (wanted <= live) return;
            List<String> launched = launcher.launchWorkers(managerId, queues.tasks(), queues.results(), wanted - live);
            log.info("scaler: {} workers wanted, {} live, launched {} {}", wanted, live, launched.size(), launched);
        } catch (Exception e) {
            log.warn("scaler: {}; retrying on the next tick", e.toString()); // never kill the scheduled task
        }
    }

    /** m = Σ ⌈remaining_j / n_j⌉ over the active jobs, each with its own client's worker-files-n. */
    static int desiredWorkers(Collection<Job> jobs) {
        int m = 0;
        for (Job job : jobs) {
            int remaining = job.remaining();
            int n = job.task.workerFilesN();
            m += (remaining + n - 1) / n;
        }
        return m;
    }

    // ---------------- finisher ----------------

    private void finish(Job job) {
        String jobId = job.task.jobId();
        try {
            Path summary = job.closeResults();
            String key = Config.summaryKey(jobId);
            withRetries("upload summary of job " + jobId, () -> aws.putFile(Config.BUCKET, key, summary));
            reply(job.task, new Done(jobId, managerId, key));
            log.info("job {}: DONE, {} results in {}s, summary at {}",
                    jobId, job.total, (System.currentTimeMillis() - job.startedAtMillis) / 1000, key);
        } catch (Exception e) {
            log.error("job {}: could not deliver its summary: {}", jobId, e.toString());
            admitted.remove(jobId); // let the client resubmit
            reject(job.task, Rejected.Reason.FAILED);
        } finally {
            jobs.remove(jobId);
            jobEnded();
            try {
                Files.deleteIfExists(workDir.resolve(jobId + ".summary.jsonl"));
            } catch (IOException ignored) {
                // a leftover temp file is harmless
            }
        }
    }

    // ---------------- replies ----------------

    private void reject(NewTask t, Rejected.Reason reason) {
        try {
            reply(t, new Rejected(t.jobId(), managerId, reason));
        } catch (Exception e) {
            log.warn("job {}: could not send REJECTED {}: {}", t.jobId(), reason, e.toString());
        }
    }

    /** Sends to the client's reply queue. A deleted reply queue means the client is gone: nothing to retry. */
    private void reply(NewTask t, Messages.Message message) throws Exception {
        try {
            withRetries("reply to job " + t.jobId(), () -> aws.send(t.replyQueueUrl(), Messages.toJson(message)));
        } catch (QueueDoesNotExistException e) {
            log.warn("job {}: reply queue no longer exists (client gone)", t.jobId());
        }
    }

    // ---------------- helpers ----------------

    private interface Step {
        void run() throws Exception;
    }

    /** Runs a step with up to 4 attempts and growing pauses (2, 4, 8 s). A deleted queue is not retried. */
    private static void withRetries(String what, Step step) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                step.run();
                return;
            } catch (QueueDoesNotExistException e) {
                throw e;
            } catch (Exception e) {
                if (attempt == 4) throw e;
                log.warn("{} failed (attempt {}), retrying: {}", what, attempt, e.toString());
                Thread.sleep(1000L << attempt);
            }
        }
    }

    /** A slot freed: an admitted job finished or failed to start. */
    private void jobEnded() {
        if (activeJobs.decrementAndGet() == 0) {
            idleSince.set(System.currentTimeMillis());
        }
    }

    /** A thread that runs {@code body} until shutdown; an exception is logged and the loop continues. */
    private Thread startLoop(String name, Step body) {
        Thread t = new Thread(() -> {
            while (running) {
                try {
                    body.run();
                } catch (Exception e) {
                    log.error("{} loop error, retrying in 5s: {}", name, e.toString());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        }, name);
        t.start();
        return t;
    }

    private static ThreadFactory named(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> new Thread(r, prefix + "-" + n.incrementAndGet());
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // a leftover temp file is harmless
        }
    }
}
