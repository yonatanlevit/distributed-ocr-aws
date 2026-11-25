package dsp.ocr.worker;

import dsp.ocr.common.AppInfo;
import dsp.ocr.common.Aws;
import dsp.ocr.common.Config;
import dsp.ocr.common.Messages;
import dsp.ocr.common.Messages.ImageTask;
import dsp.ocr.common.Messages.OcrDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.util.List;

/**
 * Worker (README section 7): one task at a time.
 * Receive an IMAGE_TASK → download → OCR → send OCR_DONE → delete the task.
 *
 * Per-image problems (bad URL, HTTP error, not an image, OCR failure) become an error result,
 * and the Worker moves on. The task is deleted only after its result was sent, so if this
 * process dies mid-task, SQS hands the task to another Worker.
 *
 * The Worker exits (and on EC2 its instance then terminates) when:
 *  - no message has arrived for WORKER_IDLE_EXIT_SECONDS (scale-down, README section 9),
 *  - its queues no longer exist (its manager is gone),
 *  - one image exceeds MAX_TASK_PROCESSING_SECONDS (the task returns to the queue).
 *
 * Usage: java -jar worker.jar <tasksQueueUrl> <resultsQueueUrl>
 */
public class Worker {
    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private static final int MAX_ERROR_LENGTH = 200;
    private static final int EXIT_PROCESSING_CAP = 3;

    private final Aws aws;
    private final String tasksQueue;
    private final String resultsQueue;
    private final Ocr ocr;
    private final ImageDownloader downloader = new ImageDownloader();
    private final VisibilityHeartbeat heartbeat;

    Worker(Aws aws, String tasksQueue, String resultsQueue, Ocr ocr) {
        this.aws = aws;
        this.tasksQueue = tasksQueue;
        this.resultsQueue = resultsQueue;
        this.ocr = ocr;
        this.heartbeat = new VisibilityHeartbeat(aws);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: java -jar worker.jar <tasksQueueUrl> <resultsQueueUrl>");
            System.exit(2);
        }
        log.info(AppInfo.banner("Worker"));
        Aws aws = new Aws();

        Ocr ocr = new Ocr();
        ocr.selfTest(); // fail fast if Tesseract isn't usable, before taking any task
        log.info("OCR self-test passed; tasks: {}, results: {}", args[0], args[1]);

        System.exit(new Worker(aws, args[0], args[1], ocr).run());
    }

    /** Runs until it's time to exit; returns the process exit code. */
    int run() {
        long lastMessageAt = System.currentTimeMillis();
        while (true) {
            try {
                List<Message> messages = aws.receive(tasksQueue, 1);
                if (messages.isEmpty()) {
                    long idleSeconds = (System.currentTimeMillis() - lastMessageAt) / 1000;
                    if (idleSeconds >= Config.WORKER_IDLE_EXIT_SECONDS) {
                        log.info("no task for {}s, exiting", idleSeconds);
                        return 0;
                    }
                    continue;
                }
                for (Message m : messages) {
                    handle(m);
                }
                lastMessageAt = System.currentTimeMillis(); // idle time starts after the work
            } catch (QueueDoesNotExistException e) {
                log.info("my queues no longer exist (the manager is gone), exiting");
                return 0;
            } catch (Exception e) {
                // An AWS call failed (network, throttling). The current task, if any, wasn't
                // deleted, so it will come back. Pause briefly and keep going.
                log.error("loop error, retrying in 5s: {}", describe(e));
                sleep(5_000);
            }
        }
    }

    private void handle(Message m) throws Exception {
        ImageTask task;
        try {
            task = (ImageTask) Messages.fromJson(m.body());
        } catch (IllegalArgumentException | ClassCastException e) {
            // Not an image task at all: retrying won't help, so drop it.
            log.warn("dropping malformed message {}: {}", m.messageId(), describe(e));
            aws.delete(tasksQueue, m);
            return;
        }

        String what = "job " + task.jobId() + " line " + task.index();
        long start = System.currentTimeMillis();
        OcrDone result;
        try (AutoCloseable ignored = heartbeat.start(tasksQueue, m.receiptHandle(), what, () -> {
            log.error("{}: still running after {}s; exiting so the task returns to the queue",
                    what, Config.MAX_TASK_PROCESSING_SECONDS);
            System.exit(EXIT_PROCESSING_CAP);
        })) {
            result = process(task);
            aws.send(resultsQueue, encode(task, result));
        }
        aws.delete(tasksQueue, m); // only after the result is safely sent

        long ms = System.currentTimeMillis() - start;
        if (!result.ok()) {
            log.info("{}: ERROR in {} ms: {}  ({})", what, ms, result.error(), task.url());
        } else {
            log.info("{}: OK in {} ms, {} chars  ({})", what, ms, result.text().length(), task.url());
        }
    }

    /**
     * The OCR_DONE body to send. If it would be too large for SQS, the text goes to S3 and the
     * message carries its key instead (README section 3). The limit is on the encoded body's
     * bytes, not the text's characters.
     */
    private String encode(ImageTask task, OcrDone result) {
        String json = Messages.toJson(result);
        if (result.text() == null || Messages.sizeInBytes(json) <= Config.largeTextThresholdBytes()) {
            return json;
        }
        String key = Config.textKey(task.jobId(), task.index());
        aws.putString(Config.BUCKET, key, result.text());
        log.info("job {} line {}: text is {} bytes, stored in S3 at {}",
                task.jobId(), task.index(), Messages.sizeInBytes(json), key);
        return Messages.toJson(OcrDone.successInS3(task, key));
    }

    /** Never throws for per-image problems: they become an error result. */
    private OcrDone process(ImageTask task) {
        try {
            byte[] image = downloader.download(task.url());
            return OcrDone.success(task, ocr.recognize(image));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return OcrDone.failure(task, "interrupted");
        } catch (Exception | LinkageError e) {
            // LinkageError: a native-library problem in Tess4J is an Error, not an Exception.
            return OcrDone.failure(task, describe(e));
        }
    }

    /** Short one-line description, e.g. "IOException: HTTP 404" or "UnresolvedAddressException". */
    static String describe(Throwable e) {
        Throwable t = e;
        // Some exceptions (e.g. ConnectException for an unknown host) carry no message; use the cause's.
        while (t.getMessage() == null && t.getCause() != null) t = t.getCause();
        String text = t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage());
        // A wrapper's own message can be vague ("Caught exception during read:"); name what it wraps.
        Throwable cause = t.getCause();
        if (cause != null && cause != t) {
            text += " (" + cause.getClass().getSimpleName() + (cause.getMessage() == null ? "" : ": " + cause.getMessage()) + ")";
        }
        text = text.replaceAll("\\s+", " ").strip();
        return text.length() > MAX_ERROR_LENGTH ? text.substring(0, MAX_ERROR_LENGTH) + "..." : text;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
