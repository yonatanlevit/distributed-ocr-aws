package dsp.ocr.worker;

import dsp.ocr.common.Aws;
import dsp.ocr.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Keeps the task being processed hidden from other Workers (README section 7).
 * Every HEARTBEAT_SECONDS it resets the message's visibility timeout to TASK_VISIBILITY_SECONDS.
 * Once MAX_TASK_PROCESSING_SECONDS have passed it stops and runs {@code onCapReached}
 * (the Worker exits, because OCR runs in native code that cannot be interrupted).
 * If this process dies, the extensions stop and SQS makes the task visible again.
 */
final class VisibilityHeartbeat {
    private static final Logger log = LoggerFactory.getLogger(VisibilityHeartbeat.class);

    private final Aws aws;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "heartbeat");
        t.setDaemon(true); // never keeps the JVM alive on its own
        return t;
    });

    VisibilityHeartbeat(Aws aws) {
        this.aws = aws;
    }

    /** Starts extending; closing the returned handle stops it. */
    AutoCloseable start(String queueUrl, String receiptHandle, String what, Runnable onCapReached) {
        long startNanos = System.nanoTime();
        AtomicReference<ScheduledFuture<?>> self = new AtomicReference<>();
        Runnable beat = () -> {
            long elapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
            if (elapsed >= Config.MAX_TASK_PROCESSING_SECONDS) {
                self.get().cancel(false);
                onCapReached.run();
                return;
            }
            try {
                aws.sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(receiptHandle)
                        .visibilityTimeout(Config.TASK_VISIBILITY_SECONDS)
                        .build());
                log.info("{}: extended visibility ({}s into processing)", what, elapsed);
            } catch (Exception e) {
                // e.g. the task was deleted just now, or a network hiccup; the next beat retries
                log.warn("{}: visibility extension failed: {}", what, e.getMessage());
            }
        };
        self.set(scheduler.scheduleAtFixedRate(beat, Config.HEARTBEAT_SECONDS, Config.HEARTBEAT_SECONDS, TimeUnit.SECONDS));
        return () -> self.get().cancel(false);
    }
}
