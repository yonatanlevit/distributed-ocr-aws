package dsp.ocr.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * Every SQS message body in the system (README section 3), and the JSON codec for them.
 *
 * Each message is a record implementing {@link Message}. In JSON it carries a "type" field
 * (e.g. "NEW_TASK"), so a receiver can decode any body with {@link #fromJson} and then
 * {@code switch} on the result. Null fields are left out of the JSON.
 */
public final class Messages {
    private Messages() {}

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = NewTask.class, name = "NEW_TASK"),
            @JsonSubTypes.Type(value = Terminate.class, name = "TERMINATE"),
            @JsonSubTypes.Type(value = ImageTask.class, name = "IMAGE_TASK"),
            @JsonSubTypes.Type(value = OcrDone.class, name = "OCR_DONE"),
            @JsonSubTypes.Type(value = Done.class, name = "DONE"),
            @JsonSubTypes.Type(value = Rejected.class, name = "REJECTED"),
    })
    public sealed interface Message permits NewTask, Terminate, ImageTask, OcrDone, Done, Rejected {}

    /**
     * Local App → manager inbox: process the input file stored in S3.
     *
     * @param jobId           unique id of this client run (also names its reply queue)
     * @param inputKey        S3 key of the uploaded input file, in {@link Config#BUCKET}
     * @param workerFilesN    the client's worker-files-n argument (images per worker)
     * @param managerClientsN the client's manager-clients-n argument (used for admission)
     * @param replyQueueUrl   where the manager sends DONE or REJECTED for this job
     */
    public record NewTask(String jobId, String inputKey, int workerFilesN, int managerClientsN,
                          String replyQueueUrl) implements Message {}

    /** Local App → manager inbox: finish the active jobs, then shut down. */
    public record Terminate(String jobId) implements Message {}

    /**
     * Manager → its tasks queue: OCR one image.
     *
     * @param index the line number in the job's input file; identifies this task within the job,
     *              even when the same URL appears on several lines
     */
    public record ImageTask(String jobId, int index, String url) implements Message {}

    /**
     * Worker → its manager's results queue: the outcome of one {@link ImageTask}.
     * On success exactly one of {@code text} / {@code textKey} is set; on failure only {@code error}.
     *
     * @param text    the recognized text (may be empty if the image has none)
     * @param textKey S3 key of the text, when it was too large for an SQS message
     * @param error   short description of what went wrong (download failed, not an image, ...)
     */
    public record OcrDone(String jobId, int index, String url, String text, String textKey, String error)
            implements Message {
        public static OcrDone success(ImageTask t, String text) {
            return new OcrDone(t.jobId(), t.index(), t.url(), text, null, null);
        }

        public static OcrDone successInS3(ImageTask t, String textKey) {
            return new OcrDone(t.jobId(), t.index(), t.url(), null, textKey, null);
        }

        public static OcrDone failure(ImageTask t, String error) {
            return new OcrDone(t.jobId(), t.index(), t.url(), null, null, error);
        }

        public boolean ok() {
            return error == null;
        }
    }

    /**
     * Manager → client reply queue: the job is finished; its summary is at {@code summaryKey}.
     * {@code managerId} names the sender, so a client ignores replies from managers it already left.
     */
    public record Done(String jobId, String managerId, String summaryKey) implements Message {}

    /** Manager → client reply queue: the job was not accepted (or could not be read). */
    public record Rejected(String jobId, String managerId, Reason reason) implements Message {
        public enum Reason {
            /** The manager already serves as many clients as this client's manager-clients-n allows. */
            FULL,
            /** The manager received TERMINATE and accepts no new jobs. */
            TERMINATING,
            /** The manager was idle too long and is shutting down. */
            RETIRING,
            /** The job's input could not be read. */
            FAILED
        }
    }

    // ---------- JSON codec ----------

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            // A field we don't know (e.g. added in a newer version) is ignored instead of failing.
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static String toJson(Message m) {
        try {
            // Serialize as the interface type so the "type" field is always written.
            return MAPPER.writerFor(Message.class).writeValueAsString(m);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode " + m, e);
        }
    }

    /** @throws IllegalArgumentException if the body isn't a valid message (e.g. garbage on a queue) */
    public static Message fromJson(String json) {
        try {
            return MAPPER.readValue(json, Message.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("not a valid message: " + e.getOriginalMessage(), e);
        }
    }

    /** Size that counts against the SQS message limit: the body's UTF-8 bytes. */
    public static int sizeInBytes(String json) {
        return json.getBytes(StandardCharsets.UTF_8).length;
    }
}
