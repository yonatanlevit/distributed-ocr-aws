package dsp.ocr.common;

import dsp.ocr.common.Messages.Done;
import dsp.ocr.common.Messages.ImageTask;
import dsp.ocr.common.Messages.Message;
import dsp.ocr.common.Messages.NewTask;
import dsp.ocr.common.Messages.OcrDone;
import dsp.ocr.common.Messages.Rejected;
import dsp.ocr.common.Messages.Terminate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MessagesTest {

    private static final ImageTask TASK = new ImageTask("job-1", 4, "http://example.com/a.png");

    private static void assertRoundTrip(Message original) {
        String json = Messages.toJson(original);
        assertEquals(original, Messages.fromJson(json), "round trip of " + json);
    }

    @Test
    void everyMessageTypeSurvivesRoundTrip() {
        assertRoundTrip(new NewTask("job-1", "jobs/job-1/input.txt", 5, 3,
                "https://sqs.us-east-1.amazonaws.com/123/client-job-1"));
        assertRoundTrip(new Terminate("job-1"));
        assertRoundTrip(TASK);
        assertRoundTrip(OcrDone.success(TASK, "PEACE"));
        assertRoundTrip(OcrDone.successInS3(TASK, "jobs/job-1/texts/4.txt"));
        assertRoundTrip(OcrDone.failure(TASK, "IOException: HTTP 404"));
        assertRoundTrip(new Done("job-1", "3f9a1c2e-7b4d-4e1a-9c0f-2d8e6b5a1f37", "jobs/job-1/summary.jsonl"));
        for (Rejected.Reason r : Rejected.Reason.values()) {
            assertRoundTrip(new Rejected("job-1", "3f9a1c2e-7b4d-4e1a-9c0f-2d8e6b5a1f37", r));
        }
    }

    @Test
    void jsonMatchesReadmeFormat() {
        // README section 3: a "type" field, and fields without a value left out.
        assertEquals("{\"type\":\"OCR_DONE\",\"jobId\":\"job-1\",\"index\":4,\"url\":\"http://example.com/a.png\",\"error\":\"IOException: HTTP 404\"}",
                Messages.toJson(OcrDone.failure(TASK, "IOException: HTTP 404")));
        assertEquals("{\"type\":\"REJECTED\",\"jobId\":\"j\",\"managerId\":\"m\",\"reason\":\"FULL\"}",
                Messages.toJson(new Rejected("j", "m", Rejected.Reason.FULL)));
    }

    @Test
    void ocrTextWithAnyCharactersSurvives() {
        String nasty = "line 1\nline 2\ttab \"quoted\" back\\slash | pipe, שלום, 你好, emoji 🙂";
        ImageTask task = new ImageTask("job-1", 0, "http://example.com/a.png?x=1&y=\"2\"");
        OcrDone decoded = (OcrDone) Messages.fromJson(Messages.toJson(OcrDone.success(task, nasty)));
        assertEquals(nasty, decoded.text());
        assertEquals(task.url(), decoded.url());
    }

    @Test
    void successAndFailureAreDistinguishable() {
        assertTrue(OcrDone.success(TASK, "").ok(), "empty text is still a success");
        assertTrue(OcrDone.successInS3(TASK, "k").ok());
        assertFalse(OcrDone.failure(TASK, "timeout").ok());
    }

    @Test
    void sizeCountsUtf8Bytes() {
        assertEquals(3, Messages.sizeInBytes("abc"));
        assertEquals(12, Messages.sizeInBytes("שלום🙂"), "4 Hebrew letters of 2 bytes each, plus a 4-byte emoji");
    }

    @Test
    void receiverCanSwitchOnDecodedType() {
        Message m = Messages.fromJson(Messages.toJson(new NewTask("j", "k", 3, 2, "q")));
        String kind = switch (m) {
            case NewTask t -> "new task with n=" + t.workerFilesN();
            case Terminate t -> "terminate";
            case ImageTask t -> "image";
            case OcrDone r -> "result";
            case Done d -> "done";
            case Rejected r -> "rejected";
        };
        assertEquals("new task with n=3", kind);
    }

    @Test
    void unknownFieldsAreIgnored() {
        Message m = Messages.fromJson("{\"type\":\"IMAGE_TASK\",\"jobId\":\"j\",\"index\":2,\"url\":\"u\",\"newField\":42}");
        assertEquals(new ImageTask("j", 2, "u"), m);
    }

    @Test
    void garbageIsRejectedWithIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> Messages.fromJson("hello world"));
        assertThrows(IllegalArgumentException.class, () -> Messages.fromJson("{\"type\":\"NO_SUCH_TYPE\"}"));
        assertThrows(IllegalArgumentException.class, () -> Messages.fromJson("{\"jobId\":\"missing type\"}"));
        assertThrows(IllegalArgumentException.class, () -> Messages.fromJson("{\"type\":\"REJECTED\",\"jobId\":\"j\",\"reason\":\"BOGUS\"}"));
    }
}
