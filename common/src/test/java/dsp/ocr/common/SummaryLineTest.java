package dsp.ocr.common;

import dsp.ocr.common.Messages.ImageTask;
import dsp.ocr.common.Messages.OcrDone;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SummaryLineTest {

    private static final ImageTask TASK = new ImageTask("job-1", 7, "http://example.com/a.png");

    @Test
    void roundTripsAndStaysOnOneLine() {
        SummaryLine line = SummaryLine.from(OcrDone.success(TASK, "first line\nsecond line"));
        String json = line.toJson();
        assertFalse(json.contains("\n"), "a JSONL record must be a single line");
        assertEquals(line, SummaryLine.fromJson(json));
    }

    @Test
    void keepsTheFieldsOfOcrDoneWithoutJobId() {
        assertEquals("{\"index\":7,\"url\":\"http://example.com/a.png\",\"error\":\"IOException: HTTP 404\"}",
                SummaryLine.from(OcrDone.failure(TASK, "IOException: HTTP 404")).toJson());
        assertEquals("jobs/job-1/texts/7.txt",
                SummaryLine.from(OcrDone.successInS3(TASK, "jobs/job-1/texts/7.txt")).textKey());
    }
}
