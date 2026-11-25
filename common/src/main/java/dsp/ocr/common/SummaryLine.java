package dsp.ocr.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dsp.ocr.common.Messages.OcrDone;

/**
 * One line of a job's summary file, {@code jobs/<jobId>/summary.jsonl} (README section 4):
 * the fields of OCR_DONE minus jobId. Lines are in the order results arrived; the Local App
 * orders them by {@code index}.
 */
public record SummaryLine(int index, String url, String text, String textKey, String error) {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static SummaryLine from(OcrDone r) {
        return new SummaryLine(r.index(), r.url(), r.text(), r.textKey(), r.error());
    }

    public boolean ok() {
        return error == null;
    }

    /** One JSON object on a single line (newlines inside the text are escaped as \n). */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode " + this, e);
        }
    }

    public static SummaryLine fromJson(String line) {
        try {
            return MAPPER.readValue(line, SummaryLine.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("not a valid summary line: " + e.getOriginalMessage(), e);
        }
    }
}
