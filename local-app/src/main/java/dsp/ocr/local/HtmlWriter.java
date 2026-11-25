package dsp.ocr.local;

import dsp.ocr.common.SummaryLine;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Writes the output page (README section 5, step 7), modelled on docs/output-example.html:
 * one paragraph per input URL, in input order.
 *  - success: the image, linked to its original URL, then the recognized text
 *  - failure: the original URL as a link, then the short error description
 * Everything from the outside world (URLs, OCR text, errors) is HTML-escaped, and only
 * http(s) URLs become links or images.
 */
final class HtmlWriter {
    private HtmlWriter() {}

    /**
     * @param lines  the summary lines, already sorted by index
     * @param textOf the recognized text of a successful line (fetches it from S3 if needed)
     */
    static void write(Path output, List<SummaryLine> lines, Function<SummaryLine, String> textOf) throws IOException {
        try (BufferedWriter out = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            out.write("""
                    <!DOCTYPE html>
                    <html><head><meta charset="utf-8"><title>OCR</title>
                    <style>body{font-family:sans-serif} img{max-width:600px} .error{color:#b00020}</style>
                    </head><body>
                    """);
            for (SummaryLine line : lines) {
                out.write(paragraph(line, line.ok() ? textOf.apply(line) : null));
            }
            out.write("</body></html>\n");
        }
    }

    static String paragraph(SummaryLine line, String text) {
        String url = line.url();
        String safe = escape(url);
        boolean web = isWebUrl(url);
        StringBuilder p = new StringBuilder("\t<p>\n\t\t");
        if (line.ok()) {
            p.append(web ? "<a href=\"" + safe + "\"><img src=\"" + safe + "\"></a>" : safe).append("<br>\n\t\t");
            p.append(text == null || text.isBlank() ? "<i>(no text found)</i>" : escape(text.strip()).replace("\n", "<br>\n\t\t"));
        } else {
            p.append(web ? "<a href=\"" + safe + "\">" + safe + "</a>" : safe)
                    .append(" <span class=\"error\">").append(escape(line.error())).append("</span>");
        }
        return p.append("\n\t</p>\n").toString();
    }

    /** Only http and https become links: anything else (e.g. "javascript:") is shown as plain text. */
    static boolean isWebUrl(String url) {
        String u = url.strip().toLowerCase(Locale.ROOT);
        return u.startsWith("http://") || u.startsWith("https://");
    }

    static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&#39;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
