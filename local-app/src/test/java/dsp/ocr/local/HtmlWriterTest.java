package dsp.ocr.local;

import dsp.ocr.common.SummaryLine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HtmlWriterTest {

    @TempDir
    Path dir;

    @Test
    void successShowsLinkedImageAndText() {
        String p = HtmlWriter.paragraph(new SummaryLine(0, "http://x.com/a.png", "PEACE", null, null), "PEACE");
        assertTrue(p.contains("<a href=\"http://x.com/a.png\"><img src=\"http://x.com/a.png\"></a><br>"), p);
        assertTrue(p.contains("PEACE"));
    }

    @Test
    void failureShowsLinkThenError() {
        String p = HtmlWriter.paragraph(new SummaryLine(1, "http://x.com/b.gif", null, null, "IOException: HTTP 404"), null);
        assertTrue(p.contains("<a href=\"http://x.com/b.gif\">http://x.com/b.gif</a> <span class=\"error\">IOException: HTTP 404</span>"), p);
        assertFalse(p.contains("<img"));
    }

    @Test
    void ocrTextAndUrlsAreEscaped() {
        String evilText = "<script>alert('x')</script> & \"quotes\"";
        String url = "http://x.com/a.png?a=1&b=\"2\"";
        String p = HtmlWriter.paragraph(new SummaryLine(0, url, evilText, null, null), evilText);
        assertFalse(p.contains("<script>"));
        assertTrue(p.contains("&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; &amp; &quot;quotes&quot;"), p);
        assertTrue(p.contains("href=\"http://x.com/a.png?a=1&amp;b=&quot;2&quot;\""), p);
    }

    @Test
    void onlyWebUrlsBecomeLinks() {
        String p = HtmlWriter.paragraph(new SummaryLine(0, "javascript:alert(1)", null, null, "IllegalArgumentException: invalid URI scheme"), null);
        assertFalse(p.contains("href"), p);
        assertTrue(p.contains("javascript:alert(1)"));
        assertTrue(HtmlWriter.isWebUrl("HTTPS://x.com/a.png"));
    }

    @Test
    void multiLineTextKeepsItsLineBreaksAndEmptyTextIsLabelled() {
        String p = HtmlWriter.paragraph(new SummaryLine(0, "http://x/a", "line 1\nline 2", null, null), "line 1\nline 2");
        assertTrue(p.contains("line 1<br>"), p);
        String empty = HtmlWriter.paragraph(new SummaryLine(0, "http://x/a", "", null, null), "");
        assertTrue(empty.contains("(no text found)"));
    }

    @Test
    void writesParagraphsInTheGivenOrderAndFetchesS3Texts() throws Exception {
        Path out = dir.resolve("out.html");
        List<SummaryLine> lines = List.of(
                new SummaryLine(0, "http://x/0", "zero", null, null),
                new SummaryLine(1, "http://x/1", null, "jobs/j/texts/1.txt", null),
                new SummaryLine(2, "http://x/2", null, null, "IOException: HTTP 404"));
        HtmlWriter.write(out, lines, l -> l.textKey() != null ? "from S3" : l.text());
        String html = Files.readString(out);
        assertTrue(html.indexOf("zero") < html.indexOf("from S3") && html.indexOf("from S3") < html.indexOf("HTTP 404"));
        assertTrue(html.startsWith("<!DOCTYPE html>") && html.strip().endsWith("</html>"));
    }
}
