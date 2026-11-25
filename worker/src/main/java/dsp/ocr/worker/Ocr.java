package dsp.ocr.worker;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Thin wrapper around Tess4J. Not thread-safe (one instance per thread); the Worker is
 * single-threaded (Decisions log #12).
 */
final class Ocr {

    /** Where Tesseract's language data may live: Homebrew on macOS, then common Linux paths. */
    private static final List<String> TESSDATA_CANDIDATES = List.of(
            "/opt/homebrew/share/tessdata",
            "/usr/local/share/tessdata",
            "/usr/share/tesseract/tessdata",
            "/usr/share/tesseract-ocr/5/tessdata",
            "/usr/share/tesseract-ocr/4.00/tessdata");

    static {
        // Tess4J loads the native libtesseract through JNA. Linux finds it in the system library
        // path by itself; on Apple Silicon Macs, Homebrew's folder must be added.
        if (System.getProperty("jna.library.path") == null && Files.isDirectory(Path.of("/opt/homebrew/lib"))) {
            System.setProperty("jna.library.path", "/opt/homebrew/lib");
        }
    }

    private final Tesseract tesseract = new Tesseract();

    Ocr() {
        tesseract.setDatapath(findTessdata());
        tesseract.setLanguage("eng");
    }

    /** Recognizes the text in an image file's bytes. Returns "" if the image has no text. */
    String recognize(byte[] imageBytes) throws IOException, TesseractException {
        BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageBytes));
        if (image == null) {
            throw new IOException("not a supported image format");
        }
        return tesseract.doOCR(image).strip();
    }

    /**
     * OCRs a small generated image. Called once at startup so a Worker with a broken Tesseract
     * install fails immediately, instead of turning every task into an error result.
     */
    void selfTest() throws Exception {
        BufferedImage img = new BufferedImage(400, 100, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, img.getWidth(), img.getHeight());
        g.setColor(Color.BLACK);
        g.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 48));
        g.drawString("HELLO 42", 20, 70);
        g.dispose();
        String text = tesseract.doOCR(img);
        if (!text.contains("HELLO")) {
            throw new IllegalStateException("OCR self-test read '" + text.strip() + "' instead of 'HELLO 42'");
        }
    }

    private static String findTessdata() {
        String fromEnv = System.getenv("TESSDATA_PREFIX");
        if (fromEnv != null && !fromEnv.isBlank()) return fromEnv;
        for (String dir : TESSDATA_CANDIDATES) {
            if (Files.exists(Path.of(dir, "eng.traineddata"))) return dir;
        }
        throw new IllegalStateException("eng.traineddata not found; set TESSDATA_PREFIX. Looked in " + TESSDATA_CANDIDATES);
    }
}
