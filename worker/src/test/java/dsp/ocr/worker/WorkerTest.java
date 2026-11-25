package dsp.ocr.worker;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.UnresolvedAddressException;

import static org.junit.jupiter.api.Assertions.*;

class WorkerTest {

    @Test
    void ocrReadsGeneratedImage() throws Exception {
        // Needs a local Tesseract install (brew install tesseract); this is what Worker startup runs too.
        new Ocr().selfTest();
    }

    @Test
    void ocrRejectsBytesThatAreNotAnImage() {
        IOException e = assertThrows(IOException.class, () -> new Ocr().recognize("<html>404</html>".getBytes()));
        assertEquals("not a supported image format", e.getMessage());
    }

    @Test
    void describeIsShortAndOneLine() {
        assertEquals("IOException: HTTP 404", Worker.describe(new IOException("HTTP 404")));
        assertEquals("UnresolvedAddressException",
                Worker.describe(new ConnectException() {{ initCause(new UnresolvedAddressException()); }}));
        assertEquals("IIOException: Caught exception during read: (OutOfMemoryError: Java heap space)",
                Worker.describe(new javax.imageio.IIOException("Caught exception during read: ",
                        new OutOfMemoryError("Java heap space"))));
        String longMsg = Worker.describe(new RuntimeException("x\n".repeat(500)));
        assertFalse(longMsg.contains("\n"));
        assertTrue(longMsg.length() <= 203);
    }
}
