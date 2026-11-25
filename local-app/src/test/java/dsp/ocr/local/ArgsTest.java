package dsp.ocr.local;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ArgsTest {

    @TempDir
    Path dir;

    private String input() throws Exception {
        Path in = dir.resolve("in.txt");
        Files.writeString(in, "http://x/a.png\n");
        return in.toString();
    }

    @Test
    void parsesTheSpecCommandLine() throws Exception {
        String out = dir.resolve("out.html").toString();
        Args a = Args.parse(new String[]{input(), out, "3", "5"});
        assertEquals(3, a.managerClientsN());
        assertEquals(5, a.workerFilesN());
        assertFalse(a.terminate());
        assertTrue(Args.parse(new String[]{input(), out, "3", "5", "terminate"}).terminate());
    }

    @Test
    void rejectsBadArguments() throws Exception {
        String in = input();
        String out = dir.resolve("out.html").toString();
        assertThrows(IllegalArgumentException.class, () -> Args.parse(new String[]{in, out, "3"}), "too few");
        assertThrows(IllegalArgumentException.class, () -> Args.parse(new String[]{in, out, "0", "5"}), "n must be >= 1");
        assertThrows(IllegalArgumentException.class, () -> Args.parse(new String[]{in, out, "3", "x"}), "not a number");
        assertThrows(IllegalArgumentException.class, () -> Args.parse(new String[]{in, out, "3", "5", "terminat"}), "typo");
        assertThrows(IllegalArgumentException.class, () -> Args.parse(new String[]{dir.resolve("nope.txt").toString(), out, "3", "5"}), "missing input");
        assertThrows(IllegalArgumentException.class, () -> Args.parse(new String[]{in, dir.resolve("no/such/dir/out.html").toString(), "3", "5"}), "bad output dir");
    }
}
