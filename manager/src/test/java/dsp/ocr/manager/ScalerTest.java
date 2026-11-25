package dsp.ocr.manager;

import dsp.ocr.common.Messages.NewTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ScalerTest {

    @TempDir
    Path dir;

    private Job job(String id, int total, int workerFilesN) throws Exception {
        return new Job(new NewTask(id, "k", workerFilesN, 10, "q"), total, dir.resolve(id + ".jsonl"));
    }

    @Test
    void readmeExample() throws Exception {
        // README section 6: job A with 47 URLs left at n = 10 needs 5 workers, job B with 12 at n = 5 needs 3.
        assertEquals(8, Manager.desiredWorkers(List.of(job("a", 47, 10), job("b", 12, 5))));
    }

    @Test
    void singleJobMatchesTheSpecRule() throws Exception {
        // The spec: one worker per n files.
        assertEquals(5, Manager.desiredWorkers(List.of(job("a", 24, 5))));
        assertEquals(1, Manager.desiredWorkers(List.of(job("a", 24, 100))));
    }

    @Test
    void shrinksAsResultsArriveAndIsZeroWhenDone() throws Exception {
        Job a = job("a", 3, 1);
        assertEquals(3, Manager.desiredWorkers(List.of(a)));
        a.record(0, "x");
        a.record(1, "y");
        assertEquals(1, Manager.desiredWorkers(List.of(a)));
        a.record(2, "z");
        assertEquals(0, Manager.desiredWorkers(List.of(a)));
        assertEquals(0, Manager.desiredWorkers(List.of()));
    }
}
