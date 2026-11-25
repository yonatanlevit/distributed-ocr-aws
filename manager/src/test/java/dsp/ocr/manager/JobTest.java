package dsp.ocr.manager;

import dsp.ocr.common.Messages.NewTask;
import dsp.ocr.manager.Job.Outcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class JobTest {

    @TempDir
    Path dir;

    private Job job(int total) throws Exception {
        return new Job(new NewTask("j", "k", 5, 3, "q"), total, dir.resolve("summary.jsonl"));
    }

    @Test
    void recordsEachIndexOnceAndCompletesExactlyOnce() throws Exception {
        Job job = job(3);
        assertEquals(Outcome.RECORDED, job.record(0, "a"));
        assertEquals(Outcome.DUPLICATE, job.record(0, "a again"));
        assertEquals(Outcome.RECORDED, job.record(2, "c"));
        assertEquals(1, job.remaining());
        assertEquals(Outcome.COMPLETED, job.record(1, "b"));
        assertEquals(Outcome.DUPLICATE, job.record(1, "b after finish"));
        assertFalse(job.tryFinish(), "already finished");
        assertEquals(List.of("a", "c", "b"), Files.readAllLines(job.closeResults()), "arrival order, no duplicates");
    }

    @Test
    void emptyJobFinishesRightAfterRegistration() throws Exception {
        Job job = job(0);
        assertTrue(job.tryFinish());
        assertFalse(job.tryFinish(), "only once");
        assertEquals(List.of(), Files.readAllLines(job.closeResults()));
    }

    @Test
    void outOfRangeIndexIsRejected() throws Exception {
        Job job = job(2);
        assertEquals(Outcome.OUT_OF_RANGE, job.record(-1, "x"));
        assertEquals(Outcome.OUT_OF_RANGE, job.record(2, "x"));
        assertEquals(2, job.remaining());
    }

    @Test
    void manyCollectorThreadsWithDuplicatesCompleteExactlyOnce() throws Exception {
        int total = 20_000;
        Job job = job(total);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<Integer>> completions = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            // Every thread delivers every result: 8 copies of each, like heavy SQS duplication.
            completions.add(pool.submit(() -> {
                int completed = 0;
                for (int i = 0; i < total; i++) {
                    if (job.record(i, "line " + i) == Outcome.COMPLETED) completed++;
                }
                return completed;
            }));
        }
        int completedCalls = 0;
        for (Future<Integer> f : completions) completedCalls += f.get();
        pool.shutdown();

        assertEquals(1, completedCalls, "exactly one call completes the job");
        List<String> lines = Files.readAllLines(job.closeResults());
        assertEquals(total, lines.size());
        assertEquals(total, new HashSet<>(lines).size(), "each index written exactly once");
    }
}
