package dsp.ocr.manager;

import dsp.ocr.common.Messages.NewTask;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;

/**
 * Bookkeeping for one admitted job (README section 6, "Job bookkeeping and completion").
 *
 * The total is known before the first task is sent, so a job can never look complete while it
 * is still being split. Results are appended to a local file that becomes the summary; a
 * {@link BitSet} of finished indices (about one bit per URL) makes duplicates harmless.
 * All methods are synchronized: several collector threads record into the same job.
 */
final class Job {

    enum Outcome {
        /** First result for this index; the job still needs more. */
        RECORDED,
        /** First result for this index, and it was the last one missing: finish the job now. */
        COMPLETED,
        /** This index already has a result (SQS delivered it twice), or the job already finished. */
        DUPLICATE,
        /** The index is outside 0..total-1; the message is corrupt. */
        OUT_OF_RANGE
    }

    final NewTask task;
    final int total;
    final long startedAtMillis = System.currentTimeMillis();

    private final BitSet done;
    private final Path resultsFile;
    private final BufferedWriter out;
    private int completed;
    private boolean finished;

    Job(NewTask task, int total, Path resultsFile) throws IOException {
        this.task = task;
        this.total = total;
        this.done = new BitSet(total);
        this.resultsFile = resultsFile;
        this.out = Files.newBufferedWriter(resultsFile, StandardCharsets.UTF_8);
    }

    /** Records one result line. Returns COMPLETED exactly once: for the call that completes the job. */
    synchronized Outcome record(int index, String summaryLine) throws IOException {
        if (index < 0 || index >= total) return Outcome.OUT_OF_RANGE;
        if (finished || done.get(index)) return Outcome.DUPLICATE;
        // Write first: if writing fails, the index stays open and the message is retried.
        out.write(summaryLine);
        out.newLine();
        out.flush();
        done.set(index);
        completed++;
        return tryFinish() ? Outcome.COMPLETED : Outcome.RECORDED;
    }

    /** True exactly once, when every index has a result. Also called right after registration (empty input). */
    synchronized boolean tryFinish() {
        if (finished || completed < total) return false;
        finished = true;
        return true;
    }

    synchronized int completed() {
        return completed;
    }

    /** URLs still without a result (used by the scaler in Step 8). */
    synchronized int remaining() {
        return total - completed;
    }

    /** Closes the results file so it can be uploaded as the summary. */
    synchronized Path closeResults() throws IOException {
        out.close();
        return resultsFile;
    }

    /** Abandons the job (it failed to start): closes and deletes its results file. */
    synchronized void discard() {
        finished = true;
        try {
            out.close();
            Files.deleteIfExists(resultsFile);
        } catch (IOException ignored) {
            // a leftover temp file is harmless
        }
    }
}
