package dsp.ocr.worker;

import dsp.ocr.common.Aws;
import dsp.ocr.common.Config;
import dsp.ocr.common.Messages;
import dsp.ocr.common.Messages.ImageTask;
import dsp.ocr.common.Messages.Message;
import dsp.ocr.common.Messages.OcrDone;
import dsp.ocr.common.Queues;
import dsp.ocr.common.Queues.ManagerQueues;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * DEVELOPMENT TOOL, standing in for a Manager until the real one exists.
 * Creates the queues of manager id "dev" (or the given id), prints the command that starts a
 * Worker on them, sends one IMAGE_TASK per line of a URL file, then collects and prints the
 * results. Tasks wait in SQS until a Worker is started.
 *
 *   java -cp worker/target/worker.jar dsp.ocr.worker.WorkerDevClient [urlFile] [managerId]
 *
 * Results belonging to other jobs (leftovers of earlier runs) are reported and deleted.
 * Texts that a Worker stored in S3 are read and then deleted.
 */
public class WorkerDevClient {
    private static final long TIMEOUT_MS = 10 * 60 * 1000;

    public static void main(String[] args) throws Exception {
        Path file = Path.of(args.length > 0 ? args[0] : "docs/input-example.txt");
        String managerId = args.length > 1 ? args[1] : "dev";
        List<String> urls = Files.readAllLines(file).stream().map(String::strip).filter(s -> !s.isEmpty()).toList();
        String jobId = "dev-" + System.currentTimeMillis();

        try (Aws aws = new Aws()) {
            ManagerQueues q = Queues.ensureManagerQueues(aws, managerId);
            System.out.println("Start a Worker for manager '" + managerId + "' with:");
            System.out.println("  java -jar worker/target/worker.jar " + q.tasks() + " " + q.results());

            for (int i = 0; i < urls.size(); i++) {
                aws.send(q.tasks(), Messages.toJson(new ImageTask(jobId, i, urls.get(i))));
            }
            System.out.printf("Sent %d tasks for job %s from %s; waiting for results...%n", urls.size(), jobId, file);

            Map<Integer, OcrDone> results = new TreeMap<>();
            Map<Integer, String> textsFromS3 = new TreeMap<>();
            long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (results.size() < urls.size() && System.currentTimeMillis() < deadline) {
                for (var m : aws.receive(q.results(), 10)) {
                    Message msg = Messages.fromJson(m.body());
                    if (msg instanceof OcrDone r && r.jobId().equals(jobId)) {
                        if (results.putIfAbsent(r.index(), r) != null) {
                            System.out.println("  duplicate result for line " + r.index() + " ignored");
                        } else {
                            if (r.textKey() != null) {
                                textsFromS3.put(r.index(), aws.getString(Config.BUCKET, r.textKey()));
                                aws.deleteObject(Config.BUCKET, r.textKey());
                            }
                            System.out.printf("  [%d/%d] line %d %s%n", results.size(), urls.size(), r.index(),
                                    !r.ok() ? "ERROR" : r.textKey() != null ? "OK (text via S3)" : "OK");
                        }
                    } else {
                        System.out.println("  discarding leftover message from another job: " + m.body());
                    }
                    aws.delete(q.results(), m);
                }
            }

            System.out.println("\n===== Results for " + jobId + " =====");
            for (int i = 0; i < urls.size(); i++) {
                OcrDone r = results.get(i);
                String outcome = r == null ? "(no result - timed out)"
                        : !r.ok() ? "ERROR: " + r.error()
                        : r.textKey() != null ? "TEXT (S3): " + oneLine(textsFromS3.get(i))
                        : "TEXT: " + oneLine(r.text());
                System.out.printf("%2d  %s%n    %s%n", i, urls.get(i), outcome);
            }
            long ok = results.values().stream().filter(OcrDone::ok).count();
            System.out.printf("%n%d results (%d OK, %d errors, %d via S3), %d missing%n",
                    results.size(), ok, results.size() - ok, textsFromS3.size(), urls.size() - results.size());
        }
    }

    private static String oneLine(String s) {
        String t = s.replaceAll("\\s+", " ").strip();
        return t.isEmpty() ? "(no text found)" : t.length() > 100 ? t.substring(0, 100) + "..." : t;
    }
}
