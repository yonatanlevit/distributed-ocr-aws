package dsp.ocr.manager;

import dsp.ocr.common.Aws;
import dsp.ocr.common.Config;
import dsp.ocr.common.Messages;
import dsp.ocr.common.Messages.Done;
import dsp.ocr.common.Messages.NewTask;
import dsp.ocr.common.Messages.Rejected;
import dsp.ocr.common.Queues;
import dsp.ocr.common.SummaryLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * DEVELOPMENT TOOL, standing in for Local Apps until the real one exists (Step 6).
 * Runs N clients at once against one manager. Each client uploads the input, creates its reply
 * queue, sends NEW_TASK, waits for DONE or REJECTED, checks the summary, and cleans up.
 *
 *   java -cp manager/target/manager.jar dsp.ocr.manager.ManagerDevClient \
 *        <managerId> <inputFile | MISSING> [clients=1] [managerClientsN=10] [workerFilesN=5]
 *
 * MISSING sends a NEW_TASK whose input file doesn't exist in S3 (expect REJECTED FAILED).
 */
public class ManagerDevClient {
    private static final long TIMEOUT_MS = 10 * 60 * 1000;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: ManagerDevClient <managerId> <inputFile|MISSING> [clients] [managerClientsN] [workerFilesN]");
            System.exit(2);
        }
        String managerId = args[0];
        boolean missing = args[1].equals("MISSING");
        Path input = missing ? null : Path.of(args[1]);
        int clients = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        int managerClientsN = args.length > 3 ? Integer.parseInt(args[3]) : 10;
        int workerFilesN = args.length > 4 ? Integer.parseInt(args[4]) : 5;
        int expected = missing ? 0 : (int) Files.readAllLines(input).stream().filter(l -> !l.isBlank()).count();

        try (Aws aws = new Aws()) {
            String inbox = Queues.ensureInbox(aws, managerId);
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            List<Future<String>> outcomes = new ArrayList<>();
            for (int c = 1; c <= clients; c++) {
                int client = c;
                outcomes.add(pool.submit(() -> runClient(aws, client, inbox, input, expected, managerClientsN, workerFilesN)));
            }
            System.out.println("\n===== Outcomes =====");
            for (Future<String> f : outcomes) System.out.println(f.get());
            pool.shutdown();
        }
    }

    private static String runClient(Aws aws, int client, String inbox, Path input, int expected,
                                    int managerClientsN, int workerFilesN) throws Exception {
        String jobId = UUID.randomUUID().toString();
        String tag = "client " + client + " (job " + jobId.substring(0, 8) + ")";
        String replyQueue = Queues.createClientQueue(aws, jobId);
        String inputKey = input == null ? "jobs/" + jobId + "/no-such-file.txt" : Config.inputKey(jobId);
        try {
            if (input != null) aws.putFile(Config.BUCKET, inputKey, input);
            long start = System.currentTimeMillis();
            aws.send(inbox, Messages.toJson(new NewTask(jobId, inputKey, workerFilesN, managerClientsN, replyQueue)));
            System.out.println(tag + ": NEW_TASK sent");

            long deadline = start + TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                for (var m : aws.receive(replyQueue, 1)) {
                    aws.delete(replyQueue, m);
                    long secs = (System.currentTimeMillis() - start) / 1000;
                    switch (Messages.fromJson(m.body())) {
                        case Rejected r -> {
                            return tag + ": REJECTED " + r.reason() + " after " + secs + "s";
                        }
                        case Done d -> {
                            return tag + ": DONE after " + secs + "s, " + checkSummary(aws, d.summaryKey(), expected);
                        }
                        default -> System.out.println(tag + ": unexpected reply " + m.body());
                    }
                }
            }
            return tag + ": TIMED OUT";
        } finally {
            aws.deleteQueue(replyQueue);
            aws.deleteObject(Config.BUCKET, inputKey);
            aws.deleteObject(Config.BUCKET, Config.summaryKey(jobId));
        }
    }

    /** Checks that every index 0..expected-1 appears exactly once, and shows a few lines in index order. */
    private static String checkSummary(Aws aws, String key, int expected) {
        List<SummaryLine> lines = aws.getString(Config.BUCKET, key).lines()
                .filter(l -> !l.isBlank()).map(SummaryLine::fromJson).toList();
        BitSet seen = new BitSet();
        int dupes = 0;
        for (SummaryLine l : lines) {
            if (seen.get(l.index())) dupes++;
            seen.set(l.index());
        }
        boolean complete = lines.size() == expected && seen.cardinality() == expected && seen.nextSetBit(expected) == -1;
        long ok = lines.stream().filter(SummaryLine::ok).count();
        StringBuilder sb = new StringBuilder();
        sb.append(lines.size()).append(" lines (").append(ok).append(" OK, ").append(lines.size() - ok).append(" errors), ")
                .append(complete && dupes == 0 ? "indices 0.." + (expected - 1) + " each exactly once" : "INCOMPLETE OR DUPLICATED");
        lines.stream().sorted(Comparator.comparingInt(SummaryLine::index)).limit(3).forEach(l -> sb.append("\n      ")
                .append(l.index()).append(": ").append(l.ok() ? oneLine(l.text()) : "ERROR " + l.error()));
        return sb.toString();
    }

    private static String oneLine(String s) {
        if (s == null) return "(text in S3)";
        String t = s.replaceAll("\\s+", " ").strip();
        return t.length() > 60 ? t.substring(0, 60) + "..." : t;
    }
}
