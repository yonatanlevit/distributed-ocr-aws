package dsp.ocr.local;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The Local App's command line, as the spec defines it:
 * {@code inputFileName outputFileName manager-clients-n worker-files-n [terminate]}
 */
record Args(Path input, Path output, int managerClientsN, int workerFilesN, boolean terminate) {

    static final String USAGE =
            "usage: java -jar local-app.jar inputFileName outputFileName manager-clients-n worker-files-n [terminate]";

    /** @throws IllegalArgumentException with a message for the user if the arguments are invalid */
    static Args parse(String[] args) {
        if (args.length < 4 || args.length > 5) {
            throw new IllegalArgumentException("expected 4 or 5 arguments, got " + args.length);
        }
        Path input = Path.of(args[0]);
        if (!Files.isReadable(input) || Files.isDirectory(input)) {
            throw new IllegalArgumentException("input file not found or not readable: " + input);
        }
        Path output = Path.of(args[1]).toAbsolutePath();
        if (!Files.isDirectory(output.getParent())) {
            throw new IllegalArgumentException("output directory does not exist: " + output.getParent());
        }
        int managerClientsN = positive(args[2], "manager-clients-n");
        int workerFilesN = positive(args[3], "worker-files-n");
        boolean terminate = false;
        if (args.length == 5) {
            if (!args[4].equals("terminate")) {
                throw new IllegalArgumentException("the optional fifth argument must be 'terminate', got '" + args[4] + "'");
            }
            terminate = true;
        }
        return new Args(input, output, managerClientsN, workerFilesN, terminate);
    }

    private static int positive(String value, String name) {
        try {
            int n = Integer.parseInt(value);
            if (n >= 1) return n;
        } catch (NumberFormatException ignored) {
            // falls through to the error below
        }
        throw new IllegalArgumentException(name + " must be a whole number of at least 1, got '" + value + "'");
    }
}
