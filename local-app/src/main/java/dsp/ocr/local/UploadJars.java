package dsp.ocr.local;

import dsp.ocr.common.Aws;
import dsp.ocr.common.Config;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Setup tool: uploads the freshly built manager and worker jars to the bucket's jars/ folder,
 * where every instance downloads them at boot (README section 12). Run after each code change,
 * from the project root, after {@code mvn package}:
 *
 *   java -cp local-app/target/local-app.jar dsp.ocr.local.UploadJars
 */
public class UploadJars {
    public static void main(String[] args) throws Exception {
        try (Aws aws = new Aws()) {
            aws.ensureBucket(Config.BUCKET);
            for (String role : new String[]{"manager", "worker"}) {
                Path jar = Path.of(role, "target", role + ".jar");
                if (!Files.isRegularFile(jar)) {
                    System.err.println("missing " + jar + ": run 'mvn package' in the project root first");
                    System.exit(1);
                }
                aws.putFile(Config.BUCKET, Config.jarKey(role), jar);
                System.out.printf("uploaded %-28s -> s3://%s/%s (%.1f MB)%n",
                        jar, Config.BUCKET, Config.jarKey(role), Files.size(jar) / 1e6);
            }
        }
    }
}
