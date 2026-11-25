package dsp.ocr.common;

import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Checks that this machine can reach S3, SQS and EC2 with its credentials.
 * Creates nothing that costs money: the bucket stays (it's needed later); the test object and
 * the test queue are deleted at the end. EC2 is only listed (read-only), never launched.
 *
 * Run from any role jar, e.g.:  java -cp local-app/target/local-app.jar dsp.ocr.common.AwsSmokeTest
 * (it can be run on EC2 later, to check the instance profile).
 */
public final class AwsSmokeTest {
    private static int failures = 0;

    public static void main(String[] args) {
        System.out.println("Region: " + Config.REGION + ", bucket: " + Config.BUCKET);
        try (Aws aws = new Aws()) {
            step("S3: bucket exists or created", () -> aws.ensureBucket(Config.BUCKET));

            step("S3: write, read back, delete an object", () -> {
                String key = "smoke-test/hello.txt";
                String sent = "hello " + System.currentTimeMillis();
                aws.putString(Config.BUCKET, key, sent);
                String got = aws.getString(Config.BUCKET, key);
                aws.deleteObject(Config.BUCKET, key);
                if (!sent.equals(got)) throw new IllegalStateException("read back '" + got + "'");
            });

            step("SQS: create queue, send, receive, delete message, delete queue", () -> {
                // Unique name per run: SQS refuses to recreate a just-deleted name for 60 s.
                String url = aws.queueUrl("dsp-ocr-smoke-test-" + System.currentTimeMillis());
                try {
                    String sent = "ping " + System.currentTimeMillis();
                    aws.send(url, sent);
                    List<Message> msgs = aws.receive(url, 1);
                    if (msgs.isEmpty()) throw new IllegalStateException("no message received within 20 s");
                    aws.delete(url, msgs.get(0));
                    if (!sent.equals(msgs.get(0).body()))
                        throw new IllegalStateException("received '" + msgs.get(0).body() + "'");
                } finally {
                    aws.sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(url).build());
                }
            });

            step("EC2: list instances (read-only)", () -> {
                Map<String, Integer> byState = new TreeMap<>();
                aws.ec2.describeInstancesPaginator(DescribeInstancesRequest.builder().build())
                        .reservations().forEach(r -> {
                            for (Instance i : r.instances())
                                byState.merge(i.state().nameAsString(), 1, Integer::sum);
                        });
                System.out.println("        instances by state: " + (byState.isEmpty() ? "none" : byState));
            });
        }
        System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }

    private static void step(String name, Runnable action) {
        try {
            action.run();
            System.out.println("  PASS  " + name);
        } catch (Exception e) {
            failures++;
            System.out.println("  FAIL  " + name + "\n        " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
