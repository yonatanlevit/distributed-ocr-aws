package dsp.ocr.local;

import dsp.ocr.common.Aws;
import dsp.ocr.common.Config;
import dsp.ocr.common.Queues;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * End-of-session cleanup (README section 16): deletes every mgr-* and client-* queue and the
 * bucket's jobs/ and logs/ folders, and reports any instance still active, so checking that
 * nothing was left behind is one step.
 *
 * It refuses while a system instance (tagged Role=OcrManager or OcrWorker) is still running,
 * since deleting a running manager's queues would break its jobs. With --force it terminates
 * those instances first. Instances without our Role tag are only reported, never touched.
 *
 *   java -cp local-app/target/local-app.jar dsp.ocr.local.Cleanup [--force]
 */
public class Cleanup {
    public static void main(String[] args) {
        boolean force = args.length > 0 && args[0].equals("--force");
        try (Aws aws = new Aws()) {
            List<Instance> active = new ArrayList<>();
            aws.ec2.describeInstancesPaginator(b -> b.filters(Filter.builder().name("instance-state-name")
                            .values("pending", "running", "stopping", "stopped").build()))
                    .reservations().forEach(r -> active.addAll(r.instances()));
            List<Instance> system = active.stream().filter(i -> role(i) != null).toList();
            List<Instance> others = active.stream().filter(i -> role(i) == null).toList();

            if (!system.isEmpty()) {
                System.out.println("System instances still active:");
                system.forEach(i -> System.out.println("  " + describe(i)));
                if (!force) {
                    System.out.println("Nothing deleted. Wait for them to finish (workers exit after 2 idle minutes,"
                            + " managers after TERMINATE or 5 idle minutes), or run with --force to terminate them.");
                    System.exit(1);
                }
                aws.ec2.terminateInstances(b -> b.instanceIds(system.stream().map(Instance::instanceId).toList()));
                System.out.println("Terminated " + system.size() + " system instance(s).");
            }

            int queues = 0;
            for (String prefix : List.of(Queues.MANAGER_PREFIX, Queues.CLIENT_PREFIX)) {
                for (var page : aws.sqs.listQueuesPaginator(b -> b.queueNamePrefix(prefix).maxResults(1000))) {
                    for (String url : page.queueUrls()) {
                        aws.deleteQueue(url);
                        queues++;
                    }
                }
            }
            int jobs = aws.deletePrefix(Config.BUCKET, "jobs/");
            int logs = aws.deletePrefix(Config.BUCKET, "logs/");

            System.out.printf("Deleted %d queue(s) (mgr-*, client-*), %d object(s) under jobs/, %d under logs/.%n",
                    queues, jobs, logs);
            System.out.println("Kept: the bucket, jars/ and the AMI.");
            if (!others.isEmpty()) {
                System.out.println("Other instances in the account (not part of this system, left alone):");
                others.forEach(i -> System.out.println("  " + describe(i)));
            } else if (system.isEmpty()) {
                System.out.println("No instances are running.");
            }
        }
    }

    private static String role(Instance i) {
        return i.tags().stream()
                .filter(t -> t.key().equals(Config.TAG_ROLE)
                        && (t.value().equals(Config.ROLE_MANAGER) || t.value().equals(Config.ROLE_WORKER)))
                .map(Tag::value).findFirst().orElse(null);
    }

    private static String describe(Instance i) {
        String name = i.tags().stream().filter(t -> t.key().equals("Name")).map(Tag::value).findFirst().orElse("");
        return i.instanceId() + "  " + i.state().nameAsString() + "  " + name;
    }
}
