package dsp.ocr.local;

import dsp.ocr.common.Aws;
import dsp.ocr.common.Ec2Launcher;
import dsp.ocr.common.Queues;
import dsp.ocr.common.Queues.ManagerQueues;

import java.util.List;

/**
 * DEVELOPMENT TOOL: launches ONE worker instance on EC2 for a manager id (default "dev"),
 * without a manager, through the instance guard. Feed it tasks with WorkerDevClient; it exits
 * after its idle timeout, uploads its log to S3, and its instance terminates.
 *
 *   java -cp local-app/target/local-app.jar dsp.ocr.local.LaunchTestWorker [managerId]
 */
public class LaunchTestWorker {
    public static void main(String[] args) {
        String managerId = args.length > 0 ? args[0] : "dev";
        try (Aws aws = new Aws()) {
            ManagerQueues q = Queues.ensureManagerQueues(aws, managerId);
            List<String> ids = new Ec2Launcher(aws).launchWorkers(managerId, q.tasks(), q.results(), 1);
            if (ids.isEmpty()) {
                System.err.println("the instance guard has no room: too many instances are active");
                System.exit(1);
            }
            System.out.println("Launched worker " + ids.get(0) + " for manager '" + managerId + "'");
            System.out.println("Feed it: java -cp worker/target/worker.jar dsp.ocr.worker.WorkerDevClient docs/input-example.txt " + managerId);
        }
    }
}
