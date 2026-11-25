package dsp.ocr.common;

import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Map;

/**
 * Queue names and creation (README section 3). Every queue belongs either to one manager
 * ({@code mgr-<id>-...}) or to one client run ({@code client-<jobId>}).
 *
 * Creation is idempotent: CreateQueue on an existing queue returns its URL as long as the
 * attributes are identical, so each queue's attributes are defined only here.
 */
public final class Queues {
    private Queues() {}

    public static final String MANAGER_PREFIX = "mgr-";
    public static final String CLIENT_PREFIX = "client-";

    public static String inboxName(String managerId)   { return MANAGER_PREFIX + managerId + "-in"; }
    public static String tasksName(String managerId)   { return MANAGER_PREFIX + managerId + "-tasks"; }
    public static String resultsName(String managerId) { return MANAGER_PREFIX + managerId + "-results"; }
    public static String dlqName(String managerId)     { return MANAGER_PREFIX + managerId + "-dlq"; }
    public static String clientName(String jobId)      { return CLIENT_PREFIX + jobId; }

    /** URLs of one manager's four queues. */
    public record ManagerQueues(String inbox, String tasks, String results, String dlq) {}

    /** A manager's inbox. Created by the Local App that launches the manager, and again by the manager. */
    public static String ensureInbox(Aws aws, String managerId) {
        return create(aws, inboxName(managerId), Map.of(
                QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(Config.INBOX_VISIBILITY_SECONDS)));
    }

    /** All four queues of a manager; called by the manager at startup. */
    public static ManagerQueues ensureManagerQueues(Aws aws, String managerId) {
        String inbox = ensureInbox(aws, managerId);
        String results = create(aws, resultsName(managerId), Map.of(
                QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(Config.DEFAULT_VISIBILITY_SECONDS)));

        // The dead-letter queue must exist first: the tasks queue's redrive policy refers to its ARN.
        String dlq = create(aws, dlqName(managerId), Map.of(
                QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(Config.DEFAULT_VISIBILITY_SECONDS),
                QueueAttributeName.MESSAGE_RETENTION_PERIOD, String.valueOf(Config.DLQ_RETENTION_SECONDS)));
        String dlqArn = aws.sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(dlq).attributeNames(QueueAttributeName.QUEUE_ARN).build())
                .attributes().get(QueueAttributeName.QUEUE_ARN);
        String redrivePolicy = "{\"deadLetterTargetArn\":\"" + dlqArn + "\","
                + "\"maxReceiveCount\":\"" + Config.MAX_RECEIVE_COUNT + "\"}";

        String tasks = create(aws, tasksName(managerId), Map.of(
                QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(Config.TASK_VISIBILITY_SECONDS),
                QueueAttributeName.REDRIVE_POLICY, redrivePolicy));

        return new ManagerQueues(inbox, tasks, results, dlq);
    }

    /** A client run's reply queue; created by the Local App, deleted by it at the end. */
    public static String createClientQueue(Aws aws, String jobId) {
        return create(aws, clientName(jobId), Map.of(
                QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(Config.DEFAULT_VISIBILITY_SECONDS)));
    }

    private static String create(Aws aws, String name, Map<QueueAttributeName, String> attributes) {
        return aws.sqs.createQueue(CreateQueueRequest.builder()
                .queueName(name).attributes(attributes).build()).queueUrl();
    }
}
