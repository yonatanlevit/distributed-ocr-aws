package dsp.ocr.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds the AWS clients (all in {@link Config#REGION}) plus small helpers used by every role.
 * Credentials are never in code: locally the SDK reads ~/.aws/credentials, and on EC2 it uses
 * the instance profile (LabInstanceProfile).
 */
public final class Aws implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Aws.class);

    /** SQS allows at most 10 entries per batch call. */
    private static final int MAX_BATCH_ENTRIES = 10;
    /** Total size of one batch call, kept well under the SQS payload limit. */
    private static final int MAX_BATCH_BYTES = 200 * 1024;
    private static final int MAX_BATCH_ATTEMPTS = 5;

    public final S3Client s3;
    public final SqsClient sqs;
    public final Ec2Client ec2;

    public Aws() {
        s3 = S3Client.builder().region(Config.REGION).build();
        sqs = SqsClient.builder().region(Config.REGION).build();
        ec2 = Ec2Client.builder().region(Config.REGION).build();
    }

    // ---------- S3 ----------

    /** Creates the bucket if it doesn't exist yet. Safe to call repeatedly. */
    public void ensureBucket(String bucket) {
        try {
            // In us-east-1 a bucket is created WITHOUT a LocationConstraint (other regions need one).
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        } catch (BucketAlreadyOwnedByYouException e) {
            return; // already ours, nothing to do
        }
        s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucket).build());
    }

    public void putString(String bucket, String key, String content) {
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromString(content));
    }

    public String getString(String bucket, String key) {
        return s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build())
                .asUtf8String();
    }

    public void putFile(String bucket, String key, Path file) {
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(file));
    }

    /** Streams an object to a local file (replacing it), so large files never sit in memory. */
    public void downloadToFile(String bucket, String key, Path file) throws IOException {
        Files.deleteIfExists(file);
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(), file);
    }

    public void deleteObject(String bucket, String key) {
        s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
    }

    /** Deletes every object whose key starts with {@code prefix}; returns how many were deleted. */
    public int deletePrefix(String bucket, String prefix) {
        int deleted = 0;
        // Each page holds up to 1000 keys, which is also the DeleteObjects limit.
        for (var page : s3.listObjectsV2Paginator(b -> b.bucket(bucket).prefix(prefix))) {
            List<ObjectIdentifier> ids = page.contents().stream()
                    .map(o -> ObjectIdentifier.builder().key(o.key()).build()).toList();
            if (ids.isEmpty()) continue;
            s3.deleteObjects(b -> b.bucket(bucket).delete(d -> d.objects(ids)));
            deleted += ids.size();
        }
        return deleted;
    }

    // ---------- SQS ----------

    /** Returns the queue URL, creating the queue (with default attributes) if needed. */
    public String queueUrl(String queueName) {
        return sqs.createQueue(CreateQueueRequest.builder().queueName(queueName).build()).queueUrl();
    }

    public void deleteQueue(String queueUrl) {
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
    }

    public void send(String queueUrl, String body) {
        sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(body).build());
    }

    /**
     * Sends many messages using batch calls (up to 10 per call). A batch call can succeed while
     * single entries fail; those entries are retried with backoff.
     *
     * @throws IllegalStateException if some entries still fail after several attempts
     */
    public void sendBatch(String queueUrl, List<String> bodies) {
        List<String> chunk = new ArrayList<>();
        int chunkBytes = 0;
        for (String body : bodies) {
            int size = Messages.sizeInBytes(body);
            if (!chunk.isEmpty() && (chunk.size() == MAX_BATCH_ENTRIES || chunkBytes + size > MAX_BATCH_BYTES)) {
                sendChunk(queueUrl, chunk);
                chunk = new ArrayList<>();
                chunkBytes = 0;
            }
            chunk.add(body);
            chunkBytes += size;
        }
        if (!chunk.isEmpty()) sendChunk(queueUrl, chunk);
    }

    private void sendChunk(String queueUrl, List<String> bodies) {
        List<SendMessageBatchRequestEntry> pending = new ArrayList<>();
        for (int i = 0; i < bodies.size(); i++) {
            pending.add(SendMessageBatchRequestEntry.builder().id(String.valueOf(i)).messageBody(bodies.get(i)).build());
        }
        for (int attempt = 1; ; attempt++) {
            List<SendMessageBatchRequestEntry> toSend = pending;
            Set<String> failed = sqs.sendMessageBatch(b -> b.queueUrl(queueUrl).entries(toSend))
                    .failed().stream().map(BatchResultErrorEntry::id).collect(Collectors.toSet());
            if (failed.isEmpty()) return;
            pending = pending.stream().filter(e -> failed.contains(e.id())).toList();
            if (attempt == MAX_BATCH_ATTEMPTS) {
                throw new IllegalStateException(pending.size() + " messages failed to send after " + attempt + " attempts");
            }
            log.warn("{} of {} batch entries failed, retrying", pending.size(), toSend.size());
            sleepQuietly(1000L << attempt);
        }
    }

    /** Long-polls (waits up to 20 s) for up to {@code max} messages; returns an empty list if none arrive. */
    public List<Message> receive(String queueUrl, int max) {
        return sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(max)
                .waitTimeSeconds(20)
                .build()).messages();
    }

    public void delete(String queueUrl, Message m) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
    }

    /**
     * Deletes up to 10 messages per call. An entry that fails is only logged: its message
     * reappears later as a duplicate, which receivers ignore.
     */
    public void deleteBatch(String queueUrl, List<Message> messages) {
        for (int from = 0; from < messages.size(); from += MAX_BATCH_ENTRIES) {
            List<Message> chunk = messages.subList(from, Math.min(from + MAX_BATCH_ENTRIES, messages.size()));
            List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
            for (int i = 0; i < chunk.size(); i++) {
                entries.add(DeleteMessageBatchRequestEntry.builder()
                        .id(String.valueOf(i)).receiptHandle(chunk.get(i).receiptHandle()).build());
            }
            var failed = sqs.deleteMessageBatch(b -> b.queueUrl(queueUrl).entries(entries)).failed();
            if (!failed.isEmpty()) {
                log.warn("{} of {} deletes failed; those messages will come back as duplicates", failed.size(), chunk.size());
            }
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        s3.close();
        sqs.close();
        ec2.close();
    }
}
