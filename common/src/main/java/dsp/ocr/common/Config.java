package dsp.ocr.common;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;

/** System-wide constants shared by the Local App, Managers and Workers (README sections 3, 4, 13). */
public final class Config {
    private Config() {}

    /** Every AWS client (S3, SQS, EC2) uses this one region. */
    public static final Region REGION = Region.US_EAST_1;

    /** The single private bucket that holds everything this system stores. */
    public static final String BUCKET = "dsp-ocr-ahub-2026";

    // ---- EC2 (README section 12) ----

    /** Ubuntu 26.04 + Java 21 + Tesseract 5.5 + AWS CLI, built by hand as README section 12 describes. */
    public static final String AMI_ID = "ami-0a40497d38c5e505c";
    /** Scale out, not up: every instance is the smallest type that fits Java and Tesseract. */
    public static final InstanceType INSTANCE_TYPE = InstanceType.T2_MICRO;
    /** Gives instances temporary AWS credentials through instance metadata; no keys anywhere. */
    public static final String INSTANCE_PROFILE = "LabInstanceProfile";
    /** Java heap cap for a 1 GiB instance (README section 12). */
    public static final String JAVA_HEAP = "-Xmx512m";

    /**
     * Most instances the system may run at once, counting every instance in the account
     * (README section 8). The Learner Lab allows 9; the assignment forbids more than 19.
     */
    public static final int MAX_INSTANCES = 9;
    /** How long a launched instance counts as "just launched" while DescribeInstances may not show it yet. */
    public static final int RECENT_LAUNCH_SECONDS = 120;

    static {
        if (MAX_INSTANCES > 19) throw new IllegalStateException("MAX_INSTANCES must never exceed 19");
    }

    public static final String TAG_ROLE = "Role";
    public static final String ROLE_MANAGER = "OcrManager";
    public static final String ROLE_WORKER = "OcrWorker";
    public static final String TAG_MANAGER_ID = "ManagerId";

    // ---- S3 layout (README section 4) ----

    /** Where the manager and worker jars live; instances download them at boot. role = "manager" or "worker". */
    public static String jarKey(String role) {
        return "jars/" + role + ".jar";
    }

    public static String inputKey(String jobId) {
        return "jobs/" + jobId + "/input.txt";
    }

    public static String summaryKey(String jobId) {
        return "jobs/" + jobId + "/summary.jsonl";
    }

    /** Where an instance uploads its log just before it shuts down. role = "manager" or "worker". */
    public static String logKey(String role, String instanceId) {
        return "logs/" + role + "-" + instanceId + ".log";
    }

    /** Where a Worker puts an OCR text too large for an SQS message. */
    public static String textKey(String jobId, int index) {
        return "jobs/" + jobId + "/texts/" + index + ".txt";
    }

    // ---- Queue settings (README sections 3, 13) ----

    /** Inbox: how long a received NEW_TASK / TERMINATE stays hidden while the intake handles it. */
    public static final int INBOX_VISIBILITY_SECONDS = 60;
    /** Tasks queue: how long a received task stays hidden unless the Worker's heartbeat extends it. */
    public static final int TASK_VISIBILITY_SECONDS = 60;
    /** Results queue, dead-letter queue and client reply queues (the SQS default). */
    public static final int DEFAULT_VISIBILITY_SECONDS = 30;
    /** A task delivered this many times without being deleted moves to the dead-letter queue. */
    public static final int MAX_RECEIVE_COUNT = 3;
    /** Dead-letter queue retention: 14 days (the maximum), longer than the tasks queue's 4-day default. */
    public static final int DLQ_RETENTION_SECONDS = 14 * 24 * 60 * 60;

    // ---- Manager (README sections 6, 13) ----

    /** The scaler's periodic tick; it also runs whenever a job is registered. */
    public static final int SCALER_TICK_SECONDS = 20;
    /** After rejecting new tasks this long while retiring, the manager deletes its queues and exits. */
    public static final int RETIRE_GRACE_SECONDS = 60;

    /**
     * A manager with no jobs for this long retires (Y, beyond the spec).
     * Overridable for testing: -Dmanager.idleRetireSeconds=&lt;seconds&gt;
     */
    public static int managerIdleRetireSeconds() {
        return Integer.getInteger("manager.idleRetireSeconds", 5 * 60);
    }

    // ---- Local App (README sections 5, 13) ----

    /** While waiting, the client checks this often that its manager's instance still exists. */
    public static final int WATCHDOG_SECONDS = 60;

    // ---- Worker (README sections 7, 9, 13) ----

    /** How often a busy Worker extends its task's visibility; one missed beat is tolerated. */
    public static final int HEARTBEAT_SECONDS = 20;
    /** Maximum processing time per image; past it the Worker exits and the task returns to the queue. */
    public static final int MAX_TASK_PROCESSING_SECONDS = 5 * 60;
    /** A Worker that has received no message for this long exits (X, about one boot time). */
    public static final int WORKER_IDLE_EXIT_SECONDS = 2 * 60;

    /**
     * An OCR_DONE message whose JSON body is larger than this (in UTF-8 bytes) carries its text
     * in S3 instead. Below the SQS message limit with a wide margin.
     * Overridable for testing: -Docr.largeTextThreshold=&lt;bytes&gt;
     */
    public static int largeTextThresholdBytes() {
        return Integer.getInteger("ocr.largeTextThreshold", 200 * 1024);
    }
}
