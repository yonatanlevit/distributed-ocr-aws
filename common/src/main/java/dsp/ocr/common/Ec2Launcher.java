package dsp.ocr.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.ShutdownBehavior;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Launches manager and worker instances (README section 12) through the instance limit guard
 * (README section 8). Every RunInstances call in the system goes through this class.
 *
 * Every instance: the system AMI, t2.micro, the lab instance profile, no key pair, its tags set
 * in the launch call, and shutdown behavior "terminate", so it is gone as soon as its program exits.
 *
 * The guard:
 *  - counts every instance in the account that isn't stopped or terminated, whatever its tags,
 *  - adds the instances this process launched recently that DescribeInstances doesn't show yet,
 *  - launches at most MAX_INSTANCES minus that count,
 *  - launches a manager only if two slots are free (one for it, one for at least one worker),
 *  - is synchronized, so threads of one process can't race past it.
 * A denied launch returns nothing; callers retry later.
 */
public final class Ec2Launcher {
    private static final Logger log = LoggerFactory.getLogger(Ec2Launcher.class);
    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9-]{1,40}");
    private static final Pattern SAFE_URL = Pattern.compile("https://[A-Za-z0-9./:_-]+");
    private static final List<String> ACTIVE_STATES = List.of("pending", "running", "stopping", "shutting-down");

    private final Aws aws;
    /** Instance id → launch time (ms), for launches DescribeInstances may not show yet. */
    private final Map<String, Long> recentLaunches = new HashMap<>();

    public Ec2Launcher(Aws aws) {
        this.aws = aws;
    }

    // ---------------- guarded launches ----------------

    /** Launches up to {@code wanted} workers for a manager, as many as the guard allows; returns their ids. */
    public synchronized List<String> launchWorkers(String managerId, String tasksQueueUrl, String resultsQueueUrl, int wanted) {
        int active = activeInstanceCount();
        int n = allowedLaunches(wanted, active, Config.MAX_INSTANCES);
        if (n < wanted) {
            log.info("instance guard: {} active of {}; launching {} of {} wanted workers",
                    active, Config.MAX_INSTANCES, n, wanted);
        }
        if (n == 0) return List.of();
        return launch(workerUserData(tasksQueueUrl, resultsQueueUrl), n, List.of(
                tag("Name", "ocr-worker-" + managerId),
                tag(Config.TAG_ROLE, Config.ROLE_WORKER),
                tag(Config.TAG_MANAGER_ID, managerId)));
    }

    /** Launches a manager if the guard has room for it and at least one worker; returns its instance id. */
    public synchronized Optional<String> launchManager(String managerId) {
        int active = activeInstanceCount();
        if (!canLaunchManager(active, Config.MAX_INSTANCES)) {
            log.info("instance guard: {} active of {}; no room for a manager and a worker", active, Config.MAX_INSTANCES);
            return Optional.empty();
        }
        return launch(managerUserData(managerId), 1, List.of(
                tag("Name", "ocr-manager-" + managerId),
                tag(Config.TAG_ROLE, Config.ROLE_MANAGER),
                tag(Config.TAG_MANAGER_ID, managerId))).stream().findFirst();
    }

    static int allowedLaunches(int wanted, int active, int max) {
        return Math.max(0, Math.min(wanted, max - active));
    }

    static boolean canLaunchManager(int active, int max) {
        return active + 2 <= max;
    }

    // ---------------- counting ----------------

    /** Every instance in the account that is not stopped or terminated, plus recent launches not yet listed. */
    synchronized int activeInstanceCount() {
        Map<String, String> states = describeStates(List.of());
        return withRecentLaunches(idsIn(states, ACTIVE_STATES), states.keySet()).size();
    }

    /**
     * Ids of a manager's live workers (pending or running), plus workers this process launched
     * recently that DescribeInstances doesn't list yet. Used by the scaler as k.
     */
    public synchronized Set<String> liveWorkers(String managerId) {
        Map<String, String> states = describeStates(List.of(
                Filter.builder().name("tag:" + Config.TAG_ROLE).values(Config.ROLE_WORKER).build(),
                Filter.builder().name("tag:" + Config.TAG_MANAGER_ID).values(managerId).build()));
        return withRecentLaunches(idsIn(states, List.of("pending", "running")), states.keySet());
    }

    /**
     * {@code active} plus this process's recent launches that are missing from {@code listed} (the
     * listing in ANY state). A recent launch that is listed, even as shutting-down or terminated,
     * is already accounted for: counting it again would keep a dead worker "alive" until its
     * recent-launch window ends and delay its replacement.
     */
    private Set<String> withRecentLaunches(Set<String> active, Set<String> listed) {
        long now = System.currentTimeMillis();
        recentLaunches.values().removeIf(launchedAt -> launchedAt < now - Config.RECENT_LAUNCH_SECONDS * 1000L);
        return withUnlisted(active, listed, recentLaunches.keySet());
    }

    static Set<String> withUnlisted(Set<String> active, Set<String> listed, Set<String> recent) {
        Set<String> result = new HashSet<>(active);
        for (String id : recent) {
            if (!listed.contains(id)) result.add(id);
        }
        return result;
    }

    private static Set<String> idsIn(Map<String, String> states, List<String> wanted) {
        Set<String> ids = new HashSet<>();
        states.forEach((id, state) -> {
            if (wanted.contains(state)) ids.add(id);
        });
        return ids;
    }

    /** Instance id → state name for every instance matching the filters (all states). */
    private Map<String, String> describeStates(List<Filter> filters) {
        Map<String, String> states = new HashMap<>();
        aws.ec2.describeInstancesPaginator(b -> b.filters(filters))
                .reservations().forEach(r -> r.instances().forEach(i -> states.put(i.instanceId(), i.state().nameAsString())));
        return states;
    }

    /** Manager ids of every manager instance that is pending or running (discovery, README section 5). */
    public List<String> runningManagers() {
        List<String> ids = new ArrayList<>();
        aws.ec2.describeInstancesPaginator(b -> b.filters(
                        Filter.builder().name("tag:" + Config.TAG_ROLE).values(Config.ROLE_MANAGER).build(),
                        Filter.builder().name("instance-state-name").values("pending", "running").build()))
                .reservations().forEach(r -> r.instances().forEach(i -> i.tags().stream()
                        .filter(t -> t.key().equals(Config.TAG_MANAGER_ID)).findFirst()
                        .ifPresent(t -> ids.add(t.value()))));
        return ids;
    }

    /** True if the manager's instance is pending or running (the client's watchdog, README section 5). */
    public boolean isManagerAlive(String managerId) {
        return !describe(List.of(
                Filter.builder().name("tag:" + Config.TAG_ROLE).values(Config.ROLE_MANAGER).build(),
                Filter.builder().name("tag:" + Config.TAG_MANAGER_ID).values(managerId).build(),
                Filter.builder().name("instance-state-name").values("pending", "running").build())).isEmpty();
    }

    /** Terminates every worker of a manager that isn't already gone; returns their ids (README section 6). */
    public List<String> terminateWorkers(String managerId) {
        Set<String> ids = describe(List.of(
                Filter.builder().name("tag:" + Config.TAG_ROLE).values(Config.ROLE_WORKER).build(),
                Filter.builder().name("tag:" + Config.TAG_MANAGER_ID).values(managerId).build(),
                Filter.builder().name("instance-state-name").values("pending", "running", "stopping", "stopped").build()));
        if (!ids.isEmpty()) {
            aws.ec2.terminateInstances(b -> b.instanceIds(ids));
        }
        return List.copyOf(ids);
    }

    private Set<String> describe(List<Filter> filters) {
        Set<String> ids = new HashSet<>();
        aws.ec2.describeInstancesPaginator(b -> b.filters(filters))
                .reservations().forEach(r -> r.instances().forEach(i -> ids.add(i.instanceId())));
        return ids;
    }

    private List<String> launch(String userData, int count, List<Tag> tags) {
        RunInstancesRequest request = RunInstancesRequest.builder()
                .imageId(Config.AMI_ID)
                .instanceType(Config.INSTANCE_TYPE)
                .minCount(1)       // launch what capacity allows, up to count
                .maxCount(count)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name(Config.INSTANCE_PROFILE).build())
                .instanceInitiatedShutdownBehavior(ShutdownBehavior.TERMINATE)
                // The Java SDK expects user-data already base64-encoded.
                .userData(Base64.getEncoder().encodeToString(userData.getBytes(StandardCharsets.UTF_8)))
                // Tags are applied atomically at launch, so an instance is never untagged.
                .tagSpecifications(TagSpecification.builder().resourceType(ResourceType.INSTANCE).tags(tags).build())
                .build();
        List<String> ids = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (Instance i : aws.ec2.runInstances(request).instances()) {
            ids.add(i.instanceId());
            recentLaunches.put(i.instanceId(), now);
        }
        return ids;
    }

    // ---------------- user-data ----------------

    static String workerUserData(String tasksQueueUrl, String resultsQueueUrl) {
        return script("worker", '"' + safeUrl(tasksQueueUrl) + "\" \"" + safeUrl(resultsQueueUrl) + '"');
    }

    static String managerUserData(String managerId) {
        return script("manager", '"' + safeId(managerId) + '"');
    }

    /**
     * The boot script: download the role's jar, run it, upload its log, shut down.
     * No "set -e": whatever fails, the log is still uploaded and the instance still shuts down
     * (and, with shutdown behavior "terminate", is terminated).
     */
    private static String script(String role, String javaArgs) {
        String jar = "/opt/ocr/" + role + ".jar";
        return String.join("\n",
                "#!/bin/bash",
                "# OCR " + role + " bootstrap (README section 12)",
                "export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
                "export AWS_DEFAULT_REGION=" + Config.REGION.id(),
                "LOG=/var/log/ocr-" + role + ".log",
                "INSTANCE_ID=$(cat /var/lib/cloud/data/instance-id)",
                "mkdir -p /opt/ocr",
                "aws s3 cp --only-show-errors s3://" + Config.BUCKET + "/" + Config.jarKey(role) + " " + jar + " >> $LOG 2>&1 \\",
                "  && java " + Config.JAVA_HEAP + " -jar " + jar + " " + javaArgs + " >> $LOG 2>&1",
                "echo \"exit code: $?\" >> $LOG",
                "aws s3 cp --only-show-errors $LOG s3://" + Config.BUCKET + "/logs/" + role + "-$INSTANCE_ID.log",
                "shutdown -h now",
                "");
    }

    /** Values go into a shell script, so only plain ids and queue URLs are accepted. */
    private static String safeId(String id) {
        if (!SAFE_ID.matcher(id).matches()) throw new IllegalArgumentException("unsafe manager id: " + id);
        return id;
    }

    private static String safeUrl(String url) {
        if (!SAFE_URL.matcher(url).matches()) throw new IllegalArgumentException("unsafe queue url: " + url);
        return url;
    }

    private static Tag tag(String key, String value) {
        return Tag.builder().key(key).value(value).build();
    }
}
