package dsp.ocr.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Ec2LauncherTest {

    private static final String TASKS = "https://sqs.us-east-1.amazonaws.com/123456789012/mgr-m1-tasks";
    private static final String RESULTS = "https://sqs.us-east-1.amazonaws.com/123456789012/mgr-m1-results";

    @Test
    void workerScriptDownloadsRunsUploadsLogAndShutsDown() {
        String s = Ec2Launcher.workerUserData(TASKS, RESULTS);
        assertTrue(s.startsWith("#!/bin/bash\n"));
        assertTrue(s.contains("aws s3 cp --only-show-errors s3://" + Config.BUCKET + "/jars/worker.jar /opt/ocr/worker.jar"));
        assertTrue(s.contains("java -Xmx512m -jar /opt/ocr/worker.jar \"" + TASKS + "\" \"" + RESULTS + "\""));
        assertTrue(s.contains("logs/worker-$INSTANCE_ID.log"));
        assertTrue(s.indexOf("java -Xmx512m") < s.indexOf("logs/worker-") && s.indexOf("logs/worker-") < s.indexOf("shutdown -h now"),
                "run, then upload the log, then shut down");
        assertFalse(s.contains("set -e"), "the log upload and shutdown must run even if the program fails");
    }

    @Test
    void managerScriptPassesOnlyTheManagerId() {
        String s = Ec2Launcher.managerUserData("m1");
        assertTrue(s.contains("java -Xmx512m -jar /opt/ocr/manager.jar \"m1\" >>"), s);
        assertTrue(s.contains("logs/manager-$INSTANCE_ID.log"));
    }

    @Test
    void noCredentialsInScripts() {
        String s = Ec2Launcher.workerUserData(TASKS, RESULTS) + Ec2Launcher.managerUserData("m1");
        assertFalse(s.contains("aws_access_key_id") || s.contains("AWS_SECRET") || s.contains("aws configure"));
    }

    @Test
    void guardNeverExceedsTheLimit() {
        assertEquals(5, Ec2Launcher.allowedLaunches(5, 2, 9), "room for all");
        assertEquals(3, Ec2Launcher.allowedLaunches(8, 6, 9), "cut down to the free slots");
        assertEquals(0, Ec2Launcher.allowedLaunches(4, 9, 9), "full");
        assertEquals(0, Ec2Launcher.allowedLaunches(4, 11, 9), "already over (e.g. instances started by hand)");
    }

    @Test
    void managerNeedsRoomForItselfAndOneWorker() {
        assertTrue(Ec2Launcher.canLaunchManager(7, 9));
        assertFalse(Ec2Launcher.canLaunchManager(8, 9), "a manager in the last slot could never get a worker");
        assertFalse(Ec2Launcher.canLaunchManager(9, 9));
    }

    @Test
    void recentLaunchesCountOnlyWhileEc2DoesNotListThem() {
        java.util.Set<String> recent = java.util.Set.of("i-new", "i-killed");
        java.util.Set<String> active = java.util.Set.of("i-old");
        // i-new: launched a moment ago, not listed yet -> counted as alive.
        // i-killed: launched recently but already listed as shutting-down -> not alive (found by a failure test).
        java.util.Set<String> listedAnyState = java.util.Set.of("i-old", "i-killed");
        assertEquals(java.util.Set.of("i-old", "i-new"), Ec2Launcher.withUnlisted(active, listedAnyState, recent));
    }

    @Test
    void acceptsAUuidAsManagerId() {
        String uuid = java.util.UUID.randomUUID().toString();
        assertTrue(Ec2Launcher.managerUserData(uuid).contains("manager.jar \"" + uuid + "\""));
        assertTrue(Queues.resultsName(uuid).length() <= 80, "SQS queue names are at most 80 characters");
    }

    @Test
    void rejectsValuesThatCouldBreakTheScript() {
        assertThrows(IllegalArgumentException.class, () -> Ec2Launcher.managerUserData("m1\"; rm -rf /"));
        assertThrows(IllegalArgumentException.class, () -> Ec2Launcher.workerUserData(TASKS + "$(reboot)", RESULTS));
    }
}
