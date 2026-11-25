package dsp.ocr.common;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class QueuesTest {

    @Test
    void namesFollowReadme() {
        assertEquals("mgr-m1-in", Queues.inboxName("m1"));
        assertEquals("mgr-m1-tasks", Queues.tasksName("m1"));
        assertEquals("mgr-m1-results", Queues.resultsName("m1"));
        assertEquals("mgr-m1-dlq", Queues.dlqName("m1"));
        assertEquals("client-abc", Queues.clientName("abc"));
    }

    @Test
    void clientQueueNameFitsSqsLimits() {
        // SQS: at most 80 characters, only alphanumerics, hyphens and underscores.
        String name = Queues.clientName(UUID.randomUUID().toString());
        assertTrue(name.length() <= 80);
        assertTrue(name.matches("[A-Za-z0-9_-]+"));
    }

    @Test
    void s3KeysFollowReadme() {
        assertEquals("jobs/j/input.txt", Config.inputKey("j"));
        assertEquals("jobs/j/summary.jsonl", Config.summaryKey("j"));
        assertEquals("jobs/j/texts/7.txt", Config.textKey("j", 7));
    }
}
