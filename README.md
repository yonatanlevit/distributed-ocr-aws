# Distributed OCR on AWS

[![CI](https://github.com/yonatanlevit/distributed-ocr-aws/actions/workflows/ci.yml/badge.svg)](https://github.com/yonatanlevit/distributed-ocr-aws/actions/workflows/ci.yml)

A fault-tolerant, auto-scaling system that runs OCR on large batches of image URLs, built on AWS **EC2**, **SQS** and **S3** in **Java 21**. Clients submit a text file of image URLs. A manager on EC2 splits the work into tasks, scales a fleet of workers to match the load, and returns an HTML page with each image and its recognized text.

Built as a two-person team project for a Distributed Systems Programming course.

### Highlights

- **Message-driven architecture.** Clients, managers and workers never call each other directly; every interaction goes through SQS queues and S3, so any component can fail or stall without blocking the rest.
- **Auto-scaling.** Each manager computes how many workers its active jobs need and launches them on EC2. Idle workers shut themselves down, and idle managers retire. One shared guard enforces the account's instance limit for every launch.
- **Fault tolerance.** At-least-once delivery with idempotent result recording, visibility-timeout heartbeats for long tasks, a dead-letter queue for poison messages, and a client watchdog that resubmits when a manager dies. Every failure scenario was tested on real EC2 instances (section 10).
- **Multi-tenant managers.** Admission control limits each manager's clients. Per-client reply queues and per-manager queues isolate jobs from each other, and extra clients start new managers.
- **Concurrency.** Bounded thread pools in the I/O-bound manager, and one OCR thread per single-vCPU worker.
- **Security.** No credentials in code or images: instances use IAM instance profiles, the bucket stays private, and the generated HTML is escaped against injection.

### Tech stack

Java 21 · Maven (multi-module) · AWS SDK for Java v2 (EC2, SQS, S3) · Tesseract OCR (tess4j) · Jackson · JUnit 5 · GitHub Actions

### Modules

| Module | Role |
|---|---|
| `local-app` | Client: uploads the input, finds or launches a manager, waits for the result, writes the HTML |
| `manager` | Runs on EC2: admits jobs, splits them into tasks, scales workers, collects results |
| `worker` | Runs on EC2: downloads one image at a time, runs OCR, reports the text or the error |
| `common` | Shared AWS helpers, message types, queue setup, and the EC2 launch guard |

Sample input: [`docs/input-example.txt`](docs/input-example.txt). Sample output: [`output/sample-output.html`](output/sample-output.html). Build and run instructions are in section 16.

The rest of this document is the full design write-up.

---

## 1. Overview

The system has three kinds of components: local applications (clients) on the user's machine, managers on EC2, and workers on EC2. They never call each other directly. All communication goes through SQS queues and one S3 bucket, so each component scales on its own, and any of them can stall or fail without blocking the others: work in progress waits durably in SQS until someone handles it.

A manager serves at most `manager-clients-n` clients, so several managers can run at once. Every queue is therefore scoped either to one manager or to one client, and each manager owns its own pool of workers.

## 2. Architecture

```
+------------------------------------------+            +-------------+
|            Local apps (many)             |<---------->|  S3 bucket  |
+------------------------------------------+            +-------------+
          |                       ^                            ^
          | NEW_TASK,             | DONE,                      |
          | TERMINATE             | REJECTED                   |
          v                       |                            |
+------------------+    +------------------+                   |
|   mgr-<id>-in    |    |  client-<jobId>  |                   |
+------------------+    +------------------+                   |
          |                       ^                            |
          v                       |                            |
+------------------------------------------+                   |
|              Manager (EC2)               |<------------------+
+------------------------------------------+
          |                       ^
          | IMAGE_TASK            | OCR_DONE
          v                       |
+------------------+    +------------------+
|  mgr-<id>-tasks  |    | mgr-<id>-results |
+------------------+    +------------------+
          |                       ^
          v                       |
+------------------------------------------+            +-------------+
|            Workers (EC2 x k)             |<-----------|  Internet   |
+------------------------------------------+            +-------------+
```

Not shown: each manager's tasks queue has a dead-letter queue, which the manager reads (section 3). A local app launches a new manager when no available one exists, and each manager launches its own workers. Every launch goes through the instance guard (section 8).

| Component | Responsible for | Never does |
|---|---|---|
| Local app | Uploading the input, finding or starting a manager, waiting for its result, writing the HTML, optionally terminating its manager | Talking to workers or handling individual images |
| Manager | Admitting clients, splitting each input into one task per URL, scaling its workers, collecting results, writing one summary per job, shutting down | Downloading images, running OCR, or writing HTML |
| Worker | One task at a time: downloading the image, running OCR, reporting the text or the error | Coordinating with other workers or tracking whole jobs |

## 3. Queues and messages

| Queue | One per | From → to | Messages | Visibility timeout |
|---|---|---|---|---|
| `mgr-<id>-in` | manager | local apps → manager | `NEW_TASK`, `TERMINATE` | 60 s |
| `mgr-<id>-tasks` | manager | manager → its workers | `IMAGE_TASK` | 60 s, extended by a heartbeat |
| `mgr-<id>-results` | manager | workers → manager | `OCR_DONE` | 30 s |
| `mgr-<id>-dlq` | manager | tasks queue (by SQS redrive) → manager | `IMAGE_TASK`s that failed 3 deliveries | 30 s |
| `client-<jobId>` | client run | manager → that client | `DONE`, `REJECTED` | 30 s |

SQS cannot deliver only the messages meant for a particular consumer. If recipients or message types shared a queue, consumers would have to receive and hand back messages that aren't theirs, wasting calls and adding latency. With many clients on one shared reply queue, each result would be received by the wrong clients many times before reaching its owner. Per-client reply queues make delivery direct. Per-manager queues let a client address the specific manager it chose, give each manager clear ownership of its workers, and reduce teardown to deleting four queues. Separate queues per message type also get their own visibility timeouts.

| Message | Fields |
|---|---|
| `NEW_TASK` | `jobId`, `inputKey`, `workerFilesN`, `managerClientsN`, `replyQueueUrl` |
| `TERMINATE` | `jobId` of the sending client |
| `IMAGE_TASK` | `jobId`, `index` (the URL's position in the input, counting from 0 and skipping blank lines), `url` |
| `OCR_DONE` | `jobId`, `index`, `url`, and either `text` (or `textKey`, see below) or `error` |
| `DONE` | `jobId`, `managerId`, `summaryKey` |
| `REJECTED` | `jobId`, `managerId`, `reason`: `FULL`, `TERMINATING`, `RETIRING`, or `FAILED` |

Replies name the manager that sent them, and a client ignores replies from any manager other than the one it is currently waiting for. Without this, a late duplicate `REJECTED` from a manager the client already left could be mistaken for the current manager's answer.

Every message is a JSON object with a `type` field naming its kind, so a receiver can decode any body and then branch on its type. Fields without a value are left out. Example `OCR_DONE` messages:

```
{"type": "OCR_DONE", "jobId": "7f3c9a1e", "index": 42, "url": "https://example.com/a.png", "text": "Hello world"}
{"type": "OCR_DONE", "jobId": "7f3c9a1e", "index": 43, "url": "https://example.com/b.png", "error": "IOException: HTTP 404"}
```

All queues are standard (non-FIFO). They deliver at least once, so duplicates are possible; the manager absorbs them by recording results by `(jobId, index)`. The key is the line index rather than the URL, because an input file can list the same URL twice. Every receive uses long polling (`WaitTimeSeconds=20`), which avoids paying for empty responses.

The manager batches its SQS calls: `SendMessageBatch` when splitting, `ReceiveMessage` with up to 10 messages when collecting, and `DeleteMessageBatch` afterwards. SQS bills per call, and every call is a network round trip, so batching cuts the manager's calls for a 1,000-URL job from about 3,000 to about 300. A batch call can succeed while individual entries fail, so the manager retries the entries listed in `failed()`; dedupe makes those retries harmless. Workers use single-message calls because they hold one message at a time.

An OCR text too large for a message (above about 200 KB) is uploaded to `jobs/<jobId>/texts/<index>.txt`, and the result carries `textKey` instead of `text`.

The tasks queue has a dead-letter queue, the standard AWS pattern for poison messages. A task is retried only when a worker dies or gives up while holding it; an ordinary failure such as a 404 is reported and deleted on the first try. If a task is delivered three times without being deleted, SQS moves it to `mgr-<id>-dlq` instead of delivering it again. At startup the manager creates the dead-letter queue, reads its ARN, and creates the tasks queue with a `RedrivePolicy` that points to it with `maxReceiveCount` 3. A message that reaches the dead-letter queue still needs a result, or its job would never complete. The manager therefore reads the dead-letter queue, records each task there as a failed line ("failed after 3 attempts"), logs it, and deletes it. The failure appears in the output like any other error. The dead-letter queue's retention period is set longer than the tasks queue's, as AWS recommends, because a message keeps its original enqueue time when it moves.

## 4. Storage and tags

```
s3://<bucket>/
  jars/manager.jar
  jars/worker.jar
  jobs/<jobId>/input.txt           uploaded by the local app
  jobs/<jobId>/summary.jsonl       written by the manager
  jobs/<jobId>/texts/<index>.txt   only for OCR text too large for a message
  logs/<role>-<instanceId>.log     each instance's log, uploaded just before it shuts down
```

The bucket stays private. Nothing has to be public: the HTML links to the original image URLs, and EC2 nodes read the jars with their instance role. Deleting `jobs/` removes everything a run produced.

The summary is JSON Lines: one result per line, in the order results arrived, with the same fields as `OCR_DONE` minus `jobId`. The local app orders the lines by `index` when writing the HTML.

| Tag | On | Written by | Used for |
|---|---|---|---|
| `Role` = `OcrManager` or `OcrWorker` | every system instance | the launcher | discovery and identification |
| `ManagerId` | managers and their workers | the launcher | naming a manager's queues, finding its workers |
| `Name` | every system instance | the launcher | readability in the EC2 console |

All tags are written once, in the same call that launches the instance, and never change. A `ManagerId` is a random UUID, generated by the local app that launches the manager.

Load and availability are deliberately not kept in tags. A tag would only be a copy of the manager's state, stale by the time anyone reads it, and keeping it current would cost an EC2 API write on every admission and finish, plus a thread to do it. The manager's admission check (section 6) is always exact and answers within about a second, so the local app asks the manager instead of reading a copy of its state.

## 5. Local application

1. Validate the arguments (`manager-clients-n` and `worker-files-n` must be at least 1), generate a `jobId` (UUID), and create the reply queue `client-<jobId>`.
2. Upload the input file to `jobs/<jobId>/input.txt`.
3. Discover the managers with `DescribeInstances`, filtered on `Role=OcrManager` and state pending or running, and skip any that already refused this job. Try them in random order, so that clients starting together don't all queue at the same manager. If none is left, launch one through the instance guard, with a new `managerId`, and create its inbox queue. `CreateQueue` is idempotent when both sides use identical attributes, so the manager can safely create it again on boot. If the guard has no room, wait with backoff, forget earlier `FULL` refusals (a full manager frees up when a job finishes), and repeat discovery.
4. Send `NEW_TASK` to the manager's inbox. If the manager is still booting, SQS holds the message, so nothing waits on the boot.
5. Long-poll the reply queue. Every 60 seconds, check the chosen manager's instance state.
6. A refusal means: try the next manager (step 3). A refusal is a `REJECTED` with reason `FULL`, `TERMINATING` or `RETIRING`, or an inbox that no longer exists. This is how the spec's availability check is done: by asking the manager, whose own count is exact. A failure is a `REJECTED` with reason `FAILED`, or a manager whose instance is gone while the client waits; it also leads back to step 3. Refusals are not limited, since trying managers in turn is normal. After three failures, the client stops and reports an error.
7. On `DONE`, download the summary and write the HTML in input order. A success shows the image, linked to its original URL, with the HTML-escaped text. A failure shows the original URL followed by the short error description. Everything that comes from outside (URLs, OCR text, errors) is HTML-escaped, and only `http` and `https` URLs become links or images, so neither an input line nor an image's text can inject markup or a `javascript:` link into the page.
8. If `terminate` was given, send `TERMINATE` to the manager that completed the job.
9. Delete the reply queue and `jobs/<jobId>/`, whether the run succeeded or failed, since a `jobId` is never reused. A shutdown hook deletes the reply queue even if the user stops the client with Ctrl+C.

The local app needs no extra threads: it is one sequential flow that mostly waits.

## 6. Manager

### Admission

The manager admits a job only if it is neither terminating nor retiring and its number of active jobs is below the job's `managerClientsN`. Otherwise it replies `REJECTED` with the reason. Admission happens in the single intake thread, and the active count increases at the same moment, so even clients arriving at the same instant cannot overfill a manager. This check is the only availability check in the system: clients learn whether a manager is available by asking it (section 5).

The check uses the arriving client's own value, which matches the spec's definition of an available manager. As a consequence, a manager can end up serving more clients than an earlier client's limit.

The manager remembers every `jobId` it has admitted and ignores duplicate deliveries of the same `NEW_TASK`.

### Threads

| Thread | How many | Work |
|---|---|---|
| Intake | 1 | Long-polls the inbox, handles `TERMINATE`, admits or rejects each `NEW_TASK`, deletes inbox messages, hands admitted jobs to the job pool |
| Job pool | small bounded pool (e.g. 4) | Downloads the input to local disk, counts its URLs, registers the job, triggers a rebalance, sends `IMAGE_TASK`s in batches of 10 |
| Collectors | 2–4 | Long-poll the results queue 10 at a time, record each result into its job, batch-delete |
| Dead-letter reader | 1 | Long-polls the dead-letter queue, records each task there as a failed line, deletes it |
| Finisher pool | small (e.g. 2) | Uploads the summary, sends `DONE`, removes the job |
| Scaler | 1, scheduled | Runs `rebalance()` every 20 s, and is also called whenever a job is registered |

The manager's work is I/O-bound (SQS, S3 and EC2 calls), so threads hide network latency and let it serve many clients at once. Every pool is bounded; there is never one thread per client. If a job's input cannot be read even after the SDK's retries, the manager replies `REJECTED` with reason `FAILED` and frees the slot, and the client resubmits.

### Job bookkeeping and completion

For each job the manager keeps its total, a `BitSet` of the indices that already have a result, a completed counter, and an append-only temp file that becomes the summary. The total is known before the first task is sent, because the job pool counts the URLs first, skipping blank lines in both the count and the send. A result for an index whose bit is already set is a duplicate and is dropped.

Completion is checked on every new result, and once right after registration to cover an empty input. Both checks run under the job's lock, so a job finishes exactly once even with several collector threads:

```java
final class Job {
    private final int total;           // known before the first task is sent
    private final BitSet done;
    private int completed;
    private boolean finished;

    Job(int total) { this.total = total; this.done = new BitSet(total); }

    /** Returns true exactly once: for the call that completes the job. */
    synchronized boolean record(int index, String resultLine) throws IOException {
        if (finished || done.get(index)) return false;   // duplicate
        appendToResultsFile(resultLine);   // first: if the write fails, the index stays open
        done.set(index);                   // and the result message is retried
        completed++;
        return tryFinish();
    }

    /** Also called once right after the job is registered (empty input). */
    synchronized boolean tryFinish() {
        if (finished || completed < total) return false;
        finished = true;
        return true;
    }
}
```

The caller hands the job to the finisher pool whenever either method returns `true`. Because the total is fixed before any task goes out and every index needs its own result, a job cannot look complete while it is still being split.

Memory per job is about one bit per URL, roughly 125 KB per million. `BitSet` indices are `int`s, so one job can hold up to about 2.1 billion URLs.

### Scaling workers

The scaler computes how many workers the current workload calls for:

`m = Σ ⌈remaining_j / n_j⌉`

For each active job j, `remaining_j` is the number of its URLs without a result and `n_j` is that client's `worker-files-n`. With k live workers, the scaler launches m − k if that is positive.

For example, job A with 47 URLs left at n = 10 gets 5 workers, and job B with 12 left at n = 5 gets 3, so m = 8. With 6 workers alive, the manager launches 2. With a single job, this is exactly the spec's rule: one worker per n files, minus the workers already running. Each client's own n is respected, and the counts come from the manager's own bookkeeping, which is exact, unlike the queue's approximate message count. k counts this manager's workers in pending or running state, plus any it launched in the last couple of minutes that `DescribeInstances` does not show yet.

A new job is the main trigger. The periodic tick covers what a new job never would:

- It replaces workers that died. Without it, a job whose workers all died would stall forever.
- It retries launches that the instance guard deferred or that failed.
- It corrects for a worker that idle-exited just before new tasks arrived.

The scaler only adds workers; shrinking is handled by the workers themselves (section 9).

### Termination

On `TERMINATE`, the manager:

1. Stops accepting new jobs.
2. Answers every later `NEW_TASK` with `REJECTED` (`TERMINATING`). This also covers a task sent before the terminate but received after it, since standard queues don't guarantee order.
3. Waits until no active jobs remain; each one finishes normally and gets its `DONE`.
4. Stops its receive loops (each ends after its current long poll) and its scaler, then terminates its workers, found by their `ManagerId` tag.
5. Deletes its four queues.
6. Exits. The boot script then uploads the manager's log to S3, and the instance's shutdown behavior terminates it.

The main thread performs this sequence; it sleeps until the intake thread signals that the manager stopped accepting. The scaler keeps running while the remaining jobs finish, so a worker that dies during that time is still replaced.

### Idle retirement (beyond the spec)

A manager that no client terminates would otherwise run forever. After Y minutes with no jobs, the manager runs the same sequence with reason `RETIRING`. It stops accepting, keeps reading its inbox for a grace period of about a minute while rejecting any `NEW_TASK`, then terminates any remaining workers, deletes its queues, and exits. Client-side rules cover the last moment: a send to a deleted inbox counts as a rejection, and the watchdog resubmits if a task landed in the inbox just as the manager left. The decision to retire is made on the intake thread, the same thread that admits jobs, so a job can never be admitted at the moment the manager decides to retire.

## 7. Workers

A worker has one processing thread and handles one message at a time; throughput grows by adding worker instances. At startup it reads its two queue URLs from its arguments and creates a single Tesseract instance, reused for every image. Before taking any task it runs OCR on a small generated image and exits if that fails, so a worker with a broken OCR installation never turns a whole job into error results.

1. Receive one message (`MaxNumberOfMessages=1`, `WaitTimeSeconds=20`). If no message has arrived for X minutes, exit.
2. Start the heartbeat: a timer thread extends the message's visibility to 60 seconds every 20 seconds, up to the maximum processing time. With a 20-second interval, one failed or late extension still leaves time for the next one before the message would become visible.
3. Download the image, with connect and read timeouts and a size cap.
4. Run OCR.
5. Send `OCR_DONE` with the text, or with the S3 key for a very large text.
6. Stop the heartbeat and delete the task.

An exception in steps 3 or 4 becomes an error result, and steps 5 and 6 still run. If sending the result fails, the task is not deleted, so it reappears and is retried. If a single image exceeds the maximum processing time, the worker exits; Tesseract runs native code, which cannot be interrupted. The message returns to the queue, and the scaler replaces the instance. A task that keeps defeating workers this way ends up in the dead-letter queue after three deliveries (section 3). If the worker's queues no longer exist because its manager is gone, it exits.

Sending before deleting is the safe order. A crash between the two produces a duplicate result, which the manager ignores. Deleting first could lose a result forever.

One thread suits a 1-vCPU instance such as t2.micro, where extra OCR threads would only compete for the same core. Holding one message at a time also means a worker never sits on tasks that idle workers could take, and a crash costs at most one task.

## 8. Instance limit guard

The assignment forbids running more than 19 instances, and the Learner Lab allows 9. This is a technical limit, not a design input. The design decides how many instances it wants (managers by the availability rule, workers by the scaling formula), and one guard decides how many may launch right now. Every `RunInstances` call, from the local app or a manager, goes through a shared launcher (`Ec2Launcher`):

- It counts every instance in the account that is not stopped or terminated, regardless of tags, so pending and shutting-down instances, and any instance started by hand, all count.
- It adds instances it launched in the last couple of minutes that `DescribeInstances` does not show yet, since the EC2 API is eventually consistent. "Does not show" means not listed in any state: a recent launch that is already listed, even as shutting down, is not counted again. A failure test found this the hard way. When the check looked only at running instances, a worker killed a minute after its launch still counted as alive until its launch window ended, delaying its replacement by 80 seconds instead of one scaler tick.
- It launches at most `MAX_INSTANCES − count` in one call. `MAX_INSTANCES` is 9 in the lab, and the code refuses any value above 19.
- It launches a manager only when two slots are free, one for the manager and one for at least one worker. That way managers can never fill every slot and wait forever for workers that cannot start.
- It is `synchronized`, so threads within one process cannot race.

A denied or failed launch is never an error, only a delay. The scaler retries on its next tick, and a client waiting for a manager retries discovery with backoff while its input stays safe in S3.

Launches from different processes at the same instant fall under the assignment's no-race assumption.

## 9. Scale-down

Workers scale down by themselves. A worker that has received no message for X minutes exits. Workers are launched with shutdown behavior set to terminate, and their script ends with `shutdown -h now`, so the instance terminates when the process exits. This is safe by construction, because a worker only quits while holding no message, and it also cleans up orphans if a manager dies. On termination or retirement, the manager terminates whatever workers remain.

X is set close to one boot time, 1–2 minutes. Idling longer than a relaunch takes costs more than relaunching, while quitting much sooner pays for boots that weren't needed. This rent-or-buy break-even never costs more than twice the best choice in hindsight. EC2 bills Linux instances per second, so terminating and relaunching later costs little beyond the boot. A software "sleep mode" would save nothing: the instance is billed whether the process polls or sleeps, and an idle long-polling worker makes only about three requests a minute.

Managers scale down on `TERMINATE` (the spec) or through idle retirement (section 6).

Alternatives considered:

| Option | Why not |
|---|---|
| Manager terminates surplus workers | It cannot tell which worker is busy, so it kills work in progress; the task survives through the visibility timeout, but the work is wasted and the job's tail stalls |
| STOP messages in the tasks queue | Graceful, since only idle workers receive messages, but leftover STOPs kill the next job's workers unless they expire |
| Drain tag on a chosen worker | Graceful and targeted, but every worker must poll EC2 and needs EC2 permissions |
| Stop or hibernate instead of terminate | User-data runs only on first boot, a stopped worker stays tied to one manager's queues, stopped disks still cost money, and per-second billing makes a fresh launch nearly as cheap |

## 10. Failure handling

| What goes wrong | What happens |
|---|---|
| Bad URL, 404, not an image, OCR exception | The worker reports an error result for that line, deletes the task, and continues; the HTML shows the URL and the error |
| An image too large to decode (a "decompression bomb": a small file declaring 25000×25000 pixels) | Java's image reader catches the `OutOfMemoryError` itself, so the worker reports an ordinary error, `IIOException: ... (OutOfMemoryError: Java heap space)`, and keeps running |
| Worker dies mid-image | The task was never deleted, so it reappears after the visibility timeout and another worker takes it; the scaler's next tick replaces the instance |
| Worker hangs | Download timeouts apply; past the maximum processing time the heartbeat stops and the worker exits, releasing the task |
| One image crashes every worker that tries it | After three deliveries, SQS moves the task to the dead-letter queue; the manager reads it there and records the line as failed, so the job still completes |
| Duplicate delivery, or a crash between sending a result and deleting its task | The manager drops results for indices it already has |
| A result fails to send | The task is not deleted, so it is retried |
| Manager stalls | Messages wait in SQS; nothing is lost |
| Manager dies | The client's watchdog sees the instance is gone and resubmits the same `NEW_TASK` (the input is still in S3); orphaned workers exit on their idle timeout. The dead manager's queues stay until the cleanup command removes them (section 16); an idle queue costs nothing, since SQS bills per request |
| Manager process hangs while its instance keeps running | Not detected (known limitation): the watchdog checks only the instance state, so that manager's clients keep waiting. A crashed process is covered, because the instance terminates when the process exits |
| Input can't be read | The manager replies `REJECTED` (`FAILED`), and the client resubmits |
| A result can't be recorded (e.g. the local disk write fails) | The result message is not deleted, so it reappears and is recorded later |
| The summary upload or the `DONE` send keeps failing | The manager retries with backoff, then replies `REJECTED` (`FAILED`) and frees the slot, and the client resubmits |
| The client's reply queue is gone | The client is gone; the manager logs it, finishes the job, and frees the slot |
| Manager full, terminating or retiring | The manager replies `REJECTED`, and the client tries another manager or launches one |
| A send to a manager's inbox fails because the queue is gone | Treated as a rejection |
| Launch denied by the guard, or fails | Retried later by the scaler or by the client's backoff |
| Transient AWS or network error | The SDK retries with backoff, and every loop catches exceptions so no single failure kills a thread |
| Client dies while waiting | The job still completes; its summary stays in S3, and its reply queue can be removed by prefix |

### Verified on EC2

Each row was run against the real system. Slow tasks came from a small helper server inside the VPC that sends its response one byte per second; it was not part of the system and was terminated afterwards.

| Scenario | Observed |
|---|---|
| A worker is killed 15 s into a 90-second task | The task became visible again once the dead worker's last heartbeat extension ran out. The scaler launched a replacement 8 s after the kill. The replacement finished the task, and the job completed |
| A task takes longer than the 60-second visibility timeout | The worker extended visibility at 20, 40, 60 and 80 s and finished after 89.7 s; no other worker received the task |
| A task is received three times and never deleted (three crashed workers, simulated by receiving without deleting) | On the fourth receive SQS moved it to the dead-letter queue; the manager recorded "failed after 3 attempts", and the job completed |
| The manager is killed mid-job | The client's watchdog noticed within 45 s, launched a new manager, and resubmitted; the job completed. The guard held the new manager to the slots left by the dead manager's orphaned workers (7 of 9 active: 2 of 5 workers launched) until they idled out |
| `TERMINATE` from one client while another client's job is running | The manager stopped accepting, waited about 2 minutes for the other job, which got its `DONE`, then terminated its workers, deleted its queues, and exited |
| A client arrives after `TERMINATE` | Refused with `TERMINATING` within 1 s. With all 9 slots taken, the guard had no room for a new manager. The client waited with backoff until the old manager exited, then launched its own and completed |
| A duplicate result, and a result for a job that doesn't exist, are injected into a manager's results queue | Logged "duplicate result ignored" and "unknown or finished job ... dropped"; the duplicate's text does not appear in the output |
| A decompression bomb in a job | An ordinary error line; the worker kept running |
| The cleanup command while the system is busy | Listed the active instances, deleted nothing, exited with code 1 |

## 11. Security

No credentials appear in code, jars, user-data, or the submission. The local app uses the SDK's default credential chain, which reads `~/.aws/credentials`, refreshed from the Academy page each lab session. Every EC2 instance is launched with the lab's instance profile (usually `LabInstanceProfile`), so the SDK and the AWS CLI on the instance obtain temporary, automatically rotated credentials from instance metadata. There is nothing to leak, which is stronger than the password-protected jar the assignment suggests. The bucket is private, and nodes read the jars with their instance role rather than through public links.

## 12. Bootstrapping

One AMI serves both roles. It is built from Ubuntu Server 26.04 LTS and contains Java 21, the AWS CLI, and Tesseract 5.5 with its native library and English language data (on Linux, tess4j uses the system library).

The AMI was created as the assignment describes: launch an instance from the base image, install the software from its console, and create an image from the EC2 Instances view. The builder instance had the lab instance profile and no key pair (EC2 Instance Connect pushes a temporary key for each session), so no key and no credentials ended up in the image. The installation commands:

```bash
sudo apt-get update
sudo NEEDRESTART_MODE=a apt-get install -y openjdk-21-jre-headless tesseract-ocr tesseract-ocr-eng libtesseract-dev unzip
wget -q https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -O awscliv2.zip
unzip -q awscliv2.zip && sudo ./aws/install && rm -rf aws awscliv2.zip
```

The AWS CLI comes from AWS's official installer rather than a snap package, since the snap service may not be ready yet when user-data runs early in boot. Before creating the image, the worker jar was downloaded and run once on the builder to confirm the OCR self-test passes, then removed: jars are never part of the image. The jars live in the private bucket, and each instance's user-data downloads the current one at boot, so a code fix doesn't require a new AMI. If AMI creation is unavailable, the same user-data script installs these packages first.

Worker user-data (the manager's is the same with `manager` in place of `worker`, and `"<managerId>"` as the only argument):

```bash
#!/bin/bash
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
export AWS_DEFAULT_REGION=us-east-1
LOG=/var/log/ocr-worker.log
INSTANCE_ID=$(cat /var/lib/cloud/data/instance-id)
mkdir -p /opt/ocr
aws s3 cp --only-show-errors s3://<bucket>/jars/worker.jar /opt/ocr/worker.jar >> $LOG 2>&1 \
  && java -Xmx512m -jar /opt/ocr/worker.jar "<tasksQueueUrl>" "<resultsQueueUrl>" >> $LOG 2>&1
echo "exit code: $?" >> $LOG
aws s3 cp --only-show-errors $LOG s3://<bucket>/logs/worker-$INSTANCE_ID.log
shutdown -h now
```

The launcher (`Ec2Launcher`) fills in the placeholders and base64-encodes the script, since the Java SDK expects user-data already encoded. It accepts only plain ids and queue URLs as values, so nothing can break out of the script. It launches with the instance profile, no key pair, its tags applied in the same call, and `InstanceInitiatedShutdownBehavior=terminate`, so when the Java process exits for any reason, the instance terminates. The manager derives its queue names from its `managerId`.

The script deliberately does not stop on errors: whether the jar fails to download, the program crashes, or it exits normally, the log is uploaded and the instance shuts down. With no way to log in to an instance, the uploaded log is how a run on EC2 is debugged afterwards, including the run of a worker that crashed.

Measured on a worker: about 50 seconds from launch to the program running (boot, jar download, OCR self-test), and the instance was terminated about 20 seconds after the program exited.

Every instance, manager or worker, is a t2.micro: the system scales out by adding instances, not up by using larger ones, and t2.micro is the smallest type that fits Java and Tesseract (a t2.nano has only 0.5 GiB). `-Xmx512m` caps the Java heap at 512 MB. The rest of the 1 GiB is left for the operating system, the JVM's own overhead (loaded classes, threads), and Tesseract, whose memory is native and lies outside the heap. A larger heap would let the process as a whole exceed the instance's memory on a large image. Without the flag, Java would cap the heap at a quarter of RAM, 256 MB, which is too small for some large images.

## 13. Tunable parameters

| Parameter | Default | Notes |
|---|---|---|
| `MAX_INSTANCES` | 9 | Lab limit; the code rejects values above 19 |
| Long-poll wait | 20 s | Every receive |
| Tasks visibility timeout | 60 s | Extended by the heartbeat |
| Heartbeat interval | 20 s | Each beat extends visibility to 60 s, so one missed beat is tolerated |
| Max processing time per image | 5 min | Then the heartbeat stops and the worker exits |
| Dead-letter `maxReceiveCount` | 3 | Redrive policy on the tasks queue |
| Dead-letter queue retention | 14 days | Longer than the tasks queue's (4-day default) |
| Download limits | 10 s connect, 30 s read, 20 MB | |
| Large-text threshold | 200 KB | Above it, the text goes to S3 |
| Scaler tick | 20 s | Plus every job registration |
| Worker idle exit (X) | 2 min | About one boot time |
| Manager idle retirement (Y) | 5 min | Beyond the spec |
| Retirement grace period | 60 s | |
| Client watchdog interval | 60 s | |
| Client backoff when no manager can be launched | 15 s, then 30 s | Each retry is one `DescribeInstances` call |
| Client failures | 3 | `FAILED` replies or lost managers; then the client reports an error. Refusals are not counted |

## 14. Design questions

**Security.** No credentials in code, jars, user-data or the submission. Local credentials come from the default chain, EC2 nodes use the lab's instance profile, and the bucket is private (section 11).

**Scalability.** Nothing is central except S3 and SQS, which scale on their own. Clients spread across managers by `manager-clients-n`, each manager with its own queues and workers, so more clients means more managers rather than one overloaded manager. A manager uses bounded thread pools and about one bit of memory per URL, streams inputs and summaries through disk, and batches its SQS calls. Each client receives its result on its own queue. With a million or a billion clients, the instance limit caps throughput, not correctness: work waits durably in SQS and drains as capacity frees up, and without the limit, capacity grows by adding managers and workers. At that scale the weak spot would be discovery, since every client scanning every manager through `DescribeInstances` runs into EC2 API rate limits; a registry of managers would replace it.

**Persistence and failures.** Every message stays in SQS until its consumer has fully handled it. A node that dies mid-task releases its message through the visibility timeout, and a task that keeps killing workers lands in a dead-letter queue and is reported as failed. Duplicates are absorbed by dedupe, and the client's watchdog covers a manager that dies. Every case is listed in section 10.

**Threads.** Threads pay off where work is I/O-bound and many independent requests must progress at once, which is the manager: its pools serve many clients in parallel while waiting on network calls, and they are bounded so load can't exhaust memory. Threads hurt where work is CPU-bound on a small machine or the flow is sequential. A worker on one vCPU runs one OCR thread, since more would just compete for the core, and the local app runs a single thread. The only helper threads elsewhere are the worker's heartbeat timer and the manager's scheduled scaler.

**Multiple clients.** Each client has its own `jobId`, reply queue, and S3 folder, and the manager tracks every job independently, so concurrent clients never see each other's results. Admission control keeps each manager within its clients' limits. On EC2, two and three clients were run at once. Two clients with `manager-clients-n` = 1: the first was admitted, the second was refused `FULL` within a second and launched a second manager, and both outputs were complete and correct. Three clients sharing one manager, one of them sending `TERMINATE` while another's job was still running: every client got exactly its own results, the running job finished before the manager shut down, and the late client was refused and served by a new manager (section 10).

**Termination.** The manager stops accepting, rejects new tasks, finishes its active jobs, terminates its workers, deletes its queues, and exits (section 6). Workers also exit when idle, and every instance terminates when its process exits, so nothing is left running. After each run, the console is checked to confirm no instances remain.

**System limitations.** The instance guard (section 8) enforces the instance limit in a single place while letting the system use every available slot when the workload needs it. SQS limits shape the details: batches of 10, long polls of at most 20 seconds, and a message size limit, hence the large-text path through S3. Lab credentials expire each session, and small instances have 1 GiB of memory, which is why inputs and summaries are streamed through disk.

**Are all workers working hard?** All of a manager's workers pull from one queue, one message at a time, so the queue balances the load by itself: no worker idles while tasks remain, and none holds tasks it isn't processing. Uneven counts are expected and healthy, because a worker that started earlier or got easier images simply takes more.

**Is the manager doing more than it should?** No. It never downloads images, runs OCR, or writes HTML; it only admits, splits, scales, collects, and summarizes (section 2).

**Is anything waiting on something else?** Only where the spec requires it: a client waits for its own result, and a terminating manager waits for its active jobs. The manager never waits for a particular worker or job, workers never coordinate with each other, and a client never waits for a manager to boot, because SQS holds its task in the meantime.

## 15. Beyond the spec

- Admission control in the manager, with `REJECTED` replies and client resubmission.
- A client-side watchdog that resubmits if its manager dies.
- Idle retirement for managers and idle exit for workers.
- A dead-letter queue on each manager's tasks queue, with poison messages reported as failed lines in the output.
- An S3 path for OCR texts too large for a message.
- A single instance guard for every launch in the system.

## 16. Running

Requirements on the local machine: Java 21 and Maven. The AWS CLI is not needed.

1. Start the AWS Academy lab and put its credentials in `~/.aws/credentials`. They expire with each lab session.
2. Check the settings in `common/src/main/java/dsp/ocr/common/Config.java`: the region (`us-east-1`), the bucket name, the AMI ID and the instance type. In a different AWS account, build the AMI as described in section 12, set its ID here, and choose a bucket name of your own (bucket names are global).
3. Build all modules and run the unit tests. This produces `local-app/target/local-app.jar`, `manager/target/manager.jar` and `worker/target/worker.jar`:
   ```
   mvn package
   ```
4. Upload the manager and worker jars, which instances download at boot. This also creates the bucket if needed. Repeat after every code change:
   ```
   java -cp local-app/target/local-app.jar dsp.ocr.local.UploadJars
   ```
5. Run the local application, which finds or launches a manager by itself:
   ```
   java -jar local-app/target/local-app.jar inputFileName outputFileName manager-clients-n worker-files-n [terminate]
   ```
6. After the session, run the cleanup command:
   ```
   java -cp local-app/target/local-app.jar dsp.ocr.local.Cleanup [--force]
   ```

It deletes every `mgr-*` and `client-*` queue and the bucket's `jobs/` and `logs/` folders, and lists any instance that is still running, so the end-of-session check that nothing was left behind is a single step. It runs by hand, when no client or manager is active, so it never races with a manager that is still booting. To make sure of that, it refuses to delete anything while an instance of this system (tagged `Role=OcrManager` or `OcrWorker`) is still running, since deleting a running manager's queues would break its jobs; `--force` terminates those instances first. Instances without that tag are only listed, never touched. Leftovers between sessions cost nothing: an idle queue is free, and the input and summary files are tiny.

| Item | Value |
|---|---|
| AMI | `ami-0a40497d38c5e505c`: Ubuntu Server 26.04 LTS with Java 21, Tesseract 5.5 and the AWS CLI, built as described in section 12 |
| Instance type (manager / workers) | t2.micro / t2.micro |
| `manager-clients-n` used | 10 |
| `worker-files-n` used | 5, so the 24 URLs of the sample got ⌈24/5⌉ = 5 workers |
| Time to process the sample input | 73 s from a cold start (no instances running) to the finished HTML: about 28 s to launch and boot the manager, then 41 s to launch 5 workers, boot them and process the 24 URLs. With `terminate`, everything had shut down 20 s later. When the manager and its workers are already running, the same input takes about 7 s |
| Command | `java -jar local-app.jar docs/input-example.txt output/sample-output.html 10 5 terminate` |
| Output | `output/sample-output.html` (24 entries: 12 with recognized text; 12 errors, since many of the sample's image URLs no longer exist: HTTP 404 or an unknown host) |
| Concurrent clients tested | Up to 3 at once on EC2 (sections 10 and 14) |
