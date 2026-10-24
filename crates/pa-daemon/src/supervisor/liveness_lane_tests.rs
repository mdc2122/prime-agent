//! Regression tests for the reserved supervisor lanes: a saturated but live
//! worker still answers a client liveness probe (`get_state`) and can still be
//! killed, while a worker that stopped draining its queue reads as a timeout
//! (unreachable), never as busy.
//!
//! Before the fix every client command, the liveness probe included, needed one
//! of the worker's `WORKER_INFLIGHT_CAPACITY` in-flight permits, and the kill
//! and shutdown routes waited for one: a saturated child could neither be told
//! apart from a wedged one nor be stopped.

use std::sync::Arc;
use std::time::Duration;

use pa_types::daemon::DaemonWorkerDescriptor;
use pa_types::platform::transport::TransportStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::{Supervisor, SupervisorOptions};
use crate::backpressure::WORKER_INFLIGHT_CAPACITY;
use crate::protocol::response_success;
use crate::registry::{ResidentWorker, WorkerReply, WorkerRequest};

struct Harness {
    supervisor: Arc<Supervisor>,
    resident: Arc<ResidentWorker>,
    cmd_tx: tokio::sync::mpsc::Sender<WorkerRequest>,
    cmd_rx: Option<tokio::sync::mpsc::Receiver<WorkerRequest>>,
    _dir: tempfile::TempDir,
}

/// One supervisor with one resident, connected child worker `w-child` whose
/// descriptor names `pid` (0: no process).
async fn harness(pid: u32) -> Harness {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let supervisor = Arc::new(
        Supervisor::new(SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .expect("supervisor"),
    );
    let start_id = (pid != 0)
        .then(|| crate::lease::get_process_start_id(pid))
        .flatten();
    let descriptor: DaemonWorkerDescriptor = serde_json::from_value(serde_json::json!({
        "version": 2,
        "workerId": "w-child",
        "pid": pid,
        "processStartId": start_id,
        "socketPath": dir.path().join("w.sock"),
        "recoveryJournalPath": dir.path().join("w.recovery.jsonl"),
        "supervisorSocketPath": dir.path().join("daemon.sock"),
        "authenticationToken": "test",
        "rootActiveSessionId": "w-child",
        "createdAt": "2026-09-26T00:00:00Z",
        "updatedAt": "2026-09-26T00:00:00Z",
        "lifecycle": "ready",
        "createCommand": {},
        "consecutiveFailures": 0,
    }))
    .expect("descriptor");
    let resident = ResidentWorker::new(
        "w-child".to_string(),
        descriptor,
        dir.path().join("w-child.json"),
    );
    supervisor.registry.insert(Arc::clone(&resident)).await;
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<WorkerRequest>(WORKER_INFLIGHT_CAPACITY);
    let epoch = resident.note_connection_live();
    resident
        .install_command_channel(epoch, cmd_tx.clone())
        .await;
    resident.note_session_ready();
    Harness {
        supervisor,
        resident,
        cmd_tx,
        cmd_rx: Some(cmd_rx),
        _dir: dir,
    }
}

/// A live, busy worker: answers `get_state` (streaming), `kill` and
/// `shutdown` at once and never answers anything else.
fn answer_control_frames(harness: &mut Harness) -> tokio::task::JoinHandle<()> {
    let mut cmd_rx = harness.cmd_rx.take().expect("command receiver");
    let resident = Arc::clone(&harness.resident);
    tokio::spawn(async move {
        while let Some(request) = cmd_rx.recv().await {
            let payload = match request.command_type.as_str() {
                "get_state" => Some(serde_json::json!({"isStreaming": true})),
                "kill" | "shutdown" => None,
                _ => continue,
            };
            if let Some(reply) = resident.pending.lock().await.remove(&request.request_id) {
                let _ = reply.send(WorkerReply::Typed(response_success(
                    None,
                    &request.command_type,
                    payload,
                )));
            }
        }
    })
}

/// Hold every in-flight permit of the worker (what 128 parked turn-long
/// requests do).
fn saturate(resident: &Arc<ResidentWorker>) -> tokio::sync::OwnedSemaphorePermit {
    Arc::clone(&resident.inflight)
        .try_acquire_many_owned(u32::try_from(WORKER_INFLIGHT_CAPACITY).expect("fits"))
        .expect("all in-flight permits")
}

/// One client exchange over a real socket: send one command, wait up to
/// `hold` for its answer, then close the connection.
async fn one_request(
    supervisor: &Arc<Supervisor>,
    command: serde_json::Value,
    hold: Duration,
) -> Option<serde_json::Value> {
    let (server_side, client_side) = tokio::net::UnixStream::pair().expect("socket pair");
    let stream: Box<dyn TransportStream> = Box::new(server_side);
    let sup = Arc::clone(supervisor);
    let connection = tokio::spawn(async move { sup.handle_client(stream).await });
    let (client_read, mut client_write) = client_side.into_split();
    let mut reader = BufReader::new(client_read);
    let mut hello = String::new();
    reader.read_line(&mut hello).await.expect("hello");
    let envelope = serde_json::json!({
        "type": "command", "id": "link-0",
        "protocol": crate::protocol::current_protocol_info(),
        "command": command,
    });
    client_write
        .write_all((serde_json::to_string(&envelope).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    let answered = tokio::time::timeout(hold, async {
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                return None;
            }
            if line.contains("\"link-0\"") {
                return serde_json::from_str::<serde_json::Value>(line.trim()).ok();
            }
        }
    })
    .await
    .ok()
    .flatten();
    drop(client_write);
    drop(reader);
    let _ = tokio::time::timeout(Duration::from_secs(5), connection).await;
    answered
}

/// A saturated but live worker still answers its liveness probe: `get_state`
/// rides the reserved probe lane once the in-flight bound is full. Before the
/// fix it answered the `worker_overloaded` refusal, which a watcher could not
/// tell apart from a wedged worker.
#[tokio::test]
async fn a_saturated_live_worker_still_answers_a_client_state_probe() {
    let mut harness = harness(0).await;
    let _responder = answer_control_frames(&mut harness);
    let _held = saturate(&harness.resident);
    let reply = one_request(
        &harness.supervisor,
        serde_json::json!({"type": "get_state", "activeSessionId": "w-child"}),
        Duration::from_secs(10),
    )
    .await
    .expect("the probe is answered");
    assert_eq!(
        reply["success"], true,
        "a saturated live worker answers its state probe: {reply}"
    );
    assert_eq!(reply["data"]["isStreaming"], true);
    assert_eq!(
        harness.resident.inflight.available_permits(),
        0,
        "the probe never needed an in-flight permit"
    );
}

/// A worker whose queue never drains (its writer pump is wedged on a socket the
/// worker stopped reading) is not "busy": the liveness probe waits out the full
/// queue inside its budget and fails as a timeout, which the RLM watcher counts
/// as unreachable. Before the fix the probe answered `worker_overloaded` at
/// once, and with overload read as busy a frozen worker was watched forever.
#[tokio::test(start_paused = true)]
async fn a_probe_to_a_worker_whose_queue_never_drains_times_out_instead_of_reading_busy() {
    let ticker = tokio::spawn(async {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
        }
    });
    let harness = harness(0).await;
    // Nothing reads the worker's queue; fill it while every in-flight
    // permit stays free.
    while harness
        .cmd_tx
        .try_send(WorkerRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            command_type: "get_state".to_string(),
            payload: serde_json::json!({}),
        })
        .is_ok()
    {}
    let reply = one_request(
        &harness.supervisor,
        serde_json::json!({"type": "get_state", "activeSessionId": "w-child"}),
        Duration::from_secs(120),
    )
    .await
    .expect("the probe fails inside its route budget");
    ticker.abort();
    assert_eq!(reply["success"], false);
    assert_ne!(
        reply["errorInfo"]["code"], "worker_overloaded",
        "a worker that never drains its queue must not read as busy: {reply}"
    );
    assert!(
        harness.resident.pending.lock().await.is_empty(),
        "the timed-out probe left no reply slot behind"
    );
}

/// The stop's `shutdown` route to a saturated worker is not queued behind the
/// client in-flight permits: it rides the reserved control lane and is
/// answered at once. Before the fix it waited for an in-flight permit until
/// its budget elapsed ("Session worker timed out").
#[tokio::test]
async fn a_shutdown_route_to_a_saturated_worker_is_not_queued_behind_client_permits() {
    let mut harness = harness(0).await;
    let _responder = answer_control_frames(&mut harness);
    let _held = saturate(&harness.resident);
    let started = std::time::Instant::now();
    let reply = harness
        .supervisor
        .route_command_typed(
            &harness.resident,
            "shutdown",
            serde_json::json!({}),
            5_000,
            crate::backpressure::RouteAdmission::SupervisorInternal,
        )
        .await
        .expect("the shutdown route is answered, not timed out");
    assert!(reply.success);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the shutdown never waited on an in-flight permit"
    );
}

/// A parent's delete of a saturated child: the client `kill` is admitted on
/// the control lane and answered at once, and the stop that follows actually
/// terminates the worker process (the stop escalation signals it when it
/// does not exit on its own; on macOS there is no pidfd, so this is the
/// identity-checked kill(2) fallback). Before the fix the kill waited 30 s for
/// an in-flight permit and failed, the shutdown route waited another 30 s and
/// failed, and on macOS no signal was ever sent: the "deleted" worker kept
/// running.
#[tokio::test]
async fn a_saturated_child_can_be_killed_and_its_process_is_gone() {
    // A real process stands in for the child worker (it never exits on its
    // own: only a signal stops it).
    let mut stand_in = std::process::Command::new("sleep")
        .arg("600")
        .spawn()
        .expect("spawn stand-in worker");
    let pid = stand_in.id();
    let mut harness = harness(pid).await;
    let _responder = answer_control_frames(&mut harness);
    let _held = saturate(&harness.resident);
    let started = std::time::Instant::now();
    let reply = one_request(
        &harness.supervisor,
        serde_json::json!({"type": "kill", "activeSessionId": "w-child"}),
        Duration::from_secs(90),
    )
    .await;
    let answered_after = started.elapsed();
    // The stop runs on every route outcome; give its escalation (TERM grace
    // 2 s) time to land.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut exited = None;
    while std::time::Instant::now() < deadline {
        if let Some(status) = stand_in.try_wait().expect("try_wait") {
            exited = Some(status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if exited.is_none() {
        let _ = stand_in.kill();
    }
    let _ = stand_in.wait();
    let reply = reply.expect("the kill is answered");
    assert_eq!(reply["success"], true, "the kill succeeds: {reply}");
    assert!(
        answered_after < Duration::from_secs(10),
        "the kill was admitted at once, not after an in-flight wait ({answered_after:?})"
    );
    assert!(
        exited.is_some(),
        "the killed child's worker process must be gone"
    );
}
