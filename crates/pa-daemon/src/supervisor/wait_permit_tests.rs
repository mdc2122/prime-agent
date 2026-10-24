//! Regression tests for the abandoned-`wait_for_idle` in-flight permit leak.
//!
//! Before the fix, a client `wait_for_idle` routed on the 24 h turn budget
//! held one of the target worker's `WORKER_INFLIGHT_CAPACITY` permits until
//! the session next went quiescent, even after the client that sent it had
//! given up and closed its socket (the per-request `SupervisorLink`). The RLM
//! settle watcher re-sends such a wait every 60 s, so a child busy for ~128
//! slices saturated its worker; every later client request answered
//! `worker_overloaded`, which the watcher counted as unreachable polls until
//! it declared the live child "Child worker unreachable".

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pa_types::daemon::DaemonWorkerDescriptor;
use pa_types::platform::transport::TransportStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::routing::{
    client_route_timeout, BUDGETED_WAIT_ROUTE_GRACE_MS, WORKER_REQUEST_TIMEOUT_MS,
};
use super::{Supervisor, SupervisorOptions};
use crate::backpressure::WORKER_INFLIGHT_CAPACITY;
use crate::protocol::response_success;
use crate::registry::{ResidentWorker, WorkerReply, WorkerRequest};
use pa_types::daemon::DaemonCommand;

fn resident(worker_id: &str) -> Arc<ResidentWorker> {
    let descriptor: DaemonWorkerDescriptor = serde_json::from_value(serde_json::json!({
        "version": 2,
        "workerId": worker_id,
        "pid": 0,
        "socketPath": "/tmp/none.sock",
        "recoveryJournalPath": "/tmp/none.jsonl",
        "supervisorSocketPath": "/tmp/none.sock",
        "authenticationToken": "test",
        "rootActiveSessionId": "none",
        "createdAt": "2026-09-26T00:00:00Z",
        "updatedAt": "2026-09-26T00:00:00Z",
        "lifecycle": "ready",
        "createCommand": {},
        "consecutiveFailures": 0,
    }))
    .expect("descriptor");
    ResidentWorker::new(
        worker_id.to_string(),
        descriptor,
        std::path::PathBuf::from("/tmp/none.descriptor.json"),
    )
}

struct BusyChild {
    supervisor: Arc<Supervisor>,
    resident: Arc<ResidentWorker>,
    parked_waits: Arc<AtomicUsize>,
    _dir: tempfile::TempDir,
}

/// A supervisor with one resident child worker that is alive and busy: it
/// answers `get_state` at once (`isStreaming: true`) and parks every
/// `wait_for_idle` forever, keeping the reply slot alive (what a real worker
/// does while a long turn or tool call runs).
async fn busy_child() -> BusyChild {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let supervisor = Arc::new(
        Supervisor::new(SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .expect("supervisor"),
    );
    let resident = resident("w-child");
    supervisor.registry.insert(Arc::clone(&resident)).await;
    let (cmd_tx, mut cmd_rx) =
        tokio::sync::mpsc::channel::<WorkerRequest>(WORKER_INFLIGHT_CAPACITY);
    let epoch = resident.note_connection_live();
    resident.install_command_channel(epoch, cmd_tx).await;
    resident.note_session_ready();
    let parked_waits = Arc::new(AtomicUsize::new(0));
    {
        let resident = Arc::clone(&resident);
        let parked_waits = Arc::clone(&parked_waits);
        tokio::spawn(async move {
            let mut parked = Vec::new();
            while let Some(request) = cmd_rx.recv().await {
                if request.command_type == "get_state" {
                    if let Some(reply) = resident.pending.lock().await.remove(&request.request_id) {
                        let _ = reply.send(WorkerReply::Typed(response_success(
                            None,
                            "get_state",
                            Some(serde_json::json!({"isStreaming": true})),
                        )));
                    }
                } else {
                    parked_waits.fetch_add(1, Ordering::SeqCst);
                    parked.push(request.request_id);
                }
            }
        });
    }
    BusyChild {
        supervisor,
        resident,
        parked_waits,
        _dir: dir,
    }
}

/// One `SupervisorLink`-shaped exchange: connect, send one command, wait up to
/// `hold` for its answer, then close the socket (the link's `LinkTimeout`).
async fn one_request(
    supervisor: &Arc<Supervisor>,
    command: serde_json::Value,
    hold: Duration,
) -> Option<String> {
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
                return Some(line.clone());
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

/// Wait (bounded) until every in-flight permit of the child is free again.
async fn permits_settle(resident: &Arc<ResidentWorker>, within: Duration) -> usize {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let available = resident.inflight.available_permits();
        if available == WORKER_INFLIGHT_CAPACITY || tokio::time::Instant::now() >= deadline {
            return available;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Layer A: a pure wait whose client disconnects is dropped with the
/// connection, releasing its permit and its pending reply slot. Before the fix
/// each abandoned wait kept its permit; after `WORKER_INFLIGHT_CAPACITY` of
/// them the busy-but-alive child answered `worker_overloaded` to everything.
#[tokio::test]
async fn abandoned_unbudgeted_waits_release_their_permits_on_disconnect() {
    let child = busy_child().await;
    let wait = serde_json::json!({
        "type": "wait_for_idle", "activeSessionId": "w-child",
        "waitForRlmQuiescence": true
    });
    for slice in 0..WORKER_INFLIGHT_CAPACITY + 8 {
        let answered =
            one_request(&child.supervisor, wait.clone(), Duration::from_millis(20)).await;
        assert!(
            answered.is_none(),
            "a busy child never answers a wait slice"
        );
        let available = permits_settle(&child.resident, Duration::from_secs(2)).await;
        assert_eq!(
            available, WORKER_INFLIGHT_CAPACITY,
            "abandoned slice {slice} kept its in-flight permit after its client left"
        );
    }
    assert!(
        child.parked_waits.load(Ordering::SeqCst) >= WORKER_INFLIGHT_CAPACITY,
        "the waits really reached the worker"
    );
    assert!(
        child.resident.pending.lock().await.is_empty(),
        "dropped waits left reply slots in the pending map"
    );
    let state = serde_json::json!({"type": "get_state", "activeSessionId": "w-child"});
    let reply = one_request(&child.supervisor, state, Duration::from_secs(5))
        .await
        .expect("get_state answers");
    assert!(
        reply.contains("\"success\":true") && !reply.contains("overloaded"),
        "the busy child must stay reachable: {reply}"
    );
}

/// Layer B: a `wait_for_idle` naming its own `timeoutMs` is bounded by it on
/// the supervisor route even while its client stays connected (a long-lived
/// connection that never disconnects cannot strand the permit either). Before
/// the fix it rode the 24 h route and never answered.
#[tokio::test]
async fn a_budgeted_wait_route_ends_at_its_budget_and_frees_its_permit() {
    let child = busy_child().await;
    let wait = serde_json::json!({
        "type": "wait_for_idle", "activeSessionId": "w-child",
        "waitForRlmQuiescence": true, "timeoutMs": 100
    });
    let started = tokio::time::Instant::now();
    let reply = one_request(&child.supervisor, wait, Duration::from_secs(10))
        .await
        .expect("a budgeted wait answers within its budget plus the route grace");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(100 + BUDGETED_WAIT_ROUTE_GRACE_MS + 2_000),
        "the budgeted route outlived its budget: {elapsed:?}"
    );
    assert!(reply.contains("\"success\":false"), "{reply}");
    assert_eq!(
        permits_settle(&child.resident, Duration::from_secs(2)).await,
        WORKER_INFLIGHT_CAPACITY
    );
    assert!(child.resident.pending.lock().await.is_empty());
}

/// The route budget table: an explicit wait budget bounds the route (plus the
/// grace that lets the worker's own refusal answer first), capped at 24 h; a
/// wait without one keeps the turn budget.
#[test]
fn client_route_timeout_honors_a_wait_budget() {
    let wait = |timeout_ms| DaemonCommand::WaitForIdle {
        id: None,
        active_session_id: "s".to_string(),
        wait_for_rlm_quiescence: Some(true),
        timeout_ms,
        rest: serde_json::Map::default(),
    };
    assert_eq!(
        client_route_timeout(&wait(Some(60_000))),
        60_000 + BUDGETED_WAIT_ROUTE_GRACE_MS
    );
    assert_eq!(client_route_timeout(&wait(None)), WORKER_REQUEST_TIMEOUT_MS);
    assert_eq!(
        client_route_timeout(&wait(Some(u64::MAX))),
        WORKER_REQUEST_TIMEOUT_MS
    );
    let parsed: DaemonCommand = serde_json::from_value(serde_json::json!({
        "type": "wait_for_idle", "activeSessionId": "s", "timeoutMs": 5
    }))
    .expect("timeoutMs parses");
    assert!(matches!(
        parsed,
        DaemonCommand::WaitForIdle {
            timeout_ms: Some(5),
            ..
        }
    ));
}
