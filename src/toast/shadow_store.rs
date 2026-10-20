//! TOAST values read out of shadow PostgreSQL instead of a ClickHouse mirror.
//!
//! Shadow stores physical TOAST heaps and indexes while replaying source WAL,
//! so external values can use local index lookups without ClickHouse chunk
//! mirror. Select with `[toast] mode = "shadow"`; see
//! `plans/shadow_toast.md`. Greenfield bootstrap and live CDC both use this
//! store.
//!
//! Store is read-only. Shadow receives data through WAL replay, so write methods
//! reject decoded `ToastRow` values.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::ops::bridge::{Bridge, BridgeError, FetchedChunks, MAX_FETCH_VALUES};
use crate::toast::xid_ceiling::{XidCeiling, follows};
use crate::toast::{ChunkStore, ChunkStoreError, FetchedValue};

/// Round-trip payload target. Always allow one value even when it exceeds limit
const FETCH_REQUEST_BYTES: usize = 64 << 20;

/// Poll cadence while waiting for shadow replay to cover a read
const REPLAY_POLL: Duration = Duration::from_millis(20);
/// Maximum poll interval while worker socket cannot accept connections
const UNREACHABLE_POLL_MAX: Duration = Duration::from_secs(1);
/// Maximum time without replay progress or a worker connection
const REPLAY_WAIT_MAX: Duration = Duration::from_secs(900);

/// Bridge populated after bootstrap starts PostgreSQL
pub type LateBridge = Arc<tokio::sync::OnceCell<Arc<Bridge>>>;

/// Already-dialled bridge as a [`LateBridge`], so callers holding either
/// reach the same constructors
pub fn bound(bridge: Arc<Bridge>) -> LateBridge {
    let cell = LateBridge::default();
    cell.set(bridge).ok();
    cell
}

/// Bridge and xid samples for shadow reads
/// Detect reused value IDs using pump's samples, returning
/// [`FetchedValue::Generation`]. Default ceiling skips generation checks
#[derive(Clone, Default)]
pub struct ShadowRead {
    pub bridge: LateBridge,
    pub ceiling: Arc<XidCeiling>,
}

/// Create a read without generation checks
impl From<LateBridge> for ShadowRead {
    fn from(bridge: LateBridge) -> Self {
        Self {
            bridge,
            ceiling: Arc::default(),
        }
    }
}

impl From<&crate::ops::oracle::Oracle> for ShadowRead {
    fn from(oracle: &crate::ops::oracle::Oracle) -> Self {
        Self {
            bridge: bound(oracle.toast_bridge()),
            ceiling: oracle.xid_ceiling(),
        }
    }
}

pub struct ShadowToastStore {
    read: ShadowRead,
    replay_wait_max: Duration,
    /// Newest replay position the bridge reported, with the bridge's
    /// redial count then. A redial may mean a restarted shadow whose replay
    /// resumed lower, so the position counts only until the next one
    replayed: tokio::sync::watch::Sender<(u64, u64)>,
    /// Held by the one waiter polling replay
    polling: tokio::sync::Mutex<()>,
}

impl ShadowToastStore {
    pub fn new(bridge: Arc<Bridge>) -> Self {
        Self::late(bound(bridge))
    }

    /// Create store that waits for bridge to become available
    pub fn late(read: impl Into<ShadowRead>) -> Self {
        Self {
            read: read.into(),
            replay_wait_max: REPLAY_WAIT_MAX,
            replayed: tokio::sync::watch::Sender::new((0, 0)),
            polling: tokio::sync::Mutex::new(()),
        }
    }

    pub fn with_replay_wait_max(mut self, max: Duration) -> Self {
        self.replay_wait_max = max;
        self
    }

    /// Return bound bridge or wait until readiness deadline
    async fn bridge(&self) -> Result<&Arc<Bridge>, ChunkStoreError> {
        let deadline = Instant::now() + self.replay_wait_max;
        loop {
            if let Some(b) = self.read.bridge.get() {
                return Ok(b);
            }
            if Instant::now() >= deadline {
                return Err(ChunkStoreError::Shadow(format!(
                    "no PostgreSQL bound to read values from after {:?}",
                    self.replay_wait_max
                )));
            }
            tokio::time::sleep(REPLAY_POLL).await;
        }
    }

    /// Replay position known since the bridge last redialled
    fn known_replay(&self, bridge: &Bridge, (at, dials): (u64, u64)) -> u64 {
        if dials == bridge.stats.reconnects.load(Ordering::Relaxed) {
            at
        } else {
            0
        }
    }

    /// Wait until shadow has applied through `through`.
    ///
    /// `WalStream` dispatches record bytes before awaiting record sink. WAL
    /// needed to reach `through` is already on its way to shadow, so this wait
    /// does not require more pump progress.
    ///
    /// Poll here instead of blocking worker, which must remain available for
    /// catalog reads. Return replay floor for standby request. Return zero for
    /// primary, which has no replay position.
    ///
    /// Supervisor restarts postmaster after a GUC floor change stops it
    /// Worker socket cannot accept connections during restart. Treat that like
    /// stalled replay and restart timeout whenever replay position changes
    async fn await_replay(&self, bridge: &Bridge, through: u64) -> Result<u64, ChunkStoreError> {
        // Primary files are complete before service and have no replay position
        if through == 0 || !bridge.info().in_recovery {
            return Ok(0);
        }
        let mut seen = self.replayed.subscribe();
        let mut since = Instant::now();
        let mut last: Option<u64> = None;
        let mut unreachable = false;
        let mut poll = REPLAY_POLL;
        loop {
            let at = self.known_replay(bridge, *seen.borrow_and_update());
            if at >= through {
                return Ok(through);
            }
            // One waiter polls, the rest follow its answers
            let turn = self.polling.try_lock();
            let polled = match turn {
                Ok(_) => bridge.replay_lsn().await,
                Err(_) => Ok(at),
            };
            if let (Ok(at), Ok(_)) = (&polled, &turn) {
                let dials = bridge.stats.reconnects.load(Ordering::Relaxed);
                self.replayed.send_if_modified(|v| {
                    let newer = v.1 != dials || *at > v.0;
                    if newer {
                        *v = (*at, dials);
                    }
                    newer
                });
            }
            match polled {
                Ok(at) if at >= through => return Ok(through),
                Ok(at) => {
                    if unreachable || last != Some(at) {
                        since = Instant::now();
                    }
                    if unreachable {
                        tracing::info!(
                            target: "walshadow::toast",
                            replay_lsn = format_args!("{at:X}"),
                            "shadow is reachable again, waiting for replay",
                        );
                    }
                    unreachable = false;
                    last = Some(at);
                    poll = REPLAY_POLL;
                }
                Err(BridgeError::Io(e)) => {
                    if !unreachable {
                        since = Instant::now();
                        tracing::warn!(
                            target: "walshadow::toast",
                            error = %e,
                            "shadow is unreachable, waiting for it to restart",
                        );
                    }
                    unreachable = true;
                    poll = (poll * 2).min(UNREACHABLE_POLL_MAX);
                }
                Err(e) => {
                    return Err(ChunkStoreError::Shadow(format!("replay position: {e}")));
                }
            }
            if since.elapsed() >= self.replay_wait_max {
                return Err(ChunkStoreError::Shadow(if unreachable {
                    format!(
                        "shadow unreachable for {:?}, value needs replay past {through:X}",
                        self.replay_wait_max
                    )
                } else {
                    format!(
                        "shadow replay stuck at {:X}, value needs {through:X} after {:?}",
                        last.unwrap_or(0),
                        self.replay_wait_max
                    )
                }));
            }
            match turn {
                Ok(_turn) => tokio::time::sleep(poll).await,
                Err(_) => drop(tokio::time::timeout(poll, seen.changed()).await),
            }
        }
    }
}

#[async_trait]
impl ChunkStore for ShadowToastStore {
    /// Treat `max_lsn` as minimum replay position. Chunks precede referring
    /// record, so reaching this position makes value available
    async fn fetch_many(
        &self,
        _db_oid: u32,
        toast_relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
    ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let bridge = self.bridge().await?;
        // Batch bound is the newest referring record, so its ceiling covers
        // every value here
        let ceiling = self.read.ceiling.at(max_lsn);
        // Bridge refuses a floor above replay, so reading at a covered bound
        // skips the replay round trip. Shadow restart can rewind replay
        // below the remembered position, so a failed read waits and retries
        let known = *self.replayed.borrow();
        if max_lsn != 0 && self.known_replay(bridge, known) >= max_lsn {
            if let Ok(got) = read_values(bridge, toast_relid, values, max_lsn, ceiling).await {
                return Ok(got);
            }
            self.replayed.send_replace((0, 0));
        }
        // Wait once for complete batch
        let floor = self.await_replay(bridge, max_lsn).await?;
        read_values(bridge, toast_relid, values, floor, ceiling).await
    }
}

/// Split resolver batch to satisfy wire limits
async fn read_values(
    bridge: &Bridge,
    toast_relid: u32,
    values: &[(u32, usize)],
    floor: u64,
    ceiling: u32,
) -> Result<Vec<FetchedValue>, ChunkStoreError> {
    let mut out = Vec::with_capacity(values.len());
    for slice in request_slices(values) {
        let got = bridge
            .fetch_toast(toast_relid, slice, floor)
            .await
            .map_err(|e| ChunkStoreError::Shadow(e.to_string()))?;
        out.extend(got.into_iter().map(|c| judge(c, ceiling)));
    }
    Ok(out)
}

/// Detect reused IDs from chunks newer than referring record
/// PostgreSQL reuses an ID only after all original chunks are gone
/// Skip check if ceiling or normal xmin is unavailable
fn judge(c: FetchedChunks, ceiling: u32) -> FetchedValue {
    if ceiling != 0 && c.xmin != 0 && follows(c.xmin, ceiling) {
        FetchedValue::Generation
    } else {
        c.value
    }
}

/// Split at first wire limit, always include at least one value
fn request_slices(values: &[(u32, usize)]) -> impl Iterator<Item = &[(u32, usize)]> {
    let mut rest = values;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let mut n = 0;
        let mut bytes = 0usize;
        while n < rest.len() && n < MAX_FETCH_VALUES {
            bytes += rest[n].1;
            n += 1;
            if bytes >= FETCH_REQUEST_BYTES {
                break;
            }
        }
        let (head, tail) = rest.split_at(n);
        rest = tail;
        Some(head)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn a_run_younger_than_the_ceiling_reads_as_a_later_generation() {
        let torn = |xmin| FetchedChunks {
            value: FetchedValue::Mismatch { got: 3 },
            xmin,
        };
        let whole = |xmin| FetchedChunks {
            value: FetchedValue::Assembled(b"abc".to_vec()),
            xmin,
        };

        assert_eq!(judge(whole(200), 100), FetchedValue::Generation);
        assert_eq!(
            judge(torn(200), 100),
            FetchedValue::Generation,
            "a partly reclaimed replacement is still a replacement",
        );
        assert_eq!(
            judge(torn(50), 100),
            FetchedValue::Mismatch { got: 3 },
            "partially reclaimed original remains a size mismatch",
        );
        assert_eq!(
            judge(whole(100), 100),
            whole(100).value,
            "equal is not newer"
        );
        assert_eq!(judge(whole(200), 0), whole(200).value, "unsampled ceiling");
        assert_eq!(judge(whole(0), 100), whole(0).value, "frozen chunks");
        assert_eq!(
            judge(
                FetchedChunks {
                    value: FetchedValue::Missing,
                    xmin: 0,
                },
                100,
            ),
            FetchedValue::Missing,
        );
        assert_eq!(
            judge(whole(4), u32::MAX - 6),
            FetchedValue::Generation,
            "xid order wraps",
        );
    }

    /// Standby shadow behind a bridge worker. Refuses reads above replay, as
    /// `ws_handle_fetch_toast` does, logs the replay position each read was
    /// served at, and on restart drops its sockets with replay rewound
    #[derive(Default)]
    struct Standby {
        replay: AtomicU64,
        restarts: AtomicU64,
        polls: AtomicU64,
        served: std::sync::Mutex<Vec<u64>>,
    }

    async fn worker(listener: tokio::net::UnixListener, shadow: Arc<Standby>) {
        use crate::ops::bridge::{PROJECTION_VERSION, PROTO_VERSION};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let shadow = shadow.clone();
            let life = shadow.restarts.load(Ordering::SeqCst);
            tokio::spawn(async move {
                let mut hdr = [0u8; 4];
                while sock.read_exact(&mut hdr).await.is_ok() {
                    let mut req = vec![0u8; u32::from_be_bytes(hdr) as usize];
                    sock.read_exact(&mut req).await.unwrap();
                    if shadow.restarts.load(Ordering::SeqCst) != life {
                        return;
                    }
                    let at = shadow.replay.load(Ordering::SeqCst);
                    let mut body = vec![0u8];
                    match req[0] {
                        1 => {
                            for v in [PROTO_VERSION, PROJECTION_VERSION, 180006] {
                                body.extend(v.to_be_bytes());
                            }
                            body.push(1);
                            body.extend(5u32.to_be_bytes());
                        }
                        4 => {
                            shadow.polls.fetch_add(1, Ordering::SeqCst);
                            body.extend(at.to_be_bytes());
                        }
                        _ if u64::from_be_bytes(req[1..9].try_into().unwrap()) > at => {
                            body = vec![1, 0, 0, 0, 5];
                            body.extend(b"below");
                        }
                        _ => {
                            shadow.served.lock().unwrap().push(at);
                            body.extend(&req[13..17]);
                            body.extend([0, 0, 0, 0, 0, 0, 0, 0, 1, b'v']);
                        }
                    }
                    let mut frame = (body.len() as u32).to_be_bytes().to_vec();
                    frame.extend(body);
                    sock.write_all(&frame).await.unwrap();
                }
            });
        }
    }

    /// Over random replay progress, restarts that drop the sockets and
    /// resume replay lower, and read bounds below and above replay: every
    /// read is served at a replay position covering its bound, never fails,
    /// and polls replay only for bounds no answer since the last redial covers
    #[tokio::test(flavor = "current_thread")]
    async fn reads_never_land_below_replay_across_restarts() {
        let v = FetchedValue::Assembled(b"v".to_vec());
        let (mut reads, mut polls) = (0u64, 0u64);
        for seed in 1..=20u64 {
            let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let mut below = |n: u64| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng % n
            };
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("bridge.sock");
            let shadow = Arc::new(Standby::default());
            shadow.replay.store(1000, Ordering::SeqCst);
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            tokio::spawn(worker(listener, shadow.clone()));
            let store = ShadowToastStore::new(Arc::new(Bridge::connect(&path).await.unwrap()));
            for _ in 0..40 {
                match below(8) {
                    0 => {
                        shadow.restarts.fetch_add(1, Ordering::SeqCst);
                        let at = shadow.replay.load(Ordering::SeqCst);
                        shadow.replay.store(at - below(at), Ordering::SeqCst);
                    }
                    1 | 2 => drop(shadow.replay.fetch_add(below(500), Ordering::SeqCst)),
                    _ => {
                        let at = shadow.replay.load(Ordering::SeqCst);
                        // One read in five waits for replay to pass its bound
                        let bound = 1 + below(at) + u64::from(below(5) == 0) * 300;
                        let catch_up = async {
                            while shadow.replay.load(Ordering::SeqCst) < bound {
                                tokio::time::sleep(Duration::from_millis(2)).await;
                                shadow.replay.fetch_add(50, Ordering::SeqCst);
                            }
                        };
                        let (got, ()) = tokio::join!(store.fetch(0, 1, 7, bound, 1), catch_up);
                        assert_eq!(got.unwrap(), v, "seed {seed} bound {bound}");
                        let served = *shadow.served.lock().unwrap().last().unwrap();
                        assert!(served >= bound, "seed {seed}: read at {served} for {bound}");
                        reads += 1;
                    }
                }
            }
            polls += shadow.polls.load(Ordering::SeqCst);
        }
        assert!(polls * 2 < reads, "{polls} replay polls for {reads} reads");
    }

    #[test]
    fn slices_respect_both_caps_and_never_drop_a_value() {
        let many: Vec<(u32, usize)> = (0..MAX_FETCH_VALUES as u32 * 2 + 7)
            .map(|i| (i, 1))
            .collect();
        let slices: Vec<_> = request_slices(&many).collect();
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].len(), MAX_FETCH_VALUES);
        assert_eq!(slices[2].len(), 7);
        assert_eq!(
            slices.iter().map(|s| s.len()).sum::<usize>(),
            many.len(),
            "every value ends up in exactly one request"
        );

        // Send oversized value alone
        let huge = [(1u32, FETCH_REQUEST_BYTES * 4), (2, 8)];
        let slices: Vec<_> = request_slices(&huge).collect();
        assert_eq!(slices, vec![&huge[..1], &huge[1..]]);

        assert_eq!(request_slices(&[]).count(), 0);
    }
}
