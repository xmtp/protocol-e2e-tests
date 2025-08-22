use crate::app::identity_lock::get_identity_lock;
use tokio::time::{sleep, Duration};
use crate::app::{
    store::{Database, GroupStore, IdentityStore, RandomDatabase},
    types::*,
};
use crate::{app, args};
use color_eyre::eyre::{self, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use crate::metrics::{record_latency, record_throughput, record_member_count, push_metrics};

pub struct GenerateGroups {
    group_store: GroupStore<'static>,
    identity_store: IdentityStore<'static>,
    // metadata_store: MetadataStore<'static>,
    network: args::BackendOpts,
}

// ----------------------------
// CSV helpers
// ----------------------------
fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Print a single metric sample to stdout in CSV form.
/// Columns: metric_kind,metric_name,value,timestamp_ms,labels
/// `labels` is a semicolon-separated k=v list (may be empty).
fn csv_metric(metric_kind: &str, metric_name: &str, value: f64, labels: &[(&str, &str)]) {
    let ts = now_unix_ms();
    let labels_str = if labels.is_empty() {
        String::new()
    } else {
        labels
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(";")
    };
    println!("{},{},{:.6},{},{}", metric_kind, metric_name, value, ts, labels_str);
}

impl GenerateGroups {
    pub fn new(db: Arc<redb::Database>, network: args::BackendOpts) -> Self {
        Self {
            group_store: db.clone().into(),
            identity_store: db.clone().into(),
            // metadata_store: db.clone().into(),
            network,
        }
    }

    #[allow(unused)]
    pub fn load_groups(&self) -> Result<Option<impl Iterator<Item = Result<Group>> + use<'_>>> {
        Ok(self
            .group_store
            .load(&self.network)?
            .map(|i| i.map(|i| Ok(i.value()))))
    }

    /// Human-readable dump of locally persisted groups (REDB).
    /// This reads what the generator saved after successful node ops.
    pub fn dump_groups_human(&self) -> eyre::Result<()> {
        let mut found = false;
        if let Some(iter) = self.load_groups()? {
            for g in iter {
                let g = g?;
                if !found {
                    println!("=== Local GroupStore dump (network: {}) ===", url::Url::from(self.network.clone()));
                    found = true;
                }
                println!(
                    "group id={} members={} created_by={}",
                    hex::encode(g.id),
                    g.members.len(),
                    hex::encode(g.created_by)
                );
                for m in &g.members {
                    println!("  - member {}", hex::encode(m));
                }
            }
        }
        if !found {
            println!("(no groups in local store for {})", url::Url::from(self.network.clone()));
        }
        Ok(())
    }

    /// Create `n` groups. We **always** add at least one member so the test touches the node.
    pub async fn create_groups(
        &self,
        n: usize,
        invitees: usize,
        concurrency: usize,
    ) -> Result<Vec<Group>> {
        let mut groups: Vec<Group> = Vec::with_capacity(n);

        let style = ProgressStyle::with_template(
            "{bar} {pos}/{len} elapsed {elapsed} remaining {eta_precise}",
        );
        let bar = ProgressBar::new(n as u64).with_style(style.unwrap());

        let mut set: tokio::task::JoinSet<Result<_, eyre::Error>> = tokio::task::JoinSet::new();
        let mut handles = vec![];

        let network = &self.network;
        let mut rng = rand::thread_rng();

        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        // ENV toggles
        let skip_sleep = std::env::var("XDBG_SKIP_SLEEP")
            .map(|v| v.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false);
        let verify_group = std::env::var("XDBG_VERIFY_GROUP")
            .map(|v| v.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false);
        let dump_groups = std::env::var("XDBG_DUMP_GROUPS")
            .map(|v| v.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false);

        // Force at least one invitee to ensure node write
        let invitee_count = invitees.max(1);

        for _ in 0..n {
            let identity = self.identity_store.random(network, &mut rng)?.unwrap();
            let invitees = self.identity_store.random_n(network, &mut rng, invitee_count)?;
            let bar_pointer = bar.clone();
            let network = network.clone();
            let semaphore = semaphore.clone();

            handles.push(set.spawn(async move {
                let _permit = semaphore.acquire().await?;
                let identity_lock = get_identity_lock(&identity.inbox_id)?;
                let _lock_guard = identity_lock.lock().await;

                debug!(address = identity.address(), "group owner");
                let client = app::client_from_identity(&identity, &network).await?;

                // Build member list to add (hex inbox ids)
                let ids = invitees
                    .iter()
                    .map(|i| hex::encode(i.inbox_id))
                    .collect::<Vec<_>>();

                // -------- group_create_client_only (sync, local) --------
                let create_start = Instant::now();
                let group = client.create_group(Default::default(), Default::default())?;
                let create_secs = create_start.elapsed().as_secs_f64();

                record_latency("group_create_client_only", create_secs);
                record_throughput("group_create_client_only");
                csv_metric(
                    "latency_seconds",
                    "group_create_client_only",
                    create_secs,
                    &[("phase", "create_group")],
                );
                csv_metric(
                    "throughput_events",
                    "group_create_client_only",
                    1.0,
                    &[("phase", "create_group")],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // -------- group_add_members (awaited, node RPC) --------
                let add_start = Instant::now();
                group.add_members_by_inbox_id(ids.as_slice()).await?;
                let add_secs = add_start.elapsed().as_secs_f64();

                record_latency("group_add_members", add_secs);
                record_member_count("group_add_members", ids.len() as f64);
                record_throughput("group_add_members");
                csv_metric(
                    "latency_seconds",
                    "group_add_members",
                    add_secs,
                    &[
                        ("phase", "add_members"),
                        ("member_count", &ids.len().to_string()),
                    ],
                );
                csv_metric(
                    "throughput_events",
                    "group_add_members",
                    1.0,
                    &[
                        ("phase", "add_members"),
                        ("member_count", &ids.len().to_string()),
                    ],
                );
                // Breadcrumb that ACK happened
                csv_metric(
                    "event",
                    "group_add_members_ack",
                    1.0,
                    &[("member", &ids[0])],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // -------- optional reader-side verification --------
                if verify_group {
                    let invitee_identity = &invitees[0];
                    let reader = app::client_from_identity(invitee_identity, &network).await?;

                    // FIX: don't move group.group_id; convert a CLONE and borrow that.
                    let gid_for_reader = group.group_id.clone().into();
                    let g2 = reader.group(&gid_for_reader)?;
                    g2.sync_with_conn().await?; // fails if membership not live
                    csv_metric(
                        "event",
                        "group_member_visible",
                        1.0,
                        &[("member", &ids[0])],
                    );
                }

                bar_pointer.inc(1);

                // Build final member list for the return struct
                let mut members = invitees
                    .into_iter()
                    .map(|i| i.inbox_id)
                    .collect::<Vec<InboxId>>();
                members.push(identity.inbox_id);

                if !skip_sleep {
                    sleep(Duration::from_secs(60)).await;
                }

                Ok(Group {
                    id: group
                        .group_id
                        .try_into()
                        .expect("Group id expected to be 32 bytes"),
                    member_size: members.len() as u32,
                    members,
                    created_by: identity.inbox_id,
                })
            }));

            // throttle fanout: avoid too many concurrent tasks
            if set.len() >= 64 {
                if let Some(group) = set.join_next().await {
                    match group {
                        Ok(group) => groups.push(group?),
                        Err(e) => error!("{}", e.to_string()),
                    }
                }
            }
        }

        while let Some(group) = set.join_next().await {
            match group {
                Ok(group) => groups.push(group?),
                Err(e) => error!("{}", e.to_string()),
            }
        }

        // Persist locally only after successful ops
        self.group_store.set_all(groups.as_slice(), &self.network)?;

        // Optional: dump local store for confirmation
        if dump_groups {
            let _ = self.dump_groups_human();
        }

        Ok(groups)
    }
}
