//! Group Generation
use crate::app::identity_lock::get_identity_lock;
use tokio::time::{sleep, Duration};
use crate::app::{
    store::{Database, GroupStore, IdentityStore, RandomDatabase},
    types::*,
};
use crate::{app, args};
use color_eyre::eyre::{self, ContextCompat, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use crate::metrics::{record_latency, record_throughput, record_member_count, push_metrics};
use xmtp_db::XmtpDb;
use xmtp_mls::{client::Client as XmtpClient, XmtpApi};

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

    /// Create n groups and always add at least one member so the test touches the node.
    /// You can choose the member count via CLI invitees or env XDBG_FORCE_INVITEES=<N>.
    pub async fn create_groups(
        &self,
        n: usize,
        invitees: usize,
        concurrency: usize,
    ) -> Result<Vec<Group>> {
        // TODO: Check if identities still exist
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
        let dump_groups = std::env::var("XDBG_DUMP_GROUPS")
            .map(|v| v.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false);

        for _ in 0..n {
            let identity = self
                .identity_store
                .random(network, &mut rng)?
                .with_context(
                    || "no local identities found in database, have identities been generated?",
                )?;
            let invitees = self.identity_store.random_n(network, &mut rng, invitees)?;
            let bar_pointer = bar.clone();
            let network = network.clone();
            let semaphore = semaphore.clone();
            handles.push(set.spawn(async move {
                let _permit = semaphore.acquire().await?;
                let identity_lock = get_identity_lock(&identity.inbox_id)?;
                let _lock_guard = identity_lock.lock().await;

                debug!(address = identity.address(), "group owner");
                let client = app::client_from_identity(&identity, &network).await?;
                let ids = invitees
                    .iter()
                    .map(|i| hex::encode(i.inbox_id))
                    .collect::<Vec<_>>();

                let flow_start = Instant::now(); // total create-with-members KPI starts here
                let create_start = Instant::now();
                let group = client.create_group(Default::default(), Default::default())?;
                let create_secs = create_start.elapsed().as_secs_f64();

                // Print the created group id (human) and emit a CSV event
                let gid_hex = hex::encode(&group.group_id);
                let creator_hex = hex::encode(identity.inbox_id);
                println!("group_created id={} created_by={}", gid_hex, creator_hex);
                csv_metric(
                    "event",
                    "group_created",
                    1.0,
                    &[("group_id", &gid_hex), ("created_by", &creator_hex)],
                );

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
                // Derived per-member metric for easy comparison across batch sizes
                let per_member = add_secs / (ids.len() as f64);
                record_latency("group_add_members_per_member", per_member);
                csv_metric(
                    "latency_seconds",
                    "group_add_members_per_member",
                    per_member,
                    &[
                        ("phase", "add_members"),
                        ("member_count", &ids.len().to_string()),
                    ],
                );

                // -------- total create -> add ACK KPI --------
                let total_secs = flow_start.elapsed().as_secs_f64();
                record_latency("group_create_with_members", total_secs);
                record_throughput("group_create_with_members");
                csv_metric(
                    "latency_seconds",
                    "group_create_with_members",
                    total_secs,
                    &[
                        ("phase", "create_with_members"),
                        ("member_count", &ids.len().to_string()),
                    ],
                );
                csv_metric(
                    "throughput_events",
                    "group_create_with_members",
                    1.0,
                    &[
                        ("phase", "create_with_members"),
                        ("member_count", &ids.len().to_string()),
                    ],
                );

                // Breadcrumb that ACK happened
                csv_metric(
                    "event",
                    "group_add_members_ack",
                    1.0,
                    &[("member_0", &ids[0]), ("group_id", &gid_hex)],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // -------- read_group_sync_latency (node read) --------
                let read_sync_start = Instant::now();
                let _ = group.sync_with_conn().await;
                let read_sync_secs = read_sync_start.elapsed().as_secs_f64();
                record_latency("read_group_sync_latency", read_sync_secs);
                record_throughput("read_group_sync_latency");
                csv_metric(
                    "latency_seconds",
                    "read_group_sync_latency",
                    read_sync_secs,
                    &[("phase", "post_add_members_sync")],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // -------- optional reader-side verification with polling --------
                let invitee_identity = &invitees_vec[0];
                let reader = app::client_from_identity(invitee_identity, &network).await?;
                let gid_for_reader = group.group_id.clone().into();

                let verify_timeout = Duration::from_secs(15);
                let poll_every = Duration::from_millis(10);
                let deadline = tokio::time::Instant::now() + verify_timeout;

                let vis_loop_start = Instant::now();
                let mut visible = false;
                while tokio::time::Instant::now() < deadline {
                    let _ = reader.sync_welcomes().await;
                    match reader.group(&gid_for_reader) {
                        Ok(g2) => {
                            if g2.sync_with_conn().await.is_ok() {
                                visible = true;
                                break;
                            }
                        }
                        Err(_) => {
                        }
                    }
                    if !skip_sleep {
                        sleep(poll_every).await;
                    }
                }
                let vis_loop_secs = vis_loop_start.elapsed().as_secs_f64();
                record_latency("read_member_visibility", vis_loop_secs);
                record_throughput("read_member_visibility");
                csv_metric(
                    "latency_seconds",
                    "read_member_visibility",
                    vis_loop_secs,
                    &[
                        ("phase", "post_add_members_visibility"),
                        ("success", if visible { "true" } else { "false" }),
                    ],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                csv_metric(
                    "event",
                    "group_member_visible",
                    if visible { 1.0 } else { 0.0 },
                    &[("member_0", &ids[0]), ("group_id", &gid_hex)],
                );

                bar_pointer.inc(1);
                let mut members = invitees
                    .into_iter()
                    .map(|i| i.inbox_id)
                    .collect::<Vec<InboxId>>();
                members.push(identity.inbox_id);

                if let Some(secs) = std::env::var("XDBG_COOLDOWN_SLEEP").ok().and_then(|s| s.parse::<u64>().ok()) {
                    std::thread::sleep(std::time::Duration::from_secs(secs));
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

            // going above 128 we hit "unable to open database errors"
            // This may be related to open file limits
            if set.len() >= 64
                && let Some(group) = set.join_next().await
            {
                match group {
                    Ok(group) => {
                        groups.push(group?);
                    }
                    Err(e) => {
                        error!("{}", e.to_string());
                    }
                }
            }
        }

        while let Some(group) = set.join_next().await {
            match group {
                Ok(group) => {
                    groups.push(group?);
                }
                Err(e) => {
                    error!("{}", e.to_string());
                }
            }
        }
        self.group_store.set_all(groups.as_slice(), &self.network)?;
        Ok(groups)
    }
}
