use crate::app::identity_lock::get_identity_lock;
use crate::{
    app::{
        self,
        store::{Database, GroupStore, IdentityStore, MetadataStore, RandomDatabase},
    },
    args,
};
use color_eyre::eyre::{self, Result, eyre};
use indicatif::{ProgressBar, ProgressStyle};
use rand::{Rng, SeedableRng, rngs::SmallRng, seq::SliceRandom};
use std::sync::Arc;
use xmtp_mls::groups::summary::SyncSummary;

// added for metrics + timing
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use crate::metrics::{record_latency, record_throughput, push_metrics};

mod content_type;

#[derive(thiserror::Error, Debug)]
enum MessageSendError {
    #[error("No group")]
    NoGroup,
    #[error(transparent)]
    Eyre(#[from] eyre::Error),
    #[error(transparent)]
    Client(#[from] xmtp_mls::client::ClientError),
    #[error(transparent)]
    Group(#[from] xmtp_mls::groups::GroupError),
    #[error(transparent)]
    Storage(#[from] xmtp_db::StorageError),
    #[error(transparent)]
    Sync(#[from] SyncSummary),
}

pub struct GenerateMessages {
    db: Arc<redb::Database>,
    network: args::BackendOpts,
    opts: args::MessageGenerateOpts,
}

impl GenerateMessages {
    pub fn new(
        db: Arc<redb::Database>,
        network: args::BackendOpts,
        opts: args::MessageGenerateOpts,
    ) -> Self {
        Self { db, network, opts }
    }

    pub async fn run(self, n: usize, concurrency: usize) -> Result<()> {
        info!(fdlimit = app::get_fdlimit(), "generating messages");
        let args::MessageGenerateOpts {
            r#loop, interval, ..
        } = self.opts;

        self.send_many_messages(self.db.clone(), n, concurrency)
            .await?;

        if r#loop {
            loop {
                info!(time = ?std::time::Instant::now(), amount = n, "sending messages");
                tokio::time::sleep(*interval).await;
                self.send_many_messages(self.db.clone(), n, concurrency)
                    .await?;
            }
        }
        Ok(())
    }

    async fn send_many_messages(
        &self,
        db: Arc<redb::Database>,
        n: usize,
        concurrency: usize,
    ) -> Result<usize> {
        let Self { network, opts, .. } = self;

        let style = ProgressStyle::with_template(
            "{bar} {pos}/{len} elapsed {elapsed} remaining {eta_precise}",
        );
        let bar = ProgressBar::new(n as u64).with_style(style.unwrap());

        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        // Read once; apply per-message regardless of success/failure
        let loop_pause_secs = std::env::var("XDBG_LOOP_PAUSE")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());

        let mut set: tokio::task::JoinSet<Result<(), eyre::Error>> = tokio::task::JoinSet::new();
        for _ in 0..n {
            let bar_pointer = bar.clone();
            let d = db.clone();
            let n = network.clone();
            let opts = opts.clone();
            let semaphore = semaphore.clone();
            let loop_pause_secs = loop_pause_secs;

            set.spawn(async move {
                let _permit = semaphore.acquire().await?;
                let res = Self::send_message(&d.clone().into(), &d.clone().into(), n, opts).await;
                bar_pointer.inc(1);
                if let Some(secs) = loop_pause_secs {
                    println!("Pausing for {}s after message iteration", secs);
                    sleep(Duration::from_secs(secs)).await;
                }

                // Propagate original result
                res.map_err(eyre::Report::from)
            });
        }

        let res = set.join_all().await;

        bar.finish();
        bar.reset();

        let errors: Vec<_> = res
            .iter()
            .filter(|r| r.is_err())
            .map(|r| r.as_ref().unwrap_err())
            .collect();

        if !errors.is_empty() {
            info!(errors = ?errors, "errors");
        }

        let msgs_sent = res
            .into_iter()
            .filter(|r| r.is_ok())
            .collect::<Vec<Result<_, _>>>();
        let key = crate::meta_key!(network);
        let meta_db: MetadataStore = db.into();
        meta_db.modify(key, |meta| {
            meta.messages += msgs_sent.len() as u32;
        })?;
        Ok(msgs_sent.len())
    }

    async fn send_message(
        group_store: &GroupStore<'static>,
        identity_store: &IdentityStore<'static>,
        network: args::BackendOpts,
        opts: args::MessageGenerateOpts,
    ) -> Result<(), MessageSendError> {
        let args::MessageGenerateOpts {
            ref max_message_size,
            ..
        } = opts;

        let rng = &mut SmallRng::from_entropy();
        let group = group_store
            .random(&network, rng)?
            .ok_or(eyre!("no group in local store"))?;
        if let Some(inbox_id) = group.members.choose(rng) {
            let key = (u64::from(&network), *inbox_id);

            // pick a separate reader if available
            let reader_inbox_opt = group.members.iter().find(|m| **m != *inbox_id).cloned();

            // each identity can only be used by one worker thread
            let identity_lock = get_identity_lock(inbox_id)?;
            let _lock_guard = identity_lock.lock().await;

            let identity = identity_store.get(key.into())?.ok_or(eyre!(
                "No identity with inbox id [{}] in local store",
                hex::encode(inbox_id)
            ))?;
            let client = app::client_from_identity(&identity, &network).await?;
            client.sync_welcomes().await?;
            let live_group = client.group(&group.id.into())?;
            live_group.sync_with_conn().await?;
            live_group.maybe_update_installations(None).await?;

            let words = rng.gen_range(0..*max_message_size);
            let words = lipsum::lipsum_words_with_rng(&mut *rng, words as usize);
            let message = content_type::new_message(words);

            // send timing + metrics
            let start = std::time::Instant::now();
            live_group.send_message(&message).await?;
            let elapsed = start.elapsed().as_secs_f64();

            record_latency("send_message", elapsed);
            record_throughput("send_message");
            csv_metric(
                "latency_seconds",
                "send_message",
                elapsed,
                &[("operation", "send_message")],
            );
            csv_metric(
                "throughput_events",
                "send_message",
                1.0,
                &[("operation", "send_message")],
            );
            push_metrics("xdbg_debug", "http://localhost:9091");

            // reader-side visibility + identity update read-path tests
            if let Some(reader_inbox) = reader_inbox_opt {
                let reader_identity = identity_store
                    .get((u64::from(&network), reader_inbox).into())?
                    .ok_or_else(|| eyre!("reader identity not found"))?;
                let reader = app::client_from_identity(&reader_identity, &network).await?;
                reader.sync_welcomes().await?;

                let gid_for_reader = live_group.group_id.clone().into();
                let r_group = reader.group(&gid_for_reader)?;

                // post-send read sync latency
                let read_sync_start = std::time::Instant::now();
                let _ = r_group.sync_with_conn().await?;
                let read_sync_secs = read_sync_start.elapsed().as_secs_f64();
                record_latency("read_group_sync_after_send", read_sync_secs);
                record_throughput("read_group_sync_after_send");
                csv_metric(
                    "latency_seconds",
                    "read_group_sync_after_send",
                    read_sync_secs,
                    &[("operation", "post_send_sync")],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // publish identity update (installations) on sender, measure
                let pub_start = std::time::Instant::now();
                let _ = live_group.maybe_update_installations(None).await?;
                let pub_secs = pub_start.elapsed().as_secs_f64();
                record_latency("identity_update_publish_latency", pub_secs);
                record_throughput("identity_update_publish_latency");
                csv_metric(
                    "latency_seconds",
                    "identity_update_publish_latency",
                    pub_secs,
                    &[("operation", "identity_update_publish")],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // reader sync welcomes after publish
                let sync_i_start = std::time::Instant::now();
                let _ = reader.sync_welcomes().await?;
                let sync_i_secs = sync_i_start.elapsed().as_secs_f64();
                record_latency("read_identity_sync_after_publish", sync_i_secs);
                record_throughput("read_identity_sync_after_publish");
                csv_metric(
                    "latency_seconds",
                    "read_identity_sync_after_publish",
                    sync_i_secs,
                    &[("operation", "identity_update_sync")],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // reader lookup of sender's association state after publish
                let sender_hex = hex::encode(identity.inbox_id);
                let reader_conn = reader.context.store().db();
                let lookup_start = std::time::Instant::now();
                let _ = reader
                    .identity_updates()
                    .get_latest_association_state(&reader_conn, &sender_hex)
                    .await?;
                let lookup_secs = lookup_start.elapsed().as_secs_f64();
                record_latency("read_identity_lookup_after_update_publish", lookup_secs);
                record_throughput("read_identity_lookup_after_update_publish");
                csv_metric(
                    "latency_seconds",
                    "read_identity_lookup_after_update_publish",
                    lookup_secs,
                    &[("operation", "identity_update_lookup")],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");
            }

            Ok(())
        } else {
            Err(MessageSendError::NoGroup)
        }
    }
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
