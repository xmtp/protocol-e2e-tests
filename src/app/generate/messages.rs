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
use rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom};
use std::sync::Arc;
use xmtp_mls::groups::summary::SyncSummary;

// added for metrics + timing
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use tokio::{task::yield_now, time::timeout};
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

        // Batch pause solely from env; if unset -> no pause
        let batch_pause_secs = std::env::var("XDBG_LOOP_PAUSE")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());

        // First batch immediately
        self.send_many_messages(self.db.clone(), n, concurrency).await?;

        if self.opts.r#loop {
            loop {
                if let Some(secs) = batch_pause_secs {
                    sleep(Duration::from_secs(secs)).await;
                }
                info!(time = ?std::time::Instant::now(), amount = n, "sending messages");
                self.send_many_messages(self.db.clone(), n, concurrency).await?;
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

        let mut set: tokio::task::JoinSet<Result<(), eyre::Error>> = tokio::task::JoinSet::new();
        for _ in 0..n {
            let bar_pointer = bar.clone();
            let d = db.clone();
            let n = network.clone();
            let opts = opts.clone();
            let semaphore = semaphore.clone();

            set.spawn(async move {
                let _permit = semaphore.acquire().await?;
                let res = Self::send_message(&d.clone().into(), &d.clone().into(), n, opts).await;
                bar_pointer.inc(1);
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
        let args::MessageGenerateOpts { ref max_message_size, .. } = opts;

        let rng = &mut SmallRng::from_entropy();
        let group = group_store
            .random(&network, rng)?
            .ok_or(eyre!("no group in local store"))?;

        // Only use members with a local identity
        let net_key_prefix = u64::from(&network);
        let mut local_members: Vec<_> = group
            .members
            .iter()
            .cloned()
            .filter(|m| {
                identity_store
                    .get((net_key_prefix, *m).into())
                    .ok()
                    .flatten()
                    .is_some()
            })
            .collect();

        if local_members.is_empty() {
            return Err(MessageSendError::NoGroup);
        }

        let inbox_id = *local_members.choose(rng).unwrap();

        local_members.retain(|m| *m != inbox_id);
        let reader_inbox_opt = local_members.choose(rng).cloned();

        let key = (net_key_prefix, inbox_id);

        // Hold the identity lock only while reading identity + creating client
        let (client, sender_inbox_bytes) = {
            let identity_lock = get_identity_lock(&inbox_id)?;
            let _guard = identity_lock.lock().await;

            let identity = identity_store.get(key.into())?.ok_or(eyre!(
                "No identity with inbox id [{}] in local store",
                hex::encode(inbox_id)
            ))?;

            let sender_inbox_bytes = identity.inbox_id;
            let client = app::client_from_identity(&identity, &network).await?;
            (client, sender_inbox_bytes)
        }; // lock dropped here

        // Helper: timeout wrapper for heavy ops (generic over error type)
        async fn with_timeout<F, T, E>(dur: Duration, fut: F) -> eyre::Result<Option<T>>
        where
            F: std::future::Future<Output = Result<T, E>>,
            E: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static,
        {
            match timeout(dur, fut).await {
                Ok(Ok(v)) => Ok(Some(v)),
                Ok(Err(e)) => Err(eyre::eyre!(e)),
                Err(_) => Ok(None), // soft timeout
            }
        }

        // Pre-send bounded syncs + yields to avoid monopolizing runtime
        let _ = with_timeout(Duration::from_secs(5), client.sync_welcomes()).await?;
        yield_now().await;

        let live_group = client.group(&group.id.into())?;
        let _ = with_timeout(Duration::from_secs(5), live_group.sync_with_conn()).await?;
        yield_now().await;

        let _ = with_timeout(Duration::from_secs(5), live_group.maybe_update_installations(None)).await?;
        yield_now().await;

        // Always max size
        let word_count = (*max_message_size).max(1) as usize;
        let message_text = lipsum::lipsum_words_with_rng(&mut *rng, word_count);
        let message = content_type::new_message(message_text);

        let start = std::time::Instant::now();
        live_group.send_message(&message).await?;
        let elapsed = start.elapsed().as_secs_f64();

        record_latency("send_message", elapsed);
        record_throughput("send_message");
        csv_metric("latency_seconds", "send_message", elapsed, &[("operation", "send_message")]);
        csv_metric("throughput_events", "send_message", 1.0, &[("operation", "send_message")]);
        push_metrics("xdbg_debug", "http://localhost:9091");

        if let Some(reader_inbox) = reader_inbox_opt {
            let reader_identity = identity_store
                .get((net_key_prefix, reader_inbox).into())?
                .ok_or_else(|| eyre!("reader identity not found"))?;
            let reader = app::client_from_identity(&reader_identity, &network).await?;

            let _ = with_timeout(Duration::from_secs(5), reader.sync_welcomes()).await?;
            yield_now().await;

            let gid_for_reader = live_group.group_id.clone().into();
            let r_group = reader.group(&gid_for_reader)?;

            let read_sync_start = std::time::Instant::now();
            let _ = with_timeout(Duration::from_secs(8), r_group.sync_with_conn()).await?;
            let read_sync_secs = read_sync_start.elapsed().as_secs_f64();
            record_latency("read_group_sync_after_send", read_sync_secs);
            record_throughput("read_group_sync_after_send");
            csv_metric("latency_seconds", "read_group_sync_after_send", read_sync_secs, &[("operation", "post_send_sync")]);
            csv_metric("throughput_events", "read_group_sync_after_send", 1.0, &[("operation", "post_send_sync")]);
            push_metrics("xdbg_debug", "http://localhost:9091");
            yield_now().await;

            let pub_start = std::time::Instant::now();
            let _ = with_timeout(Duration::from_secs(5), live_group.maybe_update_installations(None)).await?;
            let pub_secs = pub_start.elapsed().as_secs_f64();
            record_latency("identity_update_publish_latency", pub_secs);
            record_throughput("identity_update_publish_latency");
            csv_metric("latency_seconds", "identity_update_publish_latency", pub_secs, &[("operation", "identity_update_publish")]);
            csv_metric("throughput_events", "identity_update_publish_latency", 1.0, &[("operation", "identity_update_publish")]);
            push_metrics("xdbg_debug", "http://localhost:9091");
            yield_now().await;

            let sync_i_start = std::time::Instant::now();
            let _ = with_timeout(Duration::from_secs(5), reader.sync_welcomes()).await?;
            let sync_i_secs = sync_i_start.elapsed().as_secs_f64();
            record_latency("read_identity_sync_after_publish", sync_i_secs);
            record_throughput("read_identity_sync_after_publish");
            csv_metric("latency_seconds", "read_identity_sync_after_publish", sync_i_secs, &[("operation", "identity_update_sync")]);
            csv_metric("throughput_events", "read_identity_sync_after_publish", 1.0, &[("operation", "identity_update_sync")]);
            push_metrics("xdbg_debug", "http://localhost:9091");
            yield_now().await;

            let sender_hex = hex::encode(sender_inbox_bytes);
            let reader_conn = reader.context.store().db();
            let lookup_start = std::time::Instant::now();
            let _ = with_timeout(
                Duration::from_secs(3),
                reader.identity_updates().get_latest_association_state(&reader_conn, &sender_hex),
            ).await?;
            let lookup_secs = lookup_start.elapsed().as_secs_f64();
            record_latency("read_identity_lookup_after_update_publish", lookup_secs);
            record_throughput("read_identity_lookup_after_update_publish");
            csv_metric("latency_seconds", "read_identity_lookup_after_update_publish", lookup_secs, &[("operation", "identity_update_lookup")]);
            csv_metric("throughput_events", "read_identity_lookup_after_update_publish", 1.0, &[("operation", "identity_update_lookup")]);
            push_metrics("xdbg_debug", "http://localhost:9091");
        }

        Ok(())
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
        labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(";")
    };
    println!("{},{},{:.6},{},{}", metric_kind, metric_name, value, ts, labels_str);
}
