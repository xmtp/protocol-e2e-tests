use crate::app::identity_lock::get_identity_lock;
use tokio::time::{sleep, Duration};
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
use std::time::{SystemTime, UNIX_EPOCH};
use xmtp_mls::groups::summary::SyncSummary;

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

        let skip_sleep = std::env::var("XDBG_SKIP_SLEEP")
            .map(|v| v.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false);

        self.send_many_messages(self.db.clone(), n, concurrency)
            .await?;

        if r#loop {
            loop {
                info!(time = ?std::time::Instant::now(), amount = n, "sending messages");
                if !skip_sleep {
                    tokio::time::sleep(*interval).await;
                }
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

        let mut set: tokio::task::JoinSet<Result<(), eyre::Error>> = tokio::task::JoinSet::new();
        for _ in 0..n {
            let bar_pointer = bar.clone();
            let d = db.clone();
            let n = network.clone();
            let opts = opts.clone();
            let semaphore = semaphore.clone();
            set.spawn(async move {
                let _permit = semaphore.acquire().await?;
                Self::send_message(&d.clone().into(), &d.clone().into(), n, opts).await?;
                bar_pointer.inc(1);
                Ok(())
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

        let skip_sleep = std::env::var("XDBG_SKIP_SLEEP")
            .map(|v| v.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false);

        let rng = &mut SmallRng::from_entropy();
        let group = group_store
            .random(&network, rng)?
            .ok_or(eyre!("no group in local store"))?;
        if let Some(inbox_id) = group.members.choose(rng) {
            let key = (u64::from(&network), *inbox_id);

            // each identity can only be used by one worker thread
            let identity_lock = get_identity_lock(inbox_id)?;
            let _lock_guard = identity_lock.lock().await;

            let identity = identity_store.get(key.into())?.ok_or(eyre!(
                "No identity with inbox id [{}] in local store",
                hex::encode(inbox_id)
            ))?;
            let client = app::client_from_identity(&identity, &network).await?;
            client.sync_welcomes().await?;
            let group = client.group(&group.id.into())?;
            group.sync_with_conn().await?;
            group.maybe_update_installations(None).await?;

            let words = rng.gen_range(0..*max_message_size);
            let words = lipsum::lipsum_words_with_rng(&mut *rng, words as usize);
            let message = content_type::new_message(words);

            let start = std::time::Instant::now();
            group.send_message(&message).await?;
            let elapsed = start.elapsed().as_secs_f64();

            crate::metrics::record_latency("send_message", elapsed);
            crate::metrics::record_throughput("send_message");

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

            crate::metrics::push_metrics("xdbg_debug", "http://localhost:9091");

            if !skip_sleep {
                sleep(Duration::from_secs(60)).await;
            }

            Ok(())
        } else {
            Err(MessageSendError::NoGroup)
        }
    }

}
