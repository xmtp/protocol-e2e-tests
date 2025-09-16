use std::{collections::HashSet, sync::Arc};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::app::store::{Database, IdentityStore};
use crate::app::{self, types::Identity};
use crate::args;

use color_eyre::eyre::{self, Result, bail};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::time::{sleep, Duration};

use crate::metrics::{record_latency, record_throughput, push_metrics};

/// Identity Generation
pub struct GenerateIdentity {
    identity_store: IdentityStore<'static>,
    network: args::BackendOpts,
}

impl GenerateIdentity {
    pub fn new(identity_store: IdentityStore<'static>, network: args::BackendOpts) -> Self {
        Self {
            identity_store,
            network,
        }
    }

    #[allow(unused)]
    pub fn load_identities(
        &self,
    ) -> Result<Option<impl Iterator<Item = Result<Identity>> + use<'_>>> {
        Ok(self
            .identity_store
            .load(&self.network)?
            .map(|i| i.map(|i| Ok(i.value()))))
    }

    /// Create identities if they don't already exist.
    /// creates specified `identities` on the
    /// gRPC local docker or development node and saves them to a file.
    /// `identities.generated`/`dev-identities.generated`. Uses this file for subsequent runs if
    /// node still has those identities.
    #[allow(unused)]
    pub async fn create_identities_if_dont_exist(
        &self,
        n: usize,
        client: &crate::DbgClient,
    ) -> Result<Vec<Identity>> {
        let connection = client.context.store().db();
        if let Some(mut identities) = self.load_identities()? {
            let first = identities.next().ok_or(eyre::eyre!("Does not exist"))??;

            let state = client
                .identity_updates()
                .get_latest_association_state(&connection, &hex::encode(first.inbox_id))
                .await?;
            info!("Found generated identities, checking for registration on backend...",);
            // we assume that if the first identity is registered, they all are
            if !state.members().is_empty() {
                return identities.collect::<Result<Vec<Identity>, _>>();
            } else {
                warn!(
                    "No identities found for network {}, clearing orphans and re-instantiating",
                    &url::Url::from(self.network.clone())
                );
                self.identity_store.clear_network(&self.network)?;
            }
        }
        info!("Could not find identities to load, creating new identities");
        let identities = self.create_identities(n, 10).await?;
        self.identity_store
            .set_all(identities.as_slice(), &self.network)?;
        Ok(identities)
    }

    pub async fn create_identities(&self, n: usize, concurrency: usize) -> Result<Vec<Identity>> {
        let mut identities: Vec<Identity> = Vec::with_capacity(n);

        let style = ProgressStyle::with_template(
            "{bar} {pos}/{len} elapsed {elapsed} remaining {eta_precise}",
        );
        let bar = ProgressBar::new(n as u64).with_style(style.unwrap());
        let mut set: tokio::task::JoinSet<Result<_, eyre::Error>> = tokio::task::JoinSet::new();

        let network = &self.network;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        // Metrics/env toggles carried over
        let version = version_label();
        let skip_sleep = std::env::var("XDBG_SKIP_SLEEP")
            .map(|v| v.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false);

        for _ in 0..n {
            let bar_pointer = bar.clone();
            let network = network.clone();
            let semaphore = semaphore.clone();
            let version = version.clone();

            set.spawn(async move {
                let _permit = semaphore.acquire().await?;

                // client init timing (temp client used to hit node)
                let client_init_start = Instant::now();
                let _tmp_client = app::temp_client(&network, None).await?;
                let client_init_secs = client_init_start.elapsed().as_secs_f64();

                record_latency(&format!("identity_client_init_{}", version), client_init_secs);
                record_throughput("identity_client_init");
                csv_metric(
                    "latency_seconds",
                    "identity_client_init",
                    client_init_secs,
                    &[("phase", "client_init"), ("version", &version)],
                );
                csv_metric(
                    "throughput_events",
                    "identity_client_init",
                    1.0,
                    &[("phase", "client_init"), ("version", &version)],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // register timing
                let wallet = crate::app::generate_wallet();
                let register_start = Instant::now();
                let user = app::new_registered_client(network.clone(), Some(&wallet)).await?;
                let register_secs = register_start.elapsed().as_secs_f64();

                record_latency(&format!("identity_register_{}", version), register_secs);
                record_throughput("identity_register");
                csv_metric(
                    "latency_seconds",
                    "identity_register",
                    register_secs,
                    &[("phase", "register"), ("version", &version)],
                );
                csv_metric(
                    "throughput_events",
                    "identity_register",
                    1.0,
                    &[("phase", "register"), ("version", &version)],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                let identity = Identity::from_libxmtp(user.identity(), wallet)?;

                // association readiness polling (read-path test)
                let tmp = Arc::new(app::temp_client(&network, None).await?);
                let conn = Arc::new(tmp.context.store().db());
                let id_hex = hex::encode(identity.inbox_id);

                let assoc_start = Instant::now();
                let timeout = Duration::from_secs(30);
                let poll_every = Duration::from_millis(200);
                let deadline = tokio::time::Instant::now() + timeout;

                let mut assoc_ready = false;
                loop {
                    let state = tmp
                        .identity_updates()
                        .get_latest_association_state(&conn, &id_hex)
                        .await?;
                    if !state.members().is_empty() {
                        assoc_ready = true;
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    if !skip_sleep {
                        sleep(poll_every).await;
                    }
                }
                let assoc_secs = assoc_start.elapsed().as_secs_f64();

                record_latency(&format!("identity_assoc_ready_{}", version), assoc_secs);
                record_throughput("identity_assoc_ready");
                csv_metric(
                    "latency_seconds",
                    "identity_assoc_ready",
                    assoc_secs,
                    &[
                        ("phase", "assoc_ready"),
                        ("version", &version),
                        ("success", if assoc_ready { "true" } else { "false" }),
                    ],
                );
                csv_metric(
                    "throughput_events",
                    "identity_assoc_ready",
                    1.0,
                    &[
                        ("phase", "assoc_ready"),
                        ("version", &version),
                        ("success", if assoc_ready { "true" } else { "false" }),
                    ],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // read sync latency (welcomes)
                let read_sync_start = Instant::now();
                let _ = tmp.sync_welcomes().await?;
                let read_sync_secs = read_sync_start.elapsed().as_secs_f64();
                record_latency("identity_read_sync_latency", read_sync_secs);
                record_throughput("identity_read_sync_latency");
                csv_metric(
                    "latency_seconds",
                    "identity_read_sync_latency",
                    read_sync_secs,
                    &[("phase", "identity_read_sync"), ("version", &version)],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                // identity lookup read latency
                let read_start = Instant::now();
                let _ = tmp
                    .identity_updates()
                    .get_latest_association_state(&conn, &id_hex)
                    .await?;
                let read_secs = read_start.elapsed().as_secs_f64();
                record_latency("read_identity_lookup_latency", read_secs);
                record_throughput("read_identity_lookup_latency");
                csv_metric(
                    "latency_seconds",
                    "read_identity_lookup_latency",
                    read_secs,
                    &[("phase", "identity_read"), ("version", &version)],
                );
                push_metrics("xdbg_debug", "http://localhost:9091");

                bar_pointer.inc(1);
                Ok(identity)
            });

            if set.len() == app::get_fdlimit()
                && let Some(identity) = set.join_next().await
            {
                match identity {
                    Ok(identity) => {
                        identities.push(identity?);
                    }
                    Err(e) => {
                        error!("{}", e.to_string());
                    }
                }
            }
        }

        while let Some(identity) = set.join_next().await {
            match identity {
                Ok(identity) => {
                    identities.push(identity?);
                }
                Err(e) => {
                    error!("{}", e.to_string());
                }
            }
        }

        self.identity_store
            .set_all(identities.as_slice(), &self.network)?;

        bar.finish();
        bar.reset();
        let mut set: tokio::task::JoinSet<Result<_, eyre::Error>> = tokio::task::JoinSet::new();
        // ensure all the identities are registered
        let tmp = Arc::new(app::temp_client(network, None).await?);
        let conn = Arc::new(tmp.context.store().db());
        let bar_ref = bar.clone();
        let future = |inbox_id: [u8; 32]| async move {
            let id = hex::encode(inbox_id);
            trace!(inbox_id = id, "getting association state");
            let state = tmp
                .identity_updates()
                .get_latest_association_state(&conn, &id)
                .await?;
            bar_ref.inc(1);
            Ok(state)
        };

        identities.as_slice().iter().for_each(|i| {
            set.spawn(future.clone()(i.inbox_id));
        });
        bar.finish_and_clear();
        let states = set.join_all().await;
        info!(
            total_states = states.len(),
            "ensuring identities registered & latest association state loaded..."
        );
        let errs = states
            .into_iter()
            .filter_map(|s| s.err())
            .map(|e| e.to_string())
            .collect::<Vec<String>>();
        let unique: HashSet<String> = HashSet::from_iter(errs.clone());
        if !unique.is_empty() {
            tracing::error!("{} errors during identity generation", errs.len());
            tracing::error!("{} unique errors during identity generation", unique.len());
            for err in unique.into_iter() {
                error!(err);
            }
            bail!("Error generation failed");
        }

        // Optional cooldown to dampen churn between test phases
        if let Some(secs) = std::env::var("XDBG_COOLDOWN_SLEEP").ok().and_then(|s| s.parse::<u64>().ok()) {
            std::thread::sleep(std::time::Duration::from_secs(secs));
        }

        Ok(identities)
    }
}

// ----------------------------
// CSV helpers + version label
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

fn version_label() -> String {
    std::env::var("XDBG_VERSION").unwrap_or_else(|_| "na".to_string())
}
