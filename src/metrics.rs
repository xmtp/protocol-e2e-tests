use prometheus::{Opts, Registry, CounterVec, GaugeVec, Encoder, TextEncoder};
use std::sync::OnceLock;
use reqwest::Client;
use tokio::task::JoinHandle;

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub struct Metrics {
    registry: Registry,
    latency: GaugeVec,
    member_count: GaugeVec,
    client: Client,
    // #[allow(dead_code)]
    // counter: CounterVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let _counter = CounterVec::new(
            Opts::new("xdbg_test_events_total", "Count of xdbg test events"),
            &["label"],
        ).expect("valid counter");

        let latency = GaugeVec::new(
            Opts::new("xdbg_operation_latency_seconds", "Latency of xdbg operations"),
            &["operation_type"],
        ).expect("valid gauge");

        let member_count = GaugeVec::new(
            Opts::new("xdbg_group_add_member_count", "Number of members added to group"),
            &["operation_type"],
        ).expect("valid gauge");

        // registry.register(Box::new(_counter.clone())).expect("register counter");
        registry.register(Box::new(latency.clone())).expect("register latency");
        registry.register(Box::new(member_count.clone())).expect("register member count");

        let client = Client::new();

        Metrics { registry, latency, member_count, client }
    }

    pub fn set_latency(&self, operation_type: &str, seconds: f64) {
        self.latency
            .with_label_values(&[operation_type])
            .set(seconds);
    }

    pub fn set_member_count(&self, operation_type: &str, count: f64) {
        self.member_count
            .with_label_values(&[operation_type])
            .set(count);
    }

    pub async fn push(&self, job: &str, push_url: &str) {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let mf = self.registry.gather();

        encoder.encode(&mf, &mut buffer).expect("encode metrics");
        let body = String::from_utf8(buffer).expect("utf8 conversion");

        let push_url = format!("{}/metrics/job/{}", push_url.trim_end_matches('/'), job);

        match self.client.post(&push_url).body(body).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    eprintln!("Failed to push metrics: {}", resp.status());
                }
            }
            Err(e) => {
                eprintln!("Error pushing metrics: {e}");
            }
        }
    }
}

pub fn init_metrics() {
    METRICS.get_or_init(|| Metrics::new());
}

pub fn record_latency(operation_type: &str, seconds: f64) {
    if let Some(metrics) = METRICS.get() {
        metrics.set_latency(operation_type, seconds);
    }
}

pub fn record_member_count(operation_type: &str, count: f64) {
    if let Some(metrics) = METRICS.get() {
        metrics.set_member_count(operation_type, count);
    }
}

pub fn push_metrics(job: &'static str, push_url: &'static str) -> Option<JoinHandle<()>> {
    if let Some(metrics) = METRICS.get() {
        Some(tokio::spawn(async move {
            metrics.push(job, push_url).await;
        }))
    } else {
        None
    }
}
