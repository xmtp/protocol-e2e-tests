use prometheus::{
    Opts, Registry, CounterVec, GaugeVec, Encoder, TextEncoder,
};
use std::sync::OnceLock;
use reqwest::blocking::Client;

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub struct Metrics {
    registry: Registry,
    counter: CounterVec,
    latency: GaugeVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let counter = CounterVec::new(
            Opts::new("xdbg_test_events_total", "Count of xdbg test events"),
            &["label"],
        ).expect("valid counter");

        let latency = GaugeVec::new(
            Opts::new("xdbg_operation_latency_seconds", "Latency of xdbg operations"),
            &["operation_type"],
        ).expect("valid gauge");

        registry.register(Box::new(counter.clone())).expect("register counter");
        registry.register(Box::new(latency.clone())).expect("register gauge");

        Metrics { registry, counter, latency }
    }

    pub fn inc(&self, label: &str) {
        self.counter.with_label_values(&[label]).inc();
    }

    pub fn set_latency(&self, operation_type: &str, seconds: f64) {
        self.latency
            .with_label_values(&[operation_type])
            .set(seconds);
    }

    pub fn push(&self, job: &str, push_url: &str) {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let mf = self.registry.gather();

        encoder.encode(&mf, &mut buffer).expect("encode metrics");
        let body = String::from_utf8(buffer).expect("utf8 conversion");

        let push_url = format!("{}/metrics/job/{}", push_url.trim_end_matches('/'), job);

        let client = Client::new();
        let result = client.post(&push_url).body(body).send();

        match result {
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

pub fn increment_metric(label: &str) {
    if let Some(metrics) = METRICS.get() {
        metrics.inc(label);
    }
}

pub fn record_latency(operation_type: &str, seconds: f64) {
    if let Some(metrics) = METRICS.get() {
        metrics.set_latency(operation_type, seconds);
    }
}

pub fn push_metrics(job: &str, push_url: &str) {
    if let Some(metrics) = METRICS.get() {
        metrics.push(job, push_url);
    }
}
