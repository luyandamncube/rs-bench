// crates\bm-generator\src\families\clickstream.rs
use crate::config::ClickstreamGeneratorConfig;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ClickstreamRow {
    pub event_time: String,
    pub user_id: u64,
    pub session_id: u64,
    pub page_id: u32,
    pub device_type: String,
    pub country_code: String,
    pub referrer_domain: String,
    pub event_type: String,
    pub revenue: f64,
    pub latency_ms: u32,
}

const DEVICE_TYPES: &[&str] = &["mobile", "desktop", "tablet"];
const EVENT_TYPES: &[&str] = &["page_view", "click", "add_to_cart", "purchase"];
const COUNTRY_CODES: &[&str] = &[
    "US", "ZA", "GB", "DE", "FR", "IN", "BR", "AU", "CA", "JP", "NL", "SG",
];
const REFERRERS: &[&str] = &[
    "google.com",
    "bing.com",
    "news.site",
    "social.app",
    "email.campaign",
    "partner.site",
    "direct",
    "search.portal",
];

pub fn generate_clickstream_rows(
    config: &ClickstreamGeneratorConfig,
) -> Vec<ClickstreamRow> {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut rows = Vec::with_capacity(config.rows as usize);

    for i in 0..config.rows {
        let purchase = rng.gen_bool(0.05);

        let row = ClickstreamRow {
            event_time: format!(
                "2026-01-{:02}T{:02}:{:02}:{:02}Z",
                1 + (i % 28),
                rng.gen_range(0..24),
                rng.gen_range(0..60),
                rng.gen_range(0..60)
            ),
            user_id: rng.gen_range(1..=config.cardinality.users),
            session_id: rng.gen_range(1..=config.cardinality.sessions),
            page_id: rng.gen_range(1..=config.cardinality.pages),
            device_type: DEVICE_TYPES[rng.gen_range(0..DEVICE_TYPES.len())].to_string(),
            country_code: COUNTRY_CODES[rng.gen_range(
                0..config.cardinality.countries.min(COUNTRY_CODES.len() as u32) as usize
            )]
            .to_string(),
            referrer_domain: REFERRERS[rng.gen_range(
                0..config.cardinality.referrers.min(REFERRERS.len() as u32) as usize
            )]
            .to_string(),
            event_type: if purchase {
                "purchase".to_string()
            } else {
                EVENT_TYPES[rng.gen_range(0..3)].to_string()
            },
            revenue: if purchase {
                rng.gen_range(5.0..500.0)
            } else {
                0.0
            },
            latency_ms: rng.gen_range(10..3000),
        };

        rows.push(row);
    }

    rows
}