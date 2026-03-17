// crates\bm-generator\tests\clickstream_generate.rs
use bm_generator::config::{
    ClickstreamCardinalityConfig, ClickstreamDistributionConfig, ClickstreamGeneratorConfig,
};
use bm_generator::families::clickstream::generate_clickstream_rows;
use bm_generator::generate_clickstream_dataset;

use std::fs;

#[test]
fn generates_clickstream_dataset_artifacts() {
    let base = std::env::temp_dir().join("bm_generator_test_clickstream");
    let _ = fs::remove_dir_all(&base);

    let config = ClickstreamGeneratorConfig {
        name: "clickstream_test".to_string(),
        family: "clickstream".to_string(),
        seed: 42,
        rows: 100,
        output_dir: base.to_string_lossy().to_string(),
        formats: vec!["csv".to_string()],
        cardinality: ClickstreamCardinalityConfig {
            users: 10,
            sessions: 20,
            pages: 5,
            countries: 3,
            referrers: 4,
        },
        distributions: ClickstreamDistributionConfig {
            null_ratio: 0.0,
            skew: "medium".to_string(),
        },
    };

    let out = generate_clickstream_dataset(config).unwrap();

    assert!(out.join("csv").join("clickstream_test.csv").exists());
    assert!(out.join("csv").join("manifest.json").exists());
    assert!(out.join("generation_config.json").exists());
    assert!(out.join("schema.json").exists());
}

#[test]
fn clickstream_generation_is_deterministic_for_same_seed() {
    let config = ClickstreamGeneratorConfig {
        name: "clickstream_test".to_string(),
        family: "clickstream".to_string(),
        seed: 42,
        rows: 10,
        output_dir: "ignored".to_string(),
        formats: vec!["csv".to_string()],
        cardinality: ClickstreamCardinalityConfig {
            users: 10,
            sessions: 20,
            pages: 5,
            countries: 3,
            referrers: 4,
        },
        distributions: ClickstreamDistributionConfig {
            null_ratio: 0.0,
            skew: "medium".to_string(),
        },
    };

    let a = generate_clickstream_rows(&config);
    let b = generate_clickstream_rows(&config);

    assert_eq!(serde_json::to_string(&a).unwrap(), serde_json::to_string(&b).unwrap());
}