// crates\bm-engine\src\error.rs
#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("bootstrap failed: {0}")]
    Bootstrap(String),

    #[error("prepare dataset failed: {0}")]
    Prepare(String),

    #[error("query execution failed: {0}")]
    Query(String),

    #[error("cleanup failed: {0}")]
    Cleanup(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("other engine error: {0}")]
    Other(String),
}