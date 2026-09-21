pub mod batch_arena;
pub mod client;
pub mod config;
pub mod connection;
pub mod frame;
pub mod server;
pub mod shutdown;

pub const PORT: usize = 6789;

type Error = Box<dyn std::error::Error + Send + Sync>;

type Result<T> = std::result::Result<T, Error>;
