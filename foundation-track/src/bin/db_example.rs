use std::time::Duration;

use bytes::Bytes;
use foundation_track::module3::db::DbDropGuard;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let db_guard = DbDropGuard::new();
    let db = db_guard.db();

    db.set(
        "Hello".to_string(),
        Bytes::from("1"),
        Some(Duration::from_secs(1)),
    );

    println!("{:?}", db);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                println!("{:?}", db);
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }
}
