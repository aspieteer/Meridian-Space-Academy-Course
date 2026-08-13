use tokio::runtime::Builder;

fn main() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(8)
        .max_blocking_threads(16)
        .thread_name("meridian-worker")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime");

    runtime.block_on(async {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    })
}
