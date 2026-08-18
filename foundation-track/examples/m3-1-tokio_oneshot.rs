use tokio::sync::{mpsc, oneshot};

enum ControlMsg {
    GetQueueDepth { reply: oneshot::Sender<usize> },
    Flush,
}

async fn aggregator(mut rx: mpsc::Receiver<ControlMsg>) {
    let mut queue: Vec<Vec<u8>> = Vec::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            ControlMsg::GetQueueDepth { reply } => {
                // reply.send consumes the sender — can only respond once.
                let _ = reply.send(queue.len());
            }
            ControlMsg::Flush => {
                println!("flushing {} frames", queue.len());
                queue.clear();
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(aggregator(rx));

    // Ask the aggregator for its current queue depth.
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ControlMsg::GetQueueDepth { reply: reply_tx })
        .await
        .unwrap();
    let depth = reply_rx.await.unwrap();
    println!("aggregator queue depth: {depth}");
}
