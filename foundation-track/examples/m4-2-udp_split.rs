use std::{sync::Arc, time::Duration};

use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:9099").await?);

    // For UdpSocket, Arc-sharing is the idiomatic split pattern
    // because both send_to and recv_from take &self.
    let recv_sock = Arc::clone(&socket);
    let send_sock = Arc::clone(&socket);

    let recv_task = tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        let sleep = tokio::time::sleep(Duration::from_secs(10));
        let mut sleep = std::pin::pin!(sleep);

        loop {
            tokio::select! {
                biased;

                res = recv_sock.recv_from(&mut buf) => {
                    match res {
                        Ok((n, addr)) => {
                            println!("recv {n} bytes from {addr}");
                        },
                        Err(e) => {
                            println!("socket error, recv side failed to receive msg: {e}");
                            continue;
                        },
                    }
                }
                _ = &mut sleep => {
                    break;
                }
            }
        }

        println!("10 seconds passed");
    });

    let send_task = tokio::spawn(async move {
        // Periodic heartbeat to a known sensor address.
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            send_sock
                .send_to(b"HEARTBEAT", "127.0.0.1:9099")
                .await
                .expect("sending heartbeat failed");
        }
    });

    println!("socket ref count: {}", Arc::strong_count(&socket));

    let _ = tokio::join!(recv_task, send_task);

    println!("socket ref count: {}", Arc::strong_count(&socket));

    Ok(())
}
