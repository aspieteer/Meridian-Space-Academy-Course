use std::{net::SocketAddr, time::Duration};

use bytes::{Buf, Bytes, BytesMut};
use http_body_util::Full;
use hyper::{StatusCode, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
};

use crate::module4::project4::frame::Frame;

pub async fn tcp_task(
    tcp_listener: TcpListener,
    mut sys_shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let listen_addr = tcp_listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    tracing::info!(listen = %listen_addr, "tcp_task: accept loop started");

    loop {
        tokio::select! {
            result = tcp_listener.accept() => {
                let (stream, addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::error!("tcp_task: accept failed: {e}");
                        return Err(e.into());
                    }
                };
                tracing::info!(%addr, "tcp_task: client connected, spawning connection task");

                let conn_shutdown_rx = sys_shutdown_rx.clone();
                tokio::spawn(run_connection(stream, addr, conn_shutdown_rx));
            }
            Ok(()) = sys_shutdown_rx.changed() => {
                if *sys_shutdown_rx.borrow() {
                    tracing::info!("tcp_task: shutdown signal received, stopping accept loop");
                    break;
                }
            }
        }
    }

    tracing::info!("tcp_task: terminated");
    Ok(())
}

/// Serve one client connection: push a telemetry frame every 10s while
/// reading length-prefixed messages (e.g. GOODBYE) from the client.
///
/// Reads and writes share the whole stream inside a single `select!` loop,
/// so there is no need to split it: `select!` drops every branch-condition
/// future before running the winning branch's handler, which releases the
/// read future's `&mut stream` borrow in time for the ticker branch's
/// `write_all` to take its own.
async fn run_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut buf = BytesMut::with_capacity(1024);
    let mut ticker = tokio::time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            result = stream.read_buf(&mut buf) => {
                match result {
                    Ok(0) => {
                        tracing::info!(%addr, "tcp_task: peer closed connection");
                        break;
                    }
                    Ok(_) => {
                        // u32 BE header indicating payload length. Leftover
                        // bytes of a partial frame stay in `buf` for the next
                        // read — do NOT clear the buffer here.
                        while buf.len() >= 4 {
                            let frame_len = u32::from_be_bytes(
                                buf[..4].try_into().expect("should be a valid u32 number"),
                            ) as usize;
                            if buf.len() < 4 + frame_len {
                                break; // wait for the rest of the frame.
                            }
                            buf.advance(4);
                            let payload = buf.split_to(frame_len);
                            match std::str::from_utf8(&payload) {
                                Ok(text) => tracing::info!(%addr, "tcp_task: message from client: {text}"),
                                Err(_) => tracing::warn!(%addr, bytes = frame_len, "tcp_task: non-UTF8 payload, skipped"),
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(%addr, "tcp_task: read from client error: {e}");
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                let frame = Frame::create_frame();
                tracing::debug!(%addr, bytes = frame.as_slice().len(), "tcp_task: writing frame");
                if let Err(e) = stream.write_all(frame.as_slice()).await {
                    tracing::warn!(%addr, "tcp_task: write failed ({e}), closing connection");
                    break;
                }
            }
            shutdown = shutdown_rx.changed() => {
                // A dropped sender counts as shutdown too.
                if shutdown.is_err() || *shutdown_rx.borrow() {
                    tracing::info!(%addr, "tcp_task: shutdown signal received in connection task");
                    break;
                }
            }
        }
    }

    // Half-close so the client observes EOF instead of a silent drop.
    let _ = stream.shutdown().await;
    tracing::debug!(%addr, "tcp_task: connection task exited");
}

pub async fn http_task(
    http_listener: TcpListener,
    mut sys_shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let listen_addr = http_listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    // NOTE: this serves cleartext HTTP/2 with prior knowledge (h2c) —
    // plain HTTP/1.1 clients fail the preface handshake here.
    tracing::info!(listen = %listen_addr, protocol = "h2c (HTTP/2 prior knowledge)", "http_task: accept loop started");

    loop {
        tokio::select! {
            result = http_listener.accept() => {
                let (stream, addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::error!("http_task: accept failed: {e}");
                        return Err(e.into());
                    }
                };
                tracing::info!(%addr, "http_task: client connected, spawning connection task");

                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    tracing::debug!(%addr, "http_task: serving connection");
                    let result =
                        hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                            .serve_connection(io, service_fn(tle_handler))
                            .await;
                    match result {
                        Ok(()) => tracing::debug!(%addr, "http_task: connection closed cleanly"),
                        Err(e) => tracing::error!(%addr, "http_task: connection error: {e}"),
                    }
                });
            }
            Ok(()) = sys_shutdown_rx.changed() => {
                if *sys_shutdown_rx.borrow() {
                    tracing::info!("http_task: shutdown signal received, stopping accept loop");
                    break;
                }
            }
        }
    }

    tracing::info!("http_task: terminated");
    Ok(())
}

async fn tle_handler(
    req: hyper::Request<hyper::body::Incoming>,
) -> anyhow::Result<hyper::Response<Full<Bytes>>> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    tracing::info!(%method, %path, "tle_handler: request received");

    match (&method, path.as_str()) {
        (&hyper::Method::GET, "/tle/0") => {
            let data =
                "{\"norad_id\":0,\"name\":\"Artemis\",\"line1\":\"Hello\",\"line2\":\"Bye\"}";
            tracing::info!(%method, %path, status = 200, "tle_handler: serving TLE record");
            Ok(hyper::Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from(data)))?)
        }
        _ => {
            let not_found = "Not Found";
            tracing::warn!(%method, %path, status = 404, "tle_handler: no route matched");
            Ok(hyper::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from(not_found)))?)
        }
    }
}
