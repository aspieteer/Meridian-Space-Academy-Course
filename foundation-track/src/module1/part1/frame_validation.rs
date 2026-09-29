use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::sync::oneshot;

// NOTE:
// The poll implementation delegates to the inner oneshot::Receiver's own poll.
// When Receiver::poll returns Pending, it has already stored the waker from cx internally.
// When tx.send(true) fires, Receiver calls that waker, which re-queues this task.
// No manual waker management is needed here because we compose with a type that already handles it correctly.
//
// This is the pattern to follow when building custom futures:
// compose with existing futures and channel primitives wherever possible.
// Write unsafe waker code only when you are bridging to a non-async notification source
// (an epoll fd, a hardware interrupt, a C library callback).

/// Represents a frame whose header CRC is being validated asynchronously.
/// The validation runs on a blocking thread; this future waits for its result.
pub struct FrameValidationFuture {
    // oneshot::Receiver implements Future directly, but we wrap it here
    // to show the polling mechanics explicitly.
    receiver: oneshot::Receiver<bool>,
}

impl FrameValidationFuture {
    pub fn new(receiver: oneshot::Receiver<bool>) -> Self {
        Self { receiver }
    }
}

impl Future for FrameValidationFuture {
    type Output = Result<(), String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Pin::new is safe here because oneshot::Receiver is Unpin.
        // For a self-referential type we'd need unsafe or box-pinning.
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(true)) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(false)) => Poll::Ready(Err("CRC validation failed".to_string())),
            Poll::Ready(Err(e)) => {
                // Sender dropped without sending — the validator thread panicked
                // or was cancelled. Treat as a validation failure, not a panic.
                Poll::Ready(Err(format!(
                    "Validator thread terminated unexpectedly: {}",
                    e
                )))
            }
            // NOTE: The key here is the [`oneshot::Receiver`] has already
            // registered cx's waker.
            //
            // The result is not ready yet. The oneshot::Receiver has already
            // registered cx's waker — it will call it when a value is sent.
            // We return Pending; the executor parks this task.
            Poll::Pending => Poll::Pending,
        }
    }
}
