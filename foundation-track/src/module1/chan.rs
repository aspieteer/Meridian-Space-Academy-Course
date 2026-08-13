use std::{
    alloc::Layout,
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

pub struct ReusableBoxFuture<'a, T> {
    boxed: Pin<Box<dyn Future<Output = T> + Send + 'a>>,
}

impl<'a, T> fmt::Debug for ReusableBoxFuture<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReusableBoxFuture").finish()
    }
}

impl<'a, T> ReusableBoxFuture<'a, T> {
    pub fn new<F>(fut: F) -> Self
    where
        F: Future<Output = T> + Send + 'a,
    {
        Self {
            boxed: Box::pin(fut),
        }
    }

    pub fn get_pin(&mut self) -> Pin<&mut (dyn Future<Output = T> + Send)> {
        self.boxed.as_mut()
    }

    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<T> {
        self.boxed.as_mut().poll(cx)
    }
}

// ===== Helper =====

fn reuse_pin_box<T, U, F, O>(boxed: Pin<Box<T>>, new_value: U, callback: F) -> Result<O, U>
where
    T: ?Sized,
    F: FnOnce(Box<U>) -> O,
{
    let layout = Layout::for_value(&*boxed);
    if layout != Layout::new::<U>() {
        return Err(new_value);
    }

    todo!()
}
