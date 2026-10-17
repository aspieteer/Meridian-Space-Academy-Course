use std::{fmt, ptr};

struct SelfRef {
    data: String,
    p: *const String,
}

impl fmt::Debug for SelfRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelfRef")
            .field("data", &self.data)
            .field(
                "p",
                &format_args!("{}: {}", std::any::type_name_of_val(&self.p), self.p.addr()),
            )
            .finish()
    }
}

impl SelfRef {
    fn new(data: String) -> Self {
        Self {
            data,
            p: ptr::null(),
        }
    }

    fn reference(&mut self) {
        self.p = &self.data;
    }
}

#[tokio::main]
async fn main() {
    let r = SelfRef::new("hello".to_string());

    let mut r = std::pin::pin!(r);
    r.as_mut().reference();

    println!("r: {r:?}");

    let align = 8usize;
    const A: usize = 0xfffffffffffffff8;
    println!("!7 is {}, {}", !(align - 1), A);
}
