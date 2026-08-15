use std::{sync::Arc, time::Duration};

use foundation_track::module2::command_queue::{Command, CommandQueue};

fn main() {
    let queue = CommandQueue::new();

    // Producer threads simulate ground network connections.
    let producers = (0..15)
        .map(|i| {
            let q = Arc::clone(&queue);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(i * 10));
                q.push(Command::new((i as u8 % 15) + 1, format!("CMD-{i:04}")));
                println!("producer {i}: pushed priority {}", (i as u8 % 15) + 1);
            })
        })
        .collect::<Vec<_>>();

    // Consumer runs on a separate thread — simulates session dispatcher.
    let q = Arc::clone(&queue);
    let consumer = std::thread::spawn(move || {
        for i in 0..5_usize {
            std::thread::sleep(Duration::from_millis((i as u64 + 1) * 100));
            let cmd = q.pop_blocking();
            println!(
                "dispatcher: executing '{}' (priority {})",
                cmd.payload(),
                cmd.priority()
            );
        }
    });

    for p in producers {
        p.join().unwrap();
    }
    consumer.join().unwrap();
}
