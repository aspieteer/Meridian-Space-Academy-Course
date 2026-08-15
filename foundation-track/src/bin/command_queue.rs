use std::{sync::Arc, time::Duration};

use foundation_track::module2::project2::{Command, CommandKind, CommandQueue, EqF32};

fn main() {
    let queue = CommandQueue::new(10);

    // Producer threads simulate ground network connections.
    let producers = (0..5)
        .map(|i| {
            let q = Arc::clone(&queue);
            std::thread::spawn(move || {
                for j in 0..20 {
                    let cmd_kind = match (i + j) % 5 {
                        0 => CommandKind::SafeMode,
                        1 => CommandKind::AbortPass,
                        2 => CommandKind::Repoint {
                            azimuth: EqF32(45.0),
                            elevation: EqF32(30.5),
                        },
                        3 => CommandKind::StatusRequest,
                        4 => CommandKind::Housekeeping,
                        _ => unreachable!("u64::MAX is impossible"),
                    };
                    std::thread::sleep(Duration::from_millis(i + 1 + (j + 1) * 2));
                    match q.push(Command::new(cmd_kind.clone())) {
                        Ok(()) => println!("[gs-{i}]: *** pushed command *** {:?}", cmd_kind),
                        Err(e) => println!("[gs-{i}]: push rejected - {e:?}"),
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    // Consumer runs on a separate thread — simulates session dispatcher.
    let q = Arc::clone(&queue);
    let metrics = queue.metrics_clone();
    let consumer = std::thread::spawn(move || {
        while let Ok(cmd) = q.pop() {
            println!("dispatcher: executing '{:?}'", cmd.kind_and_priority());
            std::thread::sleep(Duration::from_millis(100));
        }
        println!(
            "\n==========================================\n=== dispatcher: queue drained, exiting ===\n=========================================="
        );
        println!(
            "\n##### Final Results #####\nmetrics: pushed={} dispatched={} safe_mode={}",
            metrics.load_pushed(),
            metrics.load_dispatched(),
            metrics.load_safe_mode_count(),
        );
    });

    // Monitoring thread.
    let q = Arc::clone(&queue);
    let metrics = queue.metrics_clone();
    let monitor = std::thread::spawn(move || {
        loop {
            if q.is_shutsown() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
            println!(
                "metrics: pushed={} dispatched={} safe_mode={}",
                metrics.load_pushed(),
                metrics.load_dispatched(),
                metrics.load_safe_mode_count(),
            );
        }
    });

    // Shutdown thread.
    let q_shutdown = Arc::clone(&queue);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        q_shutdown.shutdown();
        println!(
            "\n==============================\n===== Signaling shutdown =====\n==============================\n"
        );
    });

    for p in producers {
        p.join().unwrap();
    }
    consumer.join().unwrap();
    monitor.join().unwrap();
}
