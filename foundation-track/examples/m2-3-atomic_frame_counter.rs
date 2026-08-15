use std::{sync::Arc, time::Duration};

use foundation_track::module2::pipeline_metrics::PipelineMetrics;

fn main() {
    let metrics = PipelineMetrics::new();

    // Simulate 4 uplink tasks.
    let workers = (0..4)
        .map(|i| {
            let m = Arc::clone(&metrics);
            std::thread::spawn(move || {
                for _ in 0..100 {
                    if m.should_stop() {
                        break;
                    }
                    m.record_frame(1024);
                    if i == 0 {
                        m.record_drop();
                    } // simulate occational drops on uplink 0
                }
            })
        })
        .collect::<Vec<_>>();

    // Monitoring thread samples every 5ms.
    let m = Arc::clone(&metrics);
    let monitor = std::thread::spawn(move || {
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(5));
            let (recv, drop, bytes) = m.snapshot();
            println!("recv={recv} drop={drop} bytes={bytes}");
        }
        m.signal_shutdown();
    });

    monitor.join().unwrap();
    for w in workers {
        w.join().unwrap();
    }
    let (recv, drop, bytes) = metrics.snapshot();
    println!("final: recv={recv} drop={drop} bytes={bytes}");
}
