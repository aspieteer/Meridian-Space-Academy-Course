use std::sync::{Arc, Mutex};

use foundation_track::module2::session_table::SessionTable;

fn accumu_counter_as_dumb(count: usize) {
    assert!(count.is_multiple_of(4), "count should be multiples of 4.");

    let command_count = Arc::new(Mutex::new(0_u64));

    let handles = (0..4)
        .map(|_| {
            let counter = Arc::clone(&command_count);
            std::thread::spawn(move || {
                for _ in 0..(count / 4) {
                    let mut count = counter.lock().unwrap();
                    *count += 1;
                }
            })
        })
        .collect::<Vec<_>>();

    for h in handles {
        h.join().unwrap();
    }

    println!("command processed: {}", command_count.lock().unwrap());
}

fn drop_guard_before_relocking() {
    let data = Mutex::new(vec![1_u32, 2, 3]);

    // BUG: guard lives to end of the if block, holding lock during the push
    {
        let guard = data.lock().unwrap();
        if guard.contains(&2) {
            drop(guard);
            data.lock().unwrap().push(4);
        }
        // Without the explicit drop, this deadlocks: the guard is still
        // alive when we try to lock again at data.lock().unwrap().push(4)
    }

    println!("{:?}", data.lock().unwrap());
}

fn rwlock_impl(table: &Arc<SessionTable>) {
    let readers = (0..50).map(|_| {
        let table = Arc::clone(table);
        std::thread::spawn(move || {
            // All reader threads can hold the read lock simultaneously.
            println!("{:?}", table.query_session(25544));
        })
    });

    let t1 = Arc::clone(table);
    let w1 = std::thread::spawn(move || {
        if let Some(val) = t1.register(25544, "gs-svalbard".to_string()) {
            eprintln!(
                "Registering new k-v pair, turns out there's already an old value: {}",
                val
            );
        }
    });

    let t2 = Arc::clone(table);
    let w2 = std::thread::spawn(move || {
        if let Some(val) = t2.register(25545, "gs-ruchetee".to_string()) {
            eprintln!(
                "Registering new k-v pair, turns out there's already an old value: {}",
                val
            );
        }
    });

    w1.join().unwrap();
    w2.join().unwrap();

    for r in readers {
        r.join().unwrap();
    }
}

fn main() {
    accumu_counter_as_dumb(100);

    drop_guard_before_relocking();

    let table = SessionTable::new();
    let table = Arc::new(table);
    rwlock_impl(&table);
}

#[test]
#[should_panic]
fn count_odd() {
    accumu_counter_as_dumb(97);
}
