use foundation_track::module2::orbital_record::{OrbitalRecord, run_conjunction_screen};

fn main() {
    let catalog: Vec<OrbitalRecord> = (0..1000)
        .map(|i| OrbitalRecord::new(i, 400.0 + (i as f64 * 0.3)))
        .collect();

    let alerts = run_conjunction_screen(&catalog, 5.0);
    println!("{} conjunction alerts generated", alerts.len());
    for alert in alerts {
        println!("{:?}", alert);
    }
}
