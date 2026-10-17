use std::{env, io};

use database_track::module1::part2::demo::run_buffer_pool_demo_from_args;

fn main() -> io::Result<()> {
    run_buffer_pool_demo_from_args(env::args_os().skip(1))
}
