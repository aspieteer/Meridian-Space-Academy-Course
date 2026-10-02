use std::{
    ffi::OsString,
    io,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    thread,
};

use super::super::{
    PAGE_HEADER_SIZE, PAGE_SIZE,
    part1::page::{PageFile, PageType},
    part2::buffer_pool::BufferPool,
};

pub const DEFAULT_PAGE_COUNT: usize = 1024;
pub const DEFAULT_POOL_SIZE: usize = 256;
pub const DEFAULT_WORKER_COUNT: usize = 8;

#[derive(Debug, Clone, Copy)]
pub struct DemoConfig {
    pub page_count: usize,
    pub pool_size: usize,
    pub worker_count: usize,
}

impl Default for DemoConfig {
    fn default() -> Self {
        Self {
            page_count: DEFAULT_PAGE_COUNT,
            pool_size: DEFAULT_POOL_SIZE,
            worker_count: DEFAULT_WORKER_COUNT,
        }
    }
}

/// Parse `[path] [page-count] [pool-size] [worker-count]` and run the demo.
pub fn run_buffer_pool_demo_from_args(args: impl IntoIterator<Item = OsString>) -> io::Result<()> {
    let mut args = args.into_iter();
    let path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("buffer-pool-demo.db"));
    let defaults = DemoConfig::default();
    let config = DemoConfig {
        page_count: parse_number(args.next(), "page count", defaults.page_count)?,
        pool_size: parse_number(args.next(), "pool size", defaults.pool_size)?,
        worker_count: parse_number(args.next(), "worker count", defaults.worker_count)?,
    };

    if args.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: database-track [path] [page-count] [pool-size] [worker-count]",
        ));
    }

    run_buffer_pool_demo(&path, config)
}

/// Concurrently allocate and modify pages, then reopen and verify them.
pub fn run_buffer_pool_demo(path: &Path, config: DemoConfig) -> io::Result<()> {
    validate_config(config)?;
    let worker_count = config
        .worker_count
        .min(config.pool_size)
        .min(config.page_count);
    let pool = BufferPool::new(config.pool_size, PageFile::open(path, PAGE_SIZE)?);

    let written_pages = thread::scope(|scope| -> io::Result<Vec<(u32, usize)>> {
        let mut workers = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let pool = pool.clone();
            workers.push(scope.spawn(move || -> io::Result<Vec<(u32, usize)>> {
                let mut pages = Vec::new();
                for sequence in (worker_id..config.page_count).step_by(worker_count) {
                    let mut page = pool.allocate_page(PageType::Data)?;
                    let message = record_message(sequence);
                    let mut data = page.read_page()?;
                    let end = PAGE_HEADER_SIZE + message.len();
                    data[PAGE_HEADER_SIZE..end].copy_from_slice(message.as_bytes());
                    page.modify_page(data)?;
                    pages.push((page.page_id(), sequence));
                }
                Ok(pages)
            }));
        }

        let mut pages = Vec::with_capacity(config.page_count);
        for worker in workers {
            pages.extend(worker.join().map_err(worker_panicked)??);
        }
        Ok(pages)
    })?;

    pool.flush_all()?;
    drop(pool);

    // Reopen the file so verification cannot accidentally use cached frames.
    let pool = BufferPool::new(config.pool_size, PageFile::open(path, PAGE_SIZE)?);
    let written_pages = Arc::new(written_pages);
    let verified = thread::scope(|scope| -> io::Result<usize> {
        let mut workers = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let pool = pool.clone();
            let written_pages = Arc::clone(&written_pages);
            workers.push(scope.spawn(move || -> io::Result<usize> {
                let mut verified = 0;
                for position in (worker_id..written_pages.len()).step_by(worker_count) {
                    let (page_id, sequence) = written_pages[position];
                    let expected = record_message(sequence);
                    let page = pool.fetch_page(page_id)?;
                    let data = page.read_page()?;
                    let end = PAGE_HEADER_SIZE + expected.len();
                    if &data[PAGE_HEADER_SIZE..end] != expected.as_bytes() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("page {page_id} contains an unexpected record"),
                        ));
                    }
                    verified += 1;
                }
                Ok(verified)
            }));
        }

        let mut verified = 0;
        for worker in workers {
            verified += worker.join().map_err(worker_panicked)??;
        }
        Ok(verified)
    })?;

    println!(
        "concurrently wrote and verified {verified} pages with {worker_count} workers and {} buffer frames",
        config.pool_size
    );
    println!(
        "{} total pages stored in {}",
        pool.page_count()?,
        path.display()
    );

    Ok(())
}

fn validate_config(config: DemoConfig) -> io::Result<()> {
    if config.page_count == 0 || config.pool_size == 0 || config.worker_count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "page count, pool size, and worker count must all be greater than zero",
        ));
    }
    Ok(())
}

fn record_message(sequence: usize) -> String {
    format!("buffered record {sequence}")
}

fn parse_number<T>(value: Option<OsString>, name: &str, default: T) -> io::Result<T>
where
    T: FromStr,
{
    let Some(value) = value else {
        return Ok(default);
    };
    value
        .to_str()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid {name}: {}", value.to_string_lossy()),
            )
        })
}

fn worker_panicked(_: Box<dyn std::any::Any + Send>) -> io::Error {
    io::Error::other("buffer-pool worker thread panicked")
}
