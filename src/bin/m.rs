// #![feature(portable_simd)]
#![deny(elided_lifetimes_in_paths)]
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::io::prelude::*;
use std::sync::Arc;
use tikv_jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
struct Measurement {
    min: i64,
    max: i64,
    count: u64,
    sum: i64,
}
unsafe fn parse(str: &str) -> i64 {
    let str = str.as_ptr();
    let mut offset = 0;
    let mut mul = 1;
    if *str.offset(0) == b'-' {
        offset = 1;
        mul = -1;
    };
    let tens = (*str.offset(offset) - 48) as i64 * 100;
    let units = (*str.offset(offset + 1) - 48) as i64 * 10;
    let dec = (*str.offset(offset + 3) - 48) as i64;
    (tens + units + dec) * mul
}
unsafe fn split_once(s: &str, c: char) -> Result<(&str, &str), ()> {
    let ptr = s.as_ptr();
    let len = s.len();
    for i in (0..len).rev() {
        if *ptr.offset(i as isize) == c as u8 {
            return Ok((s.get_unchecked(0..i), s.get_unchecked(i + 1..len)));
        }
    }
    Err(())
}

fn process(
    line: &str,
    map: &mut std::collections::HashMap<String, Measurement, rustc_hash::FxBuildHasher>,
) {
    let (name, meas) = unsafe { split_once(line, ';').unwrap() };
    let meas = unsafe { parse(meas) };
    // println!("{} {}", name, meas);
    let val = map.get_mut(name);
    match val {
        Some(e) => {
            e.max = e.max.max(meas);
            e.min = e.min.min(meas);
            e.count += 1;
            e.sum += meas;
        }
        None => {
            map.insert(
                name.to_string(),
                Measurement {
                    min: meas,
                    max: meas,
                    count: 1,
                    sum: meas,
                },
            );
        }
    }
}

fn main() {
    let mut args = std::env::args();
    args.next();
    // let threads = std::thread::available_parallelism().unwrap().get();
    let src_path = args.next().unwrap();
    let dest_path = args.next().unwrap();
    let src_file = Arc::new(std::fs::File::open(&src_path).unwrap());
    let src = Arc::new(unsafe { memmap::MmapOptions::new().map(&src_file).unwrap() });
    let src_str = Arc::new(simdutf8::basic::from_utf8(&src).unwrap());
    let mut dest = std::fs::File::create(dest_path).unwrap();
    // let mut map = FxHashMap::<String, Measurement>::default();
    // let (discard_tx, discard_rx) = std::sync::mpsc::channel::<String>();
    // let (result_tx, result_rx) = std::sync::mpsc::channel::<FxHashMap<String, Measurement>>();
    // let bytes_per_thread = src_file.metadata().unwrap().size() as usize / threads;
    let map = src_str
        .par_lines()
        .fold(
            || FxHashMap::<String, Measurement>::default(),
            |mut acc, line| {
                process(line, &mut acc);
                acc
            },
        )
        .reduce(
            || FxHashMap::<String, Measurement>::default(),
            |mut acc, map| {
                for (name, meas) in map {
                    let val = acc.get_mut(&name);
                    match val {
                        Some(e) => {
                            e.max = e.max.max(meas.max);
                            e.min = e.min.min(meas.min);
                            e.count += meas.count;
                            e.sum += meas.sum;
                        }
                        None => {
                            acc.insert(name, meas);
                        }
                    }
                }
                acc
            },
        );
    // println!("main2");
    write!(dest, "{{").unwrap();
    let mut out = map.into_iter().collect::<Vec<_>>();
    out.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));
    for (name, meas) in out {
        write!(
            &mut dest,
            "{name}={min:.1}/{mean:.1}/{max:.1}, ",
            min = meas.min as f64 / 10.0,
            max = meas.max as f64 / 10.0,
            mean = ((meas.sum as f64 / 10.0) / meas.count as f64)
        )
        .unwrap();
    }
    write!(dest, "}}").unwrap();
}
