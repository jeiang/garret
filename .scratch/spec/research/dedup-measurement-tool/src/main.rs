// chunkmeter: stream NARs from the nix store through FastCDC (v2020) at several
// parameter sets in a single pass, emitting per-chunk (blake3, size) records as JSONL.
//
// Usage: chunkmeter <paths-file> <out.jsonl> [--jobs N] [--zstd]
//   paths-file lines: "<narSize> <storePath>"
//   --zstd: additionally record per-chunk zstd-3 compressed sizes and a
//           whole-NAR zstd-3 size (set "nar-zstd3"). CPU-heavy; use on samples.
//
// NARs below the 64 KiB threshold (attic's nar-size-threshold) are not chunked:
// they are emitted as a single whole-NAR record with set "whole".

use std::io::{BufRead, BufWriter, Read, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};

const THRESHOLD: u64 = 65536;
const PARAM_SETS: &[(u32, u32, u32, &str)] = &[
    (16 * 1024, 64 * 1024, 256 * 1024, "16-64-256"),
    (64 * 1024, 256 * 1024, 1024 * 1024, "64-256-1024"),
    (256 * 1024, 1024 * 1024, 4096 * 1024, "256-1024-4096"),
    (512 * 1024, 2048 * 1024, 8192 * 1024, "512-2048-8192"),
];

struct ChanReader {
    rx: mpsc::Receiver<Arc<Vec<u8>>>,
    cur: Option<Arc<Vec<u8>>>,
    pos: usize,
}

impl Read for ChanReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if let Some(cur) = &self.cur {
                let rem = cur.len() - self.pos;
                if rem > 0 {
                    let n = rem.min(buf.len());
                    buf[..n].copy_from_slice(&cur[self.pos..self.pos + n]);
                    self.pos += n;
                    return Ok(n);
                }
                self.cur = None;
            }
            match self.rx.recv() {
                Ok(b) => {
                    self.cur = Some(b);
                    self.pos = 0;
                }
                Err(_) => return Ok(0), // EOF
            }
        }
    }
}

struct CountWriter(u64);
impl Write for CountWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn hex16(hash: &blake3::Hash) -> String {
    hash.as_bytes()[..16].iter().map(|b| format!("{:02x}", b)).collect()
}

fn dump_nar(path: &str) -> std::process::Child {
    Command::new("nix-store")
        .arg("--dump")
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nix-store --dump")
}

fn process_small(path: &str, out: &Mutex<BufWriter<std::fs::File>>, zstd_mode: bool) {
    let mut child = dump_nar(path);
    let mut data = Vec::new();
    child.stdout.take().unwrap().read_to_end(&mut data).unwrap();
    if !child.wait().unwrap().success() {
        eprintln!("WARN: dump failed for {}", path);
        return;
    }
    let h = hex16(&blake3::hash(&data));
    let z = if zstd_mode {
        format!(",\"zsize\":{}", zstd::bulk::compress(&data, 3).unwrap().len())
    } else {
        String::new()
    };
    let line = format!(
        "{{\"path\":\"{}\",\"set\":\"whole\",\"nar_size\":{}{},\"chunks\":[[\"{}\",{}]]}}\n",
        path,
        data.len(),
        z,
        h,
        data.len()
    );
    out.lock().unwrap().write_all(line.as_bytes()).unwrap();
}

fn process_large(path: &str, out: &Mutex<BufWriter<std::fs::File>>, zstd_mode: bool) {
    let mut child = dump_nar(path);
    let mut stdout = child.stdout.take().unwrap();

    let n_consumers = PARAM_SETS.len() + if zstd_mode { 1 } else { 0 };
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..n_consumers {
        let (tx, rx) = mpsc::sync_channel::<Arc<Vec<u8>>>(4);
        txs.push(tx);
        rxs.push(rx);
    }

    let mut lines: Vec<String> = Vec::new();
    std::thread::scope(|s| {
        // reader
        s.spawn(move || {
            let mut buf = vec![0u8; 4 * 1024 * 1024];
            loop {
                let mut filled = 0;
                while filled < buf.len() {
                    match stdout.read(&mut buf[filled..]).unwrap() {
                        0 => break,
                        n => filled += n,
                    }
                }
                if filled == 0 {
                    break;
                }
                let arc = Arc::new(buf[..filled].to_vec());
                for tx in &txs {
                    let _ = tx.send(arc.clone());
                }
                if filled < buf.len() {
                    break;
                }
            }
            drop(txs);
        });

        let mut handles = Vec::new();
        let mut rx_iter = rxs.into_iter();
        for (min, avg, max, name) in PARAM_SETS {
            let rx = rx_iter.next().unwrap();
            handles.push(s.spawn(move || {
                let reader = ChanReader { rx, cur: None, pos: 0 };
                let cdc = fastcdc::v2020::StreamCDC::new(reader, *min, *avg, *max);
                let mut total: u64 = 0;
                let mut ztotal: u64 = 0;
                let mut chunks = String::new();
                let mut first = true;
                for res in cdc {
                    let chunk = res.expect("chunking error");
                    let h = hex16(&blake3::hash(&chunk.data));
                    total += chunk.length as u64;
                    if !first {
                        chunks.push(',');
                    }
                    first = false;
                    if zstd_mode {
                        let cz = zstd::bulk::compress(&chunk.data, 3).unwrap().len();
                        ztotal += cz as u64;
                        chunks.push_str(&format!("[\"{}\",{},{}]", h, chunk.length, cz));
                    } else {
                        chunks.push_str(&format!("[\"{}\",{}]", h, chunk.length));
                    }
                }
                let z = if zstd_mode { format!(",\"zsize\":{}", ztotal) } else { String::new() };
                format!(
                    "{{\"path\":\"{}\",\"set\":\"{}\",\"nar_size\":{}{},\"chunks\":[{}]}}\n",
                    path, name, total, z, chunks
                )
            }));
        }
        if zstd_mode {
            let rx = rx_iter.next().unwrap();
            handles.push(s.spawn(move || {
                let mut reader = ChanReader { rx, cur: None, pos: 0 };
                let mut enc = zstd::stream::Encoder::new(CountWriter(0), 3).unwrap();
                let total = std::io::copy(&mut reader, &mut enc).unwrap();
                let cw = enc.finish().unwrap();
                format!(
                    "{{\"path\":\"{}\",\"set\":\"nar-zstd3\",\"nar_size\":{},\"zsize\":{},\"chunks\":[]}}\n",
                    path, total, cw.0
                )
            }));
        }
        for h in handles {
            lines.push(h.join().unwrap());
        }
    });

    if !child.wait().unwrap().success() {
        eprintln!("WARN: dump failed for {}", path);
        return;
    }
    let mut o = out.lock().unwrap();
    for l in &lines {
        o.write_all(l.as_bytes()).unwrap();
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let paths_file = &args[1];
    let out_file = &args[2];
    let mut jobs = 3usize;
    let mut zstd_mode = false;
    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--jobs" => {
                jobs = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--zstd" => {
                zstd_mode = true;
                i += 1;
            }
            other => panic!("unknown arg {}", other),
        }
    }

    let entries: Vec<(u64, String)> = std::io::BufReader::new(std::fs::File::open(paths_file).unwrap())
        .lines()
        .map(|l| {
            let l = l.unwrap();
            let (sz, p) = l.split_once(' ').unwrap();
            (sz.parse().unwrap(), p.to_string())
        })
        .collect();

    let out = Mutex::new(BufWriter::new(std::fs::File::create(out_file).unwrap()));
    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let n = entries.len();

    std::thread::scope(|s| {
        for _ in 0..jobs {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= n {
                    break;
                }
                let (sz, p) = &entries[i];
                if *sz < THRESHOLD {
                    process_small(p, &out, zstd_mode);
                } else {
                    process_large(p, &out, zstd_mode);
                }
                let d = done.fetch_add(1, Ordering::SeqCst) + 1;
                if d % 250 == 0 {
                    eprintln!("progress: {}/{}", d, n);
                }
            });
        }
    });
    out.lock().unwrap().flush().unwrap();
    eprintln!("done: {} paths", n);
}
