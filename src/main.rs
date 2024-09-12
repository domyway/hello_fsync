mod traces;

use anyhow::Result;
use byteorder::{BigEndian, WriteBytesExt};
use clap::{command, Parser};
use crc32fast::Hasher;
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use opentelemetry::global;
use tokio::sync::Mutex;
use tokio::{task, try_join};

/// Struct to parse command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Size of data to write in each iteration (in bytes)
    #[arg(long, default_value_t = 4 * 1024)] // Default 4K
    file_size: usize,

    /// Number of concurrent tasks
    #[arg(long, default_value_t = 8)] // Default 8 tasks
    num_tasks: usize,

    /// Number of fsync operations per task
    #[arg(long, default_value_t = 100000)] // Default 100000 iterations
    num_iterations: usize,

    /// Path to the test file
    #[arg(long, default_value = "/data/test_fsync_benchmark")]
    file_path: String,

    /// use trace benchmark
    #[arg(long, default_value = "n")]
    server: String,
    #[arg(long, default_value = "n")]
    client: String,
    #[arg(long, default_value_t = 120)]
    client_time: usize,
    #[arg(long, default_value = "n")]
    local: String,
}

async fn fsync_benchmark(
    file_lock: Arc<Mutex<Writer>>,
    file_size: usize,
    num_iterations: usize,
) -> Result<()> {
    let mut tasks = vec![];
    for _ in 0..num_iterations {
        // Write some data to the file
        let file_lock = file_lock.clone();
        let task = tokio::spawn(async move {
            let data = vec![0u8; file_size];
            let mut file = file_lock.lock().await;
            file.write(&data, true).expect("Failed to write data");
        });

        tasks.push(task);
    }

    for task in tasks {
        _ = task.await.expect("Task failed");
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    // Parse command-line arguments
    let args = Args::parse();
    let total_time = 0u128;
    if args.local.eq("y") {
        let file_path = args.file_path;
        // delete the file if it already exists
        if std::path::Path::new(&file_path).exists() {
            remove_file(&file_path).expect("Failed to delete file");
        }

        let init_size = 1024 * 1024 * 128; // 128 MB
        let buffer_size = 1024 * 16; // 16 KB
        let writer = Writer::new(file_path, init_size, buffer_size).expect("Failed to create writer");
        let mut tasks = Vec::new();
        let file_lock = Arc::new(Mutex::new(writer));
        // Launch multiple tasks to perform fsync in parallel
        for _ in 0..args.num_tasks {
            let file_size = args.file_size;
            let num_iterations = args.num_iterations;
            let file_lock_clone = file_lock.clone();
            let task = task::spawn(async move {
                fsync_benchmark(file_lock_clone, file_size, num_iterations).await
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete and gather total fsync times
        for task in tasks {
            _ = task.await.expect("Task failed");
        }
    }

    if args.client.eq("y") {
        let benchmark_task = tokio::spawn(async move {
            let _ = crate::traces::init_tracer_otlp().unwrap();
            let _tracer = global::tracer("fsync_benchmark");
            for i in 0..args.num_tasks {
                let mut tasks = Vec::new();
                // Launch multiple tasks to perform fsync in parallel
                for _ in 0..args.num_iterations {
                    let task = task::spawn(async move {
                        crate::traces::create_span_with_trace_id("d4d2f8a41ebd9810538d5bd72c520889").await;
                    });
                    tasks.push(task);
                }
                // Wait for all tasks to complete and gather total fsync times
                for task in tasks {
                    _ = task.await.expect("Task failed");
                }
                println!("start service num_tasks {i}");
            }
        });

        try_join!(benchmark_task).expect("benchmark_task Failed");
        tokio::time::sleep(Duration::from_secs(args.client_time as u64)).await;
    }

    if args.server.eq("y"){
        let _ = crate::traces::init_common_grpc_server().await;
        // 确保所有 trace 数据都被导出
        global::shutdown_tracer_provider();
    }


    println!(
        "Benchmark completed: Total time for all fsync operations = {} µs",
        total_time
    );
}

const SOFT_MAX_BUFFER_LEN: usize = 1024 * 128; // 128KB

pub const FILE_TYPE_IDENTIFIER_LEN: usize = 13;
type FileTypeIdentifier = [u8; FILE_TYPE_IDENTIFIER_LEN];
const FILE_TYPE_IDENTIFIER: &FileTypeIdentifier = b"OPENOBSERVEV2";

pub struct Writer {
    path: PathBuf,
    f: BufWriter<File>,
    bytes_written: usize,
    uncompressed_bytes_written: usize,
    buffer: Vec<u8>,
}

impl Writer {
    pub fn new(path: impl Into<PathBuf>, init_size: u64, buffer_size: usize) -> Result<Self> {
        let path = path.into();
        create_dir_all(path.parent().unwrap())?;
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        if init_size > 0 {
            f.set_len(init_size)?;
            f.seek(SeekFrom::Start(0))?;
        }

        if let Err(e) = f.write_all(FILE_TYPE_IDENTIFIER) {
            _ = remove_file(&path);
            return Err(anyhow::anyhow!(
                "failed to write file type identifier: {}",
                e
            ));
        }
        let bytes_written = FILE_TYPE_IDENTIFIER.len();

        if let Err(e) = f.sync_all() {
            _ = remove_file(&path);
            return Err(anyhow::anyhow!("failed to sync file: {}", e));
        }

        Ok(Self {
            path,
            f: BufWriter::with_capacity(buffer_size, f),
            bytes_written,
            uncompressed_bytes_written: bytes_written,
            buffer: Vec::with_capacity(8 * 1204),
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Return the number of bytes written (compressed, uncompressed) to the
    /// file.
    pub fn size(&self) -> (usize, usize) {
        (self.bytes_written, self.uncompressed_bytes_written)
    }

    /// write the data to the wal file
    pub fn write(&mut self, data: &[u8], sync: bool) -> Result<()> {
        // Ensure the write buffer is always empty before using it.
        self.buffer.clear();
        // And shrink the buffer below the maximum permitted size should the odd
        // large batch grow it. This is a NOP if the size is less than
        // SOFT_MAX_BUFFER_LEN already.
        self.buffer.shrink_to(SOFT_MAX_BUFFER_LEN);

        // Only designed to support chunks up to `u32::max` bytes long.
        let uncompressed_len = data.len();
        u32::try_from(uncompressed_len)?;

        // The chunk header is two u32 values, so write a dummy u64 value and
        // come back to fill them in later.
        self.buffer.write_u64::<BigEndian>(0)?;

        // Compress the payload into the reused buffer, recording the crc hash
        // as it is wrote.
        let mut encoder = snap::write::FrameEncoder::new(HasherWrapper::new(&mut self.buffer));
        encoder.write_all(data)?;
        let (checksum, buf) = encoder
            .into_inner()
            .expect("cannot fail to flush to a Vec")
            .finalize();

        // Adjust the compressed length to take into account the u64 padding above.
        let compressed_len = buf.len() - std::mem::size_of::<u64>();
        let compressed_len = u32::try_from(compressed_len)?;

        // Go back and write the chunk header values
        let mut buf = std::io::Cursor::new(buf);
        buf.set_position(0);

        buf.write_u32::<BigEndian>(checksum)?;
        buf.write_u32::<BigEndian>(compressed_len)?;

        // Write the entire buffer to the file
        let buf = buf.into_inner();
        let bytes_written = buf.len();

        self.f.write_all(buf)?;

        // fsync the fd
        if sync {
            self.sync()?;
        }

        self.bytes_written += bytes_written;
        self.uncompressed_bytes_written += uncompressed_len;

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.f.flush()?;
        self.f.get_ref().sync_data()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.sync()
    }
}

/// A [`HasherWrapper`] acts as a [`Write`] decorator, recording the crc
/// checksum of the data wrote to the inner [`Write`] implementation.
struct HasherWrapper<W> {
    inner: W,
    hasher: Hasher,
}

impl<W> HasherWrapper<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Hasher::default(),
        }
    }

    fn finalize(self) -> (u32, W) {
        (self.hasher.finalize(), self.inner)
    }
}

impl<W> Write for HasherWrapper<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
