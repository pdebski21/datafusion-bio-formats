use async_stream::try_stream;
use bytes::Bytes;
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async,
};
use futures::StreamExt;
use futures::stream::BoxStream;
use log::{debug, info};
use noodles::bed;
use noodles::bed::Record;
use noodles_bgzf::{AsyncReader, MultithreadedReader};
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Error};
use std::num::NonZero;
use tokio::io::AsyncReadExt;
use tokio::runtime::Handle;
use tokio::task::spawn_blocking;
use tokio_util::io::{StreamReader, SyncIoBridge};

pub async fn get_remote_bed_bgzf_reader<const N: usize>(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<bed::io::Reader<N, BufReader<Cursor<Vec<u8>>>>, Error> {
    let mut inner = get_remote_stream_bgzf_async(file_path.clone(), object_storage_options).await?;
    let mut buffer = Vec::new();
    inner.read_to_end(&mut buffer).await?;
    let cursor = Cursor::new(buffer);

    // Create a buffered reader
    let buf_reader = BufReader::new(cursor);
    let reader = bed::io::Reader::<N, _>::new(buf_reader);
    Ok(reader)
}

pub async fn get_remote_bed_reader<const N: usize>(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<bed::io::Reader<N, BufReader<Cursor<Vec<u8>>>>, Error> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options).await?;
    let mut stream_reader = StreamReader::new(stream);
    let mut buffer = Vec::new();
    stream_reader.read_to_end(&mut buffer).await?;
    let cursor = Cursor::new(buffer);

    // Create a buffered reader
    let buf_reader = BufReader::new(cursor);
    let reader = bed::io::Reader::<N, _>::new(buf_reader);
    Ok(reader)
}

pub fn get_local_bed_bgzf_reader<const N: usize>(
    file_path: String,
    thread_num: usize,
) -> Result<noodles::bed::Reader<N, MultithreadedReader<File>>, Error> {
    debug!(
        "Reading BED file from local storage with {} threads",
        thread_num
    );
    File::open(file_path)
        .map(|f| {
            noodles_bgzf::MultithreadedReader::with_worker_count(
                NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(bed::io::Reader::<N, _>::new)
}

pub fn get_local_bed_reader<const N: usize>(
    file_path: String,
) -> Result<bed::Reader<N, BufReader<File>>, Error> {
    debug!("Reading BED file from local storage with sync reader");
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    Ok(bed::io::Reader::<N, _>::new(reader))
}

pub enum BedRemoteReader<const N: usize> {
    BGZF(bed::io::Reader<N, BufReader<Cursor<Vec<u8>>>>),
    PLAIN(bed::io::Reader<N, BufReader<Cursor<Vec<u8>>>>),
}

macro_rules! impl_bed_remote_reader {
    ($($n:expr),*) => {
        $(
            impl BedRemoteReader<$n> {
                pub async fn new(file_path: String, object_storage_options: ObjectStorageOptions) -> Self {
                    info!("Creating remote BED reader: {}", object_storage_options);
                    let compression_type = get_compression_type(
                        file_path.clone(),
                        object_storage_options.clone().compression_type,
                    );
                    match compression_type {
                        CompressionType::BGZF => {
                            let reader = get_remote_bed_bgzf_reader::<$n>(file_path, object_storage_options).await.unwrap();
                            BedRemoteReader::BGZF(reader)
                        }
                        CompressionType::NONE => {
                            let reader = get_remote_bed_reader::<$n>(file_path, object_storage_options).await.unwrap();
                            BedRemoteReader::PLAIN(reader)
                        }
                        _ => panic!("Compression type not supported."),
                    }
                }
            }
            impl BedRemoteReader<$n> {
                pub fn get_reader(&mut self) -> &mut bed::io::Reader<$n, BufReader<Cursor<Vec<u8>>>> {
                    match self {
                        BedRemoteReader::BGZF(reader) => reader,
                        BedRemoteReader::PLAIN(reader) => reader,
                    }
                }
                pub async fn read_records_stream(&mut self) -> BoxStream<'_, Result<bed::Record<$n>, Error>> {
                    let reader = self.get_reader();
                    try_stream! {
                        loop {
                            let mut record = bed::Record::default();
                            match reader.read_record(&mut record) {
                                Ok(0) => break, // EOF
                                Ok(_) => yield record,
                                Err(e) => Err(e)?,
                            }
                        }
                    }.boxed()
    }

            }

        )*
    };
}

// Generate implementations for N = 3, 4, 5, 6
impl_bed_remote_reader!(3, 4, 5, 6);
