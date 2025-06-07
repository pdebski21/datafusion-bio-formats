use datafusion_bio_format_core::object_storage::{ObjectStorageOptions, get_remote_stream};
use noodles::bam;
use noodles::bam::io::Reader;
use noodles_bgzf;
use noodles_bgzf::MultithreadedReader;
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::Error;
use std::num::NonZero;
use tokio_util::io::StreamReader;

pub async fn get_remote_bam_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<
    bam::r#async::io::Reader<
        noodles_bgzf::AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>,
    >,
    Error,
> {
    let stream = get_remote_stream(file_path.clone(), object_storage_options).await?;
    let reader = bam::r#async::io::Reader::new(StreamReader::new(stream));
    Ok(reader)
}

pub async fn get_local_bam_reader(
    file_path: String,
    thread_num: usize,
) -> Reader<Result<MultithreadedReader<File>, Error>> {
    let decoder = File::open(file_path).map(|f| {
        noodles_bgzf::MultithreadedReader::with_worker_count(NonZero::new(thread_num).unwrap(), f)
    });
    let reader = Reader::from(decoder);
    reader
}

pub enum BamReader {
    Local(Reader<noodles_bgzf::Reader<File>>),
    Remote(
        bam::r#async::io::Reader<
            noodles_bgzf::AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>,
        >,
    ),
}

impl BamReader {}
