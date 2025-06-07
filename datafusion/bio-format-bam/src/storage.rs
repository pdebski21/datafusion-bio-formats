use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_remote_stream, get_storage_type,
};
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use noodles::bam;
use noodles::bam::Record;
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
) -> Result<Reader<MultithreadedReader<File>>, Error> {
    let reader = File::open(file_path)
        .map(|f| {
            noodles_bgzf::MultithreadedReader::with_worker_count(
                NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(bam::io::Reader::from);
    reader
}

pub enum BamReader {
    Local(Reader<MultithreadedReader<File>>),
    Remote(
        bam::r#async::io::Reader<
            noodles_bgzf::AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>,
        >,
    ),
}

impl BamReader {
    pub async fn new(
        file_path: String,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> Self {
        let storage_type = get_storage_type(file_path.clone());
        match storage_type {
            StorageType::LOCAL => {
                let thread_num = thread_num.unwrap_or(1);
                let reader = get_local_bam_reader(file_path, thread_num).await.unwrap();
                BamReader::Local(reader)
            }
            StorageType::AZBLOB | StorageType::GCS | StorageType::S3 => {
                let object_storage_options = object_storage_options
                    .expect("ObjectStorageOptions must be provided for remote storage");
                let reader = get_remote_bam_reader(file_path, object_storage_options)
                    .await
                    .unwrap();
                BamReader::Remote(reader)
            }
            _ => panic!("Unsupported storage type for BAM file: {:?}", storage_type),
        }
    }
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            BamReader::Local(reader) => {
                reader.read_header().unwrap();
                stream::iter(reader.records()).boxed()
            }
            BamReader::Remote(reader) => {
                reader.read_header().await.unwrap();
                reader.records().boxed()
            }
        }
    }
}
