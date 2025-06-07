use bytes::Bytes;
use datafusion::arrow;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_bio_format_core::object_storage::StorageType;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_core::object_storage::{
    get_compression_type, get_remote_stream, get_remote_stream_bgzf, get_storage_type,
};
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use log::debug;
use log::info;
use noodles::vcf;
use noodles::vcf::io::Reader;
use noodles::vcf::{Header, Record};
use noodles_bgzf::{AsyncReader, MultithreadedReader};
use opendal::FuturesBytesStream;
use std::fs::File;
use std::io::Error;
use std::num::NonZero;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio_util::io::StreamReader;

pub async fn get_remote_vcf_bgzf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>> {
    let inner = get_remote_stream_bgzf(file_path.clone(), object_storage_options)
        .await
        .unwrap();
    let reader = vcf::r#async::io::Reader::new(inner);
    reader
}

pub async fn get_remote_vcf_reader(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>> {
    let inner = StreamReader::new(
        get_remote_stream(file_path.clone(), object_storage_options)
            .await
            .unwrap(),
    );
    vcf::r#async::io::Reader::new(inner)
}

pub fn get_local_vcf_bgzf_reader(
    file_path: String,
    thread_num: usize,
) -> Result<Reader<MultithreadedReader<File>>, Error> {
    debug!(
        "Reading VCF file from local storage with {} threads",
        thread_num
    );
    File::open(file_path)
        .map(|f| {
            noodles_bgzf::MultithreadedReader::with_worker_count(
                NonZero::new(thread_num).unwrap(),
                f,
            )
        })
        .map(vcf::io::Reader::new)
}

pub async fn get_local_vcf_reader(
    file_path: String,
) -> Result<vcf::r#async::io::Reader<BufReader<tokio::fs::File>>, Error> {
    debug!("Reading VCF file from local storage with async reader");
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(BufReader::new)
        .map(vcf::r#async::io::Reader::new)?;
    Ok(reader)
}

pub async fn get_local_vcf_header(
    file_path: String,
    thread_num: usize,
) -> Result<vcf::Header, Error> {
    let compression_type = get_compression_type(file_path.clone(), None);
    let header = match compression_type {
        CompressionType::BGZF | CompressionType::GZIP => {
            let mut reader = get_local_vcf_bgzf_reader(file_path, thread_num)?;
            reader.read_header()?
        }
        CompressionType::NONE => {
            let mut reader = get_local_vcf_reader(file_path).await?;
            reader.read_header().await?
        }
        _ => panic!("Compression type not supported."),
    };
    Ok(header)
}

pub async fn get_remote_vcf_header(
    file_path: String,
    object_storage_options: ObjectStorageOptions,
) -> Result<vcf::Header, Error> {
    info!(
        "Getting remote VCF header with options: {}",
        object_storage_options
    );
    let compression_type = get_compression_type(
        file_path.clone(),
        object_storage_options.clone().compression_type,
    );
    let header = match compression_type {
        CompressionType::BGZF | CompressionType::GZIP => {
            let mut reader = get_remote_vcf_bgzf_reader(file_path, object_storage_options).await;
            reader.read_header().await?
        }
        CompressionType::NONE => {
            let mut reader = get_remote_vcf_reader(file_path, object_storage_options).await;
            reader.read_header().await?
        }
        _ => panic!("Compression type not supported."),
    };
    Ok(header)
}

pub async fn get_header(
    file_path: String,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Result<vcf::Header, Error> {
    let storage_type = get_storage_type(file_path.clone());
    let header = match storage_type {
        StorageType::LOCAL => get_local_vcf_header(file_path, 1).await?,
        _ => get_remote_vcf_header(file_path, object_storage_options.unwrap().clone()).await?,
    };
    Ok(header)
}

pub enum VcfRemoteReader {
    BGZF(vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>),
    PLAIN(vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>),
}

impl VcfRemoteReader {
    pub async fn new(file_path: String, object_storage_options: ObjectStorageOptions) -> Self {
        info!("Creating remote VCF reader: {}", object_storage_options);
        let compression_type = get_compression_type(
            file_path.clone(),
            object_storage_options.clone().compression_type,
        );
        match compression_type {
            CompressionType::BGZF | CompressionType::GZIP => {
                let reader = get_remote_vcf_bgzf_reader(file_path, object_storage_options).await;
                VcfRemoteReader::BGZF(reader)
            }
            CompressionType::NONE => {
                let reader = get_remote_vcf_reader(file_path, object_storage_options).await;
                VcfRemoteReader::PLAIN(reader)
            }
            _ => panic!("Compression type not supported."),
        }
    }

    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfRemoteReader::BGZF(reader) => reader.read_header().await,
            VcfRemoteReader::PLAIN(reader) => reader.read_header().await,
        }
    }

    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfRemoteReader::BGZF(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
            VcfRemoteReader::PLAIN(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
        }
    }

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfRemoteReader::BGZF(reader) => reader.records().boxed(),
            VcfRemoteReader::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

pub enum VcfLocalReader {
    BGZF(Reader<MultithreadedReader<File>>),
    PLAIN(vcf::r#async::io::Reader<BufReader<tokio::fs::File>>),
}

impl VcfLocalReader {
    pub async fn new(file_path: String, thread_num: usize) -> Self {
        let compression_type = get_compression_type(file_path.clone(), None);
        match compression_type {
            CompressionType::BGZF | CompressionType::GZIP => {
                let reader = get_local_vcf_bgzf_reader(file_path, thread_num).unwrap();
                VcfLocalReader::BGZF(reader)
            }
            CompressionType::NONE => {
                let reader = get_local_vcf_reader(file_path).await.unwrap();
                VcfLocalReader::PLAIN(reader)
            }
            _ => panic!("Compression type not supported."),
        }
    }

    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfLocalReader::BGZF(reader) => reader.read_header(),
            VcfLocalReader::PLAIN(reader) => reader.read_header().await,
        }
    }

    pub fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfLocalReader::BGZF(reader) => stream::iter(reader.records()).boxed(),
            VcfLocalReader::PLAIN(reader) => reader.records().boxed(),
        }
    }

    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfLocalReader::BGZF(reader) => {
                let header = reader.read_header()?;
                Ok(get_info_fields(&header).await)
            }
            VcfLocalReader::PLAIN(reader) => {
                let header = reader.read_header().await?;
                Ok(get_info_fields(&header).await)
            }
        }
    }
}

pub async fn get_info_fields(header: &Header) -> arrow::array::RecordBatch {
    let info_fields = header.infos();
    let mut field_names = StringBuilder::new();
    let mut field_types = StringBuilder::new();
    let mut field_descriptions = StringBuilder::new();
    for (field_name, field) in info_fields {
        field_names.append_value(field_name);
        field_types.append_value(field.ty().to_string());
        field_descriptions.append_value(field.description());
    }
    // build RecordBatch
    let field_names = field_names.finish();
    let field_types = field_types.finish();
    let field_descriptions = field_descriptions.finish();
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("type", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("description", arrow::datatypes::DataType::Utf8, false),
    ]);
    let record_batch = arrow::record_batch::RecordBatch::try_new(
        SchemaRef::from(schema.clone()),
        vec![
            Arc::new(field_names),
            Arc::new(field_types),
            Arc::new(field_descriptions),
        ],
    )
    .unwrap();
    record_batch
}

pub enum VcfReader {
    Local(VcfLocalReader),
    Remote(VcfRemoteReader),
}

impl VcfReader {
    pub async fn new(
        file_path: String,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> Self {
        let storage_type = get_storage_type(file_path.clone());
        info!(
            "Storage type for VCF file {}: {:?}",
            file_path, storage_type
        );
        match storage_type {
            StorageType::LOCAL => {
                VcfReader::Local(VcfLocalReader::new(file_path, thread_num.unwrap_or(1)).await)
            }
            _ => VcfReader::Remote(
                VcfRemoteReader::new(file_path, object_storage_options.unwrap()).await,
            ),
        }
    }

    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfReader::Local(reader) => reader.read_header().await,
            VcfReader::Remote(reader) => reader.read_header().await,
        }
    }

    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match self {
            VcfReader::Local(reader) => reader.describe().await,
            VcfReader::Remote(reader) => reader.describe().await,
        }
    }

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfReader::Local(reader) => reader.read_records(),
            VcfReader::Remote(reader) => reader.read_records().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_compression_type_from_string() {
        assert!(matches!(
            CompressionType::from_string("gz".to_string()),
            CompressionType::GZIP
        ));
        assert!(matches!(
            CompressionType::from_string("bgz".to_string()),
            CompressionType::BGZF
        ));
        assert!(matches!(
            CompressionType::from_string("none".to_string()),
            CompressionType::NONE
        ));
    }

    // #[test]
    // fn test_compression_type_get_compression_type() {
    //     assert_eq!(CompressionType::GZIP.get_compression_type(), "gz");
    //     assert_eq!(CompressionType::BGZF.get_compression_type(), "bgz");
    //     assert_eq!(CompressionType::NONE.get_compression_type(), "none");
    // }

    // #[test]
    // fn test_get_file_path() {
    //     let file_path = "s3://bucket/path/to/file.vcf.bgz".to_string();
    //     assert_eq!(get_file_path(file_path), "path/to/file.vcf.bgz");
    // }

    // #[test]
    // fn test_get_compression_type() {
    //     assert!(matches!(
    //         get_compression_type("file.vcf".to_string()),
    //         CompressionType::NONE
    //     ));
    //     assert!(matches!(
    //         get_compression_type("file.vcf.gz".to_string()),
    //         CompressionType::GZIP
    //     ));
    //     assert!(matches!(
    //         get_compression_type("file.vcf.bgz".to_string()),
    //         CompressionType::BGZF
    //     ));
    // }
    #[test]
    fn test_get_storage_type_with_file_prefix() {
        let file_path = "file://path/to/file".to_string();
        assert!(matches!(get_storage_type(file_path), StorageType::LOCAL));
    }
    // #[test]
    // fn test_get_bucket_name_with_gcs() {
    //     let file_path = "gs://my-bucket/path/to/file".to_string();
    //     let expected_bucket_name = "my-bucket";
    //     assert_eq!(get_bucket_name(file_path), expected_bucket_name);
    // }
    // #[test]
    // fn test_get_bucket_name_with_azblob() {
    //     let file_path = "abfs://my-container/path/to/file".to_string();
    //     let expected_bucket_name = "my-container";
    //     assert_eq!(get_bucket_name(file_path), expected_bucket_name);
    // }
    #[test]
    fn test_get_storage_type_local() {
        let file_path = "local://path/to/file".to_string();
        assert!(matches!(get_storage_type(file_path), StorageType::LOCAL));
    }
    #[test]
    fn test_get_storage_type_remote() {
        let file_path = "s3://bucket/path/to/file".to_string();
        assert!(matches!(get_storage_type(file_path), StorageType::S3));
    }
    // #[test]
    // fn test_get_bucket_name() {
    //     let file_path = "s3://my-bucket/path/to/file".to_string();
    //     let expected_bucket_name = "my-bucket";
    //     assert_eq!(get_bucket_name(file_path), expected_bucket_name);
    // }
    #[test]
    fn test_get_local_vcf_bgzf_reader_local() {
        let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
        let thread_num = 1;
        let mut reader = get_local_vcf_bgzf_reader(file_path, thread_num).unwrap();
        let header = reader.read_header().unwrap();
        assert!(header.infos().len() > 0);
    }
    #[tokio::test]
    async fn test_get_local_vcf_header_local() {
        let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
        let thread_num = 1;
        let header = get_local_vcf_header(file_path, thread_num).await.unwrap();
        assert!(header.infos().len() > 0);
    }
    // #[tokio::test]
    // async fn test_get_header_local() {
    //     let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
    //     let header = get_header(file_path).await.unwrap();
    //     assert!(header.infos().len() > 0);
    // }
    #[tokio::test]
    async fn test_vcf_local_reader_new_bgzf() {
        let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
        let thread_num = 2;
        let vcf_reader = VcfLocalReader::new(file_path, thread_num).await;
        assert!(matches!(vcf_reader, VcfLocalReader::BGZF(_)));
    }
    #[tokio::test]
    async fn test_vcf_local_reader_read_header_bgzf() {
        let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
        let thread_num = 2;
        let mut vcf_reader = VcfLocalReader::new(file_path, thread_num).await;
        let header = vcf_reader.read_header().await.unwrap();
        assert!(header.infos().len() > 0);
    }
    #[tokio::test]
    async fn test_vcf_local_reader_describe_bgzf() {
        let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
        let thread_num = 2;
        let mut vcf_reader = VcfLocalReader::new(file_path, thread_num).await;
        let record_batch = vcf_reader.describe().await.unwrap();
        assert!(record_batch.num_columns() > 0);
    }
    #[tokio::test]
    async fn test_vcf_local_reader_read_records_bgzf() {
        let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
        let thread_num = 2;
        let mut vcf_reader = VcfLocalReader::new(file_path, thread_num).await;
        let mut records = vcf_reader.read_records();
        assert!(records.next().await.is_some());
    }
    // #[tokio::test]
    // async fn test_vcf_reader_read_header_remote_bgzf() {
    //     let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
    //     let mut vcf_reader = VcfReader::new(file_path, None, Some(64), Some(8)).await;
    //     let header = vcf_reader.read_header().await.unwrap();
    //     assert!(header.infos().len() > 0);
    // }
    // #[tokio::test]
    // async fn test_vcf_reader_describe_remote_bgzf() {
    //     let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
    //     let mut vcf_reader = VcfReader::new(file_path, None, Some(64), Some(8)).await;
    //     let record_batch = vcf_reader.describe().await.unwrap();
    //     assert!(record_batch.num_columns() > 0);
    // }
    // #[tokio::test]
    // async fn test_vcf_reader_read_records_remote_bgzf() {
    //     let file_path = "testdata/mock_chr21.vcf.bgz".to_string();
    //     let mut vcf_reader = VcfReader::new(file_path, None, Some(64), Some(8)).await;
    //     let mut records = vcf_reader.read_records().await;
    //     let first_record = records.next().await;
    //     assert!(first_record.is_some());
    // }
}
