use std::fs::File;
use std::io::Error;
use std::num::NonZero;
use async_stream::stream;
use bytes::Bytes;
use futures::{stream, StreamExt};
use futures::stream::BoxStream;
use log::debug;
use noodles::{bgzf, vcf};
use noodles::vcf::io::Reader;
use noodles::vcf::Record;
use noodles_bgzf::{AsyncReader, MultithreadedReader};
use opendal::{FuturesBytesStream, Operator};
use opendal::layers::{LoggingLayer, RetryLayer, TimeoutLayer};
use opendal::services::{Gcs, S3};
use tokio::io::BufReader;
use tokio_util::io::StreamReader;


pub enum CompressionType {
    GZIP,
    BGZF,
    NONE,
}

impl CompressionType {
    fn get_compression_type(&self) -> String {
        match self {
            CompressionType::GZIP => "gz".to_string(),
            CompressionType::BGZF => "bgz".to_string(),
            CompressionType::NONE => "none".to_string(),
        }
    }

    fn from_string(compression_type: String) -> Self {
        match compression_type.to_lowercase().as_str() {
            "gz" => CompressionType::GZIP,
            "bgz" => CompressionType::BGZF,
            "none" => CompressionType::NONE,
            _ => panic!("Invalid compression type"),
        }
    }

}

pub enum StorageType {
    GCS,
    S3,
    AZBLOB,
    LOCAL,
}

impl StorageType {
    fn get_object_storage_type(&self) -> String {
        match self {
            StorageType::GCS => "gcs".to_string(),
            StorageType::S3 => "s3".to_string(),
            StorageType::AZBLOB => "azblob".to_string(),
            StorageType::LOCAL => "local".to_string(),
        }
    }

    fn from_string(object_storage_type: String) -> Self {
        match object_storage_type.as_str() {
            "gs" => StorageType::GCS,
            "s3" => StorageType::S3,
            "abfs" => StorageType::AZBLOB,
            "local" => StorageType::LOCAL,
            "file" => StorageType::LOCAL,
            _ => panic!("Invalid object storage type"),
        }
    }
}

fn get_file_path(file_path: String) -> String {
    //extract the file path from the file path
    let file_path = file_path.split("://").last().unwrap().split('/').skip(1).collect::<Vec<&str>>().join("/");
    //return the file path
    file_path.to_string()
}




pub fn get_compression_type(file_path: String) -> CompressionType {
    //extract the file extension from path
    if file_path.to_lowercase().ends_with(".vcf") {
        return CompressionType::NONE;
    }
    let file_extension = file_path.split('.').last().unwrap();
    //return the compression type
    CompressionType::from_string(file_extension.to_string())
}



pub async fn get_remote_stream_bgzf(file_path: String, chunk_size: usize, concurrent_fetches: usize) ->  Result<AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>, opendal::Error> {
    let remote_stream = StreamReader::new(get_remote_stream(file_path.clone(), chunk_size, concurrent_fetches).await?);
    Ok(bgzf::r#async::Reader::new(remote_stream))

}

pub fn get_storage_type(file_path: String) -> StorageType {
    //extract the file system prefix from the file path
    let file_system_prefix = file_path.split("://").next();
    let file_system_prefix = if file_path == file_system_prefix.unwrap() {
        None
    } else {
        file_system_prefix
    };
    match file_system_prefix {
        Some(prefix) => StorageType::from_string(prefix.to_string()),
        None => StorageType::LOCAL
    }

}





fn get_bucket_name(file_path: String) -> String {
    //extract the bucket name from the file path
    let bucket_name = file_path.split("://").last().unwrap().split('/').next().unwrap();
    //return the bucket name
    bucket_name.to_string()
}

pub async fn get_remote_stream(file_path: String, chunk_size: usize, concurrent_fetches: usize) ->  Result<FuturesBytesStream, opendal::Error> {
    let storage_type = get_storage_type(file_path.clone());
    let bucket_name = get_bucket_name(file_path.clone());
    let file_path = get_file_path(file_path.clone());
    match storage_type {
        StorageType::GCS => {
            let builder = Gcs::default()
                .bucket(bucket_name.as_str())
                .disable_vm_metadata()
                .allow_anonymous();
            let operator =  Operator::new(builder)?
                .layer(TimeoutLayer::new().with_io_timeout(std::time::Duration::from_secs(120)))
                .layer(RetryLayer::new().with_max_times(3))
                .layer(LoggingLayer::default())
                .finish();
            operator.reader_with(file_path.as_str())
                .chunk(chunk_size * 1024 * 1024)
                .concurrent(concurrent_fetches)
                .await?.into_bytes_stream(..).await
        }
        StorageType::S3 => {
            let builder = S3::default()
                .region(&S3::detect_region("https://s3.amazonaws.com", bucket_name.as_str()).await.unwrap())
                .bucket(bucket_name.as_str())
                .disable_ec2_metadata()
                .allow_anonymous();
            let operator =  Operator::new(builder)?
                .layer(TimeoutLayer::new().with_io_timeout(std::time::Duration::from_secs(120)))
                .layer(RetryLayer::new().with_max_times(3))
                .layer(LoggingLayer::default())
                .finish();
            operator.reader_with(file_path.as_str()).concurrent(1).await?.into_bytes_stream(..).await
        }

        _ => panic!("Invalid object storage type"),
    }
}

pub async fn get_remote_vcf_bgzf_reader(file_path: String, chunk_size: usize, concurrent_fetches: usize) -> vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>> {
    let inner = get_remote_stream_bgzf(file_path.clone(), chunk_size, concurrent_fetches).await.unwrap();
    let reader = vcf::r#async::io::Reader::new(inner);
    reader
}

pub async fn get_remote_vcf_reader(file_path: String, chunk_size: usize, concurrent_fetches: usize) -> vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>> {
    let inner = StreamReader::new(get_remote_stream(file_path.clone(), chunk_size, concurrent_fetches).await.unwrap());
    let reader = vcf::r#async::io::Reader::new(inner);
    reader
}




pub fn get_local_vcf_bgzf_reader(file_path: String, thread_num: usize) -> Result<Reader<MultithreadedReader<File>>, Error> {
    debug!("Reading VCF file from local storage with {} threads", thread_num);
    File::open(file_path)
        .map(|f| noodles_bgzf::MultithreadedReader::with_worker_count(NonZero::new(thread_num).unwrap(), f))
        .map(vcf::io::Reader::new)
}


pub async fn get_local_vcf_reader(file_path: String) -> Result<vcf::r#async::io::Reader<BufReader<tokio::fs::File>>, Error> {
    debug!("Reading VCF file from local storage with async reader");
    let reader = tokio::fs::File::open(file_path)
        .await
        .map(BufReader::new)
        .map(vcf::r#async::io::Reader::new)?;
    Ok(reader)
}


pub async fn get_local_vcf_header(file_path: String, thread_num: usize) -> Result<vcf::Header, Error> {
    let compression_type = get_compression_type(file_path.clone());
    let header = match compression_type {
        CompressionType::BGZF | CompressionType::GZIP=> {
            let mut reader = get_local_vcf_bgzf_reader(file_path, thread_num)?;
            reader.read_header()?
        }
        CompressionType::NONE => {
            let mut reader = get_local_vcf_reader(file_path).await?;
            reader.read_header().await?
        }
    };
    Ok(header)
}

pub async fn get_remote_vcf_header(file_path: String, chunk_size: usize, concurrent_fetches: usize) -> Result<vcf::Header, Error> {
    let compression_type = get_compression_type(file_path.clone());
    let header = match compression_type {
        CompressionType::BGZF | CompressionType::GZIP=> {
            let mut reader = get_remote_vcf_bgzf_reader(file_path, chunk_size, concurrent_fetches).await;
            reader.read_header().await?
        }
        CompressionType::NONE => {
            let mut reader = get_remote_vcf_reader(file_path, chunk_size, concurrent_fetches).await;
            reader.read_header().await?
        }
    };
    Ok(header)
}

pub async fn get_header(file_path: String) -> Result<vcf::Header, Error> {
    let storage_type = get_storage_type(file_path.clone());
    let header = match storage_type {
        StorageType::LOCAL => {
            get_local_vcf_header(file_path, 1).await?
        }
        _ => {
            get_remote_vcf_header(file_path, 64,1).await?
        }
    };
    Ok(header)
}

pub enum VcfRemoteReader {
    BGZF( vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>),
    PLAIN( vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>)
}



impl VcfRemoteReader {
    pub async fn new(file_path: String, chunk_size: usize, concurrent_fetches: usize) -> Self {
        let compression_type = get_compression_type(file_path.clone());
        match compression_type {
            CompressionType::BGZF | CompressionType::GZIP=> {
                let reader = get_remote_vcf_bgzf_reader(file_path, chunk_size, concurrent_fetches).await;
                VcfRemoteReader::BGZF(reader)
            }
            CompressionType::NONE => {
                let reader = get_remote_vcf_reader(file_path, chunk_size, concurrent_fetches).await;
                VcfRemoteReader::PLAIN(reader)
            }
        }
    }
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfRemoteReader::BGZF(reader) => {
                reader.read_header().await
            }
            VcfRemoteReader::PLAIN(reader) => {
                reader.read_header().await
            }
        }
    }

    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfRemoteReader::BGZF(reader) => {
                reader.records().boxed()
            }
            VcfRemoteReader::PLAIN(reader) => {
                reader.records().boxed()
            }
        }
    }
}

pub enum VcfLocalReader {
    BGZF(Reader<MultithreadedReader<File>>),
    PLAIN(vcf::r#async::io::Reader<BufReader<tokio::fs::File>>)
}

impl VcfLocalReader {
    pub async fn new(file_path: String, thread_num: usize) -> Self {
        let compression_type = get_compression_type(file_path.clone());
        match compression_type {
            CompressionType::BGZF | CompressionType::GZIP=> {
                let reader = get_local_vcf_bgzf_reader(file_path, thread_num).unwrap();
                VcfLocalReader::BGZF(reader)
            }
            CompressionType::NONE => {
                let reader = get_local_vcf_reader(file_path).await.unwrap();
                VcfLocalReader::PLAIN(reader)
            }
        }
    }
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match self {
            VcfLocalReader::BGZF(reader) => {
                reader.read_header()
            }
            VcfLocalReader::PLAIN(reader) => {
                reader.read_header().await
            }
        }
    }

    pub fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match self {
            VcfLocalReader::BGZF(reader) => {
                stream::iter(reader.records()).boxed()
            }
            VcfLocalReader::PLAIN(reader) => {
                reader.records().boxed()
            }
        }
    }
}