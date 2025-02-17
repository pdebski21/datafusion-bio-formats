use noodles::bgzf;
use noodles_bgzf::AsyncReader;
use opendal::{FuturesBytesStream, Operator};
use opendal::layers::LoggingLayer;
use opendal::services::Gcs;
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
        match compression_type.as_str() {
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
    let file_extension = file_path.split('.').last().unwrap();
    //return the compression type
    CompressionType::from_string(file_extension.to_string())
}



pub async fn get_remote_stream_bgzf(file_path: String) ->  Result<AsyncReader<StreamReader<FuturesBytesStream, bytes::Bytes>>, opendal::Error> {
    let remote_stream = StreamReader::new(get_remote_stream(file_path.clone()).await?);
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

pub async fn get_remote_stream(file_path: String) ->  Result<FuturesBytesStream, opendal::Error> {
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
                .layer(LoggingLayer::default())
                .finish();
            operator.reader_with(file_path.as_str()).await?.into_bytes_stream(..).await
        }

        _ => panic!("Invalid object storage type"),
    }
}







