use std::fs::File;
use std::io::Error;
use std::num::NonZero;
use std::sync::Arc;
use async_stream::stream;
use bytes::Bytes;
use datafusion::arrow;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::RecordBatch;
use futures::{stream, StreamExt};
use futures::stream::BoxStream;
use log::{debug, info};
use noodles::{bgzf, vcf};
use noodles::vcf::io::Reader;
use noodles::vcf::{Header, Record};
use noodles_bgzf::{AsyncReader, MultithreadedReader};
use opendal::{FuturesBytesStream, Operator};
use opendal::layers::{LoggingLayer, RetryLayer, TimeoutLayer};
use opendal::services::{Gcs, S3};
use tokio::io::BufReader;
use tokio_util::io::StreamReader;
use std::time::Instant;
use std::collections::HashMap;
use std::sync::Mutex;
use once_cell::sync::Lazy;

use crate::storage::{get_compression_type, get_storage_type, StorageType};

// Configuration for VCF reading
#[derive(Clone, Debug)]
pub struct VcfConfig {
    pub thread_num: usize,
    pub chunk_size: usize,
    pub concurrent_fetches: usize,
    pub batch_size: usize,
    pub cache_size: usize,
    pub timeout_secs: u64,
    pub retry_times: usize,
}

impl Default for VcfConfig {
    fn default() -> Self {
        Self {
            thread_num: 1,
            chunk_size: 64,
            concurrent_fetches: 8,
            batch_size: 1024,
            cache_size: 100,
            timeout_secs: 120,
            retry_times: 3,
        }
    }
}

// Cache for VCF headers to avoid repeated reads
static HEADER_CACHE: Lazy<Mutex<HashMap<String, (Header, Instant)>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

// Unified VCF reader trait
pub trait VcfReaderTrait {
    async fn read_header(&mut self) -> Result<vcf::Header, Error>;
    async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error>;
    async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>>;
}

// Remote VCF reader implementation
pub struct VcfRemoteReader {
    reader: VcfRemoteReaderInner,
    config: VcfConfig,
}

enum VcfRemoteReaderInner {
    BGZF(vcf::r#async::io::Reader<AsyncReader<StreamReader<FuturesBytesStream, Bytes>>>),
    PLAIN(vcf::r#async::io::Reader<StreamReader<FuturesBytesStream, Bytes>>)
}

impl VcfRemoteReader {
    pub async fn new(file_path: String, config: VcfConfig) -> Self {
        let compression_type = get_compression_type(file_path.clone());
        let reader = match compression_type {
            crate::storage::CompressionType::BGZF | crate::storage::CompressionType::GZIP => {
                let inner = crate::storage::get_remote_stream_bgzf(file_path.clone(), config.chunk_size, config.concurrent_fetches).await.unwrap();
                VcfRemoteReaderInner::BGZF(vcf::r#async::io::Reader::new(inner))
            }
            crate::storage::CompressionType::NONE => {
                let inner = StreamReader::new(crate::storage::get_remote_stream(file_path.clone(), config.chunk_size, config.concurrent_fetches).await.unwrap());
                VcfRemoteReaderInner::PLAIN(vcf::r#async::io::Reader::new(inner))
            }
        };
        
        Self { reader, config }
    }
}

impl VcfReaderTrait for VcfRemoteReader {
    async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match &mut self.reader {
            VcfRemoteReaderInner::BGZF(reader) => reader.read_header().await,
            VcfRemoteReaderInner::PLAIN(reader) => reader.read_header().await,
        }
    }
    
    async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        let header = self.read_header().await?;
        Ok(crate::storage::get_info_fields(&header).await)
    }
    
    async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match &mut self.reader {
            VcfRemoteReaderInner::BGZF(reader) => reader.records().boxed(),
            VcfRemoteReaderInner::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

pub struct VcfLocalReader {
    reader: VcfLocalReaderInner,
    config: VcfConfig,
}

enum VcfLocalReaderInner {
    BGZF(Reader<MultithreadedReader<File>>),
    PLAIN(vcf::r#async::io::Reader<BufReader<tokio::fs::File>>)
}

impl VcfLocalReader {
    pub async fn new(file_path: String, config: VcfConfig) -> Self {
        let compression_type = get_compression_type(file_path.clone());
        let reader = match compression_type {
            crate::storage::CompressionType::BGZF | crate::storage::CompressionType::GZIP => {
                let reader = crate::storage::get_local_vcf_bgzf_reader(file_path, config.thread_num).unwrap();
                VcfLocalReaderInner::BGZF(reader)
            }
            crate::storage::CompressionType::NONE => {
                let reader = crate::storage::get_local_vcf_reader(file_path).await.unwrap();
                VcfLocalReaderInner::PLAIN(reader)
            }
        };
        
        Self { reader, config }
    }
}

impl VcfReaderTrait for VcfLocalReader {
    async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        match &mut self.reader {
            VcfLocalReaderInner::BGZF(reader) => reader.read_header(),
            VcfLocalReaderInner::PLAIN(reader) => reader.read_header().await,
        }
    }
    
    async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        let header = self.read_header().await?;
        Ok(crate::storage::get_info_fields(&header).await)
    }
    
    async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match &mut self.reader {
            VcfLocalReaderInner::BGZF(reader) => stream::iter(reader.records()).boxed(),
            VcfLocalReaderInner::PLAIN(reader) => reader.records().boxed(),
        }
    }
}

pub struct VcfReader {
    reader: VcfReaderInner,
    config: VcfConfig,
}

enum VcfReaderInner {
    Local(VcfLocalReader),
    Remote(VcfRemoteReader)
}

impl VcfReader {
    pub async fn new(file_path: String, config: Option<VcfConfig>) -> Self {
        let config = config.unwrap_or_default();
        let storage_type = get_storage_type(file_path.clone());
        
        let reader = match storage_type {
            StorageType::LOCAL => {
                VcfReaderInner::Local(VcfLocalReader::new(file_path.clone(), config.clone()).await)
            }
            _ => {
                VcfReaderInner::Remote(VcfRemoteReader::new(file_path.clone(), config.clone()).await)
            }
        };
        
        Self { reader, config }
    }
    
    pub async fn read_header(&mut self) -> Result<vcf::Header, Error> {
        // Check cache first using file path as key
        let cache_key = match &self.reader {
            VcfReaderInner::Local(reader) => format!("local:{}", reader.config.thread_num),
            VcfReaderInner::Remote(reader) => format!("remote:{}:{}", reader.config.chunk_size, reader.config.concurrent_fetches),
        };
        
        let mut cache = HEADER_CACHE.lock().unwrap();
        
        if let Some((header, timestamp)) = cache.get(&cache_key) {
            // Check if cache is still valid (less than 1 hour old)
            if timestamp.elapsed().as_secs() < 3600 {
                return Ok(header.clone());
            }
        }
        
        // If not in cache or expired, read the header
        let header = match &mut self.reader {
            VcfReaderInner::Local(reader) => reader.read_header().await?,
            VcfReaderInner::Remote(reader) => reader.read_header().await?,
        };
        
        // Update cache
        cache.insert(cache_key, (header.clone(), Instant::now()));
        
        Ok(header)
    }
    
    pub async fn describe(&mut self) -> Result<arrow::array::RecordBatch, Error> {
        match &mut self.reader {
            VcfReaderInner::Local(reader) => reader.describe().await,
            VcfReaderInner::Remote(reader) => reader.describe().await,
        }
    }
    
    pub async fn read_records(&mut self) -> BoxStream<'_, Result<Record, Error>> {
        match &mut self.reader {
            VcfReaderInner::Local(reader) => reader.read_records().await,
            VcfReaderInner::Remote(reader) => reader.read_records().await,
        }
    }
    
    pub fn get_config(&self) -> &VcfConfig {
        &self.config
    }
} 