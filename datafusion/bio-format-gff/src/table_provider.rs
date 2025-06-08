use crate::storage::{GffLocalReader, GffRemoteReader};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use futures::executor::block_on;
use futures_util::StreamExt;
use log::debug;
use noodles_gff::feature::record_buf::Attributes;
use noodles_gff::feature::record_buf::attributes::field::Value;
use std::sync::Arc;

fn get_attribute_names_and_types(attributes: &Attributes) -> (Vec<String>, Vec<DataType>) {
    let mut attribute_names = Vec::new();
    let mut attribute_types = Vec::new();

    for (tag, value) in attributes.as_ref().iter() {
        attribute_names.push(tag.to_string());
        match value {
            Value::String(s) => {
                attribute_types.push(DataType::Utf8);
            }
            Value::Array(arr) => {
                attribute_types.push(DataType::List(Arc::new(Field::new(
                    tag.to_string(),
                    DataType::Utf8,
                    true,
                ))));
            }
        }
    }
    (attribute_names, attribute_types)
}
fn determine_schema(attributes: &Attributes) -> datafusion::common::Result<SchemaRef> {
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::Utf8, true),
    ];

    let attributes = get_attribute_names_and_types(attributes);
    for (name, dtype) in attributes.0.iter().zip(attributes.1.iter()) {
        fields.push(Field::new(name, dtype.clone(), true));
    }

    let schema = Schema::new(fields);
    debug!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

#[derive(Clone, Debug)]
pub struct GffTableProvider {
    file_path: String,
    schema: SchemaRef,
    thread_num: Option<usize>,
    object_storage_options: Option<ObjectStorageOptions>,
}

impl GffTableProvider {
    pub fn new(
        file_path: String,
        thread_num: Option<usize>,
        object_storage_options: Option<ObjectStorageOptions>,
    ) -> datafusion::common::Result<Self> {
        let storage_type = get_storage_type(file_path.clone());
        let attributes = match storage_type {
            StorageType::LOCAL => {
                let mut reader = block_on(GffLocalReader::new(file_path.clone(), 1))?;
                block_on(reader.get_attributes())
            }
            StorageType::AZBLOB | StorageType::S3 | StorageType::GCS => {
                let mut reader = block_on(GffRemoteReader::new(
                    file_path.clone(),
                    object_storage_options.clone().unwrap(),
                ))?;
                block_on(reader.get_attributes())
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Unsupported storage type for GFF files".to_string(),
                ));
            }
        };
        let schema = determine_schema(&attributes)?;
        Ok(Self {
            file_path,
            schema,
            thread_num,
            object_storage_options,
        })
    }
}
