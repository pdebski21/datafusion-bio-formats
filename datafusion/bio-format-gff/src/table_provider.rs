use crate::physcial_exec::GffExec;
use crate::storage::{GffLocalReader, GffRemoteReader};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{ExecutionMode, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, StorageType, get_storage_type,
};
use futures::executor::block_on;
use futures_util::StreamExt;
use log::debug;
use noodles_gff::feature::record_buf::Attributes;
use noodles_gff::feature::record_buf::attributes::field::Value;
use std::any::Any;
use std::sync::Arc;

pub fn get_attribute_names_and_types(attributes: &Attributes) -> (Vec<String>, Vec<DataType>) {
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
        Field::new("phase", DataType::UInt32, true), //FIXME:: can be downcasted to 8
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
    attributes: Attributes,
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
            attributes,
            schema,
            thread_num,
            object_storage_options,
        })
    }
}

#[async_trait]
impl TableProvider for GffTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
        // todo!()
    }

    fn schema(&self) -> SchemaRef {
        debug!("VcfTableProvider::schema");
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
        // todo!()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("GffTableProvider::scan");

        fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    Arc::new(Schema::new(vec![Field::new("dummy", DataType::Null, true)]))
                }
                Some(indices) => {
                    let projected_fields: Vec<Field> =
                        indices.iter().map(|&i| schema.field(i).clone()).collect();
                    Arc::new(Schema::new(projected_fields))
                }
                None => schema.clone(),
            }
        }

        let schema = project_schema(&self.schema, projection);

        Ok(Arc::new(GffExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            attributes: self.attributes.clone(),
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}
