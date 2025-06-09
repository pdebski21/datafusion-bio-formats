use crate::physcial_exec::GffExec;
use crate::storage::{GffLocalReader, GffRemoteReader};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
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
fn determine_schema() -> datafusion::common::Result<SchemaRef> {
    let fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::UInt32, true), //FIXME:: can be downcasted to 8
        Field::new(
            "attributes",
            DataType::List(FieldRef::from(Box::new(
                // each list element is a struct { tag: Utf8 (non-null), value: Utf8 (nullable) }
                Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("tag", DataType::Utf8, false), // tag must be non-null
                        Field::new("value", DataType::Utf8, true), // value may be null
                    ])),
                    true, // each struct element itself is non-null
                ),
            ))),
            true, // the "attributes" list itself may be null
        ),
    ];
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
        let schema = determine_schema()?;
        Ok(Self {
            file_path,
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
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
            object_storage_options: self.object_storage_options.clone(),
        }))
    }
}
