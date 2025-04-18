use std::any::Any;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanBuilder, Float32Array, Float32Builder, Int32Array, Int32Builder, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{ExecutionMode, ExecutionPlan, PlanProperties};
use futures::executor::block_on;
use log::debug;
use noodles::vcf::header::Infos;
use noodles::vcf::header::record::value::map::info::{Number, Type};
use crate::physical_exec::VcfExec;
use crate::storage::get_header;

async fn determine_schema_from_header(
    file_path: &str,
    info_fields: &Option<Vec<String>>,
    _format_fields: &Option<Vec<String>>,
) -> datafusion::common::Result<SchemaRef> {

    let header = get_header(file_path.to_string()).await?;
    let header_infos = header.infos();

    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end",DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
    ];

    match info_fields   {
        Some(infos) => {
            for tag in infos {
                let dtype = info_to_arrow_type(&header_infos, tag);
                // fields.push(Field::new(tag.to_lowercase(), dtype, true));
                let nullable = true; // or infer from header/tag definition
                fields.push(Field::new(tag.to_lowercase(), dtype, nullable));
            }
        }
        _ => {}
    }
    let schema = Schema::new(fields);
    // println!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

#[derive(Clone, Debug)]
pub struct VcfTableProvider {
    file_path: String,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    schema: SchemaRef,
    thread_num: Option<usize>,
    chunk_size: Option<usize>,
    concurrent_fetches: Option<usize>,

}

impl VcfTableProvider {
    pub fn new(
        file_path: String,
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        thread_num: Option<usize>,
        chunk_size: Option<usize>,
        concurrent_fetches: Option<usize>,
    ) -> datafusion::common::Result<Self> {
        let schema = block_on(determine_schema_from_header(&file_path, &info_fields, &format_fields))?;
        Ok(Self {
            file_path,
            info_fields,
            format_fields,
            schema,
            thread_num,
            chunk_size,
            concurrent_fetches,
        })
    }
}



#[async_trait]
impl TableProvider for VcfTableProvider {
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

    async fn scan(&self, _state: &dyn Session, projection: Option<&Vec<usize>>, _filters: &[Expr], limit: Option<usize>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        debug!("VcfTableProvider::scan");

        fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
            match projection {
                Some(indices) if indices.is_empty() => {
                    Arc::new(Schema::new(vec![Field::new("dummy", DataType::Null, true)]))
                }
                Some(indices) => {
                    let projected_fields: Vec<Field> = indices.iter().map(|&i| schema.field(i).clone()).collect();
                    Arc::new(Schema::new(projected_fields))
                }
                None => schema.clone(),
            }
        }

        let schema = project_schema(&self.schema, projection);

        // let schema = match projection {
        //     Some(p) => {
        //         if p.len() == 0 {
        //             Arc::new(Schema::new(vec![
        //                 Field::new("dummy", DataType::Null, true),
        //             ]))
        //         }
        //         else {
        //             let schema_fields = self.schema.fields();
        //             let proj = p.clone();
        //             let mut fields: Vec<Field> = Vec::with_capacity(proj.len());
        //             for i in proj {
        //                 fields.push(schema_fields[i].deref().clone());
        //             }
        //             Arc::new(Schema::new(fields))
        //         }
        //     },
        //     None => {
        //         self.schema.clone()
        //     }
        // };

        Ok(Arc::new(VcfExec {
            cache: PlanProperties::new( EquivalenceProperties::new(schema.clone()),
                                        Partitioning::UnknownPartitioning(1),
                                        ExecutionMode::Bounded),
            file_path: self.file_path.clone(),
            schema: schema.clone(),
            info_fields: self.info_fields.clone(),
            format_fields: self.format_fields.clone(),
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
            chunk_size: self.chunk_size,
            concurrent_fetches: self.concurrent_fetches,
        }))
    }
}




pub fn info_to_arrow_type(infos: &Infos, field: &str) -> DataType {
    match infos.get(field) {
        Some(t) => {
            let inner = match t.ty() {
                Type::Integer => DataType::Int32,
                Type::String | Type::Character => DataType::Utf8,
                Type::Float => DataType::Float32,
                Type::Flag => DataType::Boolean,
            };
            match t.number() {
                Number::Count(1) | Number::Count(0) => inner,
                _ => DataType::new_list(inner, true),
                // Number::Unbounded | Number::Unknown | Number::Count(_) => DataType::List(Arc::new(Field::new("item", inner.clone(), true))),
            }
        },
        None => {
            log::warn!("VCF tag '{}' not found in header; defaulting to Utf8", field);
            DataType::Utf8
        }
    }
}

#[derive(Debug)]
pub enum OptionalField {
    Int32Builder(Int32Builder),
    ArrayInt32Builder(ListBuilder<Int32Builder>),
    Float32Builder(Float32Builder),
    ArrayFloat32Builder(ListBuilder<Float32Builder>),
    BooleanBuilder(BooleanBuilder),
    ArrayBooleanBuilder(ListBuilder<BooleanBuilder>),
    Utf8Builder(StringBuilder),
    ArrayUtf8Builder(ListBuilder<StringBuilder>),
}

impl OptionalField {
    pub(crate) fn new(data_type: &DataType, batch_size: usize) -> Result<OptionalField, ArrowError> {
        match data_type {
            DataType::Int32 => Ok(OptionalField::Int32Builder(Int32Builder::with_capacity(batch_size))),
            DataType::List(f) => match f.data_type() {
                DataType::Int32 => Ok(OptionalField::ArrayInt32Builder(ListBuilder::with_capacity(Int32Builder::new(), batch_size),)),
                DataType::Float32 => Ok(OptionalField::ArrayFloat32Builder(ListBuilder::with_capacity(Float32Builder::new(), batch_size),)),
                DataType::Utf8 => Ok(OptionalField::ArrayUtf8Builder(ListBuilder::with_capacity(StringBuilder::new(), batch_size),)),
                DataType::Boolean => Ok(OptionalField::ArrayBooleanBuilder(ListBuilder::with_capacity(BooleanBuilder::new(), batch_size),)),
                _ => Err(ArrowError::SchemaError("Unsupported list inner data type".into())),
            },
            DataType::Float32 => Ok(OptionalField::Float32Builder(Float32Builder::new())),
            DataType::Utf8 => Ok(OptionalField::Utf8Builder(StringBuilder::new())),
            DataType::Boolean => Ok(OptionalField::BooleanBuilder(BooleanBuilder::with_capacity(batch_size))),
            _ => Err(ArrowError::SchemaError("Unsupported data type".into())),
        }
    }

    pub fn append_int(&mut self, value: i32) -> Result<(), ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Invalid builder".into())),
        }
    }

    pub fn append_boolean(&mut self, value: bool) -> Result<(), ArrowError> {
        match self {
            OptionalField::BooleanBuilder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected BooleanBuilder".into())),
        }
    }

    pub fn append_array_int(&mut self, value: Vec<i32>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayInt32Builder(builder) => {
                builder.values().append_slice(&value);
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayInt32Builder".into())),
        }
    }

    pub fn append_float(&mut self, value: f32) -> Result<(), ArrowError> {
        match self {
            OptionalField::Float32Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected Float32Builder".into())),
        }
    }

    pub fn append_array_float(&mut self, value: Vec<f32>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayFloat32Builder(builder) => {
                builder.values().append_slice(&value);
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayFloat32Builder".into())),
        }
    }

    pub fn append_string(&mut self, value: &str) -> Result<(), ArrowError> {
        match self {
            OptionalField::Utf8Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected Utf8Builder".into())),
        }
    }

    pub fn append_array_string(&mut self, value: Vec<String>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayUtf8Builder(builder) => {
                for v in value {
                    builder.values().append_value(&v);
                }
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayUtf8Builder".into())),
        }
    }

    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayInt32Builder(builder) => Ok(builder.append_null()),
            OptionalField::Utf8Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayUtf8Builder(builder) => Ok(builder.append_null()),
            OptionalField::Float32Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayFloat32Builder(builder) => Ok(builder.append_null()),
            OptionalField::BooleanBuilder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayBooleanBuilder(builder) => Ok(builder.append_null()),
        }
    }

    pub fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
    match self {
        OptionalField::Int32Builder(builder) => Ok(Arc::new(builder.finish())),
        OptionalField::ArrayInt32Builder(builder) => Ok(Arc::new(builder.finish())),
        OptionalField::Utf8Builder(builder) => Ok(Arc::new(builder.finish())),
        OptionalField::ArrayUtf8Builder(builder) => Ok(Arc::new(builder.finish())),
        OptionalField::Float32Builder(builder) => Ok(Arc::new(builder.finish())),
        OptionalField::ArrayFloat32Builder(builder) => Ok(Arc::new(builder.finish())),
        OptionalField::BooleanBuilder(builder) => Ok(Arc::new(builder.finish())),
        OptionalField::ArrayBooleanBuilder(builder) => Ok(Arc::new(builder.finish())),
    }
}
}


