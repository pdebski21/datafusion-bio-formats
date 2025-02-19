use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::ptr::null;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayBuilder, ArrayRef, BooleanBuilder, Float32Array, Float32Builder, Float64Builder, Int32Array, Int32Builder, Int64Builder, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{ExecutionMode, ExecutionPlan, PlanProperties};
use futures::executor::block_on;
use log::debug;
use noodles::vcf::Header;
use noodles::vcf::header::record::value::map::info::{Number, Type};
use rust_htslib::bcf;
use rust_htslib::bcf::{IndexedReader, Read};
use rust_htslib::bcf::header::TagType;
use crate::physical_exec::VcfExec;
use crate::storage::{get_local_vcf_reader, get_remote_vcf_reader, get_storage_type, StorageType};

async fn determine_schema_from_header(
    file_path: &str,
    info_fields: &Option<Vec<String>>,
    format_fields: &Option<Vec<String>>,
) -> datafusion::common::Result<SchemaRef> {

    let storage_type = get_storage_type(file_path.to_string());
    let header = match storage_type {
        StorageType::LOCAL =>  get_local_vcf_reader(file_path.to_string(), 1)?.read_header()?,
        _ => get_remote_vcf_reader(file_path.to_string()).await.read_header().await?
    };

    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("pos_start", DataType::UInt32, false),
        Field::new("pos_end",DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
    ];

    match info_fields   {
        Some(infos) => {
            for tag in infos {
                let dtype = info_to_arrow_type(&header, tag);
                fields.push(Field::new(tag.to_lowercase(), dtype, true));
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
}

impl VcfTableProvider {
    pub fn new(
        file_path: String,
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        thread_num: Option<usize>,
    ) -> datafusion::common::Result<Self> {
        let schema = block_on(determine_schema_from_header(&file_path, &info_fields, &format_fields)).unwrap();
        Ok(Self {
            file_path,
            info_fields,
            format_fields,
            schema,
            thread_num
        })
    }
}



#[async_trait]
impl TableProvider for VcfTableProvider {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        todo!()
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, _filters: &[Expr], limit: Option<usize>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(VcfExec {
            cache: PlanProperties::new( EquivalenceProperties::new(self.schema.clone()),
                                        Partitioning::UnknownPartitioning(1),
                                        ExecutionMode::Bounded),
            file_path: self.file_path.clone(),
            schema: self.schema.clone(),
            info_fields: self.info_fields.clone(),
            format_fields: self.format_fields.clone(),
            projection: projection.cloned(),
            limit,
            thread_num: self.thread_num,
        }))
    }
}


pub fn info_to_arrow_type(header: &Header, field: &str) ->DataType {
    match header.infos().get(field) {
        Some(t) => {
            let inner = match t.ty() {
                Type::Integer => DataType::Int32,
                Type::String | Type::Character => DataType::Utf8,
                Type::Float => DataType::Float32,
                Type::Flag => DataType::Boolean,
                _ => panic!("Unsupported tag type"),
            };
            match t.number() {
                Number::Count(1) | Number::Count(0) => inner,
                _ => DataType::new_list(inner, true),
            }
        },
        None => panic!("Tag not found in header"),
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

    pub(crate) fn new(data_type: &DataType, batch_size: usize) -> OptionalField {
        match data_type {
            DataType::Int32 => OptionalField::Int32Builder(Int32Builder::with_capacity(batch_size)),
            DataType::List(f) => {
                match f.data_type() {
                    DataType::Int32 => OptionalField::ArrayInt32Builder(ListBuilder::with_capacity(Int32Builder::new(), batch_size)),
                    DataType::Float32 => OptionalField::ArrayFloat32Builder(ListBuilder::with_capacity(Float32Builder::new(), batch_size)),
                    DataType::Utf8 => OptionalField::ArrayUtf8Builder(ListBuilder::with_capacity(StringBuilder::new() , batch_size)),
                    DataType::Boolean => OptionalField::ArrayBooleanBuilder(ListBuilder::with_capacity(BooleanBuilder::new(), batch_size)),
                    _ => panic!("Unsupported data type"),
                }
            },
            DataType::Float32 => OptionalField::Float32Builder(Float32Builder::new()),
            DataType::Utf8 => OptionalField::Utf8Builder(StringBuilder::new()),
            DataType::Boolean => OptionalField::BooleanBuilder(BooleanBuilder::with_capacity(batch_size)),
            _ => panic!("Unsupported data type"),
        }
    }
    pub fn append_int(&mut self, value: i32) {
        match self {
            OptionalField::Int32Builder(builder) => builder.append_value(value),
            _ => panic!("Unsupported data type"),
        }
    }

    pub fn append_boolean(&mut self, value: bool) {
        match self {
            OptionalField::BooleanBuilder(builder) => builder.append_value(value),
            _ => panic!("Unsupported data type"),
        }
    }

    pub fn append_array_int(&mut self, value: Vec<i32>) {
        match self {
            OptionalField::ArrayInt32Builder(builder) => {
                let a = Int32Array::from(value);
                builder.values().append_slice(a.values());
                builder.append(true);
            },
            _ => panic!("Unsupported data type"),
        }
    }
    pub fn append_float(&mut self, value: f32) {
        match self {
            OptionalField::Float32Builder(builder) => builder.append_value(value),

            _ => panic!("Unsupported data type"),
        }
    }
    pub fn append_array_float(&mut self, value: Vec<f32>) {
        match self {
            OptionalField::ArrayFloat32Builder(builder) => {
                let a = Float32Array::from(value);
                builder.values().append_slice(a.values());
                builder.append(true);
            },
            _ => panic!("Unsupported data type"),
        }
    }

    pub fn append_string(&mut self, value: &str) {
        match self {
            OptionalField::Utf8Builder(builder) => builder.append_value(value),
            _ => panic!("Unsupported data type"),
        }
    }
    pub fn append_array_string(&mut self, value: Vec<String>) {
        match self {
            OptionalField::ArrayUtf8Builder(builder) => {
                for v in value {
                    builder.values().append_value(&v);
                }
                builder.append(true);
            },
            _ => panic!("Unsupported data type"),
        }
    }


    pub fn append_null(&mut self){
        match self {
            OptionalField::Int32Builder(builder) => builder.append_null(),
            OptionalField::ArrayInt32Builder(builder) => builder.append_null(),
            OptionalField::Utf8Builder(builder) => builder.append_null(),
            OptionalField::ArrayUtf8Builder(builder) => builder.append_null(),
            OptionalField::Float32Builder(builder) => builder.append_null(),
            OptionalField::ArrayFloat32Builder(builder) => builder.append_null(),
            OptionalField::BooleanBuilder(builder) => builder.append_null(),
            OptionalField::ArrayBooleanBuilder(builder) => builder.append_null(),

        }
    }
    pub fn finish(&mut self) -> ArrayRef {
        match self {
            OptionalField::Int32Builder(builder) => Arc::new(builder.finish()),
            OptionalField::ArrayInt32Builder(builder) => Arc::new(builder.finish()),
            OptionalField::Utf8Builder(builder) => Arc::new(builder.finish()),
            OptionalField::ArrayUtf8Builder(builder) => Arc::new(builder.finish()),
            OptionalField::Float32Builder(builder) => Arc::new(builder.finish()),
            OptionalField::ArrayFloat32Builder(builder) => Arc::new(builder.finish()),
            OptionalField::BooleanBuilder(builder) => Arc::new(builder.finish()),
            OptionalField::ArrayBooleanBuilder(builder) => Arc::new(builder.finish()),
        }
    }

}


