use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{ExecutionMode, ExecutionPlan, PlanProperties};
use rust_htslib::bcf;
use rust_htslib::bcf::{IndexedReader, Read};
use crate::physical_exec::VcfExec;

fn determine_schema_from_header(
    file_path: &str,
    info_fields: &[String],
    format_fields: &[String],
) -> datafusion::common::Result<SchemaRef> {
    // let mut reader = bcf::Reader::from_path(file_path)
    //     .map_err(|e| DataFusionError::Execution(format!("Failed to open VCF: {:?}", e)))?;
    // let header = reader.header();

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

    // for tag in info_fields {
    //     let dtype = match header.info_type(tag.as_bytes()) {
    //         Ok((t, _)) => match t {
    //             bcf::header::TagType::Flag => DataType::Boolean,
    //             bcf::header::TagType::Integer => DataType::Int32,
    //             bcf::header::TagType::Float => DataType::Float64,
    //             bcf::header::TagType::String => DataType::Utf8,
    //         },
    //         Err(_) => DataType::Utf8,
    //     };
    //     fields.push(Field::new(tag, dtype, true));
    // }
    // for tag in format_fields {
    //     let dtype = match header.format_type(tag.as_bytes()) {
    //         Ok((t, _)) => match t {
    //             bcf::header::TagType::Flag => DataType::Boolean,
    //             bcf::header::TagType::Integer => DataType::Int32,
    //             bcf::header::TagType::Float => DataType::Float64,
    //             bcf::header::TagType::String => DataType::Utf8,
    //         },
    //         Err(_) => DataType::Utf8,
    //     };
    //     fields.push(Field::new(tag, dtype, true));
    // }
    let schema = Schema::new(fields);
    // println!("Schema: {:?}", schema);
    Ok(Arc::new(schema))
}

#[derive(Clone, Debug)]
pub struct VcfTableProvider {
    file_path: String,
    info_fields: Vec<String>,
    format_fields: Vec<String>,
    schema: SchemaRef,
    thread_num: Option<usize>,
}

impl VcfTableProvider {
    pub fn new(
        file_path: String,
        info_fields: Vec<String>,
        format_fields: Vec<String>,
        thread_num: Option<usize>,
    ) -> datafusion::common::Result<Self> {
        let schema = determine_schema_from_header(&file_path, &info_fields, &format_fields)?;
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