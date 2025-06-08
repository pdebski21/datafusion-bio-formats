// use std::any::Any;
// use std::fmt::{Debug, Formatter};
// use std::sync::Arc;
// use async_stream::__private::AsyncStream;
// use async_stream::try_stream;
// use datafusion::arrow::array::RecordBatch;
// use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
// use datafusion::execution::{SendableRecordBatchStream, TaskContext};
// use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
// use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
// use futures_util::{StreamExt, TryStreamExt};
// use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
// use log::debug;
// use noodles::vcf::header::Infos;
// use noodles_gff::feature::record_buf::Attributes;
// use noodles_gff::feature::record_buf::attributes::field::Value;
// use datafusion_bio_format_core::table_utils::OptionalField;
// use crate::storage::GffRemoteReader;
//
// #[allow(dead_code)]
// pub struct GffExec {
//     pub(crate) file_path: String,
//     pub(crate) schema: SchemaRef,
//     pub(crate) projection: Option<Vec<usize>>,
//     pub(crate) cache: PlanProperties,
//     pub(crate) limit: Option<usize>,
//     pub(crate) thread_num: Option<usize>,
//     pub(crate) object_storage_options: Option<ObjectStorageOptions>,
// }
//
// impl Debug for GffExec {
//     fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
//         Ok(())
//     }
// }
//
// impl DisplayAs for GffExec {
//     fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
//         Ok(())
//     }
// }
//
//
// // impl ExecutionPlan for GffExec {
// //     fn name(&self) -> &str {
// //         "GffExec"
// //     }
// //
// //     fn as_any(&self) -> &dyn Any {
// //         self
// //     }
// //
// //     fn properties(&self) -> &PlanProperties {
// //         &self.cache
// //     }
// //
// //     fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
// //         vec![]
// //     }
// //
// //     fn with_new_children(
// //         self: Arc<Self>,
// //         _children: Vec<Arc<dyn ExecutionPlan>>,
// //     ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
// //         Ok(self)
// //     }
// //
// //     fn execute(
// //         &self,
// //         _partition: usize,
// //         context: Arc<TaskContext>,
// //     ) -> datafusion::common::Result<SendableRecordBatchStream> {
// //         debug!("GffExec::execute");
// //         debug!("Projection: {:?}", self.projection);
// //         let batch_size = context.session_config().batch_size();
// //         let schema = self.schema.clone();
// //         let fut = get_stream(
// //             self.file_path.clone(),
// //             schema.clone(),
// //             batch_size,
// //             self.thread_num,
// //             self.projection.clone(),
// //             self.object_storage_options.clone(),
// //         );
// //         let stream = futures::stream::once(fut).try_flatten();
// //         Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
// //     }
// // }
//

//
//
// fn set_attribute_builders(
//     batch_size: usize,
//     attributes_names_and_types: (Vec<String>, Vec<DataType>),
//     attribute_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
// ) {
//     let (attribute_names, attribute_types) = attributes_names_and_types;
//     for (name, data_type) in attribute_names.into_iter().zip(attribute_types.into_iter()) {
//         let field = OptionalField::new(&data_type, batch_size).unwrap();
//         attribute_builders.0.push(name);
//         attribute_builders.1.push(data_type);
//         attribute_builders.2.push(field);
//     }
// }
//
// async fn get_remote_gff_stream(
//     file_path: String,
//     schema: SchemaRef,
//     batch_size: usize,
//     attributes: &Attributes,
//     projection: Option<Vec<usize>>,
//     object_storage_options: Option<ObjectStorageOptions>,
// ) -> datafusion::error::Result<
//     AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
// > {
//     let mut reader = GffRemoteReader::new(file_path.clone(), object_storage_options.unwrap()).await;
//     let attributes  = get_attribute_names_and_types(attributes);
//     let mut attribute_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
//         (Vec::new(), Vec::new(), Vec::new());
//     set_attribute_builders(batch_size, attributes, &mut attribute_builders);
//
//
//     let stream = try_stream! {
//         // Create vectors for accumulating record data.
//         let mut chroms: Vec<String> = Vec::with_capacity(batch_size);
//         let mut poss: Vec<u32> = Vec::with_capacity(batch_size);
//         let mut pose: Vec<u32> = Vec::with_capacity(batch_size);
//         let mut ids: Vec<String> = Vec::with_capacity(batch_size);
//         let mut refs: Vec<String> = Vec::with_capacity(batch_size);
//         let mut alts: Vec<String> = Vec::with_capacity(batch_size);
//         let mut quals: Vec<f64> = Vec::with_capacity(batch_size);
//         let mut filters: Vec<String> = Vec::with_capacity(batch_size);
//         // add infos fields vector of vectors of different types
//
//         debug!("Info fields: {:?}", info_builders);
//
//         let mut record_num = 0;
//         let mut batch_num = 0;
//
//
//         // Process records one by one.
//
//         let mut records = reader.read_records().await;
//         while let Some(result) = records.next().await {
//             let record = result?;  // propagate errors if any
//             chroms.push(record.reference_sequence_name().to_string());
//             poss.push(record.variant_start().unwrap()?.get() as u32);
//             pose.push(get_variant_end(&record, &header));
//             ids.push(record.ids().iter().map(|v| v.to_string()).collect::<Vec<String>>().join(";"));
//             refs.push(record.reference_bases().to_string());
//             alts.push(record.alternate_bases().iter().map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join("|"));
//             quals.push(record.quality_score().unwrap_or(Ok(0.0)).unwrap() as f64);
//             filters.push(record.filters().iter(&header).map(|v| v.unwrap_or(".").to_string()).collect::<Vec<String>>().join(";"));
//             load_infos(Box::new(record), &header, &mut info_builders)?;
//             record_num += 1;
//             // Once the batch size is reached, build and yield a record batch.
//             if record_num % batch_size == 0 {
//                 debug!("Record number: {}", record_num);
//                 let batch = build_record_batch(
//                     Arc::clone(&schema.clone()),
//                     &chroms,
//                     &poss,
//                     &pose,
//                     &ids,
//                     &refs,
//                     &alts,
//                     &quals,
//                     &filters,
//                     Some(&builders_to_arrays(&mut info_builders.2)), projection.clone(),
//                     // if infos.is_empty() { None } else { Some(&infos) },
//
//                 )?;
//                 batch_num += 1;
//                 debug!("Batch number: {}", batch_num);
//                 yield batch;
//                 // Clear vectors for the next batch.
//                 chroms.clear();
//                 poss.clear();
//                 pose.clear();
//                 ids.clear();
//                 refs.clear();
//                 alts.clear();
//                 quals.clear();
//                 filters.clear();
//
//             }
//         }
//         // If there are remaining records that don't fill a complete batch,
//         // yield them as well.
//         if !chroms.is_empty() {
//             let batch = build_record_batch(
//                 Arc::clone(&schema.clone()),
//                 &chroms,
//                 &poss,
//                 &pose,
//                 &ids,
//                 &refs,
//                 &alts,
//                 &quals,
//                 &filters,
//                 Some(&builders_to_arrays(&mut info_builders.2)), projection.clone(),
//                 // if infos.is_empty() { None } else { Some(&infos) },
//             )?;
//             yield batch;
//         }
//     };
//     Ok(stream)
//
// }
