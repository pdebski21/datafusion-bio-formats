// use std::any::Any;
// use std::fmt::{Debug, Formatter};
// use std::sync::Arc;
// use datafusion::arrow::datatypes::SchemaRef;
// use datafusion::execution::{SendableRecordBatchStream, TaskContext};
// use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
// use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
// use futures_util::TryStreamExt;
// use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
// use log::debug;
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
// impl ExecutionPlan for GffExec {
//     fn name(&self) -> &str {
//         "GffExec"
//     }
//
//     fn as_any(&self) -> &dyn Any {
//         self
//     }
//
//     fn properties(&self) -> &PlanProperties {
//         &self.cache
//     }
//
//     fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
//         vec![]
//     }
//
//     fn with_new_children(
//         self: Arc<Self>,
//         _children: Vec<Arc<dyn ExecutionPlan>>,
//     ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
//         Ok(self)
//     }
//
//     fn execute(
//         &self,
//         _partition: usize,
//         context: Arc<TaskContext>,
//     ) -> datafusion::common::Result<SendableRecordBatchStream> {
//         debug!("GffExec::execute");
//         debug!("Projection: {:?}", self.projection);
//         let batch_size = context.session_config().batch_size();
//         let schema = self.schema.clone();
//         let fut = get_stream(
//             self.file_path.clone(),
//             schema.clone(),
//             batch_size,
//             self.thread_num,
//             self.projection.clone(),
//             self.object_storage_options.clone(),
//         );
//         let stream = futures::stream::once(fut).try_flatten();
//         Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
//     }
// }
