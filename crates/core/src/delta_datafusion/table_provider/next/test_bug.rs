use crate::DeltaTable;
use crate::delta_datafusion::DeltaScanConfig;
use crate::kernel::{DataType, PrimitiveType, StructField, StructType};
use arrow::array::{Int64Array, TimestampMillisecondArray};
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

async fn get_table_and_provider() -> (DeltaTable, Arc<crate::delta_datafusion::DeltaScanNext>) {
    let physical_schema = StructType::try_new(vec![StructField::new(
        "id".to_string(),
        DataType::Primitive(PrimitiveType::Long),
        true,
    )])
    .unwrap();

    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(physical_schema.fields().cloned())
        .await
        .unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            true,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();

    let table = crate::operations::write::WriteBuilder::new(
        table.log_store(),
        table.state.clone().map(|s| s.snapshot().clone()),
    )
    .with_input_batches(vec![batch])
    .await
    .unwrap();

    let logical_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let config = DeltaScanConfig::default().with_schema(logical_schema.clone());

    let provider = crate::delta_datafusion::DeltaScanNext::new(
        table.state.as_ref().unwrap().snapshot().clone(),
        config,
    )
    .unwrap()
    .with_log_store(table.log_store());

    (table, Arc::new(provider))
}

fn register_table(
    ctx: &SessionContext,
    name: &str,
    table: &DeltaTable,
    provider: Arc<crate::delta_datafusion::DeltaScanNext>,
) {
    ctx.runtime_env().register_object_store(
        table.log_store().root_url(),
        table.log_store().object_store(None),
    );
    ctx.register_table(name, provider).unwrap();
}

#[tokio::test]
async fn test_delta_scan_config_schema_override_scan() {
    let (table, provider) = get_table_and_provider().await;

    let ctx = SessionContext::new();
    register_table(&ctx, "test_table", &table, provider);

    let df = ctx.sql("SELECT id FROM test_table").await.unwrap();
    let res = df.collect().await;
    assert!(res.is_ok(), "Expected OK, but got: {:?}", res);

    let batches = res.unwrap();
    assert_eq!(
        batches[0].schema().fields()[0].data_type(),
        &ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
    );
}

#[tokio::test]
async fn test_delta_scan_config_schema_override_filter() {
    let (table, provider) = get_table_and_provider().await;

    let ctx = SessionContext::new();
    register_table(&ctx, "test_table", &table, provider);

    let df = ctx
        .sql("SELECT id FROM test_table WHERE id < '2020-01-01T00:00:00Z'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(
        batches[0].schema().fields()[0].data_type(),
        &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
        "Filter output schema does not match logical schema"
    );
}

#[tokio::test]
async fn test_delta_scan_config_schema_override_insert() {
    let (table, provider) = get_table_and_provider().await;

    let ctx = SessionContext::new();
    register_table(&ctx, "test_table", &table, provider.clone());
    let state = ctx.state();

    let logical_schema = provider.schema();

    let batch = RecordBatch::try_new(
        logical_schema.clone(),
        vec![Arc::new(TimestampMillisecondArray::from(vec![2000]))],
    )
    .unwrap();

    let mem_table = MemTable::try_new(logical_schema.clone(), vec![vec![batch]]).unwrap();
    let input = mem_table.scan(&state, None, &[], None).await.unwrap();

    let write_plan = provider
        .insert_into(&state, input, InsertOp::Append)
        .await
        .unwrap();
    let res = datafusion::physical_plan::collect_partitioned(write_plan, ctx.task_ctx()).await;
    assert!(res.is_ok(), "Expected OK, but got: {:?}", res);
}

#[tokio::test]
async fn test_delta_scan_config_file_column_projection() {
    let (table, _) = get_table_and_provider().await;

    let logical_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let config = DeltaScanConfig::default()
        .with_schema(logical_schema.clone())
        .with_file_column_name("_file");

    let provider = crate::delta_datafusion::DeltaScanNext::new(
        table.state.as_ref().unwrap().snapshot().clone(),
        config,
    )
    .unwrap()
    .with_log_store(table.log_store());

    let ctx = SessionContext::new();
    register_table(&ctx, "test_table", &table, Arc::new(provider));

    let df = ctx.sql("SELECT id FROM test_table").await.unwrap();
    let res = df.collect().await;
    assert!(res.is_ok(), "Expected OK, but got: {:?}", res);

    let batches = res.unwrap();
    assert_eq!(
        batches[0].schema().fields().len(),
        1,
        "Expected exactly 1 field after projection, but got: {:?}",
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );
}
