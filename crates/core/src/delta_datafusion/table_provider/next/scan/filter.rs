use std::sync::Arc;

use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter_pushdown::FilterDescription;
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{HashMap, plan_err};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::utils::conjunction;
use datafusion::prelude::Expr;
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_features::TableFeature;
use delta_kernel::{Expression, Predicate, PredicateRef};

use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::engine::{
    to_datafusion_expr, to_delta_expression, to_delta_predicate,
};
use crate::delta_datafusion::table_provider::next::FILE_ID_COLUMN_DEFAULT;

pub(crate) fn gather_filters_for_pushdown(
    parent_filters: Vec<Arc<dyn PhysicalExpr>>,
    children: &[&Arc<dyn ExecutionPlan>],
) -> Result<FilterDescription> {
    // TODO(roeap): this will likely not do much for column mapping enabled tables
    // since the default methods determines this based on existence of columns in child
    // schemas. In the case of column mapping all columns will have a different name.
    FilterDescription::from_children(parent_filters, children)
}

pub(crate) fn supports_filters_pushdown(
    filter: &[&Expr],
    config: &TableConfiguration,
    scan_config: &DeltaScanConfig,
) -> Vec<TableProviderFilterPushDown> {
    let file_id_field = scan_config
        .file_column_name
        .as_deref()
        .unwrap_or(FILE_ID_COLUMN_DEFAULT);

    // Parquet predicate pushdown is enabled only when we can safely apply it at read time.
    // Deletion vectors require preserving row order for selection masks, and row tracking
    // disables predicate pushdown in the read plan.
    let parquet_pushdown_enabled = scan_config.enable_parquet_pushdown
        && !config.is_feature_enabled(&TableFeature::RowTracking)
        && !config.is_feature_enabled(&TableFeature::DeletionVectors);
    filter
        .iter()
        .map(|f| process_predicate(f, config, file_id_field, parquet_pushdown_enabled).pushdown)
        .collect()
}

pub(crate) fn process_filters(
    filters: &[Expr],
    config: &TableConfiguration,
    scan_config: &DeltaScanConfig,
) -> Result<(Option<PredicateRef>, Option<Expr>)> {
    let file_id_field = scan_config
        .file_column_name
        .as_deref()
        .unwrap_or(FILE_ID_COLUMN_DEFAULT);

    let parquet_pushdown_enabled = scan_config.enable_parquet_pushdown
        && !config.is_feature_enabled(&TableFeature::RowTracking)
        && !config.is_feature_enabled(&TableFeature::DeletionVectors);
    let (parquet, kernel): (Vec<_>, Vec<_>) = filters
        .iter()
        .map(|f| process_predicate(f, config, file_id_field, parquet_pushdown_enabled))
        .map(|p| (p.parquet_predicate, p.kernel_predicate))
        .unzip();
    let parquet = if config.is_feature_enabled(&TableFeature::ColumnMapping) {
        conjunction(
            parquet
                .iter()
                .flatten()
                .filter_map(|ex| rewrite_expression((*ex).clone(), config).ok()),
        )
    } else {
        conjunction(parquet.iter().flatten().map(|ex| (*ex).clone()))
    };
    let kernel = (!kernel.is_empty()).then(|| Predicate::and_from(kernel.into_iter().flatten()));
    Ok((kernel.map(Arc::new), parquet))
}

struct ProcessedPredicate<'a> {
    pub pushdown: TableProviderFilterPushDown,
    pub kernel_predicate: Option<Predicate>,
    pub parquet_predicate: Option<&'a Expr>,
}

fn process_predicate<'a>(
    expr: &'a Expr,
    config: &TableConfiguration,
    file_id_column: &str,
    parquet_pushdown_enabled: bool,
) -> ProcessedPredicate<'a> {
    let cols = config.metadata().partition_columns();
    let only_partition_refs = expr.column_refs().iter().all(|c| cols.contains(&c.name));
    let any_partition_refs =
        only_partition_refs || expr.column_refs().iter().any(|c| cols.contains(&c.name));
    let has_file_id = expr.column_refs().iter().any(|c| file_id_column == c.name);

    if has_file_id {
        // file-id filters cannot be evaluated in kernel and must not be pushed to parquet.
        // Mark as Unsupported so DataFusion keeps a post-scan filter for correctness.
        return ProcessedPredicate {
            pushdown: TableProviderFilterPushDown::Unsupported,
            kernel_predicate: None,
            parquet_predicate: None,
        };
    }

    // TODO(roeap): we may allow pusing predicates referencing partition columns
    // into the parquet scan, if the table has materialized partition columns
    let _has_partition_data = config.is_feature_enabled(&TableFeature::MaterializePartitionColumns);

    // Try to convert the expression into a kernel predicate
    if let Ok(kernel_predicate) = to_delta_predicate(expr) {
        let (pushdown, parquet_predicate) = if only_partition_refs {
            // All references are to partition columns so the kernel
            // scan can fully handle the predicate and return exact results
            (TableProviderFilterPushDown::Exact, None)
        } else if any_partition_refs {
            // Some references are to partition columns, so the kernel
            // scan can only handle the predicate on best effort. Since the
            // parquet scan cannot reference partition columns, we do not
            // push down any predicate to parquet
            (TableProviderFilterPushDown::Inexact, None)
        } else {
            // For non-partition predicates we can *attempt* Parquet pushdown, but it is not a
            // correctness boundary (it may be partially applied or skipped). Keep this Inexact so
            // DataFusion retains a post-scan Filter.
            (
                TableProviderFilterPushDown::Inexact,
                parquet_pushdown_enabled.then_some(expr),
            )
        };
        return ProcessedPredicate {
            pushdown,
            kernel_predicate: Some(kernel_predicate),
            parquet_predicate,
        };
    }

    // If there are any partition column references, we cannot
    // push down the predicate to parquet scan
    if any_partition_refs {
        return ProcessedPredicate {
            pushdown: TableProviderFilterPushDown::Unsupported,
            kernel_predicate: None,
            parquet_predicate: None,
        };
    }

    ProcessedPredicate {
        pushdown: TableProviderFilterPushDown::Inexact,
        kernel_predicate: None,
        parquet_predicate: parquet_pushdown_enabled.then_some(expr),
    }
}

fn rewrite_expression(expr: Expr, config: &TableConfiguration) -> Result<Expr> {
    let logical_fields = config.schema().leaves(None);
    let (logical_names, _) = logical_fields.as_ref();
    let physical_schema = config
        .schema()
        .make_physical(config.column_mapping_mode())
        .leaves(None);
    let (physical_names, _) = physical_schema.as_ref();
    let name_mapping: HashMap<_, _> = logical_names.iter().zip(physical_names).collect();
    let transformed = expr.transform(|node| match &node {
        // Scalar functions might be field a field access for a nested column
        // (e.g. `a.b.c`), so we might be able to handle them here as well
        Expr::Column(_) | Expr::ScalarFunction(_) => {
            let col_name = to_delta_expression(&node)?;
            if let Expression::Column(name) = &col_name {
                if let Some(physical_name) = name_mapping.get(name) {
                    return Ok(Transformed::yes(to_datafusion_expr(
                        &Expression::Column((*physical_name).clone()),
                        // This is just a dummy datatype, since column re-writes
                        // do not require datatype information
                        &KernelDataType::BOOLEAN,
                    )?));
                } else {
                    return plan_err!("Column '{name}' not found in physical schema");
                }
            }
            Ok(Transformed::no(node))
        }
        _ => Ok(Transformed::no(node)),
    })?;

    Ok(transformed.data)
}
