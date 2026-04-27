#include <Interpreters/InterpreterUniqueUpdateQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Storages/IStorage.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>
#include <Core/Settings.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

/// Find the unique projection in storage metadata and extract its key column names.
/// Returns empty Names if no unique projection exists.
/// Note: only returns the dedup key columns, NOT the version column.
Names getUniqueKeyColumns(const StorageMetadataPtr & metadata_snapshot)
{
    for (const auto & projection : metadata_snapshot->getProjections())
    {
        auto * unique_index = dynamic_cast<const ProjectionIndexUnique *>(projection.index.get());
        if (unique_index)
        {
            /// projection.required_columns = unique_key_columns + optional version_column.
            /// Filter out the version column if present.
            const auto & version_col = unique_index->getVersionColumnName();
            Names result;
            result.reserve(projection.required_columns.size());
            for (const auto & col : projection.required_columns)
            {
                if (col != version_col)
                    result.push_back(col);
            }
            return result;
        }
    }
    return {};
}

static ASTPtr constructSelectQuery(const ASTUpdateQuery & query, const std::vector<ASTPtr> & column_expressions, const String & database, const String & table)
{
    auto select = make_intrusive<ASTSelectQuery>();
    auto expr = make_intrusive<ASTExpressionList>();
    for (auto & col : column_expressions)
    {
        expr->children.emplace_back(col->clone());
    }

    auto table_expression_node_ast = make_intrusive<ASTTableIdentifier>(database, table);
    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    auto tables_elem = make_intrusive<ASTTablesInSelectQueryElement>();
    auto table_expr = make_intrusive<ASTTableExpression>();
    tables->children.push_back(tables_elem);
    tables_elem->table_expression = table_expr;
    tables_elem->children.push_back(table_expr);
    table_expr->children.push_back(table_expression_node_ast);
    table_expr->database_and_table_name = table_expr->children.back();

    select->setExpression(ASTSelectQuery::Expression::SELECT, expr);
    select->setExpression(ASTSelectQuery::Expression::WHERE, query.predicate->clone());
    select->setExpression(ASTSelectQuery::Expression::TABLES, tables);

    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select));
    auto res = make_intrusive<ASTSelectWithUnionQuery>();
    res->union_mode = SelectUnionMode::UNION_ALL;
    res->children.push_back(std::move(list_of_selects));
    res->list_of_selects = res->children.back();
    res->list_of_modes.push_back(SelectUnionMode::UNION_ALL);

    return res;
}

static ASTPtr buildInsertQuery(ASTPtr select, const ASTUpdateQuery & ast, const StorageID & id)
{
    auto res = make_intrusive<ASTInsertQuery>();
    res->select = std::move(select);
    res->children.push_back(res->select);
    res->table_id = id;
    if (ast.database)
    {
        res->database = ast.database->clone();
        res->children.push_back(res->database);
    }
    if (ast.table)
    {
        res->table = ast.table->clone();
        res->children.push_back(res->table);
    }
    return res;
}

InterpreterUniqueUpdateQuery::InterpreterUniqueUpdateQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : InterpreterUniqueUpdateQuery(query_ptr_, Context::createCopy(context_))
{
}

InterpreterUniqueUpdateQuery::InterpreterUniqueUpdateQuery(const ASTPtr & query_ptr_, ContextMutablePtr mutable_context)
    : InterpreterInsertQuery(
        query_ptr_,
        mutable_context,
        /* allow_materialized_ */ false,
        /* no_squash_ */ false,
        /* no_destination */ false,
        /* async_insert_ */ false)
    , mutable_context_holder(std::move(mutable_context))
{
}

ASTPtr InterpreterUniqueUpdateQuery::rewriteQueryIfNeed(DB::ASTPtr query_ptr)
{
    auto context = getContext();
    const Settings & settings = context->getSettingsRef();
    auto & query = query_ptr->as<ASTUpdateQuery &>();

    /// Resolve the target table.
    auto id = context->resolveStorageID(StorageID{query.getDatabase(), query.getTable()});
    StoragePtr table = DatabaseCatalog::instance().getTable(id, context);

    /// Obtain metadata of storage.
    auto table_lock = table->lockForShare(context->getInitialQueryId(), settings[Setting::lock_acquire_timeout]);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto query_sample_block = metadata_snapshot->getSampleBlockInsertable();
    const ColumnsDescription & columns_desc = metadata_snapshot->getColumns();

    /// Collect updated columns and their update expressions.
    for (const ASTPtr & assignment_ast : query.assignments->children)
    {
        const auto & assignment = assignment_ast->as<ASTAssignment &>();
        auto insertion = column_to_update_expression.emplace(assignment.column_name, assignment.expression());
        if (!insertion.second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Multiple assignments in the single statement to column {}",
                            backQuote(assignment.column_name));
        if (!columns_desc.has(assignment.column_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The column {} cannot be updated because it does not belong to the table {}",
                            backQuote(assignment.column_name), id.getFullTableName());
    }

    /// Forbid updating immutable columns (primary key, unique key, ORDER BY).
    auto forbid_updating_columns = [this](const Names & columns, const char * kind)
    {
        for (const auto & col : columns)
        {
            if (column_to_update_expression.contains(col))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot update {} column {}", kind, backQuote(col));
        }
    };

    forbid_updating_columns(metadata_snapshot->getColumnsRequiredForPrimaryKey(), "primary key");
    forbid_updating_columns(getUniqueKeyColumns(metadata_snapshot), "unique key");
    forbid_updating_columns(metadata_snapshot->getColumnsRequiredForSortingKey(), "ORDER BY");
    /// Note: the version column IS allowed to be updated — users may want to
    /// bump it to control conflict resolution in versioned upsert mode.

    /// Build the SELECT expression list: for each insertable column, use the
    /// update expression if provided, otherwise read the original column value.
    std::vector<ASTPtr> column_expressions;
    column_expressions.reserve(query_sample_block.columns());
    for (const auto & col : query_sample_block)
    {
        const auto updated_expr_it = column_to_update_expression.find(col.name);
        if (updated_expr_it != column_to_update_expression.end())
            column_expressions.emplace_back(updated_expr_it->second->clone());
        else
            column_expressions.push_back(make_intrusive<ASTIdentifier>(col.name));
    }

    /// Transform to: INSERT INTO table SELECT ... FROM table WHERE <predicate>
    auto select = constructSelectQuery(query, column_expressions, id.database_name, id.table_name);
    return buildInsertQuery(std::move(select), query, id);
}

}
