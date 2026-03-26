#pragma once

#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Find the unique projection in storage metadata and extract its key column names.
/// Returns empty Names if no unique projection exists.
Names getUniqueKeyColumns(const StorageMetadataPtr & metadata_snapshot);
/** Interprets the UPDATE query for unique merge tree.
  * Rewrites UPDATE into INSERT SELECT so that the unique projection handles
  * deduplication (upsert semantics).
  */
class InterpreterUniqueUpdateQuery : public InterpreterInsertQuery
{
public:
    InterpreterUniqueUpdateQuery(const ASTPtr & query_ptr_, ContextPtr context_);

protected:
    ASTPtr rewriteQueryIfNeed(ASTPtr query) override;

private:
    /// Delegating constructor: receives an already-created mutable context.
    InterpreterUniqueUpdateQuery(const ASTPtr & query_ptr_, ContextMutablePtr mutable_context);

    /// Must hold a strong reference to the mutable context copy, because the
    /// base class `WithMutableContext` only stores a `weak_ptr`.  Without this
    /// the context would be destroyed immediately after construction, causing
    /// "Context has expired" exception on any subsequent `getContext` call.
    ContextMutablePtr mutable_context_holder;
    std::unordered_map<String, ASTPtr> column_to_update_expression;
};

}
