/// Microbenchmark for UniqueKeyEncoding::encodeBlock.
///
/// Measures per-row throughput for the UNIQUE KEY byte-comparable
/// serialization path - the exact function refactored in PR #109010
/// (centralized type-switch -> per-column virtual methods on IColumn).
///
/// Each benchmark name encodes: BM_encodeBlock/<Type>_K<K>_B<batch>[_Perm]
///   K    = number of key columns chained
///   B    = batch size (rows)
///   Perm = permutation variant (unsorted writer path)
///
/// Run with:
///   ./benchmark_unique_key_encoding --benchmark_filter=.
///   ./benchmark_unique_key_encoding --benchmark_filter=UInt64 --benchmark_format=json
///
/// To compare before/after the refactoring:
///   1. Build at the "before" commit (parent of the refactoring), run:
///      ./benchmark_unique_key_encoding --benchmark_format=json > before.json
///   2. Build at the "after" commit, run:
///      ./benchmark_unique_key_encoding --benchmark_format=json > after.json
///   3. Compare:
///      python3 -c "import json,sys; ..."
///      or use ../contrib/google-benchmark/tools/compare.py
///
/// NOTE: On the "before" version the output container is std::vector<String>;
/// change the EncodedVector alias below to match.

#include <base/extended_types.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/randomSeed.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

using namespace DB;
using namespace DB::UniqueKeyEncoding;

namespace
{

/// Output container type.
/// After refactoring: VectorWithMemoryTracking<String>.
/// Before refactoring: std::vector<String> - change this alias to match.
using EncodedVector = VectorWithMemoryTracking<String>;

/// Generous limit for benchmark purposes - never expected to trigger.
constexpr size_t MAX_ENCODED_SIZE = 1 << 20; // 1 MiB

/// ─── Data generators ──────────────────────────────────────────────────────

std::vector<ColumnPtr> makeUInt64Columns(size_t K, size_t n)
{
    std::mt19937_64 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnUInt64::create();
        auto & data = col->getData();
        data.resize(n);
        for (size_t i = 0; i < n; ++i)
            data[i] = rng();
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeInt64Columns(size_t K, size_t n)
{
    std::mt19937_64 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnInt64::create();
        auto & data = col->getData();
        data.resize(n);
        for (size_t i = 0; i < n; ++i)
            data[i] = static_cast<Int64>(rng());
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeFloat64Columns(size_t K, size_t n)
{
    std::mt19937_64 rng(randomSeed());
    std::uniform_real_distribution<Float64> dist(-1e6, 1e6);
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnFloat64::create();
        auto & data = col->getData();
        data.resize(n);
        for (size_t i = 0; i < n; ++i)
            data[i] = dist(rng);
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeStringColumns(size_t K, size_t n, size_t avg_len = 16)
{
    std::mt19937 rng(randomSeed());
    std::uniform_int_distribution<size_t> len_dist(4, 2 * avg_len);
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnString::create();
        for (size_t i = 0; i < n; ++i)
        {
            const size_t len = len_dist(rng);
            std::string s(len, static_cast<char>('a' + (rng() % 26)));
            col->insertData(s.data(), s.size());
        }
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeNullableStringColumns(size_t K, size_t n)
{
    std::mt19937 rng(randomSeed());
    std::uniform_int_distribution<size_t> len_dist(4, 32);
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto nested = ColumnString::create();
        auto null_map = ColumnUInt8::create();
        for (size_t i = 0; i < n; ++i)
        {
            if (rng() % 10 == 0)
            {
                /// ~10% NULLs - exercises the null-flag encoding path
                nested->insertDefault();
                null_map->insert(static_cast<UInt64>(1));
            }
            else
            {
                const size_t len = len_dist(rng);
                std::string s(len, static_cast<char>('a' + (rng() % 26)));
                nested->insertData(s.data(), s.size());
                null_map->insert(static_cast<UInt64>(0));
            }
        }
        cols.push_back(ColumnNullable::create(std::move(nested), std::move(null_map)));
    }
    return cols;
}

std::vector<ColumnPtr> makeNullableUInt64Columns(size_t K, size_t n)
{
    std::mt19937_64 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto nested = ColumnUInt64::create();
        auto null_map = ColumnUInt8::create();
        auto & nested_data = nested->getData();
        auto & null_data = null_map->getData();
        nested_data.resize(n);
        null_data.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            nested_data[i] = rng();
            null_data[i] = (rng() % 10 == 0) ? 1 : 0;
        }
        cols.push_back(ColumnNullable::create(std::move(nested), std::move(null_map)));
    }
    return cols;
}

std::vector<ColumnPtr> makeDecimal64Columns(size_t K, size_t n)
{
    std::mt19937_64 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnDecimal<Decimal64>::create(0, 3);
        auto & data = col->getData();
        data.resize(n);
        for (size_t i = 0; i < n; ++i)
            data[i] = Decimal64(static_cast<Int64>(rng() % 1000000));
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeUUIDColumns(size_t K, size_t n)
{
    std::mt19937_64 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnUUID::create();
        auto & data = col->getData();
        data.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            /// Construct UUID from a single uint64 - value variety does not
            /// affect encoding cost for fixed-size types (byte-swap only).
            data[i] = UUID(UInt128(rng()));
        }
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeFixedStringColumns(size_t K, size_t n, size_t fixed_len = 16)
{
    std::mt19937 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnFixedString::create(fixed_len);
        for (size_t i = 0; i < n; ++i)
        {
            std::string s(fixed_len, static_cast<char>('a' + (rng() % 26)));
            col->insertData(s.data(), s.size());
        }
        cols.push_back(std::move(col));
    }
    return cols;
}

/// ─── Benchmark function ───────────────────────────────────────────────────

void BM_EncodeBlock(benchmark::State & state, const std::vector<ColumnPtr> & cols, bool use_permutation)
{
    const size_t n = cols[0]->size();
    const size_t ncols = cols.size();

    /// Build a fixed random permutation once (models the unsorted writer path
    /// where encodeBlock receives a pre-computed permutation).
    IColumn::Permutation perm;
    if (use_permutation)
    {
        perm.resize(n);
        std::mt19937 rng(42);
        for (size_t i = 0; i < n; ++i)
            perm[i] = i;
        std::shuffle(perm.begin(), perm.end(), rng);
    }

    const IColumn::Permutation * perm_ptr = use_permutation ? &perm : nullptr;
    Columns uk_columns(cols.begin(), cols.end());

    for (auto _ : state)
    {
        EncodedVector encoded;
        encodeBlock(uk_columns, perm_ptr, MAX_ENCODED_SIZE, encoded);
        benchmark::DoNotOptimize(encoded.data());
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
    state.counters["rows/iter"] = static_cast<double>(n);
    state.counters["K"] = static_cast<double>(ncols);
    state.counters["perm"] = use_permutation ? 1.0 : 0.0;
}

/// ─── Registration macro ───────────────────────────────────────────────────

#define REGISTER_ENCODE(tag, make_fn, K, B, use_perm)                                   \
    do                                                                                  \
    {                                                                                   \
        static const auto cols_##tag##_K##K##_B##B##_P##use_perm = make_fn(K, B);      \
        benchmark::RegisterBenchmark(                                                   \
            "BM_encodeBlock/" #tag "_K" #K "_B" #B "_Perm" #use_perm,                   \
            [](benchmark::State & st)                                                   \
            { BM_EncodeBlock(st, cols_##tag##_K##K##_B##B##_P##use_perm, use_perm); }); \
    } while (false)

} // namespace

int main(int argc, char ** argv)
{
    // ─── UInt64 (most common integer key type) ──────────────────────────────
    REGISTER_ENCODE(UInt64, makeUInt64Columns, 1, 4096, false);
    REGISTER_ENCODE(UInt64, makeUInt64Columns, 1, 16384, false);
    REGISTER_ENCODE(UInt64, makeUInt64Columns, 1, 65536, false);
    REGISTER_ENCODE(UInt64, makeUInt64Columns, 3, 16384, false);
    REGISTER_ENCODE(UInt64, makeUInt64Columns, 3, 65536, false);
    REGISTER_ENCODE(UInt64, makeUInt64Columns, 1, 16384, true);
    REGISTER_ENCODE(UInt64, makeUInt64Columns, 3, 16384, true);

    // ─── Int64 ──────────────────────────────────────────────────────────────
    REGISTER_ENCODE(Int64, makeInt64Columns, 1, 16384, false);
    REGISTER_ENCODE(Int64, makeInt64Columns, 3, 16384, false);
    REGISTER_ENCODE(Int64, makeInt64Columns, 3, 16384, true);

    // ─── Float64 ────────────────────────────────────────────────────────────
    REGISTER_ENCODE(Float64, makeFloat64Columns, 1, 16384, false);
    REGISTER_ENCODE(Float64, makeFloat64Columns, 3, 16384, false);

    // ─── String (variable length, ~16 B avg) ────────────────────────────────
    REGISTER_ENCODE(String, makeStringColumns, 1, 4096, false);
    REGISTER_ENCODE(String, makeStringColumns, 1, 16384, false);
    REGISTER_ENCODE(String, makeStringColumns, 3, 16384, false);
    REGISTER_ENCODE(String, makeStringColumns, 3, 16384, true);

    // ─── Nullable(String) ───────────────────────────────────────────────────
    REGISTER_ENCODE(NullableString, makeNullableStringColumns, 1, 16384, false);
    REGISTER_ENCODE(NullableString, makeNullableStringColumns, 3, 16384, false);
    REGISTER_ENCODE(NullableString, makeNullableStringColumns, 3, 16384, true);

    // ─── Nullable(UInt64) ───────────────────────────────────────────────────
    REGISTER_ENCODE(NullableUInt64, makeNullableUInt64Columns, 1, 16384, false);
    REGISTER_ENCODE(NullableUInt64, makeNullableUInt64Columns, 3, 16384, false);

    // ─── Decimal64 ──────────────────────────────────────────────────────────
    REGISTER_ENCODE(Decimal64, makeDecimal64Columns, 1, 16384, false);
    REGISTER_ENCODE(Decimal64, makeDecimal64Columns, 3, 16384, false);

    // ─── UUID ───────────────────────────────────────────────────────────────
    REGISTER_ENCODE(UUID, makeUUIDColumns, 1, 16384, false);
    REGISTER_ENCODE(UUID, makeUUIDColumns, 3, 16384, false);

    // ─── FixedString(16) ────────────────────────────────────────────────────
    REGISTER_ENCODE(FixedString, makeFixedStringColumns, 1, 16384, false);
    REGISTER_ENCODE(FixedString, makeFixedStringColumns, 3, 16384, false);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
