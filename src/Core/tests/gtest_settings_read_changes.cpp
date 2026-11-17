#include <gtest/gtest.h>

#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <algorithm>
#include <iterator>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

struct Entry
{
    String name;
    UInt64 flags = 0;
    String value;
};

String serialize(const std::vector<Entry> & entries)
{
    WriteBufferFromOwnString out;
    for (const auto & entry : entries)
    {
        BaseSettingsHelpers::writeString(entry.name, out);
        BaseSettingsHelpers::writeFlags(static_cast<BaseSettingsHelpers::Flags>(entry.flags), out);
        BaseSettingsHelpers::writeString(entry.value, out);
    }
    BaseSettingsHelpers::writeString("", out);
    return out.str();
}

/// Compares `Settings::readChanges` with `read` + `changes`, including the order of built-in settings.
void expectSameChanges(const String & data)
{
    ReadBufferFromString old_in(data);
    Settings settings;
    settings.read(old_in, SettingsWriteFormat::STRINGS_WITH_FLAGS);
    SettingsChanges expected = settings.changes();

    ReadBufferFromString new_in(data);
    SettingsChanges actual = Settings::readChanges(new_in, SettingsWriteFormat::STRINGS_WITH_FLAGS);
    EXPECT_TRUE(new_in.eof());

    /// Custom settings come from an unordered map in `changes`, so only built-in ones have a defined order.
    auto is_builtin = [](const SettingChange & change) { return Settings::hasBuiltin(change.name); };
    SettingsChanges expected_builtin;
    SettingsChanges actual_builtin;
    std::copy_if(expected.begin(), expected.end(), std::back_inserter(expected_builtin), is_builtin);
    std::copy_if(actual.begin(), actual.end(), std::back_inserter(actual_builtin), is_builtin);
    EXPECT_EQ(expected_builtin, actual_builtin);

    auto by_name = [](const SettingChange & lhs, const SettingChange & rhs) { return lhs.name < rhs.name; };
    std::stable_sort(expected.begin(), expected.end(), by_name);
    std::stable_sort(actual.begin(), actual.end(), by_name);
    EXPECT_EQ(expected, actual);
}

/// Both readers must fail on the same input with the same error code.
void expectSameError(const String & data, int expected_code)
{
    auto error_code = [&](auto && read)
    {
        try
        {
            ReadBufferFromString in(data);
            read(in);
        }
        catch (const Exception & e)
        {
            return e.code();
        }
        return 0;
    };

    EXPECT_EQ(error_code([](ReadBuffer & in) { Settings().read(in, SettingsWriteFormat::STRINGS_WITH_FLAGS); }), expected_code);
    EXPECT_EQ(error_code([](ReadBuffer & in) { Settings::readChanges(in, SettingsWriteFormat::STRINGS_WITH_FLAGS); }), expected_code);
}

}

TEST(SettingsReadChanges, EquivalentToRead)
{
    expectSameChanges(serialize({}));

    Settings settings;
    settings.set("max_threads", Field(UInt64(1)));
    settings.set("send_logs_level", Field("none"));
    settings.set("log_comment", Field("a'b"));
    settings.set("compatibility", Field("23.8"));
    settings.set("max_execution_time", Field(Float64(1.5)));
    settings.set("run_query_in_background", Field(true));

    WriteBufferFromOwnString out;
    settings.write(out, SettingsWriteFormat::STRINGS_WITH_FLAGS);
    expectSameChanges(out.str());
}

TEST(SettingsReadChanges, WireEdgeCases)
{
    /// Out of index order, an alias and its canonical name, repeated names (later values win),
    /// custom settings and unknown non-important settings (skipped with a warning).
    expectSameChanges(serialize({
        {"use_uncompressed_cache", 0, "1"},
        {"max_threads", 0, "4"},
        {"enable_analyzer", 0, "0"},
        {"no_such_setting", 0, "x"},
        {"allow_experimental_analyzer", 0, "1"},
        {"max_threads", 0, "auto"},
        {"max_block_size", BaseSettingsHelpers::Flags::CUSTOM, Field(UInt64(1000)).dump()},
        {"custom_a", BaseSettingsHelpers::Flags::CUSTOM, Field("x").dump()},
        {"custom_b", BaseSettingsHelpers::Flags::CUSTOM, Field(Int64(-3)).dump()},
        {"custom_a", BaseSettingsHelpers::Flags::CUSTOM, Field("y").dump()},
    }));

    expectSameError(serialize({{"no_such_setting", BaseSettingsHelpers::Flags::IMPORTANT, "1"}}), ErrorCodes::UNKNOWN_SETTING);
    expectSameError(
        serialize(std::vector<Entry>(MAX_SETTINGS_IN_A_SEQUENCE + 1, Entry{"max_threads", 0, "1"})), ErrorCodes::TOO_LARGE_ARRAY_SIZE);
}

TEST(SettingsReadChanges, InvalidValue)
{
    String data = serialize({{"max_threads", 0, "not_a_number"}});
    int code = 0;
    try
    {
        ReadBufferFromString in(data);
        Settings().read(in, SettingsWriteFormat::STRINGS_WITH_FLAGS);
    }
    catch (const Exception & e)
    {
        code = e.code();
    }
    ASSERT_NE(code, 0);
    expectSameError(data, code);
}

TEST(SettingsReadChanges, BinaryFormatFallback)
{
    Settings settings;
    settings.set("max_threads", Field(UInt64(3)));
    settings.set("log_comment", Field("abc"));

    WriteBufferFromOwnString out;
    settings.write(out, SettingsWriteFormat::BINARY);

    ReadBufferFromString in(out.str());
    EXPECT_EQ(Settings::readChanges(in, SettingsWriteFormat::BINARY), settings.changes());
}
