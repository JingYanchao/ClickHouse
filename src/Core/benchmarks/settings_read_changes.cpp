#include <benchmark/benchmark.h>

#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace
{

String makeSettings(size_t count)
{
    Settings defaults;
    WriteBufferFromOwnString out;
    auto names = defaults.getAllRegisteredNames();
    for (size_t i = 0; i < count && i < names.size(); ++i)
    {
        BaseSettingsHelpers::writeString(String{names[i]}, out);
        BaseSettingsHelpers::writeFlags(BaseSettingsHelpers::Flags(0), out);
        BaseSettingsHelpers::writeString(defaults.getDefaultValueString(names[i]), out);
    }
    BaseSettingsHelpers::writeString("", out);
    return out.str();
}

void BM_SettingsRead(benchmark::State & state)
{
    String data = makeSettings(state.range(0));
    for (auto _ [[maybe_unused]] : state)
    {
        ReadBufferFromString in(data);
        Settings settings;
        settings.read(in, SettingsWriteFormat::STRINGS_WITH_FLAGS);
        auto changes = settings.changes();
        benchmark::DoNotOptimize(changes);
    }
}

void BM_SettingsReadChanges(benchmark::State & state)
{
    String data = makeSettings(state.range(0));
    for (auto _ [[maybe_unused]] : state)
    {
        ReadBufferFromString in(data);
        auto changes = Settings::readChanges(in, SettingsWriteFormat::STRINGS_WITH_FLAGS);
        benchmark::DoNotOptimize(changes);
    }
}

}

BENCHMARK(BM_SettingsRead)->Arg(0)->Arg(1)->Arg(8)->Arg(64);
BENCHMARK(BM_SettingsReadChanges)->Arg(0)->Arg(1)->Arg(8)->Arg(64);
