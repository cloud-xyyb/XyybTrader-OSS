# 公开 SEL 测试：源文件显式枚举。
add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/googletest ${CMAKE_BINARY_DIR}/googletest EXCLUDE_FROM_ALL)
add_executable(xt_public_tests
    backtest/CtaGatewayContractTest.cpp
    backtest/SelConsumedManifestTest.cpp
    backtest/XtSelBacktestRunnerTest.cpp
    backtest/XtSelBtArtifactsTest.cpp
    backtest/XtSelCorporateActionTest.cpp
    backtest/XtSelTradabilityTest.cpp
    cta/XtCtaMatchEngineCausalityTest.cpp
    cta/XtCtaMatchEngineIsolationTest.cpp
    data/SelCalendarFallbackTest.cpp
    data/XtDatalakeReaderTest.cpp
    data/XtDatalakeRootTest.cpp
    engine/sel/SelTaskSchedulerTest.cpp
    engine/sel/XtSelBacktestE2ETest.cpp
    engine/sel/XtSelEngineSinkTest.cpp
    engine/sel/XtSelEngineTest.cpp
    engine/sel/XtSelOffsetInferenceTest.cpp
    strategy/SelWeightsCsvStrategyTest.cpp
    ${CMAKE_SOURCE_DIR}/xt_strategy_plugin/sel_weights_csv/SelWeightsCsvStrategy.cpp
)
target_include_directories(xt_public_tests PRIVATE
    ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/api ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine ${CMAKE_SOURCE_DIR}/xt_config
)
target_link_libraries(xt_public_tests PRIVATE
    gtest_main xt_bt_sel xt_bt_sel_gateway xt_engine_sel xt_data_datalake xt_stra
    xt_bt_hft xt_bt_data xt_bt_reporter xt_report xt_account xt_risk xt_cost
    xt_config xt_logger xt_runtime xt_core xt_util yaml-cpp OpenSSL::Crypto dl
)
include(GoogleTest)
gtest_discover_tests(xt_public_tests DISCOVERY_MODE PRE_TEST)
