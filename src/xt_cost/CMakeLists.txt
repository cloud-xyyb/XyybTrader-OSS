# xt_cost — Trade cost/fee model.
#
# 业务边界：fee / slippage / commission / maker-rebate 计算与配置解析。
#
# Dependencies:
# - xt_core: XtTradeRecord 等 POD 类型
# - xt_config: XtCommodityCatalog / IFeeRuleProvider

add_library(xt_cost STATIC
    ${CMAKE_SOURCE_DIR}/xt_cost/XtFillCostModel.cpp
    ${CMAKE_SOURCE_DIR}/xt_cost/XtFillCostApplier.cpp
    ${CMAKE_SOURCE_DIR}/xt_cost/XtTradeCostConfigParser.cpp
)
target_include_directories(xt_cost PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_cost
)
target_link_libraries(xt_cost PUBLIC xt_core xt_config)
