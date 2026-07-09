#pragma once

#include "xt_account/venue/binance/BinanceAccountTruthSource.h"

struct BinanceTdCfg;

namespace binance_u {

BinanceAccountTruthSource::IncomeProvider make_binance_income_provider(const BinanceTdCfg& cfg);

}
