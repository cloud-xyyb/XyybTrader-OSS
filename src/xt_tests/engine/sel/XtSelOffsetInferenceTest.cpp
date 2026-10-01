// regression test for the M1-5 sell-to-open
// fix in XtSelBacktestRunner. The offset-inference logic was extracted
// from the inline lambda into `infer_offset_from_pos` (xt_core/XtOffset.h)
// so this test can verify all 4 corner cases without spinning up a full
// backtest. If anyone reverts cf5c3a9c (the hardcode that returned Close
// for every SELL regardless of pos), these tests will catch it.

#include <gtest/gtest.h>

#include "xt_core/order/XtOffset.h"

namespace {

// ─── BUY direction ─────────────────────────────────────────────────────────

TEST(XtSelOffsetInference, BuyWithFlatPos_Opens) {
    // pos = 0, BUY → open long (Open)
    EXPECT_EQ(infer_offset_from_pos(0, /*is_buy=*/true), XtOffset::Open);
}

TEST(XtSelOffsetInference, BuyWithLongPos_Opens) {
    // pos > 0 (already long), BUY → add to long (Open)
    EXPECT_EQ(infer_offset_from_pos(1000, /*is_buy=*/true), XtOffset::Open);
}

TEST(XtSelOffsetInference, BuyWithShortPos_Closes) {
    // pos < 0 (short), BUY → cover short (Close)
    EXPECT_EQ(infer_offset_from_pos(-1000, /*is_buy=*/true), XtOffset::Close);
}

// ─── SELL direction ────────────────────────────────────────────────────────

TEST(XtSelOffsetInference, SellWithFlatPos_OpensShort) {
    // pos = 0, SELL → sell-to-open short (Open). THIS is the M1-5 fix.
    // Pre-cf5c3a9c the runner hardcoded SELL → Close, blocking shorts.
    EXPECT_EQ(infer_offset_from_pos(0, /*is_buy=*/false), XtOffset::Open);
}

TEST(XtSelOffsetInference, SellWithShortPos_OpensMoreShort) {
    // pos < 0, SELL → add to short (Open)
    EXPECT_EQ(infer_offset_from_pos(-500, /*is_buy=*/false), XtOffset::Open);
}

TEST(XtSelOffsetInference, SellWithLongPos_ClosesLong) {
    // pos > 0, SELL → close long (Close). The "textbook" case.
    EXPECT_EQ(infer_offset_from_pos(1000, /*is_buy=*/false), XtOffset::Close);
}

// ─── Boundary: pos == 0 explicitly tested for both directions ──────────────

TEST(XtSelOffsetInference, ZeroPos_BothDirectionsOpen) {
    // From flat, any direction opens a new position. Critical for the
    // first trade of a backtest where qty_book is empty.
    EXPECT_EQ(infer_offset_from_pos(0, true), XtOffset::Open);
    EXPECT_EQ(infer_offset_from_pos(0, false), XtOffset::Open);
}

// 5 regression: composite scenario

TEST(XtSelOffsetInference, M1_5_RegressionTrace) {
    // Replay the failure mode that cf5c3a9c fixed.
    //
    // Trial: share_unlock factor signal = -1 always (mechanical short).
    // SEL strategy submits weight = -0.1 per event → engine computes
    // diff_pos_fp = -<large> from zero. Slicer emits is_buy=false child.
    //
    // Pre-cf5c3a9c: lambda hardcoded `offset = is_buy ? Open : Close`
    //   → first SELL on flat pos got offset=Close.
    //   → trades.csv emitted 248 BUY/OPEN + 249 SELL/CLOSE, zero shorts.
    //
    // Post-cf5c3a9c + M1-16 extraction:
    EXPECT_EQ(infer_offset_from_pos(0, /*is_buy=*/false), XtOffset::Open)
        << "M1-5 regression: SELL on flat pos must Open a short, not Close";

    // After the short opens (pos = -100), if the strategy decides to
    // cover (BUY for that symbol), the offset must be Close:
    EXPECT_EQ(infer_offset_from_pos(-100, /*is_buy=*/true), XtOffset::Close)
        << "M1-5 regression: BUY to cover short must Close, not Open";
}

}  // namespace
