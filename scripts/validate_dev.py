#!/usr/bin/env python3
"""Public regression and exact test-inventory gate. All temporary files are scoped."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET

if not __debug__:
    raise RuntimeError("Validation requires Python assertions enabled; remove -O/PYTHONOPTIMIZE")

ROOT = Path(__file__).resolve().parents[1]
HFT = ["ag2206_only", "ag2212_only", "roll_29col", "holiday",
       "ag2206_vollimit", "ag2206_vollimit_split"]

def run(args, env=None):
    result = subprocess.run([str(x) for x in args], cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if result.returncode:
        raise RuntimeError("command failed: " + str(args) + "\n" + result.stdout[-6000:])
    return result.stdout

def build_dir():
    path = Path(os.environ.get("XT_OSS_BUILD_DIR", str(Path.home() / "builds/xyybtrader-oss-dev-jammy"))).resolve()
    cache = (path / "CMakeCache.txt").read_text()
    match = re.search(r"^CMAKE_HOME_DIRECTORY:INTERNAL=(.+)$", cache, re.M)
    if not match or Path(match[1]).resolve() != (ROOT / "src").resolve():
        raise RuntimeError("build belongs to another source tree")
    return path

def plugin_env():
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("GTEST_"):
            del env[key]
    names = ["libhft_tick_revert_strategy.so", "libsel_weights_csv_strategy.so"]
    for name in names:
        p = ROOT / "var/plugins/strategies" / name
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        if (p.with_suffix(p.suffix + ".sha256")).read_text().split()[0] != digest:
            raise RuntimeError("plugin sidecar mismatch: " + name)
    env.update(CLOUDTRADER_PLUGIN_ALLOWLIST_ENFORCE="1", CLOUDTRADER_PLUGIN_STRICT_SIGNING="1",
               CLOUDTRADER_PLUGIN_ALLOWLIST=",".join(names), XT_REPO_ROOT=str(ROOT))
    return env

def tests(build, env):
    inventory = json.loads((ROOT / "src/xt_tests/expected_tests.json").read_text())
    expected = {x["name"] for x in inventory}
    assert len(expected) == len(inventory) == 289
    for entry in inventory:
        assert hashlib.sha256((ROOT / entry["source"]).read_bytes()).hexdigest() == entry["sha256"]
    listing = run([build / "xt_tests/xt_public_tests", "--gtest_list_tests"], env)
    names, suite = set(), ""
    for line in listing.splitlines():
        if line.startswith("  ") and suite:
            names.add(suite + line.strip().split()[0])
        elif line and not line.startswith(" ") and line.rstrip().endswith("."):
            suite = line.strip()
    assert names == expected, ("gtest inventory mismatch", sorted(expected - names), sorted(names - expected))
    ctest = json.loads(run(["ctest", "--test-dir", build, "-N", "--show-only=json-v1"], env))
    actual = [x["name"] for x in ctest["tests"]]
    assert len(actual) == len(expected) and set(actual) == expected, "CTest inventory mismatch"
    with tempfile.TemporaryDirectory(prefix="oss-dev-tests-") as temp:
        report = Path(temp) / "junit.xml"
        run(["ctest", "--test-dir", build, "--output-on-failure", "-j", "4", "--output-junit", report], env)
        cases = ET.parse(report).getroot().findall(".//testcase")
        assert len(cases) == len(expected) and {x.attrib["name"] for x in cases} == expected
        assert all(x.find("skipped") is None and x.find("failure") is None and
                   x.find("error") is None and x.attrib.get("status", "run") not in ("notrun", "disabled")
                   for x in cases), "skip/failure in JUnit"
    print("TESTS PASS=289 SKIP=0; gtest/CTest/JUnit inventories match")

def regression(build, env, only_hft, scenarios):
    selected = scenarios or HFT
    if not set(selected) <= set(HFT):
        raise ValueError("unknown HFT scenario")
    count = 0
    with tempfile.TemporaryDirectory(prefix="oss-dev-regression-") as temp:
        base = Path(temp)
        for scn in selected:
            job = base / scn
            job.mkdir()
            text = (ROOT / ("configs/baselines/" + scn + ".yaml")).read_text()
            text = text.replace("${PROJECT_DIR}", str(ROOT))
            text, n = re.subn(r"(?m)^(\s*report_dir:).*$", lambda m: m[1] + " " + str(job / "output"), text)
            assert n == 1
            (job / "backtest.yaml").write_text(text)
            (job / "market.yaml").write_text("{}\n")
            run([build / "XtBackTestRunner", "--config", job / "backtest.yaml"], env)
            results = list(job.rglob("fills.csv"))
            assert len(results) == 1, (scn, results)
            assert results[0].read_bytes() == (ROOT / ("data/regression/baselines/" + scn + "/expected_fills.csv")).read_bytes(), "DRIFT: " + scn
            print("BIT-EXACT HFT " + scn)
            count += 1
        if not only_hft:
            for scn, folder, golden in [
                ("demo", "sel_demo", "trades_baseline.csv"),
                ("ca", "sel_ca", "expected_trades.csv"),
                ("large", "sel_demo_large_wt_parity", "expected_trades.csv")]:
                job = base / ("sel_" + scn)
                job.mkdir()
                text = (ROOT / ("configs/backtest_sel_" + scn + ".yaml")).read_text()
                text, n = re.subn(r"(?m)^(\s*output_dir:).*$", lambda m: m[1] + " " + str(job / "output"), text)
                assert n == 1
                config = job / "backtest.yaml"
                config.write_text(text)
                run([build / "XtSelBacktestRunner", config], env)
                results = list(job.rglob("trades.csv"))
                assert len(results) == 1, (scn, results)
                expected = ROOT / "data/regression" / folder / golden
                assert results[0].read_bytes() == expected.read_bytes(), "DRIFT: SEL " + scn
                assert (results[0].parent / "summary.json").is_file()
                if scn == "ca":
                    assert (results[0].parent / "corporate_action_events.csv").read_bytes() == (
                        ROOT / "data/regression/sel_ca/expected_corporate_action_events.csv").read_bytes(), "DRIFT: CA events"
                    count += 1
                print("BIT-EXACT SEL " + scn)
                count += 1
    print("REGRESSION PASS=" + str(count) + " DRIFT=0 FAIL=0")

def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--regression-only", action="store_true")
    parser.add_argument("--hft", action="store_true")
    parser.add_argument("scenarios", nargs="*")
    args = parser.parse_args()
    build, env = build_dir(), plugin_env()
    if not args.regression_only:
        tests(build, env)
    regression(build, env, args.hft, args.scenarios)

if __name__ == "__main__":
    main()
