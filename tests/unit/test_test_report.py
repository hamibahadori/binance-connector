"""Unit tests for the test-results report generator (scripts/test_report.py)."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "test_report.py"

JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" errors="0" failures="1" skipped="1" tests="4" time="3.5"
  timestamp="2026-11-07T10:00:00" hostname="host">
  <testcase classname="tests.unit.test_sizing" name="test_rounds_down" time="0.01"/>
  <testcase classname="tests.e2e.test_demo_service" name="test_a_position_is_opened_protected_watched_closed_and_reported" time="40.2"/>
  <testcase classname="tests.unit.test_sizing" name="test_broken" time="0.02"><failure message="assert 1 == 2">trace</failure></testcase>
  <testcase classname="tests.unit.test_sizing" name="test_later" time="0"><skipped message="not yet"/></testcase>
</testsuite></testsuites>
"""


@pytest.fixture
def report_module():
    specification = importlib.util.spec_from_file_location("test_report_script", SCRIPT)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


@pytest.fixture
def junit_file(tmp_path) -> Path:
    path = tmp_path / "run.xml"
    path.write_text(JUNIT, encoding="utf-8")
    return path


def test_the_report_summarises_each_run(report_module, junit_file):
    markdown = report_module.render([junit_file])

    assert "| run.xml | 4 | 2 | 1 | 1 |" in markdown
    assert "**Overall: 2 passed, 1 failed, 1 skipped of 4.**" in markdown


def test_failures_are_listed_with_their_message(report_module, junit_file):
    markdown = report_module.render([junit_file])

    assert "tests/unit/test_sizing.py::test_broken" in markdown
    assert "assert 1 == 2" in markdown


def test_acceptance_criteria_show_the_tests_that_prove_them(report_module, junit_file):
    markdown = report_module.render([junit_file])

    criterion_lines = [line for line in markdown.splitlines() if line.startswith("| 7 |")]
    assert criterion_lines and "passed" in criterion_lines[0]
    assert "test_a_position_is_opened_protected_watched_closed_and_reported" in criterion_lines[0]


def test_a_criterion_whose_tests_did_not_run_says_so(report_module, junit_file):
    markdown = report_module.render([junit_file])

    criterion_lines = [line for line in markdown.splitlines() if line.startswith("| 8 |")]
    assert "not run" in criterion_lines[0]
