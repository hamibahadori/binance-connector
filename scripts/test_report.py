"""Turn pytest JUnit XML into the Markdown test-results report (spec §13.10).

    pytest --junitxml=reports/unit-functional.xml
    pytest -m e2e --junitxml=reports/e2e.xml
    python scripts/test_report.py --output docs/test-results.md reports/unit-functional.xml reports/e2e.xml

The report has: a summary per run, the spec §14 acceptance criteria with the tests that
prove each one and their outcome, any failures with their message, and every test by file.
"""

import sys
import xml.etree.ElementTree as ElementTree
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

E2E = "tests/e2e/test_demo_service.py::"
FUNCTIONAL = "tests/functional/"
UNIT = "tests/unit/"

ACCEPTANCE_CRITERIA: list[tuple[int, str, list[str]]] = [
    (1, "Receiving requests from Django", [
        E2E + "test_a_position_is_opened_protected_watched_closed_and_reported",
        E2E + "test_invalid_requests_are_rejected_and_reported",
        FUNCTIONAL + "test_requests_api.py::", UNIT + "test_intake_service.py::"]),
    (2, "Identifying the correct user and account", [
        E2E + "test_invalid_requests_are_rejected_and_reported",
        E2E + "test_two_users_trade_at_once_without_mixing",
        UNIT + "test_account_repository.py::", UNIT + "test_account_clients.py::"]),
    (3, "Several users at once, data fully separated", [
        E2E + "test_two_users_trade_at_once_without_mixing",
        UNIT + "test_execution_manager.py::"]),
    (4, "Creating and managing a simulated position", [
        E2E + "test_a_position_is_opened_protected_watched_closed_and_reported",
        E2E + "test_a_limit_entry_rests_until_django_cancels_it",
        UNIT + "test_position_manager_open.py::", UNIT + "test_lifecycle_states.py::"]),
    (5, "Receiving and showing the live price", [
        E2E + "test_a_position_is_opened_protected_watched_closed_and_reported",
        UNIT + "test_price_feed.py::", UNIT + "test_presentation.py::"]),
    (6, "Live reports over WebSocket (with resync)", [
        E2E + "test_a_position_is_opened_protected_watched_closed_and_reported",
        E2E + "test_requests_survive_a_worker_restart_and_the_feed_resyncs",
        UNIT + "test_live_feed.py::", FUNCTIONAL + "test_websocket_server.py::"]),
    (7, "Receiving close commands from Django", [
        E2E + "test_a_position_is_opened_protected_watched_closed_and_reported",
        E2E + "test_a_limit_entry_rests_until_django_cancels_it",
        UNIT + "test_position_manager_close.py::"]),
    (8, "Simulated close on TP/SL", [
        E2E + "test_a_take_profit_or_stop_loss_closes_the_position_on_binance",
        UNIT + "test_stream_events.py::"]),
    (9, "Recording every event and state", [
        E2E + "test_a_position_is_opened_protected_watched_closed_and_reported",
        UNIT + "test_position_repository.py::", UNIT + "test_report_outbox.py::"]),
    (10, "Preventing duplicate execution", [
        E2E + "test_a_position_is_opened_protected_watched_closed_and_reported",
        E2E + "test_invalid_requests_are_rejected_and_reported",
        UNIT + "test_request_repository.py::"]),
    (11, "Recovery after a restart", [
        E2E + "test_requests_survive_a_worker_restart_and_the_feed_resyncs",
        UNIT + "test_reconciliation.py::"]),
    (12, "Handling errors and disconnects/reconnects", [
        E2E + "test_requests_survive_a_worker_restart_and_the_feed_resyncs",
        UNIT + "test_position_manager_resilience.py::", UNIT + "test_user_data_stream.py::",
        FUNCTIONAL + "test_secret_audit.py::"]),
    (13, "The final report to Django", [
        E2E + "test_a_position_is_opened_protected_watched_closed_and_reported",
        UNIT + "test_report_dispatcher.py::"]),
]
"""Each criterion with the tests that prove it: E2E scenarios on Demo first, then the unit
and functional files that pin the same behaviour down."""

@dataclass(frozen=True)
class Outcome:
    node: str
    """``tests/unit/test_x.py::test_name``"""
    result: str
    """passed, failed, error or skipped"""
    seconds: float
    message: str


@dataclass(frozen=True)
class Run:
    name: str
    timestamp: str
    outcomes: list[Outcome]

    def count(self, result: str) -> int:
        return sum(1 for outcome in self.outcomes if outcome.result == result)


def load(path: Path) -> Run:
    root = ElementTree.parse(path).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    outcomes = []
    for case in suite.iter("testcase"):
        node = case.get("classname", "").replace(".", "/") + ".py::" + case.get("name", "")
        result, message = "passed", ""
        for tag in ("failure", "error", "skipped"):
            child = case.find(tag)
            if child is not None:
                result = {"failure": "failed", "error": "error", "skipped": "skipped"}[tag]
                message = (child.get("message") or "").strip()
        outcomes.append(Outcome(node, result, float(case.get("time") or 0), message))
    return Run(path.name, suite.get("timestamp", ""), outcomes)


def _criterion_status(patterns: list[str], outcomes: list[Outcome]) -> tuple[str, list[str]]:
    """The criterion's result, and the tests (E2E by name, others by file) that actually ran."""
    named = []
    matched = []
    for pattern in patterns:
        hits = [outcome for outcome in outcomes if outcome.node.startswith(pattern) and outcome.result != "skipped"]
        if hits:
            matched += hits
            label = pattern.split("::")[-1] if pattern.startswith(E2E) else pattern.rstrip(":").split("/")[-1]
            named.append(f"{label} (E2E)" if pattern.startswith(E2E) else f"{label} ({len(hits)})")
    if not matched:
        return "not run", []
    passed = all(outcome.result == "passed" for outcome in matched)
    return ("passed" if passed else "**FAILED**"), named

def render(paths: list[Path]) -> str:
    runs = [load(path) for path in paths]
    outcomes = [outcome for run in runs for outcome in run.outcomes]
    lines = ["# Test results", "",
             "Generated by `scripts/test_report.py` from pytest's JUnit XML. "
             "How to reproduce: [install.md](install.md#5-run-the-tests).", "",
             "- **Unit and functional** tests need no network: Binance is simulated from replies and stream "
             "events recorded on Demo.",
             "- **E2E** tests run the whole service against **Binance Futures Demo**, with real orders. They "
             "use a fake Django receiver and a WebSocket client.",
             "- Without `E2E_SECOND_API_KEY`/`_SECRET`, the multi-user scenario's second user shares the first "
             "user's Demo login and trades a second symbol. The service still keeps the two accounts apart, "
             "but they are not two Binance logins.", "",
             "## Summary", "",
             "| Run | Tests | Passed | Failed | Skipped | Time (s) | Started |",
             "|---|---|---|---|---|---|---|"]
    for run in runs:
        failed = run.count("failed") + run.count("error")
        lines.append(f"| {run.name} | {len(run.outcomes)} | {run.count('passed')} | {failed} | "
                     f"{run.count('skipped')} | {sum(outcome.seconds for outcome in run.outcomes):.1f} | "
                     f"{run.timestamp} |")
    total_failed = sum(1 for outcome in outcomes if outcome.result in ("failed", "error"))
    total_passed = sum(1 for outcome in outcomes if outcome.result == "passed")
    total_skipped = sum(1 for outcome in outcomes if outcome.result == "skipped")
    lines += ["", f"**Overall: {total_passed} passed, {total_failed} failed, {total_skipped} skipped "
                  f"of {len(outcomes)}.**", ""]

    lines += ["## Acceptance criteria (spec §14)", "",
              "| # | Criterion | Result | Proven by |", "|---|---|---|---|"]
    for number, title, patterns in ACCEPTANCE_CRITERIA:
        status, named = _criterion_status(patterns, outcomes)
        lines.append(f"| {number} | {title} | {status} | {', '.join(f'`{name}`' for name in named)} |")
    lines.append("")

    failures = [outcome for outcome in outcomes if outcome.result in ("failed", "error")]
    if failures:
        lines += ["## Failures", ""]
        lines += [f"- `{outcome.node}`: {outcome.message.splitlines()[0] if outcome.message else outcome.result}"
                  for outcome in failures]
        lines.append("")

    lines += ["## Every test", ""]
    by_file: dict[str, list[Outcome]] = defaultdict(list)
    for outcome in outcomes:
        by_file[outcome.node.split("::")[0]].append(outcome)
    for file_name in sorted(by_file):
        file_outcomes = by_file[file_name]
        passed = sum(1 for outcome in file_outcomes if outcome.result == "passed")
        lines += [f"<details><summary><code>{file_name}</code>: {passed}/{len(file_outcomes)} passed</summary>", ""]
        lines += [f"- {'✅' if outcome.result == 'passed' else '⏭️' if outcome.result == 'skipped' else '❌'} "
                  f"`{outcome.node.split('::')[-1]}` ({outcome.seconds:.2f} s)" for outcome in file_outcomes]
        lines += ["", "</details>", ""]
    return "\n".join(lines)


def main(arguments: list[str]) -> int:
    """``test_report.py [--output FILE] XML...``: write to FILE (UTF-8, LF), or print."""
    output = None
    if arguments[:1] == ["--output"] and len(arguments) >= 2:
        output, arguments = Path(arguments[1]), arguments[2:]
    if not arguments:
        print(__doc__, file=sys.stderr)
        return 2
    markdown = render([Path(argument) for argument in arguments]) + "\n"
    if output is not None:
        output.write_text(markdown, encoding="utf-8", newline="\n")
    else:
        sys.stdout.reconfigure(encoding="utf-8", newline="\n")
        sys.stdout.write(markdown)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
