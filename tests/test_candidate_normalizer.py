from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


_MODULE_PATH = Path(__file__).resolve().parents[1] / "layer4" / "candidate_normalizer.py"
_SPEC = spec_from_file_location("candidate_normalizer_test_module", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
DefaultCandidateNormalizationLayer = _MODULE.DefaultCandidateNormalizationLayer


def test_extract_sql_uses_last_sql_tag_when_multiple_are_present() -> None:
    normalizer = DefaultCandidateNormalizationLayer()

    text = """
<SQL>
SELECT old_version FROM demo
</SQL>
<think>
Need a better rewrite before final answer.
</think>
<SQL>
SELECT final_version FROM demo
</SQL>
"""

    assert normalizer._extract_sql(text) == "SELECT final_version FROM demo"


def test_extract_sql_ignores_think_tag_when_no_sql_tag_exists() -> None:
    normalizer = DefaultCandidateNormalizationLayer()

    text = """
<think>
I should optimize with a CTE.
</think>
SELECT employee_id FROM employee;
"""

    assert normalizer._extract_sql(text) == "SELECT employee_id FROM employee"
