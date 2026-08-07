"""Regression test for I5: cp.py's WW dedup must keep all fields of ONE
representative row together, never mixing an end date from one row with
the start date/vehicle details of a different row in the same WW group
(a "chimera" record describing a contract that occurred in no single
source row). See AGENTS.md / cp.py's module docstring."""
from unittest.mock import patch

import pandas as pd

import cp
from lib.pipeline_log import PipelineLogger


def test_dedup_keeps_representative_rows_own_end_date_not_a_later_row():
    # Same WW, two rows: Row A has a real IMM but an earlier end date;
    # Row B has no real IMM (WW-style placeholder) but a later end date.
    # The old code picked Row A (real IMM wins) for every field EXCEPT
    # Date fin contrat, which got hoisted from Row B (latest end date
    # across the whole group) -- producing a contract with a start/end
    # date pair that never occurred together in the source data.
    df = pd.DataFrame({
        "Gestionnaire": ["G1", "G1"],
        "WW": ["WW123", "WW123"],
        "IMM": ["AB-123-CD", "WW123"],
        "NUM chassis": ["CHASSIS-A", "CHASSIS-B"],
        "Marque": ["Renault", "Peugeot"],
        "Modèle": ["Clio", "208"],
        "Libellé version long": ["Clio V", "208 II"],
        "Type location": ["LLD", "LLD"],
        "Date MCE": ["01/01/2024", "01/01/2024"],
        "Date début contrat": ["01/01/2024", "01/06/2024"],
        "Date fin contrat": ["01/06/2025", "01/01/2026"],
        "Type": ["VP", "VP"],
        "Jockey": ["", ""],
    })

    logger = PipelineLogger("cp")
    with patch("cp.pd.read_excel", return_value=df):
        out = cp.extract_transform(b"fake", logger)

    assert len(out) == 1
    row = out.iloc[0]
    assert row["IMM"] == "AB-123-CD", "real IMM should still win the representative-row pick"
    assert row["Date début contrat"] == "2024-01-01T00:00:00.000Z", "start date must come from the SAME row as IMM"
    assert row["Date fin contrat"] == "2025-06-01T00:00:00.000Z", (
        "end date must be the representative row's OWN end date, not hoisted from a "
        "different row in the WW group"
    )
    assert row["Modèle"] == "Clio", "vehicle details must come from the same representative row too"
