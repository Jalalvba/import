"""Phase 2: km/qte/pu are stored as native BSON numbers, not strings.

Covers lib.transform.clean_numeric() directly, plus an end-to-end check
through ds.py/bc.py's extract_transform() + df_to_mongo_records() that
the numeric fields actually land as native int/float (or None) in the
Mongo-ready record, never as a string and never silently coerced to 0."""
from unittest.mock import patch

import pandas as pd
import pytest

import bc
import ds
from lib.mongo import df_to_mongo_records
from lib.pipeline_log import PipelineLogger
from lib.transform import clean_numeric


# ── clean_numeric() unit tests ──────────────────────────────────────────────

def test_clean_numeric_parses_numeric_string_to_int():
    assert clean_numeric("1200") == 1200
    assert isinstance(clean_numeric("1200"), int)


def test_clean_numeric_parses_decimal_string_to_float():
    assert clean_numeric("1234.5") == 1234.5
    assert isinstance(clean_numeric("1234.5"), float)


def test_clean_numeric_strips_thousands_separators():
    # comma/space are thousands separators here, never a decimal point --
    # matches jalal's $replaceAll(",", "")/$replaceAll(" ", "") convention.
    assert clean_numeric("1,200") == 1200
    assert clean_numeric("1 200") == 1200


def test_clean_numeric_blank_is_none():
    assert clean_numeric("") is None
    assert clean_numeric("   ") is None
    assert clean_numeric(None) is None
    assert clean_numeric(float("nan")) is None


def test_clean_numeric_malformed_is_none_not_zero():
    assert clean_numeric("N/A") is None
    assert clean_numeric("abc") is None
    assert clean_numeric("12ab") is None


def test_clean_numeric_passes_through_native_numbers():
    assert clean_numeric(42) == 42
    assert clean_numeric(42.5) == 42.5
    assert clean_numeric(42.0) == 42  # whole floats collapse to int


# ── df_to_mongo_records(): numeric fields are native, not stringified ──────

def test_df_to_mongo_records_keeps_numeric_columns_native():
    df = pd.DataFrame({
        "n_ds": ["DS1", "DS2", "DS3"],
        "km": [1200, None, 42.5],
    })
    records = df_to_mongo_records(df, date_columns=[], numeric_columns=["km"])

    assert records[0]["km"] == 1200
    assert isinstance(records[0]["km"], int)
    assert records[1]["km"] is None  # explicit null, not dropped
    assert "km" in records[1]        # key present, not omitted
    assert records[2]["km"] == 42.5


def test_df_to_mongo_records_other_columns_still_stringified():
    df = pd.DataFrame({"n_ds": [123], "km": [1200]})
    records = df_to_mongo_records(df, date_columns=[], numeric_columns=["km"])
    assert records[0]["n_ds"] == "123"
    assert isinstance(records[0]["n_ds"], str)


# ── End-to-end through ds.py / bc.py ────────────────────────────────────────

DS_COLUMNS = [
    "Societe", "Site", "Date DS", "N°DS", "Date interv", "N° intervention",
    "Code art", "Désignation article", "Désignation Consomation ", "Qté",
    "Mt HT DS ", "Immatriculation", "Parc", "Type Parc",
    "Désignation véhicule", "ref CP", "Client Final", "Raison Social",
    "Detenteur DS", "A Facturè", "Affectation", "Client facturé    ",
    "Statut facture", "KM", "User", "Type DS", "CMD Num", "Réceptionné",
    "Soldé", "Old CMD Num", "Founisseur", "Code entité", "ENTITE",
    "Description", "Besoin pièce", "Type de DS", " DS d'origine",
    "Code Client ", "Client DS", "SITE DS", "Date d'entrèe",
    "Dètenteur parc", "Marque", "Locat Parc", "Effectue le", "Technicein",
    "FACTURE PAR ", "Prix Unitaire ds", "Dernier Prix Achat NET",
    "Demande satisfaite", "N° Facture",
]


def _ds_row(**overrides):
    row = {col: "x" for col in DS_COLUMNS}
    row.update({
        "N°DS": "DS1", "Date DS": "01/01/2026", "KM": "1200", "Qté": "5",
    })
    row.update(overrides)
    return row


def test_ds_pipeline_produces_native_numeric_km_and_qte():
    df = pd.DataFrame([_ds_row()])
    logger = PipelineLogger("ds")
    with patch("ds.pd.read_excel", return_value=df):
        out = ds.extract_transform(b"fake", logger)

    records = df_to_mongo_records(out, ds.DATE_COLUMNS, ds.NUMERIC_COLUMNS)
    assert records[0]["km"] == 1200
    assert isinstance(records[0]["km"], int)
    assert records[0]["qte"] == 5
    assert isinstance(records[0]["qte"], int)


def test_ds_pipeline_blank_km_is_null_not_dropped_not_zero():
    df = pd.DataFrame([_ds_row(**{"KM": ""})])
    logger = PipelineLogger("ds")
    with patch("ds.pd.read_excel", return_value=df):
        out = ds.extract_transform(b"fake", logger)

    records = df_to_mongo_records(out, ds.DATE_COLUMNS, ds.NUMERIC_COLUMNS)
    assert records[0]["km"] is None
    assert "km" in records[0]


def test_ds_pipeline_malformed_km_is_null_and_logs_warning():
    df = pd.DataFrame([_ds_row(**{"KM": "N/A"})])
    logger = PipelineLogger("ds")
    with patch("ds.pd.read_excel", return_value=df):
        out = ds.extract_transform(b"fake", logger)

    records = df_to_mongo_records(out, ds.DATE_COLUMNS, ds.NUMERIC_COLUMNS)
    assert records[0]["km"] is None

    warnings = [s for s in logger.steps if s["status"] == "warning" and "km" in s["detail"]]
    assert warnings, "expected a warning step for the malformed km value"
    assert "N/A" in warnings[0]["detail"]


BC_COLUMNS = [
    "Site", "N° BC    ", "Immatriculation", "Date BC", "Code frs",
    "Fournisseurs", "Cat. Article", "Code article", "Description article",
    "Famille Principale", "Nature article", "Sous-nature article", "PU",
    "Qté", "Reste à livrè", "Montant Reste à livrè", "Prix brut pièce",
    "Remise article", "Montant HT ", "Total HT BC", "Entité", "Marque",
    "Signé", "Réceptionné", "Cde origine", "type Solde", "Soldé",
    "N° DS", "Type DS", "Client DS", "Ville", "Cree par",
    " Derniere Date Reception",
]


def _bc_row(**overrides):
    row = {col: "x" for col in BC_COLUMNS}
    row.update({
        "N° BC    ": "BC1", "Date BC": "01/01/2026", "PU": "1234.50", "Qté": "3",
    })
    row.update(overrides)
    return row


def test_bc_pipeline_produces_native_numeric_pu_and_qte():
    df = pd.DataFrame([_bc_row()])
    logger = PipelineLogger("bc")
    with patch("bc.pd.read_excel", return_value=df):
        out = bc.extract_transform(b"fake", logger)

    records = df_to_mongo_records(out, bc.DATE_COLUMNS, bc.NUMERIC_COLUMNS)
    assert records[0]["pu"] == 1234.5
    assert isinstance(records[0]["pu"], float)
    assert records[0]["qte"] == 3
    assert isinstance(records[0]["qte"], int)


def test_bc_pipeline_malformed_pu_is_null_and_logs_warning():
    df = pd.DataFrame([_bc_row(**{"PU": "garbage"})])
    logger = PipelineLogger("bc")
    with patch("bc.pd.read_excel", return_value=df):
        out = bc.extract_transform(b"fake", logger)

    records = df_to_mongo_records(out, bc.DATE_COLUMNS, bc.NUMERIC_COLUMNS)
    assert records[0]["pu"] is None

    warnings = [s for s in logger.steps if s["status"] == "warning" and "pu" in s["detail"]]
    assert warnings, "expected a warning step for the malformed pu value"
    assert "garbage" in warnings[0]["detail"]
