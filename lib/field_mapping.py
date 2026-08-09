"""
lib/field_mapping.py
---------------------
Single, permanent source of truth for renaming dirty, DMS-exported Excel
column names to clean, consistent MongoDB field names. The source DMS/SGA
export will always produce dirty column names (trailing spaces, typos like
"Technicein", "Founisseur") -- that cannot be fixed at the source, so this
translation runs on every pipeline execution, forever, not as a one-time
fix. See CLAUDE.md / the field-mapping deploy plan for the resolution
history behind specific entries (e.g. why "Entité" is dropped rather than
renamed).

See also validate_against_registry() below: this file's mapping strings
are hand-typed and have drifted from reality before (a trailing-space
typo silently broke a rename for months). That function cross-checks the
mapping against field_registry.json -- a live scan of what MongoDB
actually contains (scripts/export_field_registry.py) -- instead of
trusting FIELD_MAPS on faith.
"""

import json
from pathlib import Path

DS_FIELD_MAP = {
    "Societe": "societe",
    "Site": "site",
    "Date DS": "date_ds",
    "N°DS": "n_ds",
    "Date interv": "date_interv",
    "N° intervention": "n_intervention",
    "Code art": "code_art",
    "Désignation article": "designation_article",
    "Désignation Consomation": "designation_consommation",
    "Qté": "qte",
    "Mt HT DS ": "mt_ht_ds",
    "Immatriculation": "immatriculation",
    "Parc": "parc",
    "Type Parc": "type_parc",
    "Désignation véhicule": "designation_vehicule",
    "ref CP": "ref_cp",
    "Client Final": "client_final",
    "Raison Social": "raison_social",
    "Detenteur DS": "detenteur_ds",
    "A Facturè": "a_facturer",
    "Affectation": "affectation",
    "Client facturé    ": "client_facture",
    "Statut facture": "statut_facture",
    "KM": "km",
    "User": "user",
    "Type DS": "type_ds",
    "CMD Num": "cmd_num",
    "Réceptionné": "receptionne",
    "Soldé": "solde",
    "Old CMD Num": "old_cmd_num",
    "Founisseur": "fournisseur",
    "Code entité": "code_entite",
    # DELIBERATE (2026-08-09): ds's "ENTITE" clean name is "entite_nom",
    # not "entite" -- deliberately different from bc's "Entité" -> "entite"
    # below. Not a naming-drift bug: ds already has a separate "code_entite"
    # (from "Code entité"), so "entite_nom" disambiguates the two ds fields;
    # bc has no such pair, so "entite" alone is unambiguous there. Do not
    # "fix" these to match each other without first checking whether jalal's
    # aggregation code (or the frontend) already queries either collection
    # by its current name -- if so, reconcile via a migration, not a silent
    # rename.
    "ENTITE": "entite_nom",
    "Description": "description",
    "Besoin pièce": "besoin_piece",
    "Type de DS": "type_ds_facturation",
    " DS d'origine": "ds_d_origine",
    "Code Client ": "code_client",
    "Client DS": "client_ds",
    "SITE DS": "site_ds",
    "Date d'entrèe": "date_d_entree",
    "Dètenteur parc": "detenteur_parc",
    "Marque": "marque",
    "Locat Parc": "locat_parc",
    "Effectue le": "effectue_le",
    "Technicein": "technicien",
    "FACTURE PAR ": "facture_par",
    "Prix Unitaire ds": "prix_unitaire_ds",
    "Dernier Prix Achat NET": "dernier_prix_achat_net",
    "Demande satisfaite": "demande_satisfaite",
    "N° Facture": "n_facture",
}

# "Entité" (short code, e.g. "ENT040") is intentionally dropped rather than
# renamed: confirmed redundant with "Code entité" (same code scheme, "Code
# entité" is the complete version -- "Entité" is blank in exactly the rows
# "Code entité" fills in) and confirmed via a jalal repo search to have no
# references, including no blank-as-signal logic, that would need it kept.
DS_DROPPED_COLUMNS = {"Entité"}

BC_FIELD_MAP = {
    "Site": "site",
    "N° BC    ": "cmd_num",
    "Immatriculation": "immatriculation",
    "Date BC": "date_bc",
    "Code frs": "code_frs",
    "Fournisseurs": "fournisseurs",
    "Cat. Article": "cat_article",
    "Code article": "code_article",
    "Description article": "description_article",
    "Famille Principale": "famille_principale",
    "Nature article": "nature_article",
    "Sous-nature article": "sous_nature_article",
    "PU": "pu",
    "Qté": "qte",
    "Reste à livrè": "reste_a_livrer",
    "Montant Reste à livrè": "montant_reste_a_livrer",
    "Prix brut pièce": "prix_brut_piece",
    "Remise article": "remise_article",
    "Montant HT ": "montant_ht",
    "Total HT BC": "total_ht_bc",
    # DELIBERATE (2026-08-09): named "entite" here, not "entite_nom" like
    # ds's equivalent field above -- see the comment on ds's "ENTITE" entry
    # for why the two collections use different clean names for the same
    # real-world concept.
    "Entité": "entite",
    "Marque": "marque",
    "Signé": "signe",
    "Réceptionné": "receptionne",
    "Cde origine": "cde_origine",
    "type Solde": "type_solde",
    "Soldé": "solde",
    "N° DS": "n_ds",
    "Type DS": "type_ds",
    "Client DS": "client_ds",
    "Ville": "ville",
    "Cree par": "cree_par",
    " Derniere Date Reception": "derniere_date_reception",
}

# DELIBERATE (2026-08-09): most of CP_FIELD_MAP below is never selected
# into cp.py's COLUMNS_NEEDED (e.g. "avenant", "conducteur",
# "bon_de_commande", "km_consomme" and others have no Mongo consumer
# today) -- this is not dead mapping code to prune. FIELD_MAPS is a
# permanent translation of every column the DMS export produces (see this
# file's module docstring); apply_field_mapping() raises if any raw Excel
# column is unmapped, so every column must have an entry here regardless
# of whether a pipeline currently uses it. If a future column looks
# "unused" in COLUMNS_NEEDED, that's expected -- check COLUMNS_NEEDED (not
# this map) to see what actually reaches Mongo; see also
# validate_against_registry()'s docstring below for why only
# COLUMNS_NEEDED entries get checked against field_registry.json.
CP_FIELD_MAP = {
    "Nature": "nature",
    "Statut": "statut",
    "Référence": "reference",
    "Client": "client",
    "Gestionnaire": "gestionnaire",
    "WW": "ww",
    "IMM": "imm",
    "NUM chassis": "num_chassis",
    "Marque": "marque",
    "Modèle": "modele",
    "Libellé version long": "libelle_version_long",
    "Durée": "duree",
    "Km prévu": "km_prevu",
    "Type location": "type_location",
    "Date MCE": "date_mce",
    "Date début contrat": "date_debut_contrat",
    "Date fin contrat": "date_fin_contrat",
    "Date début facturation": "date_debut_facturation",
    "Date fin facturation": "date_fin_facturation",
    "Etat livraison": "etat_livraison",
    "Date livraison": "date_livraison",
    "Etat restitution": "etat_restitution",
    "Date restitution": "date_restitution",
    "Etat prorogation": "etat_prorogation",
    "Date prorogation": "date_prorogation",
    "Avenant": "avenant",
    "VH relais": "vh_relais",
    "Type": "type_vh_relais",
    "Date début RL": "date_debut_rl",
    "Date fin RL": "date_fin_rl",
    "KM départ": "km_depart",
    "Dernier KM": "dernier_km",
    "Date dernier KM": "date_dernier_km",
    "Total relais": "total_relais",
    "KM consommé": "km_consomme",
    "Ecart KM": "ecart_km",
    "Projection km": "projection_km",
    "Ecart%": "ecart_pct",
    "Conducteur": "conducteur",
    "Intersociété": "intersociete",
    "Type véhicule": "type_vehicule",
    "Bon de commande": "bon_de_commande",
    "Date création": "date_creation",
    "Jockey": "jockey",
    "Code Argus": "code_argus",
}

PARC_FIELD_MAP = {
    "ID": "id",
    "Société": "societe",
    "Client": "client",
    "Marque": "marque",
    "Modèle": "modele",
    "Immatriculation": "immatriculation",
    "Numéro WW": "numero_ww",
    "N° de chassis": "n_de_chassis",
    "Etat véhicule": "etat_vehicule",
    "Observation VH": "observation_vh",
    "Date MCE": "date_mce",
    "Reçu": "recu",
    "Date réception": "date_reception",
    "Vendu": "vendu",
    "Date vente": "date_vente",
    "Type location": "type_location",
    "Epave": "epave",
    "Suivi véhicule": "suivi_vehicule",
    "BC": "bc",
    "Type véhicule": "type_vehicule",
    "Locataire": "locataire",
    "Prix achat net HT": "prix_achat_net_ht",
}

FIELD_MAPS = {
    "ds": DS_FIELD_MAP,
    "bc": BC_FIELD_MAP,
    "cp": CP_FIELD_MAP,
    "parc": PARC_FIELD_MAP,
}

DROPPED_COLUMNS = {
    "ds": DS_DROPPED_COLUMNS,
    "bc": set(),
    "cp": set(),
    "parc": set(),
}


def apply_field_mapping(df, collection: str):
    """Rename df's raw Excel columns to clean Mongo keys via
    FIELD_MAPS[collection], drop any column in DROPPED_COLUMNS[collection]
    (kept out of the map deliberately -- e.g. ds's "Entité", redundant with
    "Code entité"), and raise loudly if the source Excel contains a column
    that's neither mapped nor an intentional drop.

    This must run immediately after pd.read_excel(), before any other
    transform logic -- every downstream consumer (lib.validate,
    lib.transform, lib.mongo) only ever sees clean keys from this point on.

    The loud failure is the safety net against the source DMS export
    silently adding a new column or renaming an existing one without
    notice: an unmapped column must never pass through unrenamed, since a
    caller expecting clean keys (validate_columns(), a Mongo write) would
    otherwise either miss it entirely or -- worse -- silently write it to
    Mongo under its original dirty name, defeating the whole point of a
    permanent mapping layer.
    """
    field_map = FIELD_MAPS[collection]
    dropped = DROPPED_COLUMNS[collection]

    unmapped = [c for c in df.columns if c not in field_map and c not in dropped]
    if unmapped:
        raise ValueError(
            f"[{collection}] unmapped raw Excel column(s): {unmapped} -- "
            f"the source export introduced a column not present in "
            f"lib.field_mapping.{collection.upper()}_FIELD_MAP (or "
            f"DROPPED_COLUMNS). Add it there before this pipeline can run."
        )

    if dropped:
        df = df.drop(columns=list(dropped), errors="ignore")
    return df.rename(columns=field_map)


_REGISTRY_PATH = Path(__file__).resolve().parent.parent / "field_registry.json"


def validate_against_registry(collection: str, columns_needed: list[str]) -> None:
    """Cross-check `collection`'s actively-used clean field names (its
    pipeline's own COLUMNS_NEEDED -- the only fields that actually reach
    Mongo, out of everything FIELD_MAPS knows how to rename) against
    field_registry.json, a live scan of what MongoDB actually contains
    today (scripts/export_field_registry.py). Call this right after
    apply_field_mapping(), before COLUMNS_NEEDED selection.

    A field counts as verified if EITHER its clean name or its original
    dirty name (per FIELD_MAPS[collection]) is present in the registry:
    the dirty name means it's live under its pre-backfill key (expected
    and fine before scripts/migrate_field_names.py has run for that
    field); the clean name means it's already been backfilled. If
    NEITHER form is found, the mapping has drifted from reality -- e.g. a
    typo in the clean name (FIELD_MAPS value), or a dirty key
    (FIELD_MAPS key) that no longer matches any real document -- and this
    raises loudly instead of letting the pipeline rely on an unverified
    field name, exactly the class of bug that motivated this check (see
    CLAUDE.md's field-registry section).

    Deliberately NOT run for every FIELD_MAPS entry, only COLUMNS_NEEDED:
    most FIELD_MAPS entries are for columns no pipeline currently selects
    into Mongo, so field_registry.json -- which only ever contains
    fields a document actually has -- can never confirm or deny them.
    """
    if not _REGISTRY_PATH.exists():
        raise FileNotFoundError(
            f"field_registry.json not found at {_REGISTRY_PATH} -- run "
            f"scripts/export_field_registry.py before running any pipeline."
        )
    registry = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
    live_fields = set(registry.get("collections", {}).get(collection, []))

    field_map = FIELD_MAPS[collection]
    clean_to_dirty = {clean: dirty for dirty, clean in field_map.items()}

    unverified = []
    for clean in columns_needed:
        dirty = clean_to_dirty.get(clean)
        if dirty is None:
            unverified.append((clean, None))
            continue
        if clean not in live_fields and dirty not in live_fields:
            unverified.append((clean, dirty))

    if unverified:
        details = "; ".join(
            f"{clean!r} (dirty key {dirty!r})" if dirty else f"{clean!r} -- no FIELD_MAP entry maps to it at all"
            for clean, dirty in unverified
        )
        raise ValueError(
            f"[{collection}] field_registry.json drift: {len(unverified)} active field(s) match "
            f"neither their clean nor dirty form in the live-scan registry: {details} -- either "
            f"FIELD_MAPS[{collection!r}] has a typo (clean-name value or dirty-key), or "
            f"field_registry.json is stale (re-run scripts/export_field_registry.py after the "
            f"latest real Mongo state)."
        )
