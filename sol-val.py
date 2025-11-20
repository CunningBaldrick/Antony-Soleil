#!/usr/bin/env python3

import requests
import pandas as pd
from urllib.parse import urlencode, quote

# -----------------------------
# CONSTANTS
# -----------------------------

# Antony, IdF identifiers
INSEE_ANTONY = "92002"
IDF_REGION_CODE = "11"
YEAR = 2023

# Base URLs
ODRE_BASE = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
ENEDIS_BASE = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets"

# ODRE register (capacity snapshot)

# Installed capacity at the end of YEAR
# For the latest information, it is the same except that it ends at -agrege
REGISTRE_DATASET = f"registre-national-installation-production-stockage-electricite-agrege-3112{YEAR % 100}"

REGISTER_COMMUNE_FIELD = "codeinseecommune"
REGISTER_REGION_FIELD = "coderegion"
REGISTER_TECH_FIELD = "filiere"            # 'Solaire'
REGISTER_POWER_FIELD = "puismaxinstallee"  # kW
REGISTER_TENSION_FIELD = "tensionraccordement"
REGISTER_NAME_FIELD = "nominstallation"

# eco2mix regional production
# There is also a "real-time" version, eco2mix-national-tr, which we want
# to use, which is why we need a good calibration for eco2mix data.
ECO2MIX_DATASET = "eco2mix-regional-cons-def"
ECO2MIX_REGION_FIELD = "code_insee_region"
ECO2MIX_REGION_CODE_IDF = "11"
ECO2MIX_DATETIME_FIELD = "date_heure"
ECO2MIX_SOLAR_FIELD = "solaire"

# Enedis annual commune production
ENEDIS_PROD_DATASET = "production-electrique-par-filiere-a-la-maille-commune"
ENEDIS_COMMUNE_FIELD = "code_commune"
ENEDIS_YEAR_FIELD = "annee"
ENEDIS_REGION_FIELD = "code_region"
ENEDIS_PV_ENERGY_FIELD = "energie_produite_annuelle_photovoltaique_enedis_mwh"


# -----------------------------
# Helpers
# -----------------------------

def fetch_ods_records(base_url: str, dataset: str, params: dict) -> pd.DataFrame:
    """
    Generic helper to call an Opendatasoft dataset (ODRE or Enedis),
    handling proper URL-encoding (spaces as %20, not '+').
    """
    base = f"{base_url}/{dataset}/records"
    if params:
        query = urlencode(params, quote_via=quote)
        url = f"{base}?{query}"
    else:
        url = base

    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    return pd.json_normalize(data.get("results", []))


def fetch_odre_records(dataset: str, params: dict) -> pd.DataFrame:
    return fetch_ods_records(ODRE_BASE, dataset, params)


def fetch_enedis_records(dataset: str, params: dict) -> pd.DataFrame:
    return fetch_ods_records(ENEDIS_BASE, dataset, params)


# -----------------------------
# Capacity queries
# -----------------------------

def get_pv_capacity_total_kw(location_field: str, location_value: str, label: str) -> float:
    """
    Sum of puismaxinstallee (kW) for all solar matching a location.
    """
    where_clause = (
        f"{location_field}='{location_value}'"
        f" AND {REGISTER_TECH_FIELD} like 'Solaire'"
    )

    params = {
        "where": where_clause,
        "select": f"sum({REGISTER_POWER_FIELD}) as p_inst_kw",
        "group_by": location_field,
        "limit": 2,
    }

    df = fetch_odre_records(REGISTRE_DATASET, params)

    if len(df) != 1:
        raise RuntimeError(f"[{label} total] Expected 1 row, got {len(df)}. Params: {params}")

    if "p_inst_kw" not in df.columns:
        raise RuntimeError(
            f"[{label} total] 'p_inst_kw' not found. Columns: {df.columns.tolist()}"
        )

    return float(df["p_inst_kw"].iloc[0])


def get_pv_capacity_antony_total_kw() -> float:
    return get_pv_capacity_total_kw(REGISTER_COMMUNE_FIELD, INSEE_ANTONY, "Antony")


def get_pv_capacity_idf_total_kw() -> float:
    return get_pv_capacity_total_kw(REGISTER_REGION_FIELD, IDF_REGION_CODE, "IdF")


# -----------------------------
# Energy queries
# -----------------------------

def get_eco2mix_idf_produced_mwh(year: int) -> float:
    """
    Total eco2mix solar energy for IdF [MWh] for one year.
    Not available at the commune level.

    'solaire' is MW at half-hourly time step.
    """
    start = f"{year}-01-01T00:00:00"
    end = f"{year + 1}-01-01T00:00:00"

    where_clause = (
        f"{ECO2MIX_REGION_FIELD}='{ECO2MIX_REGION_CODE_IDF}'"
        f" AND {ECO2MIX_DATETIME_FIELD} >= date'{start}'"
        f" AND {ECO2MIX_DATETIME_FIELD} < date'{end}'"
    )

    params = {
        "where": where_clause,
        "select": f"sum({ECO2MIX_SOLAR_FIELD}) as sum_solar_mw",
        "group_by": ECO2MIX_REGION_FIELD,
        "limit": 2,
    }

    df = fetch_odre_records(ECO2MIX_DATASET, params)

    if len(df) != 1:
        raise RuntimeError(f"[eco2mix] Expected 1 row, got {len(df)}. Params: {params}")

    if "sum_solar_mw" not in df.columns:
        raise RuntimeError(
            f"[eco2mix] 'sum_solar_mw' not found. Columns: {df.columns.tolist()}"
        )

    sum_mw = float(df["sum_solar_mw"].iloc[0] or 0.0)
    return sum_mw * 0.5  # half-hourly → MWh


def get_enedis_produced_mwh(location_field: str, location_value: str, year: int, label: str) -> float:
    where_clause = (
        f"{ENEDIS_YEAR_FIELD}=date'{year}'"
        f" AND {location_field}='{location_value}'"
    )

    params = {
        "where": where_clause,
        "select": f"sum({ENEDIS_PV_ENERGY_FIELD}) as energy_mwh",
        "group_by": location_field,
        "limit": 2,
    }

    df = fetch_enedis_records(ENEDIS_PROD_DATASET, params)

    if len(df) != 1:
        raise RuntimeError(
            f"[Enedis label] Expected 1 row, got {len(df)}. Params: {params}"
        )

    if "energy_mwh" not in df.columns:
        raise RuntimeError(
            f"[Enedis label] 'energy_mwh' not found. Columns: {df.columns.tolist()}"
        )

    return float(df["energy_mwh"].iloc[0])


def get_enedis_antony_produced_mwh(year: int) -> float:
    return get_enedis_produced_mwh(ENEDIS_COMMUNE_FIELD, INSEE_ANTONY, year, "Antony")


def get_enedis_idf_produced_mwh(year: int) -> float:
    return get_enedis_produced_mwh(ENEDIS_REGION_FIELD, IDF_REGION_CODE, year, "IdF")


# -----------------------------
# MAIN
# -----------------------------

def main():
    print(f"Fetching installed PV capacities for {YEAR} from ODRE register...\n")

    p_ant_total = get_pv_capacity_antony_total_kw()
    p_idf_total = get_pv_capacity_idf_total_kw()

    print(f"{YEAR} Antony total PV capacity:		{p_ant_total:,.1f} kW")
    print(f"{YEAR} IdF total PV capacity:		{p_idf_total:,.1f} kW")
    R_cap = p_ant_total / p_idf_total
    print(f"Capacity ratio R_cap = Capacity_Antony / Capacity_IdF = {R_cap:.6f}")


    print(f"\n\nFetching regional solar annual energy for {YEAR} from eco2mix...\n")

    e_idf_total_eco = get_eco2mix_idf_produced_mwh(YEAR)

    print(f"{YEAR} IdF total eco2mix PV production:	{e_idf_total_eco:,.0f} MWh")

    # Sanity check: implied yield
    if p_idf_total > 0:
        yield_mwh_per_kw = e_idf_total_eco / p_idf_total
        print(f"\nImplied IdF PV yield (should be around 1): {yield_mwh_per_kw:.3f} MWh/kW/year")

    e_ant_naive = R_cap * e_idf_total_eco
    print(f"\nNaive Antony estimate via R_cap * eco2mix: {e_ant_naive:,.1f} MWh")


    print(f"\n\nFetching Enedis annual PV production for Antony {YEAR}...")

    e_ant_enedis = get_enedis_antony_produced_mwh(YEAR)

    print(f"{YEAR} Enedis Antony PV production:	{e_ant_enedis:,.1f} MWh")


    K = e_ant_enedis / e_ant_naive
    print(f"\n\nCalibration factor K = Enedis / naive = {K:.3f}")

    print("\nFor real-time estimate, use:")
    print("    Antony_solar(t) ≈ K * R_cap(latest) * eco2mix_IdF_solar(t)")
    print(f"with    K     = {K:.3f}")
    print(f"Calibrated on year {YEAR}.")

if __name__ == "__main__":
    main()
