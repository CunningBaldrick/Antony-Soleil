#!/usr/bin/env python3

import requests
import pandas as pd
from typing import Any
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

REGISTRE_DATASET = f"registre-national-installation-production-stockage-electricite-agrege"

def registre_dataset(year: int | None = None) -> str:
    """
    Returns the data set for installed capacity at the end of the given year,
    or for the latest available time if none.
    """
    return REGISTRE_DATASET + (f"-3112{year % 100}" if year else "")

REGISTER_REGION_FIELD = "coderegion"
REGISTER_COMMUNE_FIELD = "codeinseecommune"
REGISTER_ISLAND_FIELD = "codeiris"
REGISTER_GEN_CODE_FIELD = "codefiliere"                       # 'SOLAI'
REGISTER_GEN_FIELD = "filiere"                                # 'Solaire'
REGISTER_NUM_FIELD = "nbinstallations"
REGISTER_TECH_CODE_FIELD = "codetechnologie"                  # 'PHOTV'
REGISTER_POWER_FIELD = "puismaxinstallee"                     # kW
REGISTER_PROD_1Y_FIELD = "energieannuelleglissanteinjectee"   # kWh
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

def fetch_ods(base_url: str, dataset: str, params: dict) -> list[dict[Any, Any]]:
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
    return data.get("results", [])


def fetch_odre(dataset: str, params: dict) -> list[dict[Any, Any]]:
    return fetch_ods(ODRE_BASE, dataset, params)


def fetch_enedis(dataset: str, params: dict) -> list[dict[Any, Any]]:
    return fetch_ods(ENEDIS_BASE, dataset, params)


# -----------------------------
# Exploration queries
# -----------------------------

def get_odre_all(location_field: str, location_value: str, year: int | None, label: str, limit: int = 20) -> list[dict[Any, Any]]:
    params = {
        "where": f"{location_field}='{location_value}'",
        "limit": limit,
    }

    return fetch_odre(registre_dataset(year), params)

def get_antony_odre_all(year: int | None = None, limit: int = 20) -> list[dict[Any, Any]]:
    return get_odre_all(REGISTER_COMMUNE_FIELD, INSEE_ANTONY, year, "Antony", limit)

def get_idf_odre_all(year: int | None = None, limit: int = 20) -> list[dict[Any, Any]]:
    return get_odre_all(REGISTER_REGION_FIELD, IDF_REGION_CODE, year, "IdF", limit)

# -----------------------------
# Capacity queries
# -----------------------------

def get_pv_capacity_total_kw(location_field: str, location_value: str, year: int | None, label: str) -> float:
    """
    Sum of puismaxinstallee (kW) for all solar matching a location.
    """
    where_clause = (
        f"{location_field}='{location_value}'"
        f" AND {REGISTER_GEN_FIELD} like 'Solaire'"
    )

    params = {
        "where": where_clause,
        "select": f"sum({REGISTER_POWER_FIELD}) as p_inst_kw",
        "group_by": location_field,
        "limit": 2,
    }

    df = pd.json_normalize(fetch_odre(registre_dataset(year), params))

    if len(df) != 1:
        raise RuntimeError(f"[{label} total] Expected 1 row, got {len(df)}. Params: {params}")

    if "p_inst_kw" not in df.columns:
        raise RuntimeError(
            f"[{label} total] 'p_inst_kw' not found. Columns: {df.columns.tolist()}"
        )

    return float(df["p_inst_kw"].iloc[0])


def get_pv_capacity_antony_total_kw(year: int | None) -> float:
    return get_pv_capacity_total_kw(REGISTER_COMMUNE_FIELD, INSEE_ANTONY, year, "Antony")


def get_pv_capacity_idf_total_kw(year: int | None) -> float:
    return get_pv_capacity_total_kw(REGISTER_REGION_FIELD, IDF_REGION_CODE, year, "IdF")


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

    df = pd.json_normalize(fetch_odre(ECO2MIX_DATASET, params))

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

    df = pd.json_normalize(fetch_enedis(ENEDIS_PROD_DATASET, params))

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


def calibrate():
    print(f"Fetching installed PV capacities for {YEAR} from ODRE register...\n")

    p_ant_total = get_pv_capacity_antony_total_kw(YEAR)
    p_idf_total = get_pv_capacity_idf_total_kw(YEAR)

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

def dump_antony() -> None:
    print("Antony")
    print()

    YEAR_FIRST = 2017
    YEAR_LAST  = 2024

    codes_iris = set()
    year_data = {}
    for year in range(YEAR_FIRST, YEAR_LAST + 1):
        data = get_antony_odre_all(year)
        year_data[year] = data
        for row in data:
            # The following should be added to the WHERE clause.  Done this way
            # for the moment to detect if there's any information about other
            # energy sources.
            code_filiere = row.get('codefiliere')
            if code_filiere != 'SOLAI':
                print(f"Skipping code filiare {code_filiere}")
                continue

            code_technologie = row.get('codetechnologie')
            if code_technologie != 'PHOTV':
                print(f"Skipping code technologie {code_technologie}")
                continue

            # If a site is disconnected, would the capacity be set to zero?
            assert row.get('datederaccordement') is None

            code_iris = row.get('codeiris')
            codes_iris.add(code_iris)

    codes_iris = sorted(list(codes_iris), key=lambda x: (x is not None, x))

#    fields = ['codeiris', 'dateraccordement', 'datederaccordement', 'datemiseenservice', 'tensionraccordement', 'puismaxinstallee', 'puismaxrac', 'nbinstallations', 'energieannuelleglissanteinjectee', 'maxpuis', 'datemiseenservice_date']
    fields = ['nbinstallations', 'puismaxinstallee', 'energieannuelleglissanteinjectee', 'nominstallation']
    print('code iris\tyear\t' + '\t'.join(fields))
    for code_iris in codes_iris:
        print(code_iris or "?????????")
        for year in range(YEAR_FIRST, YEAR_LAST + 1):
            for row in year_data[year]:
                if row.get('codeiris') == code_iris:
                    print(f'\t\t{year}\t' + '\t'.join([str(row.get(f)) for f in fields]))
        print()

# -----------------------------
# MAIN
# -----------------------------

def main() -> None:
#    calibrate()
#    dump_antony()

    for year in range(2020, 2050):
        params = {
            "where": (
                f"{REGISTER_COMMUNE_FIELD}='{INSEE_ANTONY}'"
                f" AND {REGISTER_GEN_CODE_FIELD} like 'SOLAI'"
                f" AND {REGISTER_TECH_CODE_FIELD} like 'PHOTV'"
            ),
            "select": (
                f"sum({REGISTER_NUM_FIELD}) AS total_inst"
                f", sum({REGISTER_POWER_FIELD}) as pow_inst_kw"
                f", sum({REGISTER_PROD_1Y_FIELD}) as prod_1y_kwh"
            ),
            "group_by": f"{REGISTER_ISLAND_FIELD}",
            "limit": 100,
        }

        try:
            data = fetch_odre(registre_dataset(year), params)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 404:
                # This yearly snapshot does not exist: stop, previous year was last
                break
            raise

        print(f"{year}: {data}")
        year = year + 1


def get_pv_capacity_antony_total_kw(year: int | None) -> float:
    return get_pv_capacity_total_kw(REGISTER_COMMUNE_FIELD, INSEE_ANTONY, year, "Antony")


if __name__ == "__main__":
    main()
