#!/usr/bin/env python3
import requests
import math

# -------------------------
# Config
# -------------------------

INSEE_ANTONY = "92002"

REGISTRE_DATASET = "registre-national-installation-production-stockage-electricite-agrege"

def registre_dataset(year: int | None = None) -> str:
    """
    Returns the data set for installed capacity at the end of the given year,
    or for the latest available time if none.
    """
    return REGISTRE_DATASET + (f"-3112{year % 100}" if year else "")

ODRE_BASE = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"

# Fields in ODRE register
REGISTER_COMMUNE_FIELD = "codeinseecommune"
REGISTER_GEN_CODE_FIELD = "codefiliere"
REGISTER_TECH_CODE_FIELD = "codetechnologie"
REGISTER_CAP_FIELD = "puismaxrac"
REGISTER_ENERGY_FIELD_OLD = "energieannuelleinjectee"  # valid before 2020
REGISTER_ENERGY_FIELD = "energieannuelleglissanteinjectee"  # valid from 2020 onward

# Irradiance config (same as widget)
OPEN_METEO_BASE = "https://satellite-api.open-meteo.com/v1/archive"
ANT_LAT = 48.75
ANT_LON = 2.29
PANEL_TILT_DEG = 30
PANEL_AZIMUTH_DEG = 0

# Years for calibration:
START_YEAR = 2017
END_YEAR = 2024


# -------------------------
# Helpers
# -------------------------

def fetch_json(url: str, params: dict | None = None) -> dict:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_antony_capacity_and_energy_year(year: int) -> tuple[float, float]:
    """
    From ODRE snapshot at 31/12/year:
      - Capacity: sum(puismaxrac) in kW
      - Energy:   sum(energieannuelleglissanteinjectee) in kWh (Antony)
    """
    dataset = registre_dataset(year)
    url = f"{ODRE_BASE}/{dataset}/records"

    params = {
        "where": (
            f"{REGISTER_COMMUNE_FIELD}='{INSEE_ANTONY}'"
            f" AND {REGISTER_GEN_CODE_FIELD} like 'SOLAI'"
            f" AND {REGISTER_TECH_CODE_FIELD} like 'PHOTV'"
        ),
        "select": (
            f"sum({REGISTER_CAP_FIELD}) as cap_kw,"
            f" sum({REGISTER_ENERGY_FIELD_OLD if year < 2020 else REGISTER_ENERGY_FIELD}) as e_kwh"
        ),
        "group_by": REGISTER_COMMUNE_FIELD,
        "limit": 5,
    }

    data = fetch_json(url, params)
    results = data.get("results", [])
    if not results:
        raise RuntimeError(f"No ODRE records for Antony in year {year} (dataset {dataset})")

    row = results[0]
    cap_kw = float(row["cap_kw"]) if row["cap_kw"] is not None else 0.0
    e_kwh = float(row["e_kwh"]) if row["e_kwh"] is not None else 0.0
    return cap_kw, e_kwh

def get_annual_irradiance_series(year: int) -> list[float]:
    """
    Return list of hourly global_tilted_irradiance values (W/m²) for the full year.
    Length will be approximately 8760 (or 8784 in leap years).
    """
    params = {
        "latitude": ANT_LAT,
        "longitude": ANT_LON,
        "hourly": "global_tilted_irradiance",
        "tilt": PANEL_TILT_DEG,
        "azimuth": PANEL_AZIMUTH_DEG,
        "timeformat": "iso8601",
        "timezone": "Europe/Paris",
        "time_resolution": "native",
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "forecast_hours": "0",
    }
    data = fetch_json(OPEN_METEO_BASE, params)
    hourly = data.get("hourly", {})
    gti = hourly.get("global_tilted_irradiance", [])
    if not gti:
        raise RuntimeError(f"No irradiance data for year {year}")
    return gti

def get_annual_irradiance_kwh_per_kwp(year: int) -> float:
    """
    Call Open-Meteo to get hourly global_tilted_irradiance for the entire year.
    Convert to annual "H" in kWh per kWp (sum(G/1000) over hours).
    """
    gti = get_annual_irradiance_series(year)

    total_kwh_per_kwp = 0.0
    for v in gti:
        if v is None:
            continue
        # Hourly step; G in W/m². 1 kWp ~ 1000 W/m².
        # So energy contribution per kWp for that hour = G/1000 kWh.
        total_kwh_per_kwp += v / 1000.0

    return total_kwh_per_kwp

# -------------------------
# Main calibration logic
# -------------------------

def compute_K_exponential(
    year: int,
    C_prev_kw: float,
    C_curr_kw: float,
    E_kwh: float,
) -> float:
    """
    Compute calibration factor K for a given year, assuming
    exponential capacity increase from C_prev_kw to C_curr_kw
    across that year, using hourly irradiance from Open-Meteo.
    """
    if C_prev_kw <= 0 or C_curr_kw <= 0:
        raise ValueError(f"Non-positive capacity for year {year}: C_prev={C_prev_kw}, C_curr={C_curr_kw}")
    if E_kwh <= 0:
        raise ValueError(f"Non-positive energy E_kwh={E_kwh} for year {year}")

    gti = get_annual_irradiance_series(year)
    N = len(gti)

    # exponential growth rate
    k = math.log(C_curr_kw / C_prev_kw)

    denom = 0.0
    for i, v in enumerate(gti):
        if v is None:
            continue
        # fraction of the year elapsed
        tau = i / (N - 1) if N > 1 else 0.0
        C_t = C_prev_kw * math.exp(k * tau)
        # per hour: convert W/m² to kWh/kWp by dividing by 1000
        denom += C_t * (v / 1000.0)

    if denom <= 0:
        raise RuntimeError(f"Computed zero or negative denom for year {year}")

    K = E_kwh / denom
    return K

def compute_K_linear(
    year: int,
    C_prev_kw: float,
    C_curr_kw: float,
    E_kwh: float,
) -> float:
    gti = get_annual_irradiance_series(year)
    N = len(gti)
    if N == 0:
        raise RuntimeError(f"No irradiance data for year {year}")
    denom = 0.0
    for i, v in enumerate(gti):
        if v is None:
            continue
        tau = i / (N - 1) if N > 1 else 0.0
        C_t = C_prev_kw + (C_curr_kw - C_prev_kw) * tau
        denom += C_t * (v / 1000.0)
    if denom <= 0:
        raise RuntimeError(f"Denom=0 for year {year}")
    return E_kwh / denom

def main():
    # 1) Get C_year and E_year for years [START_YEAR .. END_YEAR]
    years = list(range(START_YEAR, END_YEAR + 1))
    cap = {}
    energy = {}

    print("Fetching ODRE capacity and energy for Antony...")
    for y in years:
        c_kw, e_kwh = get_antony_capacity_and_energy_year(y)
        cap[y] = c_kw
        energy[y] = e_kwh
        print(f"  {y}: C={c_kw:.1f} kW, E={e_kwh/1000:.1f} MWh")

    print("\nFetching annual irradiance from Open-Meteo...")
    H = {}
    for y in range(START_YEAR + 1, END_YEAR + 1):
        H[y] = get_annual_irradiance_kwh_per_kwp(y)
        print(f"  {y}: H={H[y]:.1f} kWh/kWp")

    print("\n=== Annual calibration K (exponential capacity) ===")
    print("Year | C_prev (kW) | C_curr (kW) | E (MWh) |   K_exp  |   K_lin")
    print("-----+-------------+------------+---------+---------+--------")

    K_exp_list = []
    K_lin_list = []

    for y in range(2018, 2025):  # or whatever range you want
        C_prev = cap[y - 1]
        C_curr = cap[y]
        E_y = energy[y]  # kWh

        if C_prev <= 0 or C_curr <= 0 or E_y <= 0:
            print(f"{y:4d} | (missing data)")
            continue

        K_exp = compute_K_exponential(y, C_prev, C_curr, E_y)
        K_lin = compute_K_linear(y, C_prev, C_curr, E_y)

        K_exp_list.append(K_exp)
        K_lin_list.append(K_lin)

        print(f"{y:4d} | {C_prev:11.1f} | {C_curr:10.1f} | {E_y/1000:7.1f} | {K_exp:7.3f} | {K_lin:7.3f}")

    if K_exp_list:
        avg_exp = sum(K_exp_list)/len(K_exp_list)
        med_exp = sorted(K_exp_list)[len(K_exp_list)//2]
        print("\nSuggested K (exponential capacity):")
        print(f"  Mean K_exp  : {avg_exp:.3f}")
        print(f"  Median K_exp: {med_exp:.3f}")

    if K_lin_list:
        avg_lin = sum(K_lin_list)/len(K_lin_list)
        med_lin = sorted(K_lin_list)[len(K_lin_list)//2]
        print("\nSuggested K (linear capacity):")
        print(f"  Mean K_lin  : {avg_lin:.3f}")
        print(f"  Median K_lin: {med_lin:.3f}")

if __name__ == "__main__":
    main()
