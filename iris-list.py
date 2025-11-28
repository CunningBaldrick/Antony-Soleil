#!/usr/bin/env python3
"""
List all IRIS for the commune of Antony (INSEE 92002)
using the georef-france-iris dataset (Opendatasoft).

Data source:
- Dataset: georef-france-iris
  Docs: https://public.opendatasoft.com/explore/dataset/georef-france-iris/
"""

import json
import urllib.parse
import urllib.request

BASE_URL = "https://public.opendatasoft.com/api/records/1.0/search/"
DATASET_ID = "georef-france-iris"
ANTONY_INSEE_CODE = "92002"  # com_code for Antony
MAX_ROWS = 200  # far more than enough for a single commune


def fetch_antony_iris():
    """
    Query Opendatasoft for all IRIS in the commune 92002 (Antony)
    and return a list of dicts {code_iris, iris_name, nice_name}.
    """
    params = {
        "dataset": DATASET_ID,
        "rows": MAX_ROWS,
        # Filter by commune INSEE code
        "refine.com_code": ANTONY_INSEE_CODE,
    }

    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    print(f"Requesting: {url}\n", flush=True)

    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    records = data.get("records", [])
    results = []

    for rec in records:
        fields = rec.get("fields", {})

        iris_code = fields.get("iris_code")  # 9-digit IRIS code
        iris_name = fields.get("iris_name")  # official IRIS name

        if not iris_code:
            continue

        # Short human-readable name
        if iris_name:
            nice_name = f"{iris_name}"
        else:
            # Fallback: unlikely, but just in case iris_name is missing
            suffix = iris_code[-3:] if len(iris_code) >= 3 else iris_code
            nice_name = f"IRIS {suffix}"

        results.append(
            {
                "code_iris": iris_code,
                "iris_name": iris_name,
                "nice_name": nice_name,
            }
        )

    # Sort by IRIS code for deterministic output
    results.sort(key=lambda r: r["code_iris"])
    return results


def main():
    iris_list = fetch_antony_iris()

    print(f"IRIS for Antony (INSEE {ANTONY_INSEE_CODE}):")
    print("code_iris\tshort_name")

    for item in iris_list:
        print(f"{item['code_iris']}\t{item['nice_name']}")


if __name__ == "__main__":
    main()
