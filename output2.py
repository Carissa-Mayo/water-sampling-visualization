#!/usr/bin/env python3
import csv
import os
import argparse

HEADERS_RESULTS_ENRICHED = [
    "result_id",
    "site_id",
    "site_name",
    "lat",
    "lon",
    "huc8",
    "state_fips",
    "county_fips5",
    "provider",
    "sample_date",
    "chemical",
    "water_type",
    "location_type",
    "activity_media",
    "activity_media_sub",
    "is_nondetect",
    "value_for_stats",
    "value_std",
    "std_unit",
    "rl_raw",
    "detect_condition",
]

HEADERS_MONTHLY_SUMMARY = [
    "chemical",
    "water_type",
    "month_start",
    "metric",
    "n",
    "n_sites",
    "nd_frac",
    "mean_value",
    "median_value",
    "ci_low",
    "ci_high",
]

HEADERS_TEST_RESULTS = [
    "chemical",
    "water_type",
    "test_name",
    "n_a",
    "n_b",
    "effect",
    "ci_low",
    "ci_high",
    "p_value",
]

def add_header(input_path: str, output_path: str, headers, delimiter: str = ",") -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(input_path, "r", newline="") as fin, open(output_path, "w", newline="") as fout:
        reader = csv.reader(fin, delimiter=delimiter)
        writer = csv.writer(fout, delimiter=delimiter, lineterminator="\n")

        # Write header row
        writer.writerow(headers)

        # Copy all rows through as-is
        expected = len(headers)

        for row in reader:
            # Skip completely empty lines
            if not row or all(c.strip() == "" for c in row):
                continue

            # If we have too many columns, the most common cause is an unquoted comma
            # inside site_name (column index 2 for results_enriched).
            if len(row) > expected and headers is HEADERS_RESULTS_ENRICHED:
                # Merge extra columns back into site_name until length matches.
                # We assume the split starts at site_name (idx 2) and pushes lat/lon right.
                while len(row) > expected:
                    row[2] = row[2] + "," + row[3]
                    del row[3]

            # If still wrong, fail loudly (don’t silently write corrupted data)
            if len(row) != expected:
                raise ValueError(
                    f"{input_path}: row has {len(row)} cols, expected {expected}. "
                    f"First fields: {row[:6]}"
                )

            writer.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser(description="Add column titles to headerless CSV exports.")
    ap.add_argument("input", help="Path to the headerless CSV you exported")
    ap.add_argument(
        "--which",
        required=True,
        choices=["results_enriched", "monthly_summary", "test_results"],
        help="Which header set to add",
    )
    ap.add_argument(
        "-o",
        "--output",
        help="Output file path (default: <input>_with_headers.csv)",
    )
    ap.add_argument(
        "-d",
        "--delimiter",
        default=",",
        help="Delimiter character (default: ,). If your file is TSV, use '\\t'.",
    )
    args = ap.parse_args()

    HEADER_MAP = {
        "results_enriched": HEADERS_RESULTS_ENRICHED,
        "monthly_summary": HEADERS_MONTHLY_SUMMARY,
        "test_results": HEADERS_TEST_RESULTS,
    }
    headers = HEADER_MAP[args.which]

    in_path = args.input
    out_path = args.output or (os.path.splitext(in_path)[0] + "_with_headers.csv")
    delim = "\t" if args.delimiter == r"\t" else args.delimiter

    add_header(in_path, out_path, headers, delimiter=delim)
    print(f"OK: wrote {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
