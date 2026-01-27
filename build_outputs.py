import pandas as pd
import numpy as np

NITRATE_RESULTS = "/Users/carissamayo/Documents/website/chemical_vis/data_raw/WA_nitrate_data.csv"
NITRATE_SITES   = "/Users/carissamayo/Documents/website/chemical_vis/data_raw/WA_nitrate_site.csv"
ORTHO_RESULTS   = "/Users/carissamayo/Documents/website/chemical_vis/data_raw/WA_orthophosphate_data.csv"
ORTHO_SITES     = "/Users/carissamayo/Documents/website/chemical_vis/data_raw/WA_orthophosphate_site.csv"

OUT_DIR = "/Users/carissamayo/Documents/website/chemical_vis/outputs"

BOOTSTRAP_N = 2000
SEED = 7
# --------------------------

rng = np.random.default_rng(SEED)

def read_wqp(path: str) -> pd.DataFrame:
    # WQP files are CSV with quoted strings
    return pd.read_csv(path, dtype=str, low_memory=False)

def to_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan

def month_start(d: pd.Series) -> pd.Series:
    return d.dt.to_period("M").dt.to_timestamp()

def standardize_units(df: pd.DataFrame) -> pd.DataFrame:
    unit = (
        df["unit_raw"]
        .fillna("")
        .str.lower()
        .str.replace(" ", "", regex=False)
    )
    chem = df["chemical"].astype(str).str.lower()
    val = pd.to_numeric(df["value_for_stats"], errors="coerce")

    df["value_std"] = np.nan
    df["std_unit"] = None

    # nitrate
    mask_n = chem.eq("nitrate")
    mask_asno3 = mask_n & unit.str.contains("asno3", na=False)
    mask_asn   = mask_n & unit.str.contains("asn", na=False) & ~unit.str.contains("asno3", na=False)

    df.loc[mask_asno3, "value_std"] = val.loc[mask_asno3] / 4.4268
    df.loc[mask_asn,   "value_std"] = val.loc[mask_asn]
    df.loc[mask_n, "std_unit"] = "mg/L as N"

    # orthophosphate
    mask_o = chem.eq("ortho")
    mask_aspo4 = mask_o & unit.str.contains("aspo4", na=False)
    mask_asp   = mask_o & unit.str.contains("asp", na=False) & ~unit.str.contains("aspo4", na=False)

    df.loc[mask_aspo4, "value_std"] = val.loc[mask_aspo4] / 3.066
    df.loc[mask_asp,   "value_std"] = val.loc[mask_asp]
    df.loc[mask_o, "std_unit"] = "mg/L as P"

    return df

def bootstrap_ci_cluster(x, site_ids, metric, B):
    mask = ~np.isnan(x)
    x = x[mask]
    site_ids = site_ids[mask]
    if x.size == 0:
        return (np.nan, np.nan, np.nan)

    unique_sites = np.unique(site_ids)
    S = len(unique_sites)
    if S == 0:
        return (np.nan, np.nan, np.nan)

    boots = np.empty(B, float)

    for b in range(B):
        sampled = rng.choice(unique_sites, size=S, replace=True)

        xb_list = []
        for s in sampled:
            xb_list.append(x[site_ids == s])   # duplicates happen naturally if s repeats
        xb = np.concatenate(xb_list) if xb_list else np.array([], float)

        boots[b] = (np.mean(xb) if metric == "mean" else np.median(xb)) if xb.size else np.nan

    boots = boots[~np.isnan(boots)]
    if boots.size == 0:
        return (np.nan, np.nan, np.nan)

    lo, hi = np.percentile(boots, [2.5, 97.5])
    point = np.mean(x) if metric == "mean" else np.median(x)
    return (point, lo, hi)

def bootstrap_diff_cluster(a: np.ndarray, a_sites: np.ndarray,
                           b: np.ndarray, b_sites: np.ndarray,
                           B: int) -> tuple:
    # drop NaNs
    ma = ~np.isnan(a)
    mb = ~np.isnan(b)
    a, a_sites = a[ma], a_sites[ma]
    b, b_sites = b[mb], b_sites[mb]

    if len(a) == 0 or len(b) == 0:
        return (np.nan, np.nan, np.nan, np.nan)

    A_sites = np.unique(a_sites)
    B_sites = np.unique(b_sites)
    if len(A_sites) == 0 or len(B_sites) == 0:
        return (np.nan, np.nan, np.nan, np.nan)

    boots = np.empty(B, dtype=float)

    for i in range(B):
        samp_A = rng.choice(A_sites, size=len(A_sites), replace=True)
        samp_B = rng.choice(B_sites, size=len(B_sites), replace=True)

        aa = a[np.isin(a_sites, samp_A)]
        bb = b[np.isin(b_sites, samp_B)]

        if aa.size == 0 or bb.size == 0:
            boots[i] = np.nan
        else:
            boots[i] = np.mean(aa) - np.mean(bb)

    boots = boots[~np.isnan(boots)]
    if boots.size == 0:
        return (np.nan, np.nan, np.nan, np.nan)

    eff = np.mean(a) - np.mean(b)
    lo, hi = np.percentile(boots, [2.5, 97.5])
    p = 2 * min(np.mean(boots <= 0), np.mean(boots >= 0))
    return (eff, lo, hi, p)

# ---------- READ ----------
r_n = read_wqp(NITRATE_RESULTS)
s_n = read_wqp(NITRATE_SITES)
r_o = read_wqp(ORTHO_RESULTS)
s_o = read_wqp(ORTHO_SITES)

# ---------- SITES MASTER ----------
sites = pd.concat([s_n, s_o], ignore_index=True)

# Keep only what we need
sites_out = pd.DataFrame({
    "site_id": sites["MonitoringLocationIdentifier"],
    "site_name": sites.get("MonitoringLocationName"),
    "lat": sites.get("LatitudeMeasure").map(to_float),
    "lon": sites.get("LongitudeMeasure").map(to_float),
    "huc8": sites.get("HUCEightDigitCode"),
    "state_fips": sites.get("StateCode"),
    "county_fips5": (
        sites.get("StateCode").fillna("").str.zfill(2) +
        sites.get("CountyCode").fillna("").str.zfill(3)
    ).replace({"": None}),
    "provider": sites.get("ProviderName")
}).dropna(subset=["site_id"]).drop_duplicates(subset=["site_id"], keep="first")

# ---------- RESULTS TIDY ----------
def tidy_results(df: pd.DataFrame, chemical: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "result_id": df["ResultIdentifier"],
        "site_id": df["MonitoringLocationIdentifier"],
        "sample_date": pd.to_datetime(df["ActivityStartDate"], errors="coerce"),
        "chemical": chemical,
        "unit_raw": df.get("ResultMeasure/MeasureUnitCode"),
        "value_raw": df.get("ResultMeasureValue").map(to_float),
        "rl_raw": df.get("DetectionQuantitationLimitMeasure/MeasureValue").map(to_float),
        "detect_condition": df.get("ResultDetectionConditionText"),
        # pull whatever exists
        "activity_media": df.get("ActivityMediaName"),
        "activity_media_sub": df.get("ActivityMediaSubdivisionName"),
        "location_type": df.get("MonitoringLocationTypeName"),
    })

    # your existing ND logic stays as is
    out["is_nondetect"] = out["detect_condition"].fillna("").str.lower().str.contains("not detected")
    out["value_for_stats"] = np.where(out["is_nondetect"] & ~np.isnan(out["rl_raw"]),
                                      out["rl_raw"] / 2.0,
                                      out["value_raw"])
    out["sample_date"] = out["sample_date"].dt.date

    # classify water type using whatever fields you have
    txt = (
        out["activity_media_sub"].fillna("") + " " +
        out["location_type"].fillna("") + " " +
        out["activity_media"].fillna("")
    ).str.lower()

    out["water_type"] = np.select(
        [
            txt.str.contains("ground"),
            txt.str.contains("well|spring|aquifer"),
            txt.str.contains("surface|stream|river|lake|reservoir|wetland|estuar"),
        ],
        ["groundwater", "groundwater", "surface_water"],
        default="unknown"
    )

    return out.dropna(subset=["result_id", "site_id"])

res = pd.concat([tidy_results(r_n, "nitrate"),
                 tidy_results(r_o, "ortho")],
                ignore_index=True)

# Drop unknown water type rows
res = res[res["water_type"].ne("unknown")].copy()

# Standardize units
res = standardize_units(res)

unit = res["unit_raw"].fillna("").str.lower().str.replace(" ", "", regex=False)

# Drop duplicates expressed as NO3 or PO4
res = res[~(
    (res["chemical"].eq("nitrate") & unit.str.contains("asno3", na=False)) |
    (res["chemical"].eq("ortho")   & unit.str.contains("aspo4", na=False))
)].copy()

# ---------- MONTHLY BOOTSTRAP CIs ----------
res_dt = res.copy()
res_dt["sample_date"] = pd.to_datetime(res_dt["sample_date"], errors="coerce")
res_dt["month_start"] = month_start(res_dt["sample_date"])
res_dt["value_std"] = pd.to_numeric(res_dt["value_std"], errors="coerce")

agg_rows = []
for (chemical, water_type), g1 in res_dt.groupby(["chemical", "water_type"]):
    for m, g2 in g1.groupby("month_start"):
        x = g2["value_std"].to_numpy(dtype=float)
        sid = g2["site_id"].to_numpy(dtype=str)
        nd = g2["is_nondetect"].to_numpy(dtype=bool)

        for metric in ["mean", "median"]:
            point, lo, hi = bootstrap_ci_cluster(x, sid, metric=metric, B=BOOTSTRAP_N)
            agg_rows.append({
                "chemical": chemical,
                "water_type": water_type,
                "month_start": m.date() if pd.notnull(m) else None,
                "n": int(np.sum(~np.isnan(x))),
                "n_sites": int(g2["site_id"].nunique()),
                "nd_frac": float(np.mean(nd)) if len(nd) else np.nan,
                "mean_value": point if metric == "mean" else np.nan,
                "median_value": point if metric == "median" else np.nan,
                "ci_low": lo,
                "ci_high": hi,
                "metric": metric
            })
agg_month = pd.DataFrame(agg_rows)
agg_month = agg_month[agg_month["n"] > 0].copy()

# ---------- HYPOTHESIS TEST: summer vs winter mean ----------
def season_label(d):
    if pd.isnull(d):
        return None
    m = d.month
    if m in [12, 1, 2]:
        return "winter"
    if m in [6, 7, 8]:
        return "summer"
    return "other"

res_dt["season"] = res_dt["sample_date"].apply(season_label)
tests = []
for (chemical, water_type), g in res_dt.groupby(["chemical", "water_type"]):
    a = g.loc[g["season"] == "summer", "value_std"].to_numpy(float)
    a_sites = g.loc[g["season"] == "summer", "site_id"].to_numpy(str)

    b = g.loc[g["season"] == "winter", "value_std"].to_numpy(float)
    b_sites = g.loc[g["season"] == "winter", "site_id"].to_numpy(str)

    eff, lo, hi, p = bootstrap_diff_cluster(a, a_sites, b, b_sites, B=BOOTSTRAP_N)

    tests.append({
        "chemical": chemical,
        "water_type": water_type,
        "test_name": "summer_vs_winter_mean",
        "n_a": int(np.sum(~np.isnan(a))),
        "n_b": int(np.sum(~np.isnan(b))),
        "effect": eff,
        "ci_low": lo,
        "ci_high": hi,
        "p_value": p
    })
test_results = pd.DataFrame(tests)
test_results = test_results[(test_results["n_a"] > 0) & (test_results["n_b"] > 0)]

# ---------- WRITE OUTPUTS (TSV) ----------
import os
os.makedirs(OUT_DIR, exist_ok=True)

sites_out.to_csv(f"{OUT_DIR}/sites.tsv", sep="\t", index=False)
res.to_csv(f"{OUT_DIR}/results.tsv", sep="\t", index=False)
agg_month.to_csv(f"{OUT_DIR}/agg_month.tsv", sep="\t", index=False)
test_results.to_csv(f"{OUT_DIR}/test_results.tsv", sep="\t", index=False)

print("Wrote outputs to:", OUT_DIR)
