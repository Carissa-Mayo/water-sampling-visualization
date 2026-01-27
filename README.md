# Water Samples Project (WQP Chemistry Pipeline)

End to end pipeline that pulls surface water chemistry samples, standardizes site + result fields across sources, loads them into a SQL database, produces clean export tables for visualization, and runs a small set of summary statistics / seasonal comparisons (example: summer vs winter nitrate and orthophosphate).

---

## Data

- **WQP (Water Quality Portal)** results and site metadata (STORET + NWIS integrated in WQP exports).
- Washington State
- Nitrate and Orthophosphate
- Ground and Surface water
- Time: 2016-2025

---

## What this project does

1. **Ingest** chemistry results and site metadata (multiple sources, same schema targets).
2. **Standardize** columns and datatypes (site IDs, lat/lon, dates, units, result values).
3. **Load** into SQL server:
   - typed tables that cast to real datatypes and enforce consistent column names
4. **Aggregate** to monthly summaries (per site, per analyte).
5. **Analyze** simple comparisons (seasonal mean differences).
6. **Export** final tables for Tableau / reporting.

___

## Results/Dashboard Overview

1. Hypothesis Testing: Nitrate groundwater samples show stroong evidence for seasonal patterns in presence. With a positive effect of 1.833 and p-value < 0.005, summer trends are much higher than winter trends. 
