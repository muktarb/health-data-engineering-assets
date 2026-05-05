"""Generate synthetic chronic disease surveillance data for pipeline demo."""

import pandas as pd
import numpy as np
import os

np.random.seed(42)

regions = [
    "Adelaide Metro North", "Adelaide Metro South", "Adelaide Hills",
    "Barossa", "Murray Mallee", "Limestone Coast", "Eyre & Western",
    "Flinders & Upper North", "Yorke & Mid North", "SA Total"
]
years = list(range(2018, 2026))
conditions = ["Type 2 Diabetes", "COPD", "Cardiovascular Disease", "Asthma", "Obesity"]
sexes = ["Male", "Female"]

rows = []
for region in regions:
    for year in years:
        for condition in conditions:
            for sex in sexes:
                age = np.random.randint(18, 95)
                population = np.random.randint(5000, 150000)
                base_rate = {"Type 2 Diabetes": 450, "COPD": 280, "Cardiovascular Disease": 520,
                             "Asthma": 350, "Obesity": 380}[condition]
                case_count = int((base_rate / 100000) * population * np.random.uniform(0.7, 1.4))
                rows.append({
                    "region": region,
                    "year": year,
                    "condition": condition,
                    "sex": sex,
                    "age": age,
                    "case_count": case_count,
                    "population": population,
                })

df = pd.DataFrame(rows)

# Inject some realistic data quality issues
# 1. A few nulls
null_indices = np.random.choice(len(df), size=8, replace=False)
df.loc[null_indices[:3], "age"] = np.nan
df.loc[null_indices[3:6], "region"] = np.nan
df.loc[null_indices[6:], "case_count"] = np.nan

# 2. A few duplicates
dupes = df.sample(5, random_state=99)
df = pd.concat([df, dupes], ignore_index=True)

# 3. An out-of-range age
df.loc[len(df)] = ["Adelaide Metro North", 2023, "COPD", "Male", 135, 42, 80000]

os.makedirs("data/raw", exist_ok=True)
df.to_csv("data/raw/chronic_disease_sample.csv", index=False)
print(f"Generated {len(df)} rows with intentional quality issues")
