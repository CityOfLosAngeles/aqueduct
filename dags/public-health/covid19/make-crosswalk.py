"""
Create crosswalk that links counties to MSA for the US.
Aggregate county population to get MSA population.
"""
import numpy as np
import pandas as pd

NBER_CROSSWALK_URL = (
    "https://data.nber.org/cbsa-csa-fips-county-crosswalk/" "cbsa2fipsxw.csv"
)

LOOKUP_TABLE_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/"
    "UID_ISO_FIPS_LookUp_Table.csv"
)


# Define functions to clean data before merging
def coerce_integer(df):
    """
    Coerce nullable columns to integers for CSV export.

    TODO: recent versions of pandas (>=0.25) support nullable integers.
    Once we can safely upgrade, we should use those and remove this function.
    """

    def integrify(x):
        return int(float(x)) if not pd.isna(x) else None

    cols = [
        "county_pop",
        "msa_pop",
    ]
    new_cols = {c: df[c].apply(integrify, convert_dtype=False) for c in cols}
    return df.assign(**new_cols)


def wrangle_nber_crosswalk(df):
    df = df[df.cbsacode.notna()]
    df = df.assign(
        cbsacode=df.cbsacode.astype(int).astype(str),
        fips_state_code=df.fipsstatecode.astype(int)
        .astype(str)
        .str.pad(width=2, side="left", fillchar="0"),
        fips_county_code=df.fipscountycode.astype(int)
        .astype(str)
        .str.pad(width=3, side="left", fillchar="0"),
        state=df.statename,
        county=df.countycountyequivalent,
        metro_micro=df.metropolitanmicropolitanstatis,
    )

    df["county_fips"] = df.fips_state_code.map(str) + df.fips_county_code.map(str)

    keep_col = [
        "cbsacode",
        "cbsatitle",
        "metro_micro",
        "county",
        "state",
        "county_fips",
        "fips_state_code",
        "fips_county_code",
    ]

    return df[keep_col]


def clean_jhu_lookup(df):
    df = df[(df.Country_Region == "US") & (df.Admin2.notna()) & (df.FIPS.notna())][
        ["Admin2", "Province_State", "FIPS", "Population"]
    ]

    df = (
        df.assign(county_fips=df.FIPS.str.pad(width=5, side="left", fillchar="0"),)
        .rename(
            columns={
                "Admin2": "county",
                "Province_State": "state",
                "Population": "county_pop",
            }
        )
        .drop(columns=["FIPS"])
    )

    return df


# (1) Import NBER crosswalk that links counties to MSAs/CBSAs
nber_crosswalk = pd.read_csv(NBER_CROSSWALK_URL)
crosswalk = wrangle_nber_crosswalk(nber_crosswalk)

# (2) Import JHU lookup table that has county and county populations
lookup_table = pd.read_csv(LOOKUP_TABLE_URL.format("master"), dtype={"FIPS": "str"})
lookup_table = clean_jhu_lookup(lookup_table)

# (3) Merge MSA crosswalk with county populations
m1 = pd.merge(
    crosswalk,
    lookup_table.drop(columns="county"),
    on=["county_fips", "state"],
    how="left",
    validate="1:1",
)

# (4) Aggregate county populations by MSA to get MSA pop
m2 = (
    m1.groupby(["cbsacode", "cbsatitle", "metro_micro"])
    .agg({"county_pop": "sum"})
    .reset_index()
    .rename(columns={"county_pop": "msa_pop"})
)

# (5) Merge in MSA pop and clean up
m3 = pd.merge(
    m1, m2, on=["cbsacode", "cbsatitle", "metro_micro"], how="left", validate="m:1"
)

m3 = (
    m3.assign(
        # JHU does not have county populations for MSAs in Puerto Rico, fill with NaN
        msa_pop=m3.apply(
            lambda row: np.nan if row.msa_pop == 0 else row.msa_pop, axis=1
        ),
    )
    .pipe(coerce_integer)
    .sort_values(["cbsacode", "fips_state_code", "fips_county_code"])
)

# Export to S3 and upload to GitHub
m3.to_csv(
    "s3://public-health-dashboard/jhu_covid19/msa_county_pop_crosswalk.csv", index=False
)
