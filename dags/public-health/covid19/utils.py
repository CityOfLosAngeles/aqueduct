"""
Functions to create county or state-specific indicators.
Use JHU county data.
"""
import altair as alt
import numpy as np
import pandas as pd
import state_abbrev_dict

from IPython.display import display

US_COUNTY_URL = (
    "http://lahub.maps.arcgis.com/sharing/rest/content/items/"
    "8aba663239fc428f8bcdc48e213e3172/data"
)

LA_CITY_URL = (
    "http://lahub.maps.arcgis.com/sharing/rest/content/items/"
    "7175fba373f541a7a19df56b6a0617f4/data"
)

TESTING_URL = (
    "http://lahub.maps.arcgis.com/sharing/rest/content/items/"
    "158dab4a07b04ecb8d47fea1746303ac/data"
)

HOSPITAL_URL = (
    "http://lahub.maps.arcgis.com/sharing/rest/content/items/"
    "3da1eb3e13a14743973c96b945bd1117/data"
)

CROSSWALK_URL = (
    "https://raw.githubusercontent.com/CityOfLosAngeles/aqueduct/master/dags/"
    "public-health/covid19/msa_county_pop_crosswalk.csv"
)

# Define chart parameters
navy = "#0A4C6A"
maroon = "#A30F23"
title_font_size = 10
font_name = "Roboto"
grid_opacity = 0
domain_opacity = 0.4
stroke_opacity = 0
time_unit = "monthdate"
chart_width = 250
chart_height = 200


# Prep county-level dataset
def prep_us_county_time_series():
    df = pd.read_csv(US_COUNTY_URL, dtype={"fips": "str"})
    df = df.assign(
        date=pd.to_datetime(df.date),
        state_abbrev=df.state.map(state_abbrev_dict.us_state_abbrev),
    )
    return df


# County-level case data
def case_indicators_county(county_state_name, start_date):
    df = prep_us_county_time_series()

    # Parse the county_state_name into county_name and state_name (abbrev)
    if "," in county_state_name:
        state_name = county_state_name.split(",")[1].strip()
        county_name = county_state_name.split(",")[0].strip()

        if len(state_name) > 2:
            state_name = state_abbrev_dict.us_state_abbrev[state_name]

        # County names don't have " County" at the end. There is a TriCounty, UT though.
        if " County" in county_name:
            county_name = county_name.replace(" County", "").strip()

    elif any(map(str.isdigit, county_state_name)):
        state_name = df[df.fips == county_state_name].state_abbrev.iloc[0]
        county_name = df[df.fips == county_state_name].county.iloc[0]

    keep_cols = [
        "county",
        "state",
        "state_abbrev",
        "fips",
        "date",
        "Lat",
        "Lon",
        "cases",
        "deaths",
        "new_cases",
        "new_deaths",
    ]

    df = (
        df[
            (df.county == county_name)
            & (df.state_abbrev == state_name)
            & (df.date >= start_date)
        ][keep_cols]
        .sort_values(["county", "state", "fips", "date"])
        .reset_index(drop=True)
    )

    name = df.county.iloc[0]
    df = make_cases_deaths_chart(df, "county", name)

    return df


# State-level case data
def case_indicators_state(state_name, start_date):
    df = prep_us_county_time_series()

    keep_cols = [
        "state",
        "state_abbrev",
        "date",
        "state_cases",
        "state_deaths",
        "new_state_cases",
        "new_state_deaths",
    ]

    df = (
        df[
            ((df.state == state_name) | (df.state_abbrev == state_name))
            & (df.date >= start_date)
        ][keep_cols]
        .sort_values(["state", "date"])
        .drop_duplicates()
        .rename(
            columns={
                "state_cases": "cases",
                "state_deaths": "deaths",
                "new_state_cases": "new_cases",
                "new_state_deaths": "new_deaths",
            }
        )
        .reset_index(drop=True)
    )

    name = df.state.iloc[0]
    df = make_cases_deaths_chart(df, "state", name)

    return df


# MSA-level case data
def case_indicators_msa(msa_name, start_date):
    group_cols = ["msa", "msa_pop", "date"]
    msa_group_cols = ["msa", "msa_pop"]

    # Merge county to MSA using crosswalk
    df = prep_us_county_time_series()

    pop = pd.read_csv(CROSSWALK_URL, dtype={"county_fips": "str", "cbsacode": "str"},)
    pop = pop[
        (pop.cbsatitle == msa_name)
        | (pop.cbsatitle.str.contains(msa_name))
        | (pop.cbsacode == msa_name)
    ][["cbsacode", "cbsatitle", "msa_pop", "county_fips"]].assign(msa=pop.cbsatitle)

    final_df = pd.merge(
        df, pop, left_on="fips", right_on="county_fips", how="inner", validate="m:1",
    )

    df = (
        final_df[final_df.date >= start_date]
        .groupby(group_cols)
        .agg({"cases": "sum", "deaths": "sum"})
        .reset_index()
    )

    # Create new cases and new deaths columns
    df = df.assign(
        new_cases=(
            df.sort_values(group_cols).groupby(msa_group_cols)["cases"].diff(periods=1)
        ),
        new_deaths=(
            df.sort_values(group_cols).groupby(msa_group_cols)["deaths"].diff(periods=1)
        ),
    )

    name = df.msa.iloc[0]
    df = make_cases_deaths_chart(df, "msa", name)

    return df


# City of LA case data
def case_indicators_lacity(start_date):
    city_df = pd.read_csv(LA_CITY_URL)
    city_df["date"] = pd.to_datetime(city_df.Date)

    df = (
        city_df[city_df.date >= start_date]
        .rename(
            columns={"City of LA Cases": "cases", "City of LA New Cases": "new_cases"}
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Derive new columns
    df = df.assign(
        # 7-day rolling average for new cases
        cases_avg7=df.new_cases.rolling(window=7).mean(),
    )

    # Make cases charts
    cases_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit=time_unit, title="date"),
            y=alt.Y("cases_avg7", title="7-day avg"),
            color=alt.value(navy),
        )
        .properties(
            title="City of LA: New Cases", width=chart_width, height=chart_height
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(gridOpacity=grid_opacity, domainOpacity=domain_opacity)
        .configure_view(strokeOpacity=stroke_opacity)
    )

    display(cases_chart)

    return df


# Sub-function to make cases and deaths chart
def make_cases_deaths_chart(df, geog, name):
    # Define chart titles
    if geog == "county":
        chart_title = f"{name} County"
    if geog == "state":
        chart_title = f"{name}"
    if geog == "msa":
        chart_title = f"{name} MSA"

    # Derive new columns
    df = df.assign(
        cases_avg7=df.new_cases.rolling(window=7).mean(),
        deaths_avg3=df.new_deaths.rolling(window=3).mean(),
        deaths_avg7=df.new_deaths.rolling(window=7).mean(),
    )

    # Make cases charts
    cases_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit=time_unit, title="date"),
            y=alt.Y("cases_avg7", title="7-day avg"),
            color=alt.value(navy),
        )
        .properties(
            title=f"{chart_title}: New Cases", width=chart_width, height=chart_height
        )
    )

    # Make deaths chart
    deaths_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit=time_unit, title="date"),
            y=alt.Y("deaths_avg3", title="3-day avg"),
            color=alt.value(maroon),
        )
        .properties(
            title=f"{chart_title}: New Deaths", width=chart_width, height=chart_height
        )
    )

    combined_chart = (
        alt.hconcat(cases_chart, deaths_chart)
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(gridOpacity=grid_opacity, domainOpacity=domain_opacity)
        .configure_view(strokeOpacity=stroke_opacity)
    )

    display(combined_chart)

    return df


# Make daily testing chart for City of LA
def testing_lacity(start_date, daily_or_monthly, lower_bound, upper_bound):
    df = pd.read_csv(TESTING_URL)
    df = df.assign(
        Date=pd.to_datetime(df.Date).dt.strftime("%-m/%-d/%y"),
        month=pd.to_datetime(df.Date).dt.month,
    )
    df = df[df.Date >= start_date]

    # Aggregate tests by month
    df = df.assign(Performed_Monthly=df.groupby("month")["Performed"].transform("sum"))

    if daily_or_monthly == "monthly":
        format_date = "%b"
        plot_col = "Performed_Monthly:Q"
        chart_title = "Monthly Tests Performed"
        df = df.drop_duplicates(subset=["month", "Performed_Monthly"])
        chart_width = 150

    if daily_or_monthly == "daily":
        format_date = "%-m/%-d"
        plot_col = "Performed:Q"
        chart_title = "Daily Tests Performed"
        chart_width = 500

    make_testing_chart(
        df, plot_col, format_date, lower_bound, upper_bound, chart_title, chart_width
    )

    return df


# Sub-function to make testing bar chart
def make_testing_chart(
    df, plot_col, format_date, lower_bound, upper_bound, chart_title, chart_width
):
    bar = (
        alt.Chart(df)
        .mark_bar(color=navy)
        .encode(
            x=alt.X(
                "Date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format=format_date),
            ),
            y=alt.Y(plot_col, title="Tests Performed"),
        )
    )

    line1 = (
        alt.Chart(pd.DataFrame({"y": [lower_bound]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )
    line2 = (
        alt.Chart(pd.DataFrame({"y": [upper_bound]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )

    testing_chart = (
        (bar + line1 + line2)
        .properties(title=chart_title, width=chart_width)
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    display(testing_chart)


# Hospital Equipment Availability
def hospital_capacity_lacity(start_date):
    df = pd.read_csv(HOSPITAL_URL)

    # Get a total count of equipment for each date-type
    df = df.assign(
        Date=pd.to_datetime(df.Date).dt.strftime("%-m/%-d/%y"),
        type_total=df.groupby(["Date", "Type"])["Count"].transform("sum"),
    )

    # Calculate number and percent available
    df = df.assign(
        n_available=df.apply(
            lambda row: row.Count if row.Status == "Available" else np.nan, axis=1
        ),
        pct_available=df.apply(
            lambda row: row.Count / row.type_total
            if row.Status == "Available"
            else np.nan,
            axis=1,
        ),
    )

    keep_col = ["Date", "Type", "type_total", "n_available", "pct_available"]

    df = df[(df.n_available.notna()) & (df.Date >= start_date)][
        keep_col
    ].drop_duplicates()

    make_hospital_chart(df)

    return df


# Sub-function to make hospital equipment availability bar chart
def make_hospital_chart(df):
    acute_color = "#04E679"
    icu_color = navy
    ventilator_color = "#e76f51"
    chart_width = 500

    base = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(
                "Date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format="%-m/%-d"),
            ),
            y=alt.Y("pct_available", title="proportion available"),
            color=alt.Color(
                "Type",
                scale=alt.Scale(
                    domain=["Acute Care Beds", "ICU Beds", "Ventilators"],
                    range=[acute_color, icu_color, ventilator_color],
                ),
            ),
        )
    )

    line1 = (
        alt.Chart(pd.DataFrame({"y": [0.3]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )

    hospital_pct_chart = (
        (base + line1)
        .properties(
            title="Proportion of Available Hospital Equipment by Type",
            width=chart_width,
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    base2 = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(
                "Date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format="%-m/%-d"),
            ),
            y=alt.Y("n_available", title="number available"),
            color=alt.Color(
                "Type",
                scale=alt.Scale(
                    domain=["Acute Care Beds", "ICU Beds", "Ventilators"],
                    range=[acute_color, icu_color, ventilator_color],
                ),
            ),
        )
    )

    hospital_num_chart = (
        base2.properties(
            title="Number of Available Hospital Equipment by Type", width=chart_width
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    display(hospital_pct_chart)
    display(hospital_num_chart)
