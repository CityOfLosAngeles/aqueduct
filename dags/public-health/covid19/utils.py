"""
Functions to create county or state-specific indicators.
Use JHU county data.
"""
import altair as alt
import pandas as pd

# County-level case data
def case_indicators_county(county_name, start_date):
    US_COUNTY_URL = "http://lahub.maps.arcgis.com/sharing/rest/content/items/8aba663239fc428f8bcdc48e213e3172/data"
    county_df = pd.read_csv(US_COUNTY_URL, dtype={"fips": "str"})

    keep_cols = [
        "county",
        "state",
        "fips",
        "date",
        "Lat",
        "Lon",
        "cases",
        "deaths",
        "new_cases",
        "new_deaths",
    ]

    county_df["date"] = pd.to_datetime(county_df.date)

    df = (
        county_df[(county_df.county == county_name) & (county_df.date >= start_date)][
            keep_cols
        ]
        .sort_values(["county", "state", "fips", "date"])
        .reset_index(drop=True)
    )

    # Derive new columns
    df = df.assign(
        # 7-day rolling average for new cases
        cases_avg7=df.new_cases.rolling(window=7).mean(),
        # 3-day rolling average for new deaths
        deaths_avg3=df.new_deaths.rolling(window=3).mean(),
    )

    # Make cases charts
    cases_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit="monthdate", title="date"),
            y=alt.Y("cases_avg7", title="7-day avg"),
            color=alt.value("#0A4C6A"),
        )
        .properties(title=f"{county_name} County: New Cases",)
        .configure_title(fontSize=14, font="Roboto", anchor="middle", color="Black")
        .configure_axis(gridOpacity=0.4,)
    )

    # Make deaths chart
    deaths_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit="monthdate", title="date"),
            y=alt.Y("deaths_avg3", title="3-day avg"),
            color=alt.value("#A30F23"),
        )
        .properties(title=f"{county_name} County: Deaths",)
        .configure_title(fontSize=14, font="Roboto", anchor="middle", color="Black")
        .configure_axis(gridOpacity=0.4,)
    )

    display(cases_chart)
    display(deaths_chart)

    return df


# State-level case data
def case_indicators_state(state_name, start_date):
    US_COUNTY_URL = "http://lahub.maps.arcgis.com/sharing/rest/content/items/8aba663239fc428f8bcdc48e213e3172/data"
    county_df = pd.read_csv(US_COUNTY_URL, dtype={"fips": "str"})

    keep_cols = [
        "state",
        "date",
        "state_cases",
        "state_deaths",
        "new_state_cases",
        "new_state_deaths",
    ]

    county_df["date"] = pd.to_datetime(county_df.date)

    df = (
        county_df[(county_df.state == state_name) & (county_df.date >= start_date)][
            keep_cols
        ]
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

    # Derive new columns
    df = df.assign(
        # 7-day rolling average for new cases
        cases_avg7=df.new_cases.rolling(window=7).mean(),
        # 3-day rolling average for new deaths
        deaths_avg3=df.new_deaths.rolling(window=3).mean(),
    )

    # Make cases charts
    cases_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit="monthdate", title="date"),
            y=alt.Y("cases_avg7", title="7-day avg"),
            color=alt.value("#0A4C6A"),
        )
        .properties(title=f"{state_name}: New Cases",)
        .configure_title(fontSize=14, font="Roboto", anchor="middle", color="Black")
        .configure_axis(gridOpacity=0.4,)
    )

    # Make deaths chart
    deaths_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit="monthdate", title="date"),
            y=alt.Y("deaths_avg3", title="3-day avg"),
            color=alt.value("#A30F23"),
        )
        .properties(title=f"{state_name}: Deaths",)
        .configure_title(fontSize=14, font="Roboto", anchor="middle", color="Black")
        .configure_axis(gridOpacity=0.4,)
    )

    display(cases_chart)
    display(deaths_chart)

    return df


# City of LA case data
def case_indicators_lacity(start_date):
    LA_CITY_URL = "http://lahub.maps.arcgis.com/sharing/rest/content/items/7175fba373f541a7a19df56b6a0617f4/data"
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
            x=alt.X("date:T", timeUnit="monthdate", title="date"),
            y=alt.Y("cases_avg7", title="7-day avg"),
            color=alt.value("#0A4C6A"),
        )
        .properties(title="City of LA: New Cases",)
        .configure_title(fontSize=14, font="Roboto", anchor="middle", color="Black")
        .configure_axis(gridOpacity=0.4,)
    )

    display(cases_chart)

    return df
