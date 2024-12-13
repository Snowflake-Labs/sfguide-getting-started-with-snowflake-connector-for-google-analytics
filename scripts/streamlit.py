import streamlit as st
import altair as alt
import json
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide", initial_sidebar_state="expanded")
session = get_active_session()

st.header("Website Analytics")


def load_data():
    query = """
        SELECT * FROM SNOWFLAKE_CONNECTOR_FOR_GOOGLE_ANALYTICS_RAW_DATA_DEST_DB.SNOWFLAKE_CONNECTOR_FOR_GOOGLE_ANALYTICS_RAW_DATA_DEST_SCHEMA.ANALYTICS_20210131__VIEW limit 10000
    """
    data = session.sql(query).to_pandas().dropna(axis=1, how="all")
    columns_to_remove = [
        "USER_PROPERTIES", "USER_LTV_CURRENCY", "ECOMMERCE_TOTAL_ITEM_QUANTITY",
        "ECOMMERCE_PURCHASE_REVENUE_IN_USD", "ECOMMERCE_UNIQUE_ITEMS", "ECOMMERCE_TRANSACTION_ID",
        "ITEMS", "USER_PROPERTIES__FLATTENED"
    ]
    new_data = data.drop(columns=columns_to_remove, errors="ignore")
    return new_data[~new_data["GEO_COUNTRY"].isin(["(not set)", "unknown"])]


def apply_filters(data):
    hours = data["EVENT_TIMESTAMP"].dt.strftime('%H').unique().tolist()
    hours.sort()
    user_ids = data["USER_PSEUDO_ID"].unique().tolist()

    with st.sidebar:
        st.write("**Filter data by hour or by User ID**")
        start = st.selectbox("Start Time", hours, index=0)
        end = st.selectbox("End Time", hours, index=len(hours) - 1)
        user_id = st.selectbox("User ID", options=["All"] + list(user_ids))

    filtered_data = data[
        (data["EVENT_TIMESTAMP"].dt.hour >= int(start)) & (data["EVENT_TIMESTAMP"].dt.hour <= int(end))
        ]
    if user_id != "All":
        filtered_data = filtered_data[filtered_data["USER_PSEUDO_ID"] == user_id]
    return filtered_data


def behavior(data):
    chart = alt.Chart(data).mark_bar().encode(
        x=alt.X("count()", title="Amount"),
        y=alt.Y("EVENT_NAME:N", title="Action"),
        color="EVENT_NAME"
    ).properties(
        title="Behavior Metrics",
    )
    st.altair_chart(chart)


def user_engagement_by_country(data):
    chart = alt.Chart(data).mark_bar().encode(
        x=alt.X("GEO_COUNTRY", title="Country"),
        y=alt.Y("count()", title="Amount"),
        color="GEO_COUNTRY"
    ).properties(
        title="User Engagement by Country"
    )
    st.altair_chart(chart)


def extract_page_location(json_str):
    try:
        if not isinstance(json_str, str):
            return None

        json_obj = json.loads(json_str)
        if isinstance(json_obj, list):
            for item in json_obj:
                if item.get("key") == "page_location":
                    return item["value"].get("string_value")

    except Exception as e:
        st.write(f"Error parsing row: {json_str}, Error: {e}")
    return None


def main():
    cleaned_data = load_data()
    filtered_data = apply_filters(cleaned_data)
    user_engagement_by_country(filtered_data)

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Data Set**")
        st.dataframe(filtered_data)
    with col2:
        behavior(filtered_data)

    filtered_data["website"] = filtered_data["EVENT_PARAMS"].apply(extract_page_location)
    st.write("**Top 5 Websites**")

    top_five_websites = filtered_data["website"].dropna().value_counts().head(5)

    for i, (website, count) in enumerate(top_five_websites.items(), start=1):
        st.write(f"{i}. **{count}** visits: {website} ")


if __name__ == "__main__":
    main()