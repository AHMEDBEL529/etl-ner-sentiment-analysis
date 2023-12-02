import streamlit as st
import duckdb
import pandas as pd
import altair as alt
from include.global_variables import global_variables as gv

# GETTING DATA

duck_db_instance_name = "dwh"  # adjust this to your db name

def get_sentiment_data(db=f"/usr/local/airflow/{duck_db_instance_name}"):
    """Function to query a local DuckDB instance for sentiment data."""
    cursor = duckdb.connect(db)
    sentiment_data = cursor.execute(
        f"""SELECT name, sentiment, count FROM {"output" + "_" + gv.MY_VIDEO_ID};"""
    ).fetchall()
    cursor.close()
    df = pd.DataFrame(sentiment_data, columns=["name", "sentiment", "count"])
    return df

sentiment_df = get_sentiment_data()

st.title("Sentiment Analysis Results")

if "num_of_groups" not in st.session_state:
    st.session_state.num_of_groups = 1

groupings = {}

for i in range(st.session_state.num_of_groups):
    unique_names = sentiment_df['name'].unique()
    selected_names = st.multiselect(f"Select names to group for entity {i + 1}:", unique_names)
    if selected_names:
        new_name = st.text_input(f"Enter a new name for the group for entity {i + 1}:", f"Group {i + 1}")
        groupings[new_name] = selected_names

if st.button("Add new entity group"):
    st.session_state.num_of_groups += 1

if st.button("Remove last entity group") and st.session_state.num_of_groups > 1:
    st.session_state.num_of_groups -= 1

if st.button("Reset all groupings"):
    st.session_state.num_of_groups = 1

if st.button("Apply Groupings"):
    for new_name, old_names in groupings.items():
        sentiment_df.loc[sentiment_df['name'].isin(old_names), 'name'] = new_name

# Filter for only POS sentiment
pos_df = sentiment_df[sentiment_df['sentiment'] == 'POS'].copy()

# Calculate total POS count
total_pos = pos_df['count'].sum()

# Calculate percentage of each entity's POS count
pos_df['percentage'] = (pos_df['count'] / total_pos) * 100

# Pie Chart for POS Sentiment
pie_chart = alt.Chart(pos_df).mark_arc().encode(
    theta=alt.Theta('percentage:Q'),
    color=alt.Color('name:N', legend=alt.Legend(title='Entities')),
    tooltip=['name', alt.Tooltip('percentage:Q', title='Percentage'), 'count']
)


st.altair_chart(pie_chart, use_container_width=True)

# Original sentiment distribution
sentiment_df_sorted = sentiment_df.groupby(['name', 'sentiment']).sum().reset_index().sort_values(by='count', ascending=False)

sentiment_chart = alt.Chart(sentiment_df_sorted).mark_bar().encode(
    x=alt.X('name:N', sort='-y'),
    y='count:Q',
    color='sentiment:N',
    tooltip=['name', 'sentiment', 'count']
).interactive()

st.altair_chart(sentiment_chart, use_container_width=True)

# Sum of all sentiments for each entity
summed_sentiments = sentiment_df.groupby('name').sum().reset_index().sort_values(by='count', ascending=False)
sum_chart = alt.Chart(summed_sentiments).mark_bar().encode(
    x=alt.X('name:N', sort='-y'),
    y=alt.Y('count:Q', title='Total Sentiment Count'),
    color='name:N',
    tooltip=['name', 'count']
).interactive()

st.altair_chart(sum_chart, use_container_width=True)
