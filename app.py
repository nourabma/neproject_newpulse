from __future__ import annotations

import os
import time

import altair as alt
import pandas as pd
import streamlit as st
from pyspark.sql.utils import AnalysisException

from llm_summary import summarise
from streaming_job import build_spark, start_streams

st.set_page_config(
    page_title="Headline Pulse",
    layout="wide",
    initial_sidebar_state="collapsed",
)


@st.cache_resource(show_spinner="Booting Spark and streaming queries")
def boot():
    os.makedirs("data/incoming", exist_ok=True)
    spark = build_spark("headline_pulse_dashboard")
    queries = start_streams(spark)
    time.sleep(2)
    return spark, queries


spark, queries = boot()

try:
    from streamlit_autorefresh import st_autorefresh

    st_autorefresh(interval=8_000, key="headline_pulse_tick")
except ImportError:
    pass


def query(sql: str) -> pd.DataFrame:
    try:
        return spark.sql(sql).toPandas()
    except AnalysisException:
        return pd.DataFrame()
    except Exception as exc:
        st.warning(f"query failed: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=90, show_spinner=False)
def cached_brief(words: tuple) -> str:
    return summarise(list(words))


st.markdown("## Headline Pulse")
st.caption(
    "Five world-news RSS feeds, streamed through Spark Structured Streaming, "
    "summarised by an LLM."
)

top_left, top_right = st.columns(2)

with top_left:
    st.markdown("**Headlines by source**")
    df_src = query("SELECT source, count FROM source_counts ORDER BY count DESC")
    if df_src.empty:
        st.info("Source counts will appear once the first batch lands.")
    else:
        chart = (
            alt.Chart(df_src)
            .mark_bar()
            .encode(
                x=alt.X("source:N", sort="-y", title=None),
                y=alt.Y("count:Q", title="headlines"),
                tooltip=["source", "count"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

with top_right:
    st.markdown("**Volume by hour (event time)**")
    df_win = query("SELECT window.start AS hour, count FROM hourly_volume ORDER BY hour")
    if df_win.empty:
        st.info("Hourly windows will appear after a few batches.")
    else:
        df_win["hour"] = pd.to_datetime(df_win["hour"])
        line = (
            alt.Chart(df_win)
            .mark_line(point=True)
            .encode(
                x=alt.X("hour:T", title="event-time hour"),
                y=alt.Y("count:Q", title="headlines"),
                tooltip=["hour:T", "count"],
            )
        )
        st.altair_chart(line, use_container_width=True)

st.markdown("---")
bottom_left, bottom_right = st.columns([2, 3])

with bottom_left:
    st.markdown("**Top keywords**")
    df_words = query("SELECT word, count FROM word_counts ORDER BY count DESC LIMIT 20")
    if df_words.empty:
        st.info("Keywords will populate after the first batch.")
    else:
        st.dataframe(df_words, hide_index=True, use_container_width=True)

with bottom_right:
    st.markdown("**Editorial brief**")
    if df_words.empty or len(df_words) < 3:
        st.write("_Brief appears once enough keywords are extracted._")
    else:
        words_tuple = tuple(df_words["word"].head(15).tolist())
        st.write(cached_brief(words_tuple))

with st.expander("Streaming query status"):
    for q in queries:
        st.text(f"{q.name}: active={q.isActive}")
    st.caption("Set ANTHROPIC_API_KEY to enable the live editorial brief.")
