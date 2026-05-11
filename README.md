# Headline Pulse

A tiny one-laptop newsroom. Five international RSS feeds are polled
on a fixed cadence; their headlines land in `data/incoming/` as
JSONL; Spark Structured Streaming reads that folder as a stream and
keeps three live aggregations in memory; a Streamlit page reads
those aggregations and asks Anthropic Claude to write a one-paragraph
editorial brief over the current keyword cloud.

## Setup

1. Python 3.9 to 3.12, Java 11 or 17 on PATH (`java -version`).
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Optional, for the live brief:
   ```
   export ANTHROPIC_API_KEY=sk-ant-...
   ```
   Without it, the dashboard prints an offline keyword digest. The
   Anthropic call is wrapped in `try/except`, so a dead API never
   takes the page down.

## Running it

Single command:

```
python run.py
```

The launcher spawns the ingester as a subprocess, waits a few seconds
so the first batch can land, then `streamlit run app.py` on
`localhost:8501`. `Ctrl-C` stops both children cleanly.

If you want to watch each stage in its own terminal instead:

```
python ingester.py          # writes data/incoming/feed-<ms>.jsonl every 50 s
python streaming_job.py     # boots Spark, keeps three queries alive
streamlit run app.py        # dashboard reads the memory tables
```

## What lives where

- `ingester.py` — `FeedPoller` class. Polls DW, France24, CBC,
  The Hindu and Times of India. Each tick writes one JSONL file
  named `feed-<unix-ms>.jsonl`. Dead feeds are caught per-source.
- `streaming_job.py` — `build_spark()` + `start_streams()`. Reads
  `data/incoming/` via `spark.readStream.json(...)` and emits three
  streaming queries to memory sinks: `source_counts`,
  `hourly_volume` (1-hour tumbling window with a 3-hour watermark)
  and `word_counts` (lowercase token split on `[^a-z]+`, length
  filter, broadcast `left_anti` join against a stopword DataFrame).
- `llm_summary.py` — `summarise(keywords)`. Calls Claude
  (`claude-haiku-4-5-20251001`) with a desk-editor system prompt;
  falls back to a plain keyword digest on any failure.
- `app.py` — Streamlit page. Bar chart for sources, line chart for
  hourly volume, table for top words, the editorial brief, plus a
  collapsed expander showing the live status of each streaming
  query. Auto-refreshes every 8 seconds.
- `run.py` — process supervisor for the one-command path.
- `reflection.md` — the T5 paragraph.

## Drop a hand-written batch to test the streaming layer

```
echo '{"source":"manual","title":"a quick brown fox","url":"x","ts":"2026-05-11T10:00:00Z"}' \
  > data/incoming/manual.jsonl
```

Spark's file-source picks it up on the next micro-batch and the
counts in the dashboard tick up.

## Notes for the demo

- Charts use Altair, so they render their own legends and tooltips
  instead of relying on `st.bar_chart`.
- The `word_counts` query removes stopwords with a streaming-static
  `left_anti` join rather than an `isin` filter, which scales better
  if the stopword list ever grows.
- All three memory sinks are `outputMode("complete")` — see
  `reflection.md` for what that costs at scale.
