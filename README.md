# Headline Pulse

Real-time RSS pulse. Five world-news feeds → Spark Structured Streaming
→ Streamlit dashboard with an LLM-written editorial brief.

```
ingester.py  ─►  data/incoming/*.jsonl  ─►  Spark file-source stream
                                              │
        ┌─────────────────────────────────────┼─────────────────────────────────────┐
        ▼                                     ▼                                     ▼
  source_counts                        hourly_volume                          word_counts
        └─────────────────────────────────────┼─────────────────────────────────────┘
                                              ▼
                                Streamlit dashboard + LLM brief
```

## Quick start

```
pip install -r requirements.txt
python run.py
```

Dashboard opens at http://localhost:8501. Ctrl-C stops both the
ingester and Streamlit.

## Live LLM brief (optional)

```
export ANTHROPIC_API_KEY=sk-ant-...
python run.py
```

Without a key the dashboard shows an offline keyword digest. The call
is wrapped in try/except, so a dead API never crashes the dashboard.

## Three-terminal mode

```
# T1: ingester
python ingester.py

# T2: Spark streaming (keep alive)
python streaming_job.py

# T4: dashboard
streamlit run app.py
```

## File map

| Task | File              | Notes                                       |
|------|-------------------|---------------------------------------------|
| T1   | ingester.py       | 5 RSS feeds, 50 s tick, dead-feed tolerant  |
| T2   | streaming_job.py  | 3 streaming queries → memory sinks          |
| T3   | llm_summary.py    | Anthropic Claude + offline fallback         |
| T4   | app.py            | Streamlit + Altair, 8 s auto-refresh        |
| T5   | reflection.md     | Scaling reflection                          |
| —    | run.py            | one-command runner                          |

## Requirements

- Python 3.9–3.12
- Java 11 or 17 on PATH (`java -version`)
- ~2 GB free RAM

## Hand-test the streaming layer

```
echo '{"source":"manual","title":"hello world","url":"x","ts":"2026-05-11T10:00:00Z"}' \
  > data/incoming/manual.jsonl
```

The `source_counts` table will pick it up on the next micro-batch.
