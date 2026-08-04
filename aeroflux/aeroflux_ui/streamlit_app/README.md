# AeroFlux — Demo UI (Streamlit)

Multi-section demo: **Home** dashboard, **Live Map** (deck.gl/WebGL flight network),
**Analyst** chat, and **Live Inference**. Runs with live data or realistic sample
data so it always demos.

## Run locally
```bash
pip install -r requirements.txt
streamlit run app.py        # http://localhost:8501
```

## Connect live data (optional)
```bash
export AEROFLUX_DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux"
export AEROFLUX_AGENT_URL="http://localhost:8000/agent"   # your RAG/LangGraph endpoint
streamlit run app.py
```
Without these it uses sample flights (no DB/LLM needed) — ideal for a portable demo.

## Reuse your trained model
Drop your existing `models/xgb_classifier_*.joblib` into `models/` and the Live
Inference page loads it automatically; otherwise it uses a heuristic.

## Deploy (cheap options, ranked)
- **Hugging Face Spaces** (free, ~16 GB): new Space → SDK "Docker" → push this
  folder. Set `AEROFLUX_DSN`/`AEROFLUX_AGENT_URL` as Space secrets if going live.
- **Your Lightsail box** (already paid): `docker build -t aeroflux-ui . &&
  docker run -p 8501:8501 --env-file .env aeroflux-ui`.
- **Streamlit Community Cloud** (free, ~1 GB): point it at this repo path.

Container is ~1–2 GB RAM in sample mode — far under Amplify's 4 GB.
