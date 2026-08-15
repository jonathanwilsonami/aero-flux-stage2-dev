# Build context is the REPO ROOT, not agent/ itself (as of Level 3,
# 2026-08-14) -- tools.py's live cloud reads need aeroflux_ml
# (state_backend_from_env()/lake_backend_from_env()), which lives in the
# sibling aeroflux/ directory, outside agent/'s own directory -- same
# reason aeroflux_ui/streamlit_app/Dockerfile widened its own context,
# see its comment. Build with:
#   docker build -f agent/Dockerfile -t <tag> .
# (.github/workflows/deploy-agent.yml already does this.)
FROM python:3.11-slim
WORKDIR /app
COPY agent/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY aeroflux/aeroflux_ml ./aeroflux_ml
COPY agent/. .
EXPOSE 8010
# python:3.11-slim has no curl -- same gotcha as aeroflux_ui/streamlit_app's
# Dockerfile (see its comment); urllib is stdlib, no extra install needed.
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8010/health', timeout=5)" || exit 1
ENTRYPOINT ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8010"]
