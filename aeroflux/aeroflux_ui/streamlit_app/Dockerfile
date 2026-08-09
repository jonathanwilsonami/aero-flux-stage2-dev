# Build context is aeroflux/ (one level up from this file), not this
# directory — data_access.py imports aeroflux_ml (state_backend_from_env /
# lake_backend_from_env), a sibling local package with no PyPI equivalent,
# so it has to be in the build context to be COPYable. Build with:
#   docker build -f aeroflux_ui/streamlit_app/Dockerfile -t <tag> aeroflux/
# (deploy.sh and .github/workflows/deploy-ui.yml both already do this.)
FROM python:3.11-slim
WORKDIR /app
COPY aeroflux_ui/streamlit_app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY aeroflux_ml ./aeroflux_ml
# data_access.py's _airport_coords() looks for this to place every airport
# on the map (28,427 ICAO codes), falling back to a bundled 24-hub list if
# it's missing — which it silently was, every deploy until now: this file
# was never in the build context, so the deployed app only ever knew 24
# airports' coordinates and dropped every flight whose origin/destination
# wasn't one of them.
COPY aeroflux_parser/data/airports.csv ./aeroflux_parser/data/airports.csv
COPY aeroflux_ui/streamlit_app .
EXPOSE 8501
# AEROFLUX_DSN / AEROFLUX_AGENT_URL are optional (sample data if unset)
# python:3.11-slim has no curl; verified on a real Lightsail deploy that
# `HEALTHCHECK CMD curl ...` silently reports "unhealthy" forever
# (exec: "curl": executable file not found in $PATH) despite the app
# genuinely serving traffic. urllib is stdlib — no extra install needed.
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health', timeout=5)" || exit 1
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
