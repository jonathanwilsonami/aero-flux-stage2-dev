# Build context is agent/ itself -- self-contained, no AeroFlux core
# package imports (Level 2 integration keeps data sample-based; see
# ../AGENT_INTEGRATION.md for the real S3/DynamoDB swap, a separate
# fast-follow). Build with:
#   docker build -f agent/Dockerfile -t <tag> agent/
# (.github/workflows/deploy-agent.yml already does this.)
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8010
# python:3.11-slim has no curl -- same gotcha as aeroflux_ui/streamlit_app's
# Dockerfile (see its comment); urllib is stdlib, no extra install needed.
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8010/health', timeout=5)" || exit 1
ENTRYPOINT ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8010"]
