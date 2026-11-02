# LLM Gateway

A robust, enterprise-grade LLM Gateway and reverse proxy built with FastAPI. It provides unified multi-provider model routing, automated failovers, cost and budget governance, OWASP Top 10 security guardrails, rate limiting, and real-time observability.

---

## Key Features

- **Multi-Provider Fallback & Reliability**
  - Seamlessly routes across upstream providers: **Groq**, **OpenRouter**, and **Google Gemini**.
  - Adaptive circuit breakers, exponential backoff with jitter, and automatic failover chains.
- **Cost & Budget Governance**
  - Real-time token usage calculation and per-request cost estimation using `models.json`.
  - Configurable budget caps, in-memory/Redis response caching, and request coalescing.
- **OWASP Security Guardrails & Defense**
  - Pre-execution prompt injection defense, jailbreak detection, and sensitive data (PII/secrets) redaction.
  - Granular API key issuance, revocation, and per-key sliding-window rate limiting.
- **Async Job Queue & DLQ**
  - Background asynchronous task execution with Dead Letter Queue (DLQ) tracking and replay.
- **Live Visualizer & Admin Dashboard**
  - Built-in operational UI for inspecting active routes, latency, circuit breaker states, and token expenditures.

---

## Repository Structure

```text
.
├── app/
│   ├── main.py            # Primary FastAPI app, routing, proxy handlers, and visualizer dashboard
│   ├── reliability.py     # Circuit breakers, multi-provider clients, and fallback logic
│   ├── security.py        # Gateway API keys, sliding-window rate limiting, and token verification
│   ├── guardrails.py      # Prompt injection defenses, secret scrubbing, and content filters
│   ├── cost.py            # Token counting, cost tracking, response cache, and request coalescing
│   ├── jobs.py            # Background job management and Dead Letter Queue (DLQ)
│   └── auth.py            # OAuth authentication and user identity management
├── components/            # UI components and voice-orb visualization primitives
├── docs/                  # Security audits and design documentation
├── scripts/               # Concurrency benchmarks, stress testing, and diagnostic utilities
├── tests/                 # Comprehensive unit, integration, and security test suite
├── models.json            # Model catalog, context limits, and token pricing matrix
├── requirements.txt       # Python dependencies
├── .env.example           # Environment configuration template
└── .gitignore             # Configured ignore rules (excluding secrets, scratch, and binaries)
```

---

## Quick Start

### 1. Prerequisites
- Python 3.10+
- (Optional) Redis for distributed caching and rate limiting

### 2. Installation

```bash
# Clone the repository
git clone <repo-url>
cd "LLM Gateway"

# Create and activate virtual environment
python -m venv venv
# On Windows:
.\venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Environment Setup

Copy `.env.example` to `.env` and fill in your provider API keys:

```bash
cp .env.example .env
```

Key environment variables:
- `GROQ_API_KEY`: Groq Cloud API key
- `OPENROUTER_API_KEY`: OpenRouter API key
- `GEMINI_API_KEY`: Google Gemini API key
- `ADMIN_SECRET_KEY`: Administrative token for managing gateway keys

### 4. Running the Gateway

Start the development server with Uvicorn:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- **Interactive API Docs (Swagger):** `http://localhost:8000/docs`
- **Gateway Dashboard & Visualizer:** `http://localhost:8000/`

---

## Testing

Run the test suite with `pytest`:

```bash
pytest tests/ -v
```

Stress tests and concurrency benchmarks are available under `scripts/`:

```bash
python scripts/benchmark_100_requests.py
```

---

## AI Ingestion & Analysis Notes

When sharing this repository with LLM assistants (e.g., ChatGPT, Claude, Cursor):
- Do **not** rely on pasting the raw GitHub repository URL in web-browsing chat, as web scrapers cannot crawl subfolders.
- Use **Claude Projects** with GitHub integration, or use **Repomix** / **Gitingest** (`https://gitingest.com/<org>/<repo>`) to create an AI-optimized context bundle.
- Temporary development scripts and build bundles in `scratch/` are excluded via `.gitignore` to minimize context token usage.
