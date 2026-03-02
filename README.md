# CVerAI Analytics

> Ask questions about your platform in plain English. Get instant, data-backed answers — powered by **DeepSeek V3**, the **Model Context Protocol (MCP)**, and an autonomous agentic reasoning loop.

---

## What this project does

Cver AI helps job seekers apply faster by auto-filling applications directly from WhatsApp and other platforms while tailoring resumes and cover letters to each role.

This project replaces that with a **natural language analytics assistant** that:

1. Receives a plain-English question through a chat interface
2. Autonomously decides which database tools to call and in what order
3. Writes and executes its own SQL queries against the live PostgreSQL database
4. Synthesises a human-readable answer complete with tables and numbers

No hardcoded queries. No dashboards to build. Just ask.

---

## Demo

**"Which job categories have grown the most in the last 30 days?"**
The model autonomously called `get_schema`, `execute_query`, and `get_analytics` — then synthesised a ranked table with growth percentages and trends.

![Job category growth analysis](screenshots/job-growth.png)

---

**"How many users do we have — how many are registered?"**
A single question triggers multiple SQL queries. The model cross-references `users`, checks soft-deletes, and breaks down registration status, roles, and email verification rates.

![User registration statistics](screenshots/user-registrations.png)

---

**"Give me the full details for this registered user." (email inserted)**
The model joins `users`, `user_profiles`, `resumes`, `work_experiences`, and `user_skills` in one response — no manual joins needed.

![User account lookup](screenshots/user-lookup.png)

---

**"Can you check if anyone used the WhatsApp feature in the last week?"**
`search_whatsapp` finds the relevant conversation thread and the model summarises who interacted, when, and what they discussed.

![WhatsApp bot activity analysis](screenshots/whatsapp-analysis.png)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          User (Browser)                             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  http://localhost:3000
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Open WebUI  (port 3000)                        │
│         Chat interface — renders markdown, tables, code             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  POST /v1/chat/completions  (OpenAI format)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DeepSeek Bridge  (port 8001)                     │
│   FastAPI — exposes an OpenAI-compatible API to Open WebUI          │
│   Runs an autonomous agentic tool-calling loop (≤15 iterations)     │
└───────────────┬─────────────────────────────────┬───────────────────┘
                │  MCP streamable-http (tools)     │  DeepSeek API
                ▼                                  ▼
┌───────────────────────────┐          ┌──────────────────────────────┐
│   MCP Server  (port 8002) │          │        DeepSeek V3           │
│   FastMCP + asyncpg       │          │   (cloud — api.deepseek.com) │
│   Exposes 4 MCP tools     │          └──────────────────────────────┘
└───────────────┬───────────┘
                │  asyncpg / PostgreSQL
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   PostgreSQL — CVerAI database                      │
│   users · job_posts · job_applications · chat_history               │
│   resumes · work_experiences · skills · user_skills · …            │
└─────────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

- **MCP streamable-http transport** — chosen over SSE because it works cleanly across Docker container boundaries without host-header validation issues.
- **OpenAI-compatible bridge** — Open WebUI (and any OpenAI-compatible client) can point at the bridge with no modification. The bridge translates to DeepSeek's API, which is OpenAI-compatible itself.
- **Agentic loop inside the streaming generator** — the MCP session context is held open for the entire duration of a streamed response, preventing mid-stream disconnects.

---

## How the agentic loop works

```
User message
    │
    ▼
DeepSeek V3 receives message + 4 MCP tool definitions
    │
    ├─► Decides: "I need to understand the schema first"
    │       └─► calls get_schema()
    │               returns table list with column names and row counts
    │
    ├─► Decides: "Now I can write the SQL"
    │       └─► calls execute_query("SELECT job_function, COUNT(*) ...")
    │               returns rows as JSON
    │
    ├─► Decides: "Let me also check recent WhatsApp complaints"
    │       └─► calls search_whatsapp(keyword="error", start_date="...")
    │               returns matching messages
    │
    └─► Has enough data → produces final markdown answer with tables
```

DeepSeek autonomously decides which tools to call, in which order, and how many times — up to 15 iterations. No query logic is hardcoded in the application. This means it can answer questions that were never anticipated at build time.

---

## MCP Tools

The MCP server exposes four tools that the model can call:

| Tool | What it does |
|------|-------------|
| `get_schema` | Introspects all tables or one specific table — returns column names, types, and live row counts |
| `execute_query` | Runs any read-only `SELECT` / `WITH` query. SQL injection guards and a row-limit cap are enforced server-side |
| `search_whatsapp` | Searches `chat_history` by keyword, phone number, user ID, date range, or message role (`user`/`assistant`) |
| `get_analytics` | Eight pre-built metrics: job growth, user registrations, application funnel, top job functions, top skills, new users without CVs, WhatsApp volume, and a side-by-side job vs. user daily series |

---

## Sample questions you can ask

**Job intelligence**
- "Which job categories have seen the highest growth in postings over the last 30 days?"
- "What is the average salary offered for Backend Engineer roles?"
- "Identify duplicate job listings posted by the same company this week."
- "Give me a daily breakdown of new job postings vs. new user registrations for the past 14 days."

**User analytics**
- "List all users who registered in the last 48 hours but haven't uploaded a CV yet."
- "How many Senior-level candidates are currently marked as Looking for Work?"
- "What is the conversion rate from registered user to job applicant this month?"
- "Find users who applied for a job today — what was their most recent job title?"

**WhatsApp insights**
- "Summarise the main reasons users reached out via WhatsApp over the weekend."
- "Are there recurring keywords in today's WhatsApp messages that suggest a bug?"
- "Find the WhatsApp conversation for user ID [UUID] and summarise it."
- "Cross-reference users who messaged on WhatsApp today with their last job application."

**Cross-domain**
- "Which skills are most common among our users but have the fewest matching job openings?"
- "Give me the 5 most active recruiters by posting volume."
- "Identify companies that posted more than 5 jobs this week but received no applications."

---

## Quick start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- A [DeepSeek API key](https://platform.deepseek.com/)
- A PostgreSQL database (direct connection or via SSH tunnel)

### 1. Clone and configure

```bash
git clone https://github.com/michaelodafe/CverAI_Analytics_MCP.git
cd cverai-mcp-analytics
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# DeepSeek
DEEPSEEK_API_KEY=sk-...
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat

# Database — direct connection
DATABASE_URL=postgresql://user:password@host:5432/dbname
```

### 2. SSH tunnel (if your database is behind a jump host)

If your database is IP-restricted and accessible only through a jump server, open a tunnel first:

```bash
chmod +x scripts/tunnel.sh
SSH_HOST=<jump-host-ip> \
DB_HOST=<db-private-ip> \
./scripts/tunnel.sh
```

Then update `.env` to route through the tunnel:

```env
# Mac / Windows
DATABASE_URL=postgresql://user:password@host.docker.internal:5433/dbname

# Linux
DATABASE_URL=postgresql://user:password@172.17.0.1:5433/dbname
```

### 3. Start the stack

```bash
docker compose up --build
```

Services start in dependency order:
1. **MCP Server** — connects to the database and starts the MCP endpoint
2. **DeepSeek Bridge** — waits for MCP Server to be healthy, then starts the API
3. **Open WebUI** — waits for the Bridge to be healthy, then opens the chat UI

### 4. Open the chat interface

Visit **http://localhost:3000**

- Sign up for a local account (credentials are stored only in the Docker volume)
- Select **deepseek-chat** from the model dropdown
- Start asking questions

---

## Development (without Docker)

### MCP Server

```bash
cd mcp-server
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
DATABASE_URL="postgresql://user:password@localhost:5432/dbname" python server.py
# MCP endpoint available at http://localhost:8000/mcp
```

### DeepSeek Bridge

```bash
cd claude-bridge
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
DEEPSEEK_API_KEY="sk-..." \
MCP_SERVER_URL="http://localhost:8000/mcp" \
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

### Test with curl

```bash
# Health check
curl http://localhost:8001/health

# Non-streaming query
curl -s -X POST http://localhost:8001/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek-chat",
    "messages": [{"role": "user", "content": "How many users registered this week?"}]
  }' | jq '.choices[0].message.content'

# Streaming query
curl -s -N -X POST http://localhost:8001/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek-chat",
    "stream": true,
    "messages": [{"role": "user", "content": "Which job categories grew most in the last 30 days?"}]
  }'
```

---

## Project structure

```
cverai-mcp-analytics/
├── mcp-server/
│   ├── server.py          # FastMCP server — 4 tools backed by asyncpg
│   ├── requirements.txt
│   └── Dockerfile
├── claude-bridge/
│   ├── main.py            # FastAPI bridge + agentic loop (streaming & non-streaming)
│   ├── requirements.txt
│   └── Dockerfile
├── scripts/
│   └── tunnel.sh          # SSH tunnel helper for IP-restricted databases
├── screenshots/           # UI screenshots for this README
├── docker-compose.yml     # Three-service stack with health-check ordering
├── .env.example           # Template — copy to .env and fill in credentials
└── README.md
```

---

## Technologies

| Layer | Technology |
|-------|-----------|
| Chat UI | [Open WebUI](https://github.com/open-webui/open-webui) |
| API Bridge | [FastAPI](https://fastapi.tiangolo.com/) + Uvicorn |
| AI Model | DeepSeek V3 (`deepseek-chat`) via DeepSeek API |
| Tool Protocol | [Model Context Protocol](https://modelcontextprotocol.io/) — streamable-http transport |
| MCP Framework | [FastMCP](https://github.com/jlowin/fastmcp) |
| Database Client | [asyncpg](https://github.com/MagicStack/asyncpg) (async PostgreSQL) |
| Containerisation | Docker Compose |

---

## Security

- **Read-only database access** — `execute_query` only permits `SELECT` and `WITH` queries. Blocked keywords (`DROP`, `DELETE`, `UPDATE`, `INSERT`, `TRUNCATE`, `ALTER`, `CREATE`, `GRANT`) are rejected server-side before any query reaches the database.
- **No credentials in code** — all secrets are passed via environment variables and read at runtime. The `.env` file is git-ignored.
- **Row cap** — `execute_query` enforces a configurable row limit (default 500, max 2000) to prevent runaway queries.
- **MCP boundary** — the AI model can only interact with the database through the four defined MCP tools. It has no direct database access.

---

## Adapting this for your own database

1. Replace the `DATABASE_URL` in `.env` with your connection string.
2. Update the system prompt in `claude-bridge/main.py` to describe your schema.
3. Optionally extend `mcp-server/server.py` with domain-specific pre-built metrics in `get_analytics`.
4. Run `docker compose up --build` — the model will discover your schema automatically via `get_schema`.
