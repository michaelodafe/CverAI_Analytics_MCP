"""
CVerAI DeepSeek Bridge
=======================
OpenAI-compatible HTTP API between Open WebUI and DeepSeek V3.

Flow:
    Open WebUI  ──POST /v1/chat/completions──►  DeepSeek Bridge
    DeepSeek Bridge ──MCP streamable-http ───►  MCP Server (/mcp endpoint)
    DeepSeek Bridge ──DeepSeek API────────────► DeepSeek V3 (deepseek-chat)
"""

import asyncio
import json
import os
import time
import uuid
from typing import AsyncIterator

import openai
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

DEEPSEEK_API_KEY  = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL    = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
MCP_SERVER_URL    = os.getenv("MCP_SERVER_URL", "http://mcp-server:8000/mcp")
MAX_TOKENS        = int(os.getenv("MAX_TOKENS", "8192"))
MAX_ITERATIONS    = int(os.getenv("MAX_ITERATIONS", "15"))

SYSTEM_PROMPT = """You are the CVerAI Analytics Assistant — an intelligent data analyst
with direct access to the CVerAI platform database through MCP tools.

CVerAI is a job-matching AI startup with ~1 900 registered users, 24 000+ job
postings, and an active WhatsApp bot for candidate interactions.

## Your capabilities
- Query job postings, user profiles, applications, and recommendations
- Analyse WhatsApp conversation logs (chat_history table)
- Compute growth trends, conversion rates, salary statistics
- Cross-reference data across multiple tables in a single answer
- Detect anomalies like duplicate postings or bug-related support messages

## Key tables
- job_posts(id, title, company_name, location, salary_info JSONB, posted_at,
            job_function, employment_type, expire_at)
- job_applications(id, user_id, job_id, status, applied_at)
- users(id, email, role, account_status, created_at, last_login_at)
- user_profiles(user_id, first_name, last_name)
- resumes(user_id, job_level, job_type, created_at)
- work_experiences(user_id, job_title, company_name, is_current)
- skills(id, name) + user_skills(user_id, skill_id)
- chat_history(id, user_id, phone_number, role, message, created_at)
- job_recommendations(user_id, job_id, match_score, status)

## How to answer
1. Call get_schema to orient yourself when table structure is unclear.
2. Use execute_query with precise, efficient SQL for data retrieval.
3. Use search_whatsapp for conversation thread lookups and keyword mining.
4. Use get_analytics for common pre-built metrics (job growth, funnels, etc.).
5. Chain tool calls as needed — you can call tools multiple times.
6. Always end with a concise, human-readable answer that includes numbers.
7. Format multi-row results as markdown tables.
"""

# ── OpenAI client (DeepSeek is OpenAI-compatible) ─────────────────────────────

ai_client = openai.AsyncOpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url=DEEPSEEK_BASE_URL,
)

# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="CVerAI DeepSeek Bridge", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── MCP ↔ OpenAI Tool Format ─────────────────────────────────────────────────


async def fetch_tools_openai(session: ClientSession) -> list[dict]:
    """Fetch MCP tools and convert to OpenAI function-calling format."""
    resp = await session.list_tools()
    tools = []
    for t in resp.tools:
        schema = t.inputSchema or {"type": "object", "properties": {}}
        tools.append({
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description or "",
                "parameters": schema,
            },
        })
    return tools


async def call_tool(session: ClientSession, name: str, arguments: str | dict) -> str:
    """Call an MCP tool and return its text result."""
    if isinstance(arguments, str):
        try:
            args = json.loads(arguments)
        except json.JSONDecodeError:
            args = {}
    else:
        args = arguments

    result = await session.call_tool(name, args)
    if result.content:
        for block in result.content:
            if hasattr(block, "text"):
                return block.text
    return json.dumps({"error": "Tool returned no content."})


# ── Agentic Loop (non-streaming) ──────────────────────────────────────────────


async def run_agentic_loop(
    session: ClientSession,
    messages: list[dict],
    tools: list[dict],
) -> str:
    current = [{"role": "system", "content": SYSTEM_PROMPT}] + [
        m for m in messages if m["role"] in ("user", "assistant")
    ]

    for _ in range(MAX_ITERATIONS):
        response = await ai_client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            max_tokens=MAX_TOKENS,
            messages=current,
            tools=tools or None,
            tool_choice="auto" if tools else None,
        )

        choice = response.choices[0]

        if choice.finish_reason == "stop":
            return choice.message.content or ""

        if choice.finish_reason == "tool_calls":
            msg = choice.message
            # Record assistant turn with all tool_use blocks
            current.append({
                "role": "assistant",
                "content": msg.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in (msg.tool_calls or [])
                ],
            })

            # Execute every tool call and feed results back
            for tc in (msg.tool_calls or []):
                tool_result = await call_tool(session, tc.function.name, tc.function.arguments)
                current.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tool_result,
                })
        else:
            # Unexpected stop reason — return whatever content we have
            return choice.message.content or "No response generated."

    return "Reached maximum reasoning steps. Please try a more focused question."


# ── Agentic Loop (streaming) ──────────────────────────────────────────────────


async def run_agentic_loop_streaming(
    session: ClientSession,
    messages: list[dict],
    tools: list[dict],
) -> AsyncIterator[str]:
    current = [{"role": "system", "content": SYSTEM_PROMPT}] + [
        m for m in messages if m["role"] in ("user", "assistant")
    ]

    for _ in range(MAX_ITERATIONS):
        stream = await ai_client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            max_tokens=MAX_TOKENS,
            messages=current,
            tools=tools or None,
            tool_choice="auto" if tools else None,
            stream=True,
        )

        full_content = ""
        tool_calls_acc: dict[int, dict] = {}  # index → {id, name, arguments}
        finish_reason = None

        async for chunk in stream:
            if not chunk.choices:
                continue
            choice = chunk.choices[0]
            delta = choice.delta

            # Stream text content
            if delta.content:
                full_content += delta.content
                yield delta.content

            # Accumulate tool call deltas
            if delta.tool_calls:
                for tc_delta in delta.tool_calls:
                    idx = tc_delta.index
                    if idx not in tool_calls_acc:
                        tool_calls_acc[idx] = {"id": "", "name": "", "arguments": ""}
                    if tc_delta.id:
                        tool_calls_acc[idx]["id"] = tc_delta.id
                    if tc_delta.function:
                        if tc_delta.function.name:
                            tool_calls_acc[idx]["name"] += tc_delta.function.name
                        if tc_delta.function.arguments:
                            tool_calls_acc[idx]["arguments"] += tc_delta.function.arguments

            if choice.finish_reason:
                finish_reason = choice.finish_reason

        if finish_reason == "stop":
            return

        if finish_reason == "tool_calls":
            tool_calls = [tool_calls_acc[i] for i in sorted(tool_calls_acc)]

            # Append assistant turn
            current.append({
                "role": "assistant",
                "content": full_content or None,
                "tool_calls": [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {"name": tc["name"], "arguments": tc["arguments"]},
                    }
                    for tc in tool_calls
                ],
            })

            # Execute each tool and feed back results
            for tc in tool_calls:
                yield f"\n\n> *Querying `{tc['name']}`...*\n\n"
                result = await call_tool(session, tc["name"], tc["arguments"])
                current.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result,
                })
        else:
            return

    yield "\n\nReached maximum reasoning steps."


# ── OpenAI Response Helpers ───────────────────────────────────────────────────


def _completion_response(text: str) -> dict:
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": DEEPSEEK_MODEL,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


def _stream_chunk(text: str) -> str:
    chunk = {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": DEEPSEEK_MODEL,
        "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": None}],
    }
    return f"data: {json.dumps(chunk)}\n\n"


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/v1/models")
async def list_models():
    return {
        "object": "list",
        "data": [
            {
                "id": DEEPSEEK_MODEL,
                "object": "model",
                "created": int(time.time()),
                "owned_by": "deepseek",
            }
        ],
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    messages: list[dict] = body.get("messages", [])
    do_stream: bool = body.get("stream", False)

    if not messages:
        raise HTTPException(status_code=400, detail="No messages provided.")

    if do_stream:
        # The MCP session MUST stay alive for the entire duration of the stream.
        # Moving the session lifecycle inside the generator ensures it isn't closed
        # the moment StreamingResponse is returned from this endpoint.
        async def event_stream():
            async with streamablehttp_client(MCP_SERVER_URL) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools = await fetch_tools_openai(session)
                    async for chunk in run_agentic_loop_streaming(session, messages, tools):
                        yield _stream_chunk(chunk)
            yield "data: [DONE]\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Non-streaming: session can be scoped to this await call.
    async with streamablehttp_client(MCP_SERVER_URL) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await fetch_tools_openai(session)
            answer = await run_agentic_loop(session, messages, tools)
    return JSONResponse(_completion_response(answer))


@app.get("/health")
async def health():
    """Lightweight health check — just confirms the bridge is up and config is valid."""
    if not DEEPSEEK_API_KEY:
        return JSONResponse(status_code=503, content={"status": "error", "error": "DEEPSEEK_API_KEY not set"})
    return {
        "status": "ok",
        "model": DEEPSEEK_MODEL,
        "mcp_server": MCP_SERVER_URL,
    }


@app.get("/")
async def root():
    return {
        "service": "CVerAI DeepSeek Bridge",
        "model": DEEPSEEK_MODEL,
        "docs": "/docs",
        "health": "/health",
    }
