"""
CVerAI Analytics MCP Server
============================
Exposes the CVerAI PostgreSQL database as MCP tools.
Claude uses these tools to answer analytical questions about jobs, users,
and WhatsApp conversations.

Supports both:
  - Direct Postgres connection (DATABASE_URL)
  - SSH-tunnelled connection (SSH_HOST + SSH_USER + SSH_KEY_PATH)
"""

import asyncio
import json
import os
import re
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

# ── Database Configuration ────────────────────────────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cverai:password@localhost:5432/cverai")

# SSH tunnel is handled externally (docker-compose sidecar or scripts/tunnel.sh)
# The MCP server simply connects to DATABASE_URL.

# ── Connection Pool ───────────────────────────────────────────────────────────

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


async def run_query(sql: str, params=None) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *(params or []))
        return [dict(row) for row in rows]


# ── MCP Server ────────────────────────────────────────────────────────────────

mcp = FastMCP(
    name="CVerAI Analytics",
    host="0.0.0.0",   # Allow inter-container connections (disables localhost-only DNS rebinding protection)
    instructions="""
You are connected to the CVerAI platform database — a job-matching and career
AI platform. The database contains:

• users / user_profiles / user_contacts — registered candidates
• job_posts (24 000+ listings) — scraped job openings
• job_applications — user applications to jobs
• job_recommendations — AI-matched job suggestions
• chat_history — WhatsApp conversations (role: user|assistant)
• resumes / work_experiences / skills / user_skills — candidate profiles
• user_analytics / user_engagement_metrics — aggregated platform metrics

Workflow for answering questions:
1. Call `get_schema` if you need to understand table structure.
2. Call `execute_query` with a precise SELECT / WITH statement.
3. For WhatsApp-specific searches use `search_whatsapp`.
4. Chain multiple tool calls to cross-reference data across tables.
5. Always include concrete numbers in your final answer.
""",
)

# ── Tool 1: Schema Discovery ──────────────────────────────────────────────────


@mcp.tool()
async def get_schema(table_name: Optional[str] = None) -> str:
    """
    Returns database schema information.

    Args:
        table_name: If provided, returns detailed columns for that specific
                    table. If omitted, returns all public tables with row
                    counts and column summaries.

    Returns:
        JSON string with table structure and row counts.
    """
    if table_name:
        sql = """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
              AND c.table_name   = $1
            ORDER BY c.ordinal_position;
        """
        rows = await run_query(sql, [table_name])
        if not rows:
            return json.dumps({"error": f"Table '{table_name}' not found."})

        try:
            count_rows = await run_query(f'SELECT COUNT(*) AS cnt FROM "{table_name}"')
            count = count_rows[0]["cnt"]
        except Exception:
            count = "unknown"

        return json.dumps(
            {"table": table_name, "row_count": count, "columns": rows},
            indent=2,
            default=str,
        )

    # All tables summary
    sql = """
        SELECT
            t.table_name,
            array_agg(c.column_name || ':' || c.data_type ORDER BY c.ordinal_position) AS columns
        FROM information_schema.tables t
        JOIN information_schema.columns c
          ON t.table_name   = c.table_name
         AND t.table_schema = c.table_schema
        WHERE t.table_schema = 'public'
          AND t.table_name NOT IN ('pg_stat_statements', 'pg_stat_statements_info')
        GROUP BY t.table_name
        ORDER BY t.table_name;
    """
    rows = await run_query(sql)

    # Attach row counts
    result = []
    pool = await get_pool()
    async with pool.acquire() as conn:
        for row in rows:
            try:
                cnt = await conn.fetchval(f'SELECT COUNT(*) FROM "{row["table_name"]}"')
            except Exception:
                cnt = -1
            result.append(
                {
                    "table": row["table_name"],
                    "row_count": cnt,
                    "columns": row["columns"],
                }
            )

    return json.dumps(result, indent=2, default=str)


# ── Tool 2: Generic Query Executor ───────────────────────────────────────────


@mcp.tool()
async def execute_query(sql: str, limit: int = 500) -> str:
    """
    Executes a read-only PostgreSQL SELECT (or WITH…SELECT) query.

    Use this tool to answer any analytical question about:
    - Job postings, applications, recommendations
    - User registrations, profiles, skills
    - Conversion rates, growth trends, salary data
    - Duplicate detections, active recruiters, engagement metrics

    Key tables to know:
    - job_posts(id, title, company_name, location, salary_info, posted_at,
                job_function, employment_type, expire_at)
    - job_applications(id, user_id, job_id, status, applied_at, created_at)
    - users(id, email, role, account_status, created_at, last_login_at)
    - user_profiles(user_id, first_name, last_name)
    - resumes(user_id, job_level, job_type, created_at)
    - work_experiences(user_id, job_title, company_name, is_current)
    - skills(id, name) + user_skills(user_id, skill_id)
    - user_analytics(user_id, total_applications, total_resumes)
    - chat_history(user_id, phone_number, role, message, created_at)

    Args:
        sql:   A valid PostgreSQL SELECT or WITH query. Always alias
               subqueries. Cast UUIDs explicitly if needed.
        limit: Cap on rows returned (default 500, max 2000).

    Returns:
        JSON array of result rows, or {"error": "..."} on failure.
    """
    # Safety: allow only SELECT / WITH statements
    normalised = sql.strip().upper()
    if not normalised.startswith(("SELECT", "WITH", "EXPLAIN")):
        return json.dumps({"error": "Only SELECT / WITH / EXPLAIN queries are permitted."})

    blocked = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", "CREATE", "GRANT"]
    for kw in blocked:
        if re.search(rf"\b{kw}\b", normalised):
            return json.dumps({"error": f"Forbidden keyword detected: {kw}"})

    # Enforce limit
    limit = min(int(limit), 2000)
    if "LIMIT" not in normalised:
        sql = sql.rstrip(";") + f" LIMIT {limit}"

    try:
        rows = await run_query(sql)
        return json.dumps(
            {"row_count": len(rows), "rows": rows},
            indent=2,
            default=str,
        )
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ── Tool 3: WhatsApp Search ───────────────────────────────────────────────────


@mcp.tool()
async def search_whatsapp(
    keyword: Optional[str] = None,
    phone_number: Optional[str] = None,
    user_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    role: Optional[str] = None,
    limit: int = 100,
) -> str:
    """
    Searches the chat_history table which stores WhatsApp conversations.

    The table schema is:
        chat_history(
            id, user_id, phone_number, role (user|assistant),
            message, message_type, metadata, created_at
        )

    Use this to:
    - Summarise what users said over a date range
    - Find messages mentioning 'error', 'upload failed', bugs, etc.
    - Retrieve a specific user's conversation thread
    - Identify recurring support themes (e.g. login issues, CV upload)

    Args:
        keyword:      Word or phrase to search for (case-insensitive).
        phone_number: Filter by WhatsApp phone number.
        user_id:      Filter by platform user UUID.
        start_date:   Lower bound (YYYY-MM-DD).
        end_date:     Upper bound (YYYY-MM-DD).
        role:         'user' for incoming messages, 'assistant' for bot replies.
        limit:        Max messages returned (default 100).

    Returns:
        JSON with matching messages plus a summary count.
    """
    conditions = ["deleted_at IS NULL"]
    params: list = []
    idx = 1

    if keyword:
        conditions.append(f"LOWER(message) LIKE LOWER(${ idx })")
        params.append(f"%{keyword}%")
        idx += 1

    if phone_number:
        conditions.append(f"phone_number = ${ idx }")
        params.append(phone_number)
        idx += 1

    if user_id:
        conditions.append(f"user_id = ${ idx }::uuid")
        params.append(user_id)
        idx += 1

    if start_date:
        conditions.append(f"created_at >= ${ idx }::timestamptz")
        params.append(start_date)
        idx += 1

    if end_date:
        conditions.append(f"created_at <= (${ idx }::date + INTERVAL '1 day')::timestamptz")
        params.append(end_date)
        idx += 1

    if role:
        conditions.append(f"role = ${ idx }")
        params.append(role)
        idx += 1

    where = " AND ".join(conditions)
    sql = f"""
        SELECT id, user_id, phone_number, role, message, message_type, created_at
        FROM chat_history
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT {min(int(limit), 500)}
    """

    try:
        rows = await run_query(sql, params)
        return json.dumps(
            {
                "matched": len(rows),
                "filters": {
                    "keyword": keyword,
                    "phone_number": phone_number,
                    "user_id": user_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "role": role,
                },
                "messages": rows,
            },
            indent=2,
            default=str,
        )
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ── Tool 4: Pre-built Analytics ───────────────────────────────────────────────


@mcp.tool()
async def get_analytics(metric: str, days: int = 30) -> str:
    """
    Runs a pre-built analytical query for common platform metrics.

    Available metrics:
      - "job_growth"           : Job posting counts per day for the last N days
      - "user_registrations"   : New user signups per day for the last N days
      - "application_funnel"   : Registered → Applied conversion stats
      - "top_job_functions"    : Most common job categories by posting volume
      - "top_skills"           : Most listed skills among registered users
      - "active_users_48h"     : Users registered in last 48 hours without a resume
      - "whatsapp_daily"       : WhatsApp message volume by day
      - "job_vs_users_daily"   : Side-by-side job postings vs user sign-ups per day

    Args:
        metric: One of the metric names listed above.
        days:   Lookback window in days (default 30).

    Returns:
        JSON with labelled result rows.
    """
    queries: dict[str, str] = {
        "job_growth": f"""
            SELECT
                DATE(posted_at)        AS date,
                job_function           AS category,
                COUNT(*)               AS postings
            FROM job_posts
            WHERE posted_at >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY DATE(posted_at), job_function
            ORDER BY date DESC, postings DESC
            LIMIT 200
        """,
        "user_registrations": f"""
            SELECT
                DATE(created_at) AS date,
                COUNT(*)         AS new_users
            FROM users
            WHERE created_at >= NOW() - INTERVAL '{days} days'
            GROUP BY DATE(created_at)
            ORDER BY date DESC
        """,
        "application_funnel": f"""
            SELECT
                COUNT(DISTINCT u.id)                                       AS total_users,
                COUNT(DISTINCT ja.user_id)                                 AS applied_at_least_once,
                ROUND(
                    100.0 * COUNT(DISTINCT ja.user_id) /
                    NULLIF(COUNT(DISTINCT u.id), 0), 2
                )                                                          AS conversion_pct
            FROM users u
            LEFT JOIN job_applications ja ON ja.user_id = u.id
            WHERE u.created_at >= NOW() - INTERVAL '{days} days'
        """,
        "top_job_functions": f"""
            SELECT
                COALESCE(job_function, 'Unknown') AS job_function,
                COUNT(*)                           AS total_posts,
                COUNT(CASE WHEN posted_at >= CURRENT_DATE - 7 THEN 1 END) AS posts_last_7d
            FROM job_posts
            WHERE posted_at >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY job_function
            ORDER BY total_posts DESC
            LIMIT 20
        """,
        "top_skills": """
            SELECT
                s.name          AS skill,
                COUNT(us.id)    AS user_count
            FROM skills s
            JOIN user_skills us ON us.skill_id = s.id
            WHERE us.deleted_at IS NULL
            GROUP BY s.name
            ORDER BY user_count DESC
            LIMIT 30
        """,
        "active_users_48h": """
            SELECT
                u.id,
                u.email,
                u.created_at,
                COUNT(r.id) AS resume_count
            FROM users u
            LEFT JOIN resumes r ON r.user_id = u.id AND r.deleted_at IS NULL
            WHERE u.created_at >= NOW() - INTERVAL '48 hours'
            GROUP BY u.id, u.email, u.created_at
            HAVING COUNT(r.id) = 0
            ORDER BY u.created_at DESC
        """,
        "whatsapp_daily": f"""
            SELECT
                DATE(created_at) AS date,
                role,
                COUNT(*)         AS messages
            FROM chat_history
            WHERE created_at >= NOW() - INTERVAL '{days} days'
              AND deleted_at IS NULL
            GROUP BY DATE(created_at), role
            ORDER BY date DESC
        """,
        "job_vs_users_daily": f"""
            SELECT
                dates.date,
                COALESCE(jp.postings, 0)  AS job_postings,
                COALESCE(ur.new_users, 0) AS new_users
            FROM (
                SELECT generate_series(
                    CURRENT_DATE - INTERVAL '{days} days',
                    CURRENT_DATE,
                    '1 day'
                )::date AS date
            ) dates
            LEFT JOIN (
                SELECT DATE(posted_at) AS date, COUNT(*) AS postings
                FROM job_posts
                GROUP BY DATE(posted_at)
            ) jp USING (date)
            LEFT JOIN (
                SELECT DATE(created_at) AS date, COUNT(*) AS new_users
                FROM users
                GROUP BY DATE(created_at)
            ) ur USING (date)
            ORDER BY date DESC
        """,
    }

    if metric not in queries:
        return json.dumps(
            {
                "error": f"Unknown metric '{metric}'.",
                "available": list(queries.keys()),
            }
        )

    try:
        rows = await run_query(queries[metric])
        return json.dumps(
            {"metric": metric, "days": days, "row_count": len(rows), "data": rows},
            indent=2,
            default=str,
        )
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # host="0.0.0.0" is set on the FastMCP instance above, so mcp.run() will
    # bind to all interfaces and disable the localhost-only DNS rebinding protection.
    mcp.run(transport="streamable-http")
