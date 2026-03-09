<div align="center">
  <img src="https://raw.githubusercontent.com/debba/tabularis/main/public/logo-sm.png" width="120" height="120" />
</div>

# tabularis-hackernews-plugin

<p align="center">

![](https://img.shields.io/github/release/debba/tabularis-hackernews-plugin.svg?style=flat)
![](https://img.shields.io/github/downloads/debba/tabularis-hackernews-plugin/total.svg?style=flat)
![Build & Release](https://github.com/debba/tabularis-hackernews-plugin/workflows/Release/badge.svg)
[![Discord](https://img.shields.io/discord/1470772941296894128?color=5865F2&logo=discord&logoColor=white)](https://discord.gg/YrZPHAwMSG)

</p>

A Hacker News plugin for [Tabularis](https://github.com/debba/tabularis), the lightweight database management tool.

This plugin turns the **HN public API into a queryable SQL database**. Stories, comments and jobs become real tables. Full SQL support via an in-memory DuckDB engine — `JOIN` across tables, `GROUP BY`, window functions, CTEs.

**No authentication required** — uses the public Firebase API. Requires `pip install requests duckdb`.

**Discord** - [Join our discord server](https://discord.gg/YrZPHAwMSG) and chat with the maintainers.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
  - [Automatic (via Tabularis)](#automatic-via-tabularis)
  - [Manual Installation](#manual-installation)
- [How It Works](#how-it-works)
- [Settings](#settings)
- [Example Queries](#example-queries)
- [Supported Operations](#supported-operations)
- [Development](#development)
- [Changelog](#changelog)
- [License](#license)

## Features

- **Zero auth** — reads the public HN Firebase API, no API key needed.
- **Real SQL** — DuckDB handles all query execution: `JOIN`, `GROUP BY`, subqueries, CTEs, window functions.
- **Auto-refresh** — configurable TTL to reload data periodically without restarting the connection.
- **4 feed types** — `top`, `new`, `best`, `ask`, `show`, `jobs`.
- **Comments table** — optionally load top-level comments per story and JOIN against them.
- **Schema Inspection** — browse tables and columns in the sidebar explorer.
- **ER Diagram** — visualize the stories ↔ comments relationship.
- **Cross-platform** — works on Linux, macOS, and Windows wherever Python 3.10+ is installed.

## Installation

### Automatic (via Tabularis)

Open **Settings → Available Plugins** in Tabularis and install **Hacker News** from the plugin registry.

### Manual Installation

1. Download the latest `hackernews-plugin.zip` from the [Releases page](https://github.com/debba/tabularis-hackernews-plugin/releases).
2. Extract the archive.
3. Install dependencies:

```bash
pip install requests duckdb
```

4. Copy `main.py` and `manifest.json` into the Tabularis plugins directory:

| OS | Plugins Directory |
|---|---|
| **Linux** | `~/.local/share/tabularis/plugins/hackernews/` |
| **macOS** | `~/Library/Application Support/com.debba.tabularis/plugins/hackernews/` |
| **Windows** | `%APPDATA%\com.debba.tabularis\plugins\hackernews\` |

5. Make the plugin executable (Linux/macOS):

```bash
chmod +x ~/.local/share/tabularis/plugins/hackernews/main.py
```

6. Restart Tabularis.

Python 3.10 or newer must be available as `python3` in your `PATH`.

## How It Works

The plugin is a single Python script that communicates with Tabularis through **JSON-RPC 2.0 over stdio**:

1. Tabularis spawns `main.py` as a child process.
2. Requests are sent as newline-delimited JSON-RPC messages to the plugin's `stdin`.
3. Responses are written to `stdout` in the same format.

On the first `execute_query`, the plugin fetches story IDs from the selected feed, loads each item from the HN API into an **in-memory DuckDB database**, and keeps it alive for the session. DuckDB handles all query execution.

If `cache_ttl_minutes` is set, the snapshot is automatically rebuilt after that interval — no need to restart the connection.

All debug output is written to `stderr` and appears in Tabularis's log viewer — `stdout` is reserved exclusively for JSON-RPC responses.

## Settings

Configure via **Settings → gear icon** next to the Hacker News driver.

| Key | Label | Type | Default | Description |
|---|---|---|---|---|
| `story_type` | Feed | select | `top` | Which HN feed: `top`, `new`, `best`, `ask`, `show`, `jobs` |
| `max_items` | Max Stories | number | `30` | How many stories to fetch (max 500) |
| `include_comments` | Include Comments | boolean | `false` | Also load top-level comments, enables the `comments` table |
| `timeout` | Timeout (s) | number | `10` | HTTP timeout in seconds for HN API requests |
| `cache_ttl_minutes` | Cache TTL (min) | number | `0` | Auto-refresh data after N minutes. `0` = disabled |

## Example Queries

```sql
-- Top 10 stories by score
SELECT title, score, by, url
FROM stories
ORDER BY score DESC
LIMIT 10;
```

```sql
-- Who posts the most? And how well do they do?
SELECT by,
       COUNT(*)   AS posts,
       AVG(score) AS avg_score,
       MAX(score) AS best_score
FROM stories
GROUP BY by
ORDER BY posts DESC
LIMIT 10;
```

```sql
-- Stories linking to GitHub
SELECT title, url, score
FROM stories
WHERE url LIKE '%github.com%'
ORDER BY score DESC;
```

```sql
-- Comment activity per story (requires Include Comments = true)
SELECT s.title,
       s.score,
       COUNT(c.id)    AS loaded_comments,
       s.descendants  AS total_comments
FROM stories s
LEFT JOIN comments c ON c.story_id = s.id
GROUP BY s.id, s.title, s.score, s.descendants
ORDER BY loaded_comments DESC;
```

```sql
-- Rank stories by score within each type using a window function
SELECT title, type, score,
       RANK() OVER (PARTITION BY type ORDER BY score DESC) AS rank_in_type
FROM stories
ORDER BY type, rank_in_type;
```

```sql
-- Stories from the last 24 hours with at least 10 points
SELECT title, score, by,
       epoch_ms(time * 1000)::TIMESTAMP AS posted_at
FROM stories
WHERE time > epoch(now()) - 86400
  AND score >= 10
ORDER BY score DESC;
```

## Supported Operations

| Method | Description |
|---|---|
| `test_connection` | Verify the HN API is reachable |
| `get_databases` | Returns `["hackernews"]` |
| `get_tables` | Lists `stories` (and `comments` if enabled) |
| `get_columns` | Get column schema for a table |
| `execute_query` | Execute SQL with pagination support |
| `get_schema_snapshot` | Full schema dump in one call (used for ER diagrams) |
| `get_all_columns_batch` | All columns for all tables in one call |
| `get_all_foreign_keys_batch` | Returns empty (no enforced FK constraints) |
| `insert_record` / `update_record` / `delete_record` | Returns error — read-only source |

## Development

### Testing the Plugin

Test the plugin directly from your shell without opening Tabularis:

```bash
chmod +x main.py

# initialize
echo '{"jsonrpc":"2.0","method":"initialize","params":{"settings":{"story_type":"top","max_items":5,"include_comments":false,"timeout":10,"cache_ttl_minutes":0}},"id":1}' \
  | python3 main.py

# execute a query (fetches live data)
echo '{"jsonrpc":"2.0","method":"execute_query","params":{"params":{"driver":"hackernews","database":"hackernews"},"query":"SELECT title, score FROM stories ORDER BY score DESC LIMIT 3","page":1,"page_size":100},"id":2}' \
  | python3 main.py
```

### Install Locally

A convenience script is provided to copy the plugin directly into your Tabularis plugins folder:

```bash
./sync.sh
```

### Tech Stack

- **Language:** Python 3.10+
- **Query engine:** DuckDB (in-memory)
- **Data source:** [HN Firebase API](https://github.com/HackerNews/API)
- **Protocol:** JSON-RPC 2.0 over stdio

## [Changelog](./CHANGELOG.md)

## License

Apache License 2.0
