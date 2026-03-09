use duckdb::{params, Connection};
use serde_json::{json, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Write};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const BASE: &str = "https://hacker-news.firebaseio.com/v0";

fn endpoint_for(story_type: &str) -> &'static str {
    match story_type {
        "new"  => "newstories",
        "best" => "beststories",
        "ask"  => "askstories",
        "show" => "showstories",
        "jobs" => "jobstories",
        _      => "topstories",
    }
}

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

struct StoryRow {
    id: i64,
    title: Option<String>,
    url: Option<String>,
    score: Option<i32>,
    by: Option<String>,
    time: Option<i64>,
    descendants: Option<i32>,
    story_type: Option<String>,
    text: Option<String>,
}

struct CommentRow {
    id: i64,
    story_id: i64,
    parent: Option<i64>,
    by: Option<String>,
    text: Option<String>,
    time: Option<i64>,
    depth: i32,
}

struct UserRow {
    id: String,
    karma: Option<i32>,
    created: Option<i64>,
    about: Option<String>,
    submitted_count: Option<i32>,
}

struct PollOptionRow {
    id: i64,
    poll_id: i64,
    text: Option<String>,
    score: Option<i32>,
}

struct HnData {
    stories: Vec<StoryRow>,
    comments: Vec<CommentRow>,
    users: Vec<UserRow>,
    poll_options: Vec<PollOptionRow>,
}

// ---------------------------------------------------------------------------
// Schema definitions
// ---------------------------------------------------------------------------

fn col(name: &str, data_type: &str, nullable: bool, pk: bool, comment: &str) -> JsonValue {
    json!({
        "name": name,
        "data_type": data_type,
        "is_nullable": nullable,
        "column_default": null,
        "is_primary_key": pk,
        "is_auto_increment": false,
        "comment": comment,
    })
}

fn stories_columns() -> JsonValue {
    json!([
        col("id",          "BIGINT",  false, true,  "Item ID"),
        col("title",       "VARCHAR", true,  false, "Story title"),
        col("url",         "VARCHAR", true,  false, "Link URL"),
        col("score",       "INTEGER", true,  false, "Upvote score"),
        col("by",          "VARCHAR", true,  false, "Author username"),
        col("time",        "BIGINT",  true,  false, "Unix timestamp"),
        col("descendants", "INTEGER", true,  false, "Total comment count"),
        col("type",        "VARCHAR", true,  false, "Item type (story, job, poll)"),
        col("text",        "VARCHAR", true,  false, "HTML body text (Ask HN, etc.)"),
    ])
}

fn comments_columns() -> JsonValue {
    json!([
        col("id",       "BIGINT",  false, true,  "Comment ID"),
        col("story_id", "BIGINT",  true,  false, "Root story ID"),
        col("parent",   "BIGINT",  true,  false, "Parent item ID (story or comment)"),
        col("by",       "VARCHAR", true,  false, "Author username"),
        col("text",     "VARCHAR", true,  false, "HTML comment body"),
        col("time",     "BIGINT",  true,  false, "Unix timestamp"),
        col("depth",    "INTEGER", true,  false, "Nesting depth (1 = direct reply to story)"),
    ])
}

fn users_columns() -> JsonValue {
    json!([
        col("id",              "VARCHAR", false, true,  "Username"),
        col("karma",           "INTEGER", true,  false, "Karma score"),
        col("created",         "BIGINT",  true,  false, "Account creation Unix timestamp"),
        col("about",           "VARCHAR", true,  false, "HTML self-description"),
        col("submitted_count", "INTEGER", true,  false, "Number of submissions"),
    ])
}

fn poll_options_columns() -> JsonValue {
    json!([
        col("id",      "BIGINT",  false, true,  "Poll option ID"),
        col("poll_id", "BIGINT",  true,  false, "Parent poll story ID"),
        col("text",    "VARCHAR", true,  false, "Option text"),
        col("score",   "INTEGER", true,  false, "Votes for this option"),
    ])
}

fn columns_for(table: &str) -> JsonValue {
    match table {
        "stories"      => stories_columns(),
        "comments"     => comments_columns(),
        "users"        => users_columns(),
        "poll_options" => poll_options_columns(),
        _              => json!([]),
    }
}

fn table_comment(table: &str) -> &'static str {
    match table {
        "stories"      => "HN stories from the selected feed",
        "comments"     => "Comments (nested up to comment_depth)",
        "users"        => "Author profiles (karma, bio, account age)",
        "poll_options" => "Voting options for poll-type stories",
        _              => "",
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct State {
    settings: JsonValue,
    db: Option<Connection>,
    settings_hash: u64,
    built_at: f64,
}

impl State {
    fn new() -> Self {
        State { settings: json!({}), db: None, settings_hash: 0, built_at: 0.0 }
    }

    fn get_u64(&self, key: &str, default: u64) -> u64 {
        self.settings.get(key).and_then(|v| v.as_u64()).unwrap_or(default)
    }

    fn get_bool(&self, key: &str, default: bool) -> bool {
        self.settings.get(key).and_then(|v| v.as_bool()).unwrap_or(default)
    }

    fn active_tables(&self) -> Vec<&'static str> {
        let mut tables = vec!["stories"];
        if self.get_bool("include_comments", false) { tables.push("comments"); }
        if self.get_bool("include_users", false)    { tables.push("users"); }
        if self.get_bool("include_polls", false)    { tables.push("poll_options"); }
        tables
    }
}

fn hash_settings(s: &JsonValue) -> u64 {
    let raw = serde_json::to_string(s).unwrap_or_default();
    let mut h = DefaultHasher::new();
    raw.hash(&mut h);
    h.finish()
}

fn now_secs() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Async HTTP fetching
// ---------------------------------------------------------------------------

async fn fetch_json(client: &reqwest::Client, url: String) -> Option<JsonValue> {
    client.get(&url).send().await.ok()?.json::<JsonValue>().await.ok()
}

async fn fetch_items_parallel(ids: &[i64], client: &reqwest::Client) -> HashMap<i64, JsonValue> {
    let futs: Vec<_> = ids.iter().map(|&id| {
        let client = client.clone();
        let url = format!("{}/item/{}.json", BASE, id);
        async move { fetch_json(&client, url).await.map(|v| (id, v)) }
    }).collect();
    futures::future::join_all(futs).await.into_iter().flatten().collect()
}

async fn fetch_comments_bfs(
    story_kids: &HashMap<i64, Vec<i64>>,
    max_depth: u32,
    max_comments: usize,
    client: &reqwest::Client,
) -> Vec<CommentRow> {
    let per_level_limit: [usize; 3] = [20, 10, 5];

    let mut all_comments: Vec<CommentRow> = Vec::new();
    let mut current: Vec<(i64, i64)> = story_kids
        .iter()
        .flat_map(|(&sid, kids)| kids.iter().take(per_level_limit[0]).map(move |&kid| (sid, kid)))
        .collect();

    for depth in 1..=max_depth {
        if current.is_empty() || all_comments.len() >= max_comments {
            break;
        }
        let remaining = max_comments - all_comments.len();
        current.truncate(remaining);

        let ids: Vec<i64> = current.iter().map(|(_, kid)| *kid).collect();
        let story_map: HashMap<i64, i64> = current.iter().map(|(sid, kid)| (*kid, *sid)).collect();
        let fetched = fetch_items_parallel(&ids, client).await;

        let limit_next = if depth < max_depth { per_level_limit[(depth as usize).min(2)] } else { 0 };
        let mut next: Vec<(i64, i64)> = Vec::new();

        for &kid_id in &ids {
            let item = match fetched.get(&kid_id) { Some(v) => v, None => continue };
            if item.get("deleted").and_then(|v| v.as_bool()).unwrap_or(false) { continue; }
            if item.get("dead").and_then(|v| v.as_bool()).unwrap_or(false)    { continue; }

            let sid = story_map[&kid_id];
            all_comments.push(CommentRow {
                id:       item.get("id").and_then(|v| v.as_i64()).unwrap_or(kid_id),
                story_id: sid,
                parent:   item.get("parent").and_then(|v| v.as_i64()),
                by:       item.get("by").and_then(|v| v.as_str()).map(String::from),
                text:     item.get("text").and_then(|v| v.as_str()).map(String::from),
                time:     item.get("time").and_then(|v| v.as_i64()),
                depth:    depth as i32,
            });

            if depth < max_depth && limit_next > 0 {
                if let Some(kids) = item.get("kids").and_then(|v| v.as_array()) {
                    for kid in kids.iter().take(limit_next) {
                        if let Some(next_id) = kid.as_i64() {
                            next.push((sid, next_id));
                        }
                    }
                }
            }
        }
        current = next;
    }
    all_comments
}

// ---------------------------------------------------------------------------
// Full HN data fetch
// ---------------------------------------------------------------------------

async fn fetch_hn_data(settings: &JsonValue) -> Result<HnData, String> {
    let story_type      = settings.get("story_type").and_then(|v| v.as_str()).unwrap_or("top");
    let max_items       = settings.get("max_items").and_then(|v| v.as_u64()).unwrap_or(30).min(500) as usize;
    let timeout         = settings.get("timeout").and_then(|v| v.as_u64()).unwrap_or(10);
    let include_comments = settings.get("include_comments").and_then(|v| v.as_bool()).unwrap_or(false);
    let comment_depth   = settings.get("comment_depth").and_then(|v| v.as_u64()).unwrap_or(1).max(1).min(3) as u32;
    let max_comments    = settings.get("max_comments").and_then(|v| v.as_u64()).unwrap_or(500).min(5000) as usize;
    let include_users   = settings.get("include_users").and_then(|v| v.as_bool()).unwrap_or(false);
    let include_polls   = settings.get("include_polls").and_then(|v| v.as_bool()).unwrap_or(false);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout))
        .build()
        .map_err(|e| e.to_string())?;

    let endpoint = endpoint_for(story_type);
    eprintln!("[hackernews] fetching {} items from /{}.json …", max_items, endpoint);

    let ids_val: JsonValue = client
        .get(format!("{}/{}.json", BASE, endpoint))
        .send().await.map_err(|e| e.to_string())?
        .json().await.map_err(|e| e.to_string())?;

    let ids: Vec<i64> = ids_val
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_i64()).take(max_items).collect())
        .unwrap_or_default();

    let items_map = fetch_items_parallel(&ids, &client).await;

    let mut stories: Vec<StoryRow>             = Vec::new();
    let mut story_kids: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut poll_stories: Vec<(i64, Vec<i64>)> = Vec::new();
    let mut author_names: HashSet<String>       = HashSet::new();

    for &id in &ids {
        let item = match items_map.get(&id) { Some(v) => v, None => continue };

        let by = item.get("by").and_then(|v| v.as_str()).map(String::from);
        if let Some(ref a) = by { author_names.insert(a.clone()); }

        stories.push(StoryRow {
            id,
            title:       item.get("title").and_then(|v| v.as_str()).map(String::from),
            url:         item.get("url").and_then(|v| v.as_str()).map(String::from),
            score:       item.get("score").and_then(|v| v.as_i64()).map(|v| v as i32),
            by,
            time:        item.get("time").and_then(|v| v.as_i64()),
            descendants: item.get("descendants").and_then(|v| v.as_i64()).map(|v| v as i32),
            story_type:  item.get("type").and_then(|v| v.as_str()).map(String::from),
            text:        item.get("text").and_then(|v| v.as_str()).map(String::from),
        });

        if include_comments {
            if let Some(kids) = item.get("kids").and_then(|v| v.as_array()) {
                let kid_ids: Vec<i64> = kids.iter().filter_map(|v| v.as_i64()).collect();
                if !kid_ids.is_empty() { story_kids.insert(id, kid_ids); }
            }
        }

        if include_polls && item.get("type").and_then(|v| v.as_str()) == Some("poll") {
            if let Some(parts) = item.get("parts").and_then(|v| v.as_array()) {
                let part_ids: Vec<i64> = parts.iter().filter_map(|v| v.as_i64()).collect();
                if !part_ids.is_empty() { poll_stories.push((id, part_ids)); }
            }
        }
    }

    // Comments
    let comments = if include_comments && !story_kids.is_empty() {
        eprintln!("[hackernews] fetching comments (depth={}, max={}) …", comment_depth, max_comments);
        fetch_comments_bfs(&story_kids, comment_depth, max_comments, &client).await
    } else {
        Vec::new()
    };

    // Collect comment authors for users table
    if include_users {
        for c in &comments {
            if let Some(ref a) = c.by { author_names.insert(a.clone()); }
        }
    }

    // Users
    let users = if include_users && !author_names.is_empty() {
        let names: Vec<String> = author_names.into_iter().collect();
        eprintln!("[hackernews] fetching {} user profiles …", names.len());
        let futs: Vec<_> = names.iter().map(|u| {
            let client = client.clone();
            let url = format!("{}/user/{}.json", BASE, u);
            async move { fetch_json(&client, url).await }
        }).collect();
        futures::future::join_all(futs).await
            .into_iter()
            .flatten()
            .filter_map(|u| {
                let id = u.get("id").and_then(|v| v.as_str()).map(String::from)?;
                Some(UserRow {
                    id,
                    karma:           u.get("karma").and_then(|v| v.as_i64()).map(|v| v as i32),
                    created:         u.get("created").and_then(|v| v.as_i64()),
                    about:           u.get("about").and_then(|v| v.as_str()).map(String::from),
                    submitted_count: u.get("submitted").and_then(|v| v.as_array()).map(|a| a.len() as i32),
                })
            })
            .collect()
    } else {
        Vec::new()
    };

    // Poll options
    let poll_options = if include_polls && !poll_stories.is_empty() {
        let all_parts: Vec<(i64, i64)> = poll_stories.iter()
            .flat_map(|(pid, parts)| parts.iter().map(|&p| (*pid, p)))
            .collect();
        let part_ids: Vec<i64> = all_parts.iter().map(|(_, p)| *p).collect();
        eprintln!("[hackernews] fetching {} poll options …", part_ids.len());
        let poll_id_map: HashMap<i64, i64> = all_parts.into_iter().map(|(pid, p)| (p, pid)).collect();
        let fetched = fetch_items_parallel(&part_ids, &client).await;
        part_ids.iter().filter_map(|&p| {
            let item = fetched.get(&p)?;
            if item.get("deleted").and_then(|v| v.as_bool()).unwrap_or(false) { return None; }
            if item.get("dead").and_then(|v| v.as_bool()).unwrap_or(false)    { return None; }
            Some(PollOptionRow {
                id:      p,
                poll_id: *poll_id_map.get(&p)?,
                text:    item.get("text").and_then(|v| v.as_str()).map(String::from),
                score:   item.get("score").and_then(|v| v.as_i64()).map(|v| v as i32),
            })
        }).collect()
    } else {
        Vec::new()
    };

    eprintln!(
        "[hackernews] loaded {} stories{}{}{}", stories.len(),
        if !comments.is_empty()     { format!(", {} comments", comments.len()) }     else { String::new() },
        if !users.is_empty()        { format!(", {} users", users.len()) }           else { String::new() },
        if !poll_options.is_empty() { format!(", {} poll options", poll_options.len()) } else { String::new() },
    );

    Ok(HnData { stories, comments, users, poll_options })
}

// ---------------------------------------------------------------------------
// DuckDB build
// ---------------------------------------------------------------------------

fn build_db(data: &HnData, include_comments: bool, include_users: bool, include_polls: bool) -> Result<Connection, String> {
    let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;

    conn.execute_batch(r#"
        CREATE TABLE stories (
            id          BIGINT,
            title       VARCHAR,
            url         VARCHAR,
            score       INTEGER,
            "by"        VARCHAR,
            time        BIGINT,
            descendants INTEGER,
            type        VARCHAR,
            text        VARCHAR
        )
    "#).map_err(|e| e.to_string())?;
    {
        let mut stmt = conn.prepare("INSERT INTO stories VALUES (?,?,?,?,?,?,?,?,?)")
            .map_err(|e| e.to_string())?;
        for s in &data.stories {
            stmt.execute(params![s.id, s.title, s.url, s.score, s.by, s.time, s.descendants, s.story_type, s.text])
                .map_err(|e| e.to_string())?;
        }
    }

    if include_comments {
        conn.execute_batch(r#"
            CREATE TABLE comments (
                id       BIGINT,
                story_id BIGINT,
                parent   BIGINT,
                "by"     VARCHAR,
                text     VARCHAR,
                time     BIGINT,
                depth    INTEGER
            )
        "#).map_err(|e| e.to_string())?;
        let mut stmt = conn.prepare("INSERT INTO comments VALUES (?,?,?,?,?,?,?)")
            .map_err(|e| e.to_string())?;
        for c in &data.comments {
            stmt.execute(params![c.id, c.story_id, c.parent, c.by, c.text, c.time, c.depth])
                .map_err(|e| e.to_string())?;
        }
    }

    if include_users {
        conn.execute_batch(r#"
            CREATE TABLE users (
                id              VARCHAR,
                karma           INTEGER,
                created         BIGINT,
                about           VARCHAR,
                submitted_count INTEGER
            )
        "#).map_err(|e| e.to_string())?;
        let mut stmt = conn.prepare("INSERT INTO users VALUES (?,?,?,?,?)")
            .map_err(|e| e.to_string())?;
        for u in &data.users {
            stmt.execute(params![u.id, u.karma, u.created, u.about, u.submitted_count])
                .map_err(|e| e.to_string())?;
        }
    }

    if include_polls {
        conn.execute_batch(r#"
            CREATE TABLE poll_options (
                id      BIGINT,
                poll_id BIGINT,
                text    VARCHAR,
                score   INTEGER
            )
        "#).map_err(|e| e.to_string())?;
        let mut stmt = conn.prepare("INSERT INTO poll_options VALUES (?,?,?,?)")
            .map_err(|e| e.to_string())?;
        for p in &data.poll_options {
            stmt.execute(params![p.id, p.poll_id, p.text, p.score])
                .map_err(|e| e.to_string())?;
        }
    }

    Ok(conn)
}

// ---------------------------------------------------------------------------
// DB cache
// ---------------------------------------------------------------------------

fn ensure_db(state: &mut State, rt: &tokio::runtime::Runtime) -> Result<(), String> {
    let current_hash = hash_settings(&state.settings);
    let ttl_minutes  = state.get_u64("cache_ttl_minutes", 0);
    let ttl_expired  = ttl_minutes > 0 && (now_secs() - state.built_at) > (ttl_minutes as f64 * 60.0);

    if state.db.is_some() && state.settings_hash == current_hash && !ttl_expired {
        return Ok(());
    }

    let include_comments = state.get_bool("include_comments", false);
    let include_users    = state.get_bool("include_users", false);
    let include_polls    = state.get_bool("include_polls", false);

    let data = rt.block_on(fetch_hn_data(&state.settings))?;
    let conn = build_db(&data, include_comments, include_users, include_polls)?;

    state.db           = Some(conn);
    state.settings_hash = current_hash;
    state.built_at      = now_secs();
    Ok(())
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

fn run_query(conn: &Connection, query: &str, page: usize, page_size: usize) -> Result<JsonValue, String> {
    let t0 = Instant::now();
    let mut stmt = conn.prepare(query).map_err(|e| e.to_string())?;
    let mut all_rows: Vec<Vec<JsonValue>> = Vec::new();

    {
        // query() executes the statement and sets the result on stmt.
        // Column metadata is only available after execution, so we collect row
        // data here with trial-and-error indexing, then read column names from
        // stmt after this block (Rows::drop only removes its reference to stmt,
        // it does NOT destroy the result, so column_count()/column_name() work).
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut row_data = Vec::new();
            let mut i = 0usize;
            loop {
                match row.get::<_, duckdb::types::Value>(i) {
                    Ok(v)  => { row_data.push(duckdb_val_to_json(v)); i += 1; }
                    Err(_) => break,
                }
            }
            all_rows.push(row_data);
        }
    } // Rows dropped here — stmt.result is still intact

    // Statement was executed; result metadata is now accessible
    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).map(|s| s.to_string()).unwrap_or_else(|_| format!("col_{}", i)))
        .collect();

    let elapsed       = t0.elapsed().as_millis() as u64;
    let total_count   = all_rows.len();
    let page_rows: Vec<Vec<JsonValue>> = if page_size > 0 && total_count > page_size {
        let offset = ((page.saturating_sub(1)) * page_size).min(total_count);
        let end    = (offset + page_size).min(total_count);
        all_rows[offset..end].to_vec()
    } else {
        all_rows
    };
    let affected_rows = page_rows.len();

    Ok(json!({
        "columns":          col_names,
        "rows":             page_rows,
        "total_count":      total_count,
        "affected_rows":    affected_rows,
        "execution_time_ms": elapsed,
    }))
}

// ---------------------------------------------------------------------------
// DuckDB value → JSON
// ---------------------------------------------------------------------------

fn duckdb_val_to_json(val: duckdb::types::Value) -> JsonValue {
    use duckdb::types::Value;
    match val {
        Value::Null         => JsonValue::Null,
        Value::Boolean(b)   => json!(b),
        Value::TinyInt(i)   => json!(i),
        Value::SmallInt(i)  => json!(i),
        Value::Int(i)       => json!(i),
        Value::BigInt(i)    => json!(i),
        Value::HugeInt(i)   => json!(i.to_string()),
        Value::UTinyInt(i)  => json!(i),
        Value::USmallInt(i) => json!(i),
        Value::UInt(i)      => json!(i),
        Value::UBigInt(i)   => json!(i),
        Value::Float(f)     => json!(f),
        Value::Double(f)    => json!(f),
        Value::Text(s)      => json!(s),
        Value::Blob(b)      => json!(format!("<binary {} bytes>", b.len())),
        Value::Timestamp(_, i) => json!(i),
        Value::Date32(i)    => json!(i),
        Value::Time64(_, i) => json!(i),
        _                   => JsonValue::Null,
    }
}

// ---------------------------------------------------------------------------
// JSON-RPC helpers
// ---------------------------------------------------------------------------

fn send_success(stdout: &mut io::Stdout, id: JsonValue, result: JsonValue) {
    let mut s = serde_json::to_string(&json!({"jsonrpc":"2.0","result":result,"id":id})).unwrap();
    s.push('\n');
    stdout.write_all(s.as_bytes()).unwrap();
    stdout.flush().unwrap();
}

fn send_error(stdout: &mut io::Stdout, id: JsonValue, code: i32, message: &str) {
    let mut s = serde_json::to_string(&json!({"jsonrpc":"2.0","error":{"code":code,"message":message},"id":id})).unwrap();
    s.push('\n');
    stdout.write_all(s.as_bytes()).unwrap();
    stdout.flush().unwrap();
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

fn main() {
    let rt     = tokio::runtime::Runtime::new().expect("tokio runtime");
    let stdin  = io::stdin();
    let mut stdout = io::stdout();
    let mut state  = State::new();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l)  => l,
            Err(e) => { eprintln!("stdin error: {}", e); break; }
        };
        if line.trim().is_empty() { continue; }

        let req: JsonValue = match serde_json::from_str(&line) {
            Ok(v)  => v,
            Err(e) => { eprintln!("parse error: {}", e); continue; }
        };

        let id     = req["id"].clone();
        let method = match req["method"].as_str() {
            Some(m) => m.to_string(),
            None    => { send_error(&mut stdout, id, -32600, "Method not specified"); continue; }
        };
        let params = &req["params"];

        match method.as_str() {

            "initialize" => {
                state.settings     = params.get("settings").cloned().unwrap_or(json!({}));
                state.db           = None;
                state.settings_hash = 0;
                send_success(&mut stdout, id, json!(null));
            }

            "test_connection" => {
                let timeout = state.get_u64("timeout", 10);
                let result = rt.block_on(async move {
                    let client = reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(timeout))
                        .build()?;
                    client.get(format!("{}/topstories.json", BASE)).send().await?.error_for_status()?;
                    Ok::<(), reqwest::Error>(())
                });
                match result {
                    Ok(())  => send_success(&mut stdout, id, json!({"success": true})),
                    Err(e)  => send_error(&mut stdout, id, -32603, &format!("Cannot reach Hacker News API: {}", e)),
                }
            }

            "get_databases" => send_success(&mut stdout, id, json!(["hackernews"])),
            "get_schemas"   => send_success(&mut stdout, id, json!([])),

            "get_tables" => {
                let tables: Vec<JsonValue> = state.active_tables().iter()
                    .map(|&t| json!({"name": t, "schema": null, "comment": table_comment(t)}))
                    .collect();
                send_success(&mut stdout, id, json!(tables));
            }

            "get_columns" => {
                let table = params.get("table").and_then(|v| v.as_str()).unwrap_or("");
                send_success(&mut stdout, id, columns_for(table));
            }

            "execute_query" => {
                let query     = match params.get("query").and_then(|v| v.as_str()) {
                    Some(q) => q.to_string(),
                    None    => { send_error(&mut stdout, id, -32602, "Missing query"); continue; }
                };
                let page      = params.get("page").and_then(|v| v.as_u64()).unwrap_or(1).max(1) as usize;
                let page_size = params.get("page_size").and_then(|v| v.as_u64()).unwrap_or(100) as usize;

                if let Err(e) = ensure_db(&mut state, &rt) {
                    send_error(&mut stdout, id, -32603, &e);
                    continue;
                }

                let result = run_query(state.db.as_ref().unwrap(), &query, page, page_size);
                match result {
                    Ok(v)  => send_success(&mut stdout, id, v),
                    Err(e) => send_error(&mut stdout, id, -32603, &e),
                }
            }

            "get_schema_snapshot" => {
                let tables = state.active_tables();
                let tables_json: Vec<JsonValue> = tables.iter()
                    .map(|&t| json!({"name": t, "schema": null, "comment": table_comment(t)}))
                    .collect();
                let columns_map: serde_json::Map<String, JsonValue> = tables.iter()
                    .map(|&t| (t.to_string(), columns_for(t)))
                    .collect();
                let fk_map: serde_json::Map<String, JsonValue> = tables.iter()
                    .map(|&t| (t.to_string(), json!([])))
                    .collect();
                send_success(&mut stdout, id, json!({
                    "tables":       tables_json,
                    "columns":      columns_map,
                    "foreign_keys": fk_map,
                }));
            }

            "get_all_columns_batch" => {
                let tables = params.get("tables").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                let result: serde_json::Map<String, JsonValue> = tables.iter()
                    .filter_map(|v| v.as_str())
                    .map(|t| (t.to_string(), columns_for(t)))
                    .collect();
                send_success(&mut stdout, id, JsonValue::Object(result));
            }

            "get_all_foreign_keys_batch" => {
                let tables = params.get("tables").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                let result: serde_json::Map<String, JsonValue> = tables.iter()
                    .filter_map(|v| v.as_str())
                    .map(|t| (t.to_string(), json!([])))
                    .collect();
                send_success(&mut stdout, id, JsonValue::Object(result));
            }

            "insert_record" | "update_record" | "delete_record" => {
                send_error(&mut stdout, id, -32603, "Hacker News is a read-only data source.");
            }

            m if m.starts_with("get_") => send_success(&mut stdout, id, json!([])),

            _ => send_error(&mut stdout, id, -32601, &format!("Method '{}' not implemented", method)),
        }
    }
}
