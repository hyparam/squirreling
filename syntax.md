# SQL syntax failures in production

What users (and the chat model) tried to run against squirreling on hyperparam.app that failed
for syntax or unsupported-feature reasons. Sourced from `web:chat:toolcall:error` events for the
`sql_query` and `add_view` tools in `/opt/hyperparam/data/log/app-*.jsonl` on origin.hyperparam.app,
covering March through early July 2026 (~1160 sql_query errors total; ~360 are syntax/unsupported,
the rest are column-not-found and file-read errors, excluded here).

Monthly trend: Mar 74, Apr 123, May 119, Jun 28, Jul 16. The drop tracks features that have since
been added to squirreling (window functions, GREATEST, MEDIAN, ARRAY_AGG, UNNEST, etc). Every item
below was re-tested against current `src/` with `parseSql` on 2026-07-06; the "already fixed"
section lists what failed in the logs but parses today.

Caveat on attribution: most of these queries were written by the chat model in the workspace chat
panel, not typed by a human. The model brings DuckDB/Postgres habits, fails, and usually retries
with a supported form (e.g. `||` fails, next call uses `CONCAT`). So each count is roughly
"failures", not "users blocked", but the retry loop burns tokens and latency, and the same habits
show up across every user.

## Top list to implement

Ranked by production failure count, still failing in current squirreling.

| # | Feature | Failures | Notes |
|---|---------|---------:|-------|
| 1 | `POSITION(needle IN haystack)` | 36 | Biggest single item. 6 fail as unknown function, 30 as `Expected ( after "IN"`. `POSITION`/`STRPOS` exist now as regular functions, but the standard SQL `IN` form is what everyone writes. Parser-level special case. |
| 2 | Array indexing `col[0]`, `col[0].field` | 19 | `col['key']` works; numeric index fails with `Expected string literal after "["`. Users index into message arrays constantly (`addresses[0].country`, `tool_call_errors[0].error`). |
| 3 | String split family: `SPLIT_PART`, `STRING_SPLIT`, `SPLIT`, `STR_SPLIT`, `REGEXP_SPLIT_TO_ARRAY`, `REGEXP_SPLIT_TO_TABLE` | 17 | No way to split a string at all today. `SPLIT_PART(str, delim, n)` (5) plus `STRING_SPLIT`+`UNNEST` for word counts is the common pattern. |
| 4 | `\|\|` string concatenation | 16 | `Unexpected character "\|"`. Standard SQL; model retries with CONCAT but always tries `\|\|` first. |
| 5 | TIMESTAMP typing: `CAST(x AS TIMESTAMP)` (7) + `TIMESTAMP '2026-01-01'` literals (7) | 14 | Cast types are limited to STRING/INT/BIGINT/FLOAT/BOOL. Anyone doing time-series filtering hits this. |
| 6 | SQLite/DuckDB JSON aliases: `JSON_GROUP_ARRAY` (5), `JSON_EXTRACT_STRING` (4), `JSON_GROUP_OBJECT` (1), `TO_JSON` (1) | 11 | Cheap: aliases onto existing JSON_ARRAYAGG / JSON_EXTRACT / JSON_OBJECT machinery. |
| 7 | `WITH RECURSIVE` | 9 | Users built message-chain traversals over parent ids. Harder; could also stay unsupported with a better error. |
| 8 | Extra aggregates: `ANY_VALUE` (3), `ARG_MIN`/`MIN_BY` (3), `LISTAGG` (1), `STDDEV` (1) | 8 | LISTAGG and STDDEV are pure aliases (STRING_AGG, STDDEV_SAMP). ANY_VALUE is trivial. |
| 9 | `ILIKE` | 6 | Parser keyword + case-insensitive LIKE. Trivial and expected by every Postgres user. |
| 10 | Regex: `REGEXP_EXTRACT_ALL` (3), `REGEXP_LIKE` (2) | 5 | REGEXP_LIKE ≈ alias for REGEXP_MATCHES. |
| 11 | JSON arrow operators `->` / `->>` | 5 | `geometry->>'coordinates'->0`. Shows up as `Expected expression but found ">"`. Sugar over JSON_QUERY/JSON_VALUE. |
| 12 | Struct functions: `STRUCT_EXTRACT` (3), `STRUCT_PACK` (1) | 4 | DuckDB vocabulary for nested data. |
| 13 | Date/time functions: `TO_TIMESTAMP`, `STRFTIME`, `DAYOFWEEK`, `WEEKDAY` | 4 | DAYOFWEEK/WEEKDAY map to existing DATE_PART. |
| 14 | Misc string: `LTRIM`/`RTRIM`, `CHARINDEX`, `CONTAINS`, `CHAR` | 4 | LTRIM/RTRIM are one-liners; CHARINDEX/CONTAINS/INSTR aliases. |
| 15 | Remaining window: aggregate `SUM(...) OVER` / `COUNT(...) OVER` (2), `FIRST_VALUE` (1), `RANK` | 3 | ROW_NUMBER/LAG/LEAD landed; running totals are the next ask. |
| 16 | Grammar misc: comma join (3), `VALUES` subquery (3), `TYPEOF` (2), `DISTINCT ON` (1), `SELECT * EXCLUDE` (1), `GLOB` (1), `~` regex operator (1), `NOT REGEXP` (1) | 13 | Long tail, one user each. Comma join already has a good error message pointing at explicit JOIN. |

A rough cut: items 1-5 are 102 of ~200 still-unfixed syntax failures. POSITION-IN, `col[0]`, `||`,
and the split family alone would have absorbed about half of everything users hit in four months.

## Runtime errors worth a look (not parse errors)

These execute but fail at evaluation. They may be intentional semantics, but they are the errors
users see most after the parser ones:

| Error | Count | Example trigger |
|-------|------:|-----------------|
| `Aggregate function SUM is not available in this context` | 14 | Aggregate referenced in a non-aggregate position, e.g. inside a scalar expression or HAVING-less context. |
| `SUBSTR/LOWER(...): does not support object/array arguments. Use CAST` | 13 | Column is a struct/array; DuckDB coerces, squirreling asks for CAST. Good error text, but auto-stringify would remove the whole class. |
| `position must be a positive integer` (SUBSTR negative start, `regexp_replace(..., 'g')`) | 9 | Negative index = count-from-end in DuckDB; `'g'` flag as 4th arg of regexp_replace is Postgres habit. Accepting `'g'` is cheap and common. |
| `Unterminated string starting at position N` | 12 | Mostly the model emitting truncated/mis-escaped SQL inside tool-call JSON, not an engine gap. Listed for completeness. |

## Already fixed (failed in logs, parses today)

For the record, these were significant in the Mar-May logs and now work: `ROW_NUMBER`/`LAG`/`LEAD`
window functions (37 failures, the #1 item at the time), `GREATEST`/`LEAST` (6), `JSON_EXTRACT` (6),
`ARRAY_AGG`/`LIST` (9), `MEDIAN`/`PERCENTILE_CONT`/`APPROX_QUANTILE` (5), `STRPOS` (3), `UNNEST` (2),
`JSON_EACH` (2), `SIZE`, `JSON_ARRAY_LENGTH`, JOIN on subquery (11), `JOIN ... USING` (5),
ORDER BY inside subqueries (19), SELECT without FROM (~10), UNION after LIMIT.

## Real failing queries from the logs

Window / analytics (now fixed, kept for flavor of what people do):

```sql
SELECT session_id, timestamp, role, SUBSTR(content,1,100) AS preview
FROM (
  SELECT session_id, timestamp, role, content,
         ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY timestamp DESC) AS rn
  FROM table
)
```

POSITION with IN (still fails, item 1):

```sql
SELECT chatSessionId,
       SUBSTR(CAST(conversation AS VARCHAR),
              GREATEST(POSITION('does not exist' IN CAST(conversation AS VARCHAR)) - 150, 1),
              500) AS snippet
FROM table
```

Array indexing (still fails, item 2):

```sql
SELECT addresses[0].country, COUNT(*) as count
FROM table WHERE addresses IS NOT NULL
GROUP BY addresses[0].country ORDER BY count DESC
```

```sql
SELECT tool_call_errors[0].error AS e FROM table LIMIT 3
```

String splitting (still fails, item 3):

```sql
SELECT trim(split_part(output, 'Response in Belarusian:', 2)) AS be_part
FROM "view:train-00000-of-00001.parquet" LIMIT 3
```

```sql
SELECT word, COUNT(*) AS cnt
FROM (SELECT UNNEST(STRING_SPLIT(text, ' ')) AS word FROM table WHERE speaker_id = 5639)
GROUP BY word ORDER BY cnt DESC LIMIT 10
```

Concat (still fails, item 4), and the model's successful retry with CONCAT:

```sql
SELECT COUNT(DISTINCT COALESCE(question,'') || '||' || COALESCE(answer,'')) AS distinct_pairs FROM table
-- retry that worked:
SELECT COUNT(DISTINCT CONCAT(COALESCE(question,''), '||', COALESCE(answer,''))) AS distinct_pairs FROM ...
```

Timestamps (still fails, item 5):

```sql
SELECT author_time, CAST(author_time AS TIMESTAMP) AS ts
FROM "Documents/code/hooks-analytics/claude_repo_commits.parquet" LIMIT 3
```

```sql
SELECT Pickup_date FROM "view:elisachen/uber-trips/uber-raw-data-janjune-15.csv"
WHERE CAST(Pickup_date AS TIMESTAMP) >= TIMESTAMP '2027-01-01 00:00:00' LIMIT 20
```

WITH RECURSIVE (still fails, item 7):

```sql
WITH RECURSIVE nums(i) AS (
  SELECT 1
  UNION ALL
  SELECT i + 1 FROM nums WHERE i < 5
) SELECT i FROM nums
```

JSON arrows (still fails, item 11):

```sql
SELECT COUNT(DISTINCT CAST(geometry->>'coordinates'->0 as STRING)) as unique_locations FROM table
```

```sql
SELECT deepseek_response->'reward' AS deepseek_reward FROM table LIMIT 3
```

EXCLUDE (long tail, item 16):

```sql
SELECT * EXCLUDE ("Unnamed: 0", " Fwd Header Length.1", " Label", " Timestamp"),
       TRIM(" Label") AS label, CAST(" Timestamp" AS TIMESTAMP) AS timestamp,
       'UDPLag' AS source_file
FROM "opfs://UDPLag.csv" LIMIT 1
```

## Who hit these

March-May failures are mostly internal dogfooding (brendan@ ~260, kenny@ 27), analyzing Claude Code
logs, which is exactly the workload the product sells, so internal failures predict customer
failures. June-July is almost entirely external users: stacey@ae.studio (16), forgptstas@gmail.com
(8), bogatyrev992@gmail.com (8), pavancasprov@gmail.com (6), vzokhan72@gmail.com (5), plus
rraught@appen.com, luke@spice.ai and a cluster of users doing Belarusian dataset cleaning.

## Reproducing the analysis

```sh
ssh hyperparam@origin.hyperparam.app \
  'grep -h "toolcall:error" /opt/hyperparam/data/log/app-*.jsonl' \
  | jq -r 'select(.metadata.name=="sql_query") | .metadata.error'
```

Failing queries are in the matching `"Tool call sql_query starting"` events
(`.metadata.args.query`), same log files.
