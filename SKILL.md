---
name: eastmoney-guba-comments
description: Crawl public Eastmoney Guba stock comments into MySQL with resumable progress, date-range filtering, adaptive concurrency, and CSV export helpers. Use for collecting public discussion data from Eastmoney Guba when the user provides stock codes and a permitted crawl window.
metadata:
  short-description: Crawl and export Eastmoney Guba comments
---

# Eastmoney Guba Comments

Use this skill to collect publicly visible Eastmoney Guba posts for a list of stock codes and save normalized records to MySQL. The crawler uses the mobile Guba data endpoint, rate limiting, retry backoff, duplicate-page detection, adaptive concurrency, and database checkpoints so interrupted runs can resume.

## Workflow

1. Copy `.env.example` to `.env` or export the variables in the shell. Always set `MYSQL_PASSWORD` explicitly; the project has no password fallback.
2. Put one six-digit stock code per line in the file referenced by `STOCK_CODE_FILE`.
3. Install dependencies from `requirements.txt` and install the Playwright browser runtime if browser fallback is needed.
4. Run `python eastmoney_data.py`.
5. Use `python data_extract.py` to export database rows into configurable CSV chunks.
6. Use `python examine.py` to compare a stock-code file with the codes already stored in MySQL.

## Configuration

The main controls are exposed as environment variables:

- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB`
- `STOCK_CODE_FILE`, `START_DATE`, `CUTOFF_DATE`
- `MAX_CONCURRENT`, `MIN_CONCURRENT`, `MAX_CONCURRENT_LIMIT`
- `RATE_LIMIT_INTERVAL`, `BROWSER_POOL_SIZE`, `CHROME_EXECUTABLE_PATH`
- `EXPORT_DIR` for CSV output

Keep request rates conservative and stop the run if Eastmoney presents a verification or access restriction page. Use only public data and follow applicable site terms, laws, and data-use permissions.

## Files

- `eastmoney_data.py`: resumable asynchronous crawler and MySQL schema initialization.
- `data_extract.py`: streaming CSV export from the `Comment` table.
- `examine.py`: reports stock codes missing from the database.
- `.env.example`: safe configuration template with no credentials.
- `stock_code.txt`: small example input list suitable for a smoke test.

