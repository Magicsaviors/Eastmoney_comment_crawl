import asyncio
import os
import random
import re
import json
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import aiomysql
from playwright.async_api import Page, Browser, BrowserContext

# =================== 配置 ===================
# 笔记本环境建议：内存 16G 取 8~10，32G 可取 12~15
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "10"))
MIN_CONCURRENT = int(os.getenv("MIN_CONCURRENT", "5"))
MAX_CONCURRENT_LIMIT = int(os.getenv("MAX_CONCURRENT_LIMIT", "15"))

# 浏览器实例较重，笔记本建议 4~6 个实例，每个实例承载多个 Context
BROWSER_POOL_SIZE = int(os.getenv("BROWSER_POOL_SIZE", "5"))
RATE_LIMIT_INTERVAL = float(os.getenv( "RATE_LIMIT_INTERVAL", "0.1"))

PAGE_GOTO_TIMEOUT = int(os.getenv("PAGE_GOTO_TIMEOUT", "40000"))
SELECTOR_TIMEOUT = int(os.getenv("SELECTOR_TIMEOUT", "8000"))

# 移动版股吧列表接口：PC 站被风控拦截时的可用数据源，100 条/页，按发帖时间倒序
API_PAGE_SIZE = int(os.getenv("API_PAGE_SIZE", "100"))
API_MIN_PAGE_SIZE = 50  # 接口在历史数据段每页最多返回 50 条，用于估算页数上界
API_MAX_PAGE_LIMIT = int(os.getenv("API_MAX_PAGE_LIMIT", "50000"))
API_URL = "https://mguba.eastmoney.com/mguba2020/interface/GetData.aspx"
API_PATH = "/webarticlelist/api/Article/WebArticleList"

# 命中身份核实页后的全局冷却时间（秒），连续命中按倍数递增
BLOCK_COOLDOWN_BASE = float(os.getenv("BLOCK_COOLDOWN_BASE", "180"))
BLOCK_COOLDOWN_MAX = float(os.getenv("BLOCK_COOLDOWN_MAX", "3600"))

DB_POOL_MINSIZE = int(os.getenv("DB_POOL_MINSIZE", "3"))
DB_POOL_MAXSIZE = int(os.getenv("DB_POOL_MAXSIZE", "16"))
DB_BATCH_PAGES = int(os.getenv("DB_BATCH_PAGES", "5"))
DB_BATCH_ROWS = int(os.getenv("DB_BATCH_ROWS", "500"))
DB_INSERT_MODE = os.getenv("DB_INSERT_MODE", "upsert").strip().lower() 

MAX_DUP_RETRIES = int(os.getenv("MAX_DUP_RETRIES", "3"))
RESUME_OVERLAP_PAGES = max(1, int(os.getenv("RESUME_OVERLAP_PAGES", "2")))

CUTOFF_DATE = os.getenv("CUTOFF_DATE", "2020-01-01 00:00:00")
START_DATE = os.getenv("START_DATE", "2024-01-01 00:00:00")
STOCK_CODE_FILE = os.getenv("STOCK_CODE_FILE", "./stock_add.txt")

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "eastmoney_comment")
CHROME_EXECUTABLE_PATH = os.getenv("CHROME_EXECUTABLE_PATH", "").strip()

DESKTOP_UAS = [
    # ========== Windows 平台（主力，占比最高） ==========
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36 Edg/155.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    
    # ========== macOS 平台 ==========
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
]

# =================== 工具函数 ===================
def parse_time(s: str) -> Optional[datetime]:
    if not s: return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try: return datetime.strptime(s.strip(), fmt)
        except ValueError: continue
    return None

def safe_int(value, default: int = 0) -> int:
    try: return int(value)
    except Exception: return default

# =================== 全局限速器（无锁优化） ===================
class RateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self.last_time = 0.0

    async def wait(self):
        loop = asyncio.get_running_loop()
        now = loop.time()
        delay = self.min_interval - (now - self.last_time)
        if delay > 0:
            await asyncio.sleep(delay)
        self.last_time = loop.time()

# =================== 全局风控冷却 ===================
class RiskControl:
    """所有任务共享的冷却闸门：任一任务撞到身份核实页，全体暂停后再试。"""

    def __init__(self):
        self.block_until = 0.0
        self.episodes = 0
        self.success_streak = 0
        self._lock = asyncio.Lock()

    def is_cooling_down(self) -> bool:
        return asyncio.get_running_loop().time() < self.block_until

    async def wait(self):
        while True:
            remaining = self.block_until - asyncio.get_running_loop().time()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 5))

    async def report_block(self) -> float:
        async with self._lock:
            now = asyncio.get_running_loop().time()
            if now < self.block_until:
                return 0.0
            self.episodes += 1
            cooldown = min(BLOCK_COOLDOWN_BASE * (2 ** (self.episodes - 1)), BLOCK_COOLDOWN_MAX)
            self.block_until = now + cooldown
            self.success_streak = 0
            print(f"[风控] 命中身份核实/限流页，全局暂停 {cooldown / 60:.1f} 分钟（连续第 {self.episodes} 次）")
            return cooldown

    async def report_success(self):
        self.success_streak += 1
        if self.success_streak >= 50 and self.episodes:
            async with self._lock:
                self.episodes = 0
                self.success_streak = 0

# =================== 自适应并发 ===================
class AdaptiveConcurrencyController:
    def __init__(self, initial: int, minimum: int, maximum: int):
        self.current = initial
        self.minimum = minimum
        self.maximum = maximum
        self.history: List[Tuple[int, int]] = []

    def update(self, blocked: int, total_pages: int):
        self.history.append((blocked, total_pages))
        if len(self.history) > 8: self.history.pop(0)
        sum_block = sum(b for b, _ in self.history)
        sum_total = sum(t for _, t in self.history)
        rate = (sum_block / sum_total) if sum_total > 0 else 0.0
        if rate > 0.25 and self.current > self.minimum:
            self.current -= 1
            print(f"[并发控制] 近8任务拦截率 {rate:.1%}，降并发到 {self.current}")
        elif rate < 0.08 and self.current < self.maximum and sum_total >= 30:
            self.current += 1
            print(f"[并发控制] 近8任务拦截率 {rate:.1%}，升并发到 {self.current}")

    def get_limit(self) -> int:
        return self.current

# =================== 数据库 ===================
class DBManager:
    def __init__(self):
        self.pool: Optional[aiomysql.Pool] = None
        self.insert_mode = DB_INSERT_MODE if DB_INSERT_MODE in {"upsert", "ignore"} else "upsert"
        self.comment_insert_sql = self._build_comment_insert_sql()

    @staticmethod
    def _progress_sql() -> str:
        return """
            INSERT INTO crawl_progress (stock_code, last_page, status) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status=IF(status='completed' AND last_page > 0, status,
                          IF(%s >= last_page, %s, status)),
                last_page=GREATEST(last_page, VALUES(last_page))
        """
 
    def _build_comment_insert_sql(self) -> str:
        if self.insert_mode == "ignore":
            return """
                INSERT IGNORE INTO `Comment`
                (platform, stock_code, post_id, post_content, post_publish_time, post_url, 
                author_id, read_count, comment_count, like_count, share_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        return """
            INSERT INTO `Comment`
            (platform, stock_code, post_id, post_content, post_publish_time, post_url, 
            author_id, read_count, comment_count, like_count, share_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                post_content = IF(VALUES(post_content) != '', VALUES(post_content), Comment.post_content),
                post_publish_time = IF(VALUES(post_publish_time) != '', VALUES(post_publish_time), Comment.post_publish_time),
                post_url = IF(VALUES(post_url) != '', VALUES(post_url), Comment.post_url),
                author_id = IF(VALUES(author_id) != '', VALUES(author_id), Comment.author_id),
                read_count = IF(VALUES(read_count) != '0', VALUES(read_count), Comment.read_count),
                comment_count = IF(VALUES(comment_count) != '0', VALUES(comment_count), Comment.comment_count),
                like_count = IF(VALUES(like_count) != '0', VALUES(like_count), Comment.like_count),
                share_count = IF(VALUES(share_count) != '0', VALUES(share_count), Comment.share_count)
        """

    async def init(self):
        if not MYSQL_PASSWORD: print("[提醒] MYSQL_PASSWORD 为空。建议通过环境变量设置。")
        conn = await aiomysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, charset="utf8mb4", autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.close()
        self.pool = await aiomysql.create_pool(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB, charset="utf8mb4", minsize=DB_POOL_MINSIZE, maxsize=DB_POOL_MAXSIZE, autocommit=False)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS `Comment` (
                        `id` INT AUTO_INCREMENT PRIMARY KEY, `platform` VARCHAR(50) NOT NULL DEFAULT '东方财富',
                        `stock_code` VARCHAR(20) NOT NULL, `post_id` VARCHAR(40) NOT NULL, `post_content` VARCHAR(1500),
                        `post_publish_time` VARCHAR(30), `post_url` VARCHAR(300),
                        `author_id` VARCHAR(50), `read_count` VARCHAR(20) DEFAULT '0', `comment_count` VARCHAR(20) DEFAULT '0',
                        `like_count` VARCHAR(20) DEFAULT '0', `share_count` VARCHAR(20) DEFAULT '0',
                        UNIQUE KEY `uk_post_id` (`post_id`), INDEX `idx_stock_code` (`stock_code`), INDEX `idx_publish_time` (`post_publish_time`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                for sql in [
                    "ALTER TABLE `Comment` ADD COLUMN IF NOT EXISTS `author_id` VARCHAR(50)",
                    "ALTER TABLE `Comment` ADD COLUMN IF NOT EXISTS `read_count` VARCHAR(20) DEFAULT '0'",
                    "ALTER TABLE `Comment` ADD COLUMN IF NOT EXISTS `comment_count` VARCHAR(20) DEFAULT '0'",
                    "ALTER TABLE `Comment` ADD COLUMN IF NOT EXISTS `like_count` VARCHAR(20) DEFAULT '0'",
                    "ALTER TABLE `Comment` ADD COLUMN IF NOT EXISTS `share_count` VARCHAR(20) DEFAULT '0'"
                ]:
                    try: await cur.execute(sql)
                    except Exception: pass
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS `crawl_progress` (
                        `stock_code` VARCHAR(20) PRIMARY KEY, `last_page` INT NOT NULL DEFAULT 0, `status` VARCHAR(20) NOT NULL DEFAULT 'pending',
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                await cur.execute("SHOW INDEX FROM `Comment`")
                indexes = {}
                for row in await cur.fetchall():
                    indexes.setdefault(row[2], {})[row[3]] = row[4]
                if not any(
                    [columns[k] for k in sorted(columns)][:2]
                    == ["stock_code", "post_publish_time"]
                    for columns in indexes.values()
                ):
                    await cur.execute(
                        "CREATE INDEX idx_stock_publish_time ON `Comment` "
                        "(stock_code, post_publish_time)"
                    )
            await conn.commit()
        print(f"数据库初始化完成 | pool={DB_POOL_MINSIZE}-{DB_POOL_MAXSIZE} | batch_pages={DB_BATCH_PAGES} | batch_rows={DB_BATCH_ROWS} | insert_mode={self.insert_mode}")

    async def get_progress(self, stock_code: str) -> Tuple[int, str]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "INSERT INTO crawl_progress (stock_code, last_page, status) "
                        "VALUES (%s, 0, 'pending') "
                        "ON DUPLICATE KEY UPDATE stock_code=stock_code", (stock_code,)
                    )
                    await cur.execute("SELECT last_page, status FROM crawl_progress WHERE stock_code=%s", (stock_code,))
                    row = await cur.fetchone()
                    await conn.commit()
                    return (max(0, safe_int(row[0])), row[1]) if row else (0, "pending")
                except Exception:
                    await conn.rollback()
                    raise

    async def get_resume_time(self, stock_code: str) -> Optional[datetime]:
        """Use the oldest stored comment in the requested range as a recovery anchor."""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("""
                        SELECT post_publish_time FROM `Comment`
                        WHERE stock_code=%s
                          AND post_publish_time >= %s AND post_publish_time <= %s
                        ORDER BY post_publish_time ASC LIMIT 1
                    """, (stock_code, CUTOFF_DATE, START_DATE))
                    row = await cur.fetchone()
                    await conn.commit()
                    return parse_time(row[0]) if row else None
                except Exception:
                    await conn.rollback()
                    raise

    async def update_progress(self, stock_code: str, last_page: int, status: str = "in_progress"):
        last_page = max(0, last_page)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(self._progress_sql(), (stock_code, last_page, status, last_page, status))
                    await conn.commit()
                except Exception: 
                    await conn.rollback()
                    raise

    async def reset_progress(self, stock_code: str):
        """Legacy entry point: never erase a committed checkpoint."""
        await self.update_progress(stock_code, 0, "pending")

    async def flush_comments_and_progress(self, stock_code: str, rows: List[Dict], last_page: int, status: str = "in_progress") -> int:
        values = self._build_comment_values(rows)
        last_page = max(0, last_page)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    affected = 0
                    if values:
                        await cur.executemany(self.comment_insert_sql, values)
                        affected = cur.rowcount
                    await cur.execute(self._progress_sql(), (stock_code, last_page, status, last_page, status))
                    await conn.commit()
                    return affected
                except Exception as e:
                    await conn.rollback()
                    print(f"    DB批量 flush 失败: {e}")
                    raise

    @staticmethod
    def _build_comment_values(rows: List[Dict]) -> List[Tuple]:
        return [
            (
                r.get("platform", "东方财富"), r.get("stock_code", ""), r.get("post_id", ""),
                r.get("post_content", "")[:1500], r.get("post_publish_time", ""), r.get("post_url", ""),
                r.get("author_id", ""), r.get("read_count", "0"),
                r.get("comment_count", "0"), r.get("like_count", "0"), r.get("share_count", "0"),
            ) for r in rows if r.get("post_id") and r.get("post_content")
        ]

    async def close(self):
        if self.pool: 
            self.pool.close()
            await self.pool.wait_closed()

# =================== 浏览器池（笔记本适配） ===================
class BrowserPool:
    def __init__(self, p, size: int, headless: bool = True):
        self.p = p; self.size = size; self.headless = headless
        self.browsers: List[Browser] = []
        self.queue: asyncio.Queue[Browser] = asyncio.Queue()

    async def init(self):
        for _ in range(self.size):
            browser = await self._launch_browser()
            self.browsers.append(browser)
            await self.queue.put(browser)
        print(f"浏览器池初始化完成：{self.size} 个 browser（已针对笔记本优化启动参数）")

    async def _launch_browser(self) -> Browser:
        launch_options = {
            "headless": self.headless,
            "args": [
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-gpu", "--disable-software-rasterizer",
                "--disable-extensions", "--disable-popup-blocking", "--disable-notifications",
                "--disable-infobars", "--mute-audio", "--disable-features=TranslateUI",
                "--disable-default-apps", "--disable-hang-monitor", "--disable-sync"
            ],
        }
        if CHROME_EXECUTABLE_PATH:
            launch_options["executable_path"] = CHROME_EXECUTABLE_PATH
        return await self.p.chromium.launch(**launch_options)

    async def acquire(self) -> Browser: return await self.queue.get()
    async def release(self, browser: Browser): await self.queue.put(browser)
    async def recreate(self, browser: Browser) -> Browser:
        try:
            if browser in self.browsers: self.browsers.remove(browser)
            await browser.close()
        except Exception: pass
        new_browser = await self._launch_browser()
        self.browsers.append(new_browser)
        return new_browser
    async def close(self):
        for browser in list(self.browsers):
            try: await browser.close()
            except Exception: pass
        self.browsers.clear()

# =================== 爬虫主体 ===================
class StockCommentCrawler:
    _BLOCK_KW = ["访问频繁", "访问受限", "请求过于频繁", "异常访问", "请稍后再试", "请完成验证",
                 "数据核实", "身份核实", "fd_guba_validate", "em_capt", "captcha", "CAPTCHA"]

    def __init__(self, rate_limiter: RateLimiter, risk_control: RiskControl):
        self.rate_limiter = rate_limiter
        self.risk = risk_control

    @staticmethod
    def _pick_desktop_ua() -> str: return random.choice(DESKTOP_UAS)

    async def _new_context_page(self, browser: Browser) -> Tuple[BrowserContext, Page]:
        ctx = await browser.new_context(
            user_agent=self._pick_desktop_ua(),
            viewport={"width": random.randint(1366, 1920), "height": random.randint(768, 1080)},
            locale="zh-CN", timezone_id="Asia/Shanghai", java_script_enabled=True
        )
        await ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en']});
        """)
        page = await ctx.new_page()
        page.set_default_timeout(PAGE_GOTO_TIMEOUT)
        
        await page.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,otf,eot,mp3,mp4,avi,webm,css}", lambda route: route.abort())
        return ctx, page

    @staticmethod
    def _clean_html(html: str) -> str:
        if not html: return ""
        t = re.sub(r"<[^>]+>", "  ", html).replace("&nbsp;", " ").replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
        return re.sub(r"\s+", " ", t).strip()

    @staticmethod
    def _extract_json_object(text: str) -> Optional[str]:
        start = text.find("{")
        if start == -1: return None
        depth, in_string, escape, quote = 0, False, False, '"'
        for i in range(start, len(text)):
            ch = text[i]
            if in_string:
                if escape: escape = False
                elif ch == "\\": escape = True
                elif ch == quote: in_string = False
                continue
            if ch in ("'", '"'): in_string, quote = True, ch
            elif ch == "{": depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0: return text[start:i + 1]
        return None

    @staticmethod
    def _make_list_url(stock_code: str, page_num: int) -> str:
        return f"https://guba.eastmoney.com/list,{stock_code}.html" if page_num <= 1 else f"https://guba.eastmoney.com/list,{stock_code}_{page_num}.html"

    @staticmethod
    def _api_post_sync(stock_code: str, page_num: int) -> Tuple[int, bytes]:
        body = urllib.parse.urlencode({
            "param": f"code={stock_code}&p={page_num}&ps={API_PAGE_SIZE}&sorttype=0",
            "plat": "wap", "version": "200", "path": API_PATH,
            "env": "1", "origin": "", "ctoken": "", "utoken": "",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{API_URL}?path={urllib.parse.quote(API_PATH, safe='')}",
            data=body, method="POST",
            headers={
                "User-Agent": StockCommentCrawler._pick_desktop_ua(),
                "Referer": f"https://mguba.eastmoney.com/mguba/list/{stock_code}",
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read() if e.fp else b""

    async def _fetch_api_page(self, stock_code: str, page_num: int) -> Tuple[Optional[List[Dict]], int]:
        """返回 (posts, total)。posts=None 表示请求失败/被拦截；[] 表示接口确认该页无数据。"""
        await self.risk.wait()
        await self.rate_limiter.wait()
        try:
            loop = asyncio.get_running_loop()
            status, raw = await loop.run_in_executor(None, self._api_post_sync, stock_code, page_num)
        except Exception as e:
            print(f"    [{stock_code}] p{page_num} 接口请求异常: {e}")
            return None, 0

        text = raw.decode("utf-8", "replace")
        if status != 200:
            if status in (403, 429, 544):
                await self.risk.report_block()
            else:
                print(f"    [{stock_code}] p{page_num} 接口 HTTP {status}")
            return None, 0

        try:
            data = json.loads(text)
        except Exception:
            if any(kw in text for kw in self._BLOCK_KW):
                await self.risk.report_block()
            else:
                print(f"    [{stock_code}] p{page_num} 接口返回非 JSON（{len(text)} 字节）")
            return None, 0

        if not isinstance(data, dict) or data.get("rc") != 1:
            rc = data.get("rc") if isinstance(data, dict) else None
            print(f"    [{stock_code}] p{page_num} 接口返回 rc={rc}")
            return None, 0

        total = safe_int(data.get("count"), 0)
        posts: List[Dict] = []
        for item in data.get("re") or []:
            pid = str(item.get("post_id") or "")
            if not pid:
                continue
            content = (item.get("post_content") or "").strip()
            if not content:
                content = (item.get("post_title") or "").strip()
            user = item.get("post_user") or {}
            posts.append({
                "url": f"https://guba.eastmoney.com/news,{stock_code},{pid}.html",
                "post_id": pid,
                "post_publish_time": item.get("post_publish_time") or item.get("post_display_time") or "",
                "post_content": content,
                "author_id": str(user.get("user_id") or ""),
                "read_count": str(item.get("post_click_count") or 0),
                "comment_count": str(item.get("post_comment_count") or 0),
                "like_count": str(item.get("post_like_count") or 0),
                "share_count": str(item.get("post_forward_count") or 0),
            })
        await self.risk.report_success()
        return posts, total

    async def scrape_list_page(self, page: Page, stock_code: str, page_num: int) -> Optional[List[Dict]]:
        posts, _ = await self._fetch_api_page(stock_code, page_num)
        return posts

    async def _is_blocked(self, page: Page) -> bool:
        try: return any(kw in (await page.content()) for kw in self._BLOCK_KW)
        except Exception: return False

    @staticmethod
    def _first_valid_time(posts: List[Dict]) -> Optional[datetime]:
        for post in posts:
            t = parse_time(post.get("post_publish_time", ""))
            if t: return t
        return None

    async def _fetch_retry(self, ctx: Optional[BrowserContext], page: Page, browser: Browser,
                           stock_code: str, page_num: int, max_retries: int = 3) -> Tuple[Optional[List[Dict]], BrowserContext, Page]:
        """抓取列表页；失败（超时/被拦截）时退避重试。返回 (posts, ctx, page)，posts=None 表示最终失败。"""
        for attempt in range(max_retries):
            posts = await self.scrape_list_page(page, stock_code, page_num)
            if posts is not None:
                return posts, ctx, page
            wait = random.uniform(1.0, 2.5) * (attempt + 1)
            print(f"  [{stock_code}] p{page_num} 获取失败，等待 {wait:.1f}s 后重试（第 {attempt + 1}/{max_retries} 次）")
            await asyncio.sleep(wait)
            await self.risk.wait()
        return None, ctx, page

    @staticmethod
    def _extract_total_pages(html: str, stock_code: str) -> int:
        """从列表页 HTML 的分页栏提取总页数（如 list,688061_91.html）。找不到返回 0。"""
        if not html: return 0
        nums = re.findall(rf"list,{re.escape(stock_code)}_(\d+)\.html", html)
        if not nums: return 0
        try: return max(int(n) for n in nums)
        except ValueError: return 0

    async def _find_max_page(self, ctx: Optional[BrowserContext], page: Page, browser: Browser,
                             stock_code: str) -> Tuple[int, BrowserContext, Page]:
        """确定页数上界。返回 (max_page, ctx, page)：0=首页无数据，-1=首页持续获取失败。
        接口的 count 是总帖数，历史段每页最多 50 条，故以 ceil(count/50) 作为安全上界；
        超出的页会在实际爬取时返回空页，按“到达数据末尾”处理。"""
        posts, total = await self._fetch_api_page(stock_code, 1)
        if posts is None:
            return -1, ctx, page
        if not posts:
            # 首页空可能是瞬时异常：复核一次再下结论
            await asyncio.sleep(random.uniform(1.0, 2.0))
            posts, total = await self._fetch_api_page(stock_code, 1)
            if posts is None:
                return -1, ctx, page
            if not posts:
                return 0, ctx, page
        upper = max(1, (total + API_MIN_PAGE_SIZE - 1) // API_MIN_PAGE_SIZE) if total > 0 else 1
        upper = min(upper, API_MAX_PAGE_LIMIT)
        print(f"[{stock_code}] 接口总帖数: {total}，页数上界: {upper}")
        return upper, ctx, page

    async def _binary_search_page(self, ctx: Optional[BrowserContext], page: Page, browser: Browser,
                                  stock_code: str, target_date: str, label: str, max_page: int = 10000,
                                  low_start: int = 1) -> Tuple[int, bool, BrowserContext, Page]:
        """二分定位目标日期页码。返回 (页码, found, ctx, page)。found=False 表示搜索范围内没有数据早于 target_date。
        low_start 为搜索下限：页码越大日期越老，结束页必然在起始页之后，故结束页搜索从起始页开始。
        加载失败或时间不可解析时抛出异常，保留原断点，避免将检索失败误判为日期边界。"""
        target = parse_time(target_date)
        if not target:
            raise ValueError(f"Invalid target date: {target_date}")
        high, low, result = max_page, low_start, low_start
        found = False
        print(f"[{stock_code}] 二分查找定位 {target_date[:10]} ({label})，搜索范围 {low}~{high}...")
        while low <= high:
            mid = (low + high) // 2
            posts, ctx, page = await self._fetch_retry(ctx, page, browser, stock_code, mid)
            if posts is None:
                raise RuntimeError(f"[{stock_code}] {label} p{mid} 加载失败，保留断点")
            if not posts:
                # 空页可能是限流假象：换新 context 复核一次，仍空才视为超出数据范围
                await asyncio.sleep(random.uniform(1.0, 2.5))
                ctx, page = await self._reset_context_page(ctx, browser)
                recheck, ctx, page = await self._fetch_retry(ctx, page, browser, stock_code, mid, max_retries=2)
                if recheck is None:
                    raise RuntimeError(f"[{stock_code}] {label} p{mid} 复核失败，保留断点")
                if not recheck:
                    # 复核仍为空：真正超出数据范围
                    high = mid - 1
                    await asyncio.sleep(random.uniform(0.05, 0.2))
                    continue
                posts = recheck
            page_time = self._first_valid_time(posts)
            if not page_time:
                # 无法解析日期不能作为二分边界。
                await asyncio.sleep(random.uniform(0.3, 0.8))
                posts, ctx, page = await self._fetch_retry(ctx, page, browser, stock_code, mid)
                if posts is None:
                    raise RuntimeError(f"[{stock_code}] {label} p{mid} 加载失败，保留断点")
                page_time = self._first_valid_time(posts) if posts else None
                if not page_time:
                    raise RuntimeError(f"[{stock_code}] {label} p{mid} 无有效时间，保留断点")
            print(f"  [{stock_code}] p{mid} -> {page_time.strftime('%Y-%m-%d %H:%M')}")
            if page_time > target:
                # 页面数据比目标日期新，需要往更老的页面（更大的页码）搜索
                low = mid + 1
            else:
                # 页面数据 <= 目标日期，记录此页并尝试往更新的页面（更小的页码）搜索
                result = mid
                found = True
                high = mid - 1
            await asyncio.sleep(random.uniform(0.1, 0.3))
        if found:
            print(f"[{stock_code}] 二分查找 {label} 完成：p{result}，found=True")
        else:
            print(f"[{stock_code}] 二分查找 {label} 完成：搜索范围内未找到早于 {target_date[:10]} 的数据，found=False")
        return max(1, result), found, ctx, page

    @staticmethod
    def _is_duplicate_page(prev_ids: set, cur_posts: List[Dict], threshold: float = 0.6) -> bool:
        if not prev_ids or not cur_posts: return False
        cur_ids = {p.get("post_id", "") for p in cur_posts if p.get("post_id")}
        if not cur_ids: return False
        return (len(prev_ids & cur_ids) / len(cur_ids)) >= threshold

    async def _reset_context_page(self, ctx: Optional[BrowserContext], browser: Browser) -> Tuple[BrowserContext, Page]:
        # 列表数据改走接口后不再需要浏览器上下文；保留该函数以兼容原有调用点
        if browser is None:
            return ctx, None
        try:
            if ctx: await ctx.close()
        except Exception: pass
        return await self._new_context_page(browser)

    async def crawl_stock(self, stock_code: str, db: DBManager) -> Dict:
        cutoff = parse_time(CUTOFF_DATE)
        start_date = parse_time(START_DATE)
        if not cutoff or not start_date or cutoff > start_date:
            raise ValueError("CUTOFF_DATE and START_DATE must define a valid date range")
        last_page, status = await db.get_progress(stock_code)
        
        # 已完成则跳过
        if status == "completed" and last_page > 0:
            print(f"[{stock_code}] 已完成，跳过")
            return {"stock_code": stock_code, "blocked": 0, "pages": 0, "saved": 0}

        browser: Optional[Browser] = None
        ctx: Optional[BrowserContext] = None
        page: Optional[Page] = None
        blocked_count, page_count, total_saved, cur_page = 0, 0, 0, max(last_page, 0)
        pending_records: List[Dict] = []
        pending_pages = 0
        committed_page = last_page
        processed_page = last_page

        async def flush_pending(last_page_to_save: int, progress_status: str, force_progress: bool = False) -> int:
            nonlocal pending_pages, total_saved, committed_page
            if not pending_records and not force_progress: return 0
            buffered_count = len(pending_records)
            checkpoint = max(committed_page, last_page_to_save)
            affected = await db.flush_comments_and_progress(
                stock_code=stock_code, 
                rows=pending_records, 
                last_page=checkpoint,
                status=progress_status
            )
            committed_page = checkpoint
            if buffered_count:
                total_saved += buffered_count
                print(f"[{stock_code}] flush 到 p{last_page_to_save}，提交 {buffered_count} 条，DB影响 {affected}，累计 {total_saved}")
            pending_records.clear()
            pending_pages = 0
            return affected

        try:
            # 先取首页确定页数上界；撞到风控时等全局冷却结束再重试
            fail_rounds = 0
            while True:
                actual_max_page, ctx, page = await self._find_max_page(ctx, page, browser, stock_code)
                if actual_max_page != -1:
                    break
                fail_rounds += 1
                if self.risk.is_cooling_down() and fail_rounds <= 5:
                    print(f"[{stock_code}] 首页获取失败，风控冷却中，等待后重试（第 {fail_rounds} 轮）")
                    await self.risk.wait()
                    continue
                print(f"[{stock_code}] 首页持续获取失败，标记 error，等待下次重试")
                await db.update_progress(stock_code, committed_page, "error")
                return {"stock_code": stock_code, "blocked": 0, "pages": 0, "saved": 0}
            if actual_max_page <= 0:
                print(f"[{stock_code}] 首页为空（复核后仍无数据），标记 error 等待下次重试")
                await db.update_progress(stock_code, committed_page, "error")
                return {"stock_code": stock_code, "blocked": 0, "pages": 0, "saved": 0}
            print(f"[{stock_code}] 页数上界: {actual_max_page}")

            # 日期锚点可恢复缺失/归零的进度，也能应对新帖导致的页码漂移。
            resume_time = await db.get_resume_time(stock_code)
            if resume_time:
                target_date = resume_time.strftime("%Y-%m-%d %H:%M:%S")
                start, found, ctx, page = await self._binary_search_page(
                    ctx, page, browser, stock_code, target_date, "恢复页", actual_max_page
                )
                # 日期定位失败时从范围起点重查，不凭空推算历史页码。
                if not found:
                    start, found, ctx, page = await self._binary_search_page(
                        ctx, page, browser, stock_code, START_DATE, "起始页", actual_max_page
                    )
                if not found:
                    await flush_pending(max(1, committed_page), "completed", force_progress=True)
                    print(f"[{stock_code}] 全部数据均晚于 {START_DATE[:10]}，无需爬取，标记完成")
                    return {"stock_code": stock_code, "blocked": 0, "pages": page_count, "saved": total_saved}
                start = max(1, start - RESUME_OVERLAP_PAGES)
                print(f"[{stock_code}] 根据已入库时间 {target_date} 恢复到 p{start}，原断点 p{last_page}")
            else:
                # 页码体系已从 PC 站（80条/页）切换为接口（100/50条/页），旧的 last_page 不可信，
                # 没有日期锚点时从 START_DATE 重新定位。
                await db.update_progress(stock_code, committed_page, "binary_searching")
                start, start_found, ctx, page = await self._binary_search_page(ctx, page, browser, stock_code, START_DATE, "起始页", actual_max_page)
                if not start_found:
                    await flush_pending(max(1, committed_page), "completed", force_progress=True)
                    print(f"[{stock_code}] 全部数据均晚于 {START_DATE[:10]}，无需爬取，标记完成")
                    return {"stock_code": stock_code, "blocked": 0, "pages": page_count, "saved": total_saved}
                start = max(1, start - RESUME_OVERLAP_PAGES)

            end, end_found, ctx, page = await self._binary_search_page(
                ctx, page, browser, stock_code, CUTOFF_DATE, "结束页",
                actual_max_page, low_start=start
            )
            end = min(end + RESUME_OVERLAP_PAGES, actual_max_page) if end_found else actual_max_page
            # 检索页尚未入库，不能将 start - 1 当作已提交断点。
            await db.update_progress(stock_code, committed_page, "in_progress")
            print(f"[{stock_code}] 爬取范围: 第{start}页 -> 第{end}页")

            # 正式爬取逻辑
            cur_page, prev_post_ids, dup_streak = start, set(), 0
            blocked_streak = 0  # 同一页连续被拦截次数
            while cur_page <= end:
                posts = await self.scrape_list_page(page, stock_code, cur_page)
                page_count += 1

                # 加载失败（超时/被拦截）：换 context 重试，不立即计入空页
                if posts is None:
                    posts, ctx, page = await self._fetch_retry(ctx, page, browser, stock_code, cur_page, max_retries=2)
                    if posts is None:
                        if self.risk.is_cooling_down():
                            blocked_count += 1
                            blocked_streak += 1
                            if blocked_streak >= MAX_DUP_RETRIES:
                                # 同一页持续被拦截：保存进度标记 error，下次续爬，避免无限卡死
                                await flush_pending(processed_page, "error", force_progress=True)
                                print(f"[{stock_code}] p{cur_page} 持续被拦截 {blocked_streak} 次，暂停并标记 error，下次续爬")
                                break
                            print(f"[{stock_code}] p{cur_page} 被拦截，等待全局冷却后重试")
                            await self.risk.wait()
                            continue
                        await flush_pending(processed_page, "error", force_progress=True)
                        print(f"[{stock_code}] p{cur_page} 持续加载失败，保留断点等待续爬")
                        break

                # 空页复核：接口按发帖时间倒序返回，空页通常意味着已越过可访问数据末尾；
                # 连续复核两次仍为空才确认到底，避免瞬时空响应造成假边界。
                if not posts:
                    for recheck in range(2):
                        await asyncio.sleep(random.uniform(0.5, 1.0) * (recheck + 1))
                        posts, _ = await self._fetch_api_page(stock_code, cur_page)
                        if posts or posts is None:
                            break
                    if posts is None:
                        await flush_pending(processed_page, "error", force_progress=True)
                        print(f"[{stock_code}] p{cur_page} 复核请求失败，保留断点等待续爬")
                        break
                    if not posts:
                        await flush_pending(max(1, processed_page), "completed", force_progress=True)
                        print(f"[{stock_code}] 已到接口可访问数据末尾（p{cur_page} 连续为空），完成")
                        break

                # 重置空页/拦截计数
                blocked_streak = 0
                
                # 重复页检测（反爬）
                if self._is_duplicate_page(prev_post_ids, posts):
                    dup_streak += 1
                    blocked_count += 1
                    if dup_streak >= MAX_DUP_RETRIES:
                        await flush_pending(processed_page, "error", force_progress=True)
                        print(f"[{stock_code}] p{cur_page} 连续重复 {dup_streak} 次，保留断点等待续爬")
                        break
                    wait = random.uniform(3, 8) * dup_streak
                    print(f"[{stock_code}] p{cur_page} 疑似静默反爬重复页，第 {dup_streak} 次，等待 {wait:.0f}s")
                    await asyncio.sleep(wait)
                    ctx, page = await self._reset_context_page(ctx, browser)
                    continue

                # 重置重复页计数
                dup_streak = 0
                
                # 处理当前页数据
                cur_post_ids = {p.get("post_id", "") for p in posts if p.get("post_id")}
                records: List[Dict] = []
                valid_times = [
                    parse_time(post.get("post_publish_time", "")) for post in posts
                ]
                if not any(valid_times):
                    await flush_pending(processed_page, "error", force_progress=True)
                    print(f"[{stock_code}] p{cur_page} 无有效时间，保留断点等待续爬")
                    break
                # 单条旧帖可能是置顶帖；整页均早于截止时间才确认完成。
                parsed_times = [t for t in valid_times if t is not None]
                hit_cutoff = bool(parsed_times) and all(t < cutoff for t in parsed_times)
                
                for post in posts:
                    pub_time = post.get("post_publish_time", "")
                    pt = parse_time(pub_time) if pub_time else None
                    if pt and (pt < cutoff or pt > start_date):
                        continue
                    content, pid = post.get("post_content", ""), post.get("post_id", "")
                    if not pid or not content:
                        continue
                    records.append({
                        "platform": "东方财富", 
                        "stock_code": stock_code, 
                        "post_id": pid, 
                        "post_content": content[:1500], 
                        "post_publish_time": pub_time, 
                        "post_url": post.get("url", ""),
                        "author_id": post.get("author_id", ""),
                        "read_count": post.get("read_count", "0"),
                        "comment_count": post.get("comment_count", "0"),
                        "like_count": post.get("like_count", "0"),
                        "share_count": post.get("share_count", "0")
                    })

                # 累加待提交数据
                pending_records.extend(records)
                pending_pages += 1
                processed_page = cur_page
                saved = len(records)
                prev_post_ids = cur_post_ids
                
                # 判断是否需要提交
                should_flush = hit_cutoff or pending_pages >= DB_BATCH_PAGES or len(pending_records) >= DB_BATCH_ROWS
                affected = 0
                if should_flush:
                    affected = await flush_pending(
                        cur_page, 
                        "completed" if hit_cutoff else "in_progress", 
                        force_progress=True
                    )
                
                print(f"[{stock_code}] p{cur_page}/{end} 写入候选 {saved} 条，DB影响 {affected}，累计 {total_saved}")
                
                # 到达截止日期则终止
                if hit_cutoff:
                    print(f"[{stock_code}] 到达截止日期 {CUTOFF_DATE}，完成")
                    break
                
                cur_page += 1
                await asyncio.sleep(random.uniform(0.15, 0.45))

            # 爬取完成，提交剩余数据并标记完成
            if cur_page > end:
                await flush_pending(end, "completed", force_progress=True)

        except Exception as e:
            print(f"[{stock_code}] 异常: {e}")
            traceback.print_exc()
            # 缓存可能尚未提交，异常时只能记录最后一次成功提交的页码。
            await db.update_progress(stock_code, committed_page, "error")
        finally:
            print(f"[{stock_code}] 结束，共写入候选 {total_saved} 条")
        
        return {
            "stock_code": stock_code, 
            "blocked": blocked_count, 
            "pages": page_count, 
            "saved": total_saved
        }

# =================== 入口调度 ===================
async def main():
    if not os.path.exists(STOCK_CODE_FILE):
        raise FileNotFoundError(f"找不到股票代码文件: {STOCK_CODE_FILE}")
    with open(STOCK_CODE_FILE, "r", encoding="utf-8") as f:
        codes = list(dict.fromkeys(line.strip() for line in f if line.strip()))
    print(f"共 {len(codes)} 只股票待爬取")
    print(f"配置：MAX_CONCURRENT={MAX_CONCURRENT}, RATE_LIMIT_INTERVAL={RATE_LIMIT_INTERVAL}，数据源=移动版股吧接口（{API_PAGE_SIZE}条/页）")

    db = DBManager()
    await db.init()
    controller = AdaptiveConcurrencyController(
        initial=MAX_CONCURRENT,
        minimum=MIN_CONCURRENT,
        maximum=MAX_CONCURRENT_LIMIT
    )
    rate_limiter = RateLimiter(RATE_LIMIT_INTERVAL)
    risk = RiskControl()
    crawler = StockCommentCrawler(rate_limiter=rate_limiter, risk_control=risk)

    i, total_codes, active_tasks = 0, len(codes), {}
    try:
        while i < total_codes or active_tasks:
            current_limit = controller.get_limit()
            # 启动新任务（不超过并发限制）
            while i < total_codes and len(active_tasks) < current_limit:
                code = codes[i]
                task = asyncio.create_task(crawler.crawl_stock(code, db))
                active_tasks[task] = code
                print(f"[调度] 启动 {code}，运行中 {len(active_tasks)}/{current_limit}，进度 {i + 1}/{total_codes}")
                i += 1
            if not active_tasks:
                break
            # 等待任务完成
            done, _ = await asyncio.wait(active_tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                code = active_tasks.pop(task)
                try:
                    result = task.result()
                    if isinstance(result, dict):
                        controller.update(result.get("blocked", 0), result.get("pages", 0))
                        print(f"[调度] 完成 {code}，pages={result.get('pages', 0)}，blocked={result.get('blocked', 0)}，saved={result.get('saved', 0)}，剩余运行中 {len(active_tasks)}")
                except Exception as e:
                    print(f"[调度] {code} 任务异常: {e}")
            await asyncio.sleep(random.uniform(0.02, 0.08))
    finally:
        # 取消剩余任务
        for task in active_tasks:
            task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)
    await db.close()
    print("\n全部爬取完成！")

if __name__ == "__main__":
    asyncio.run(main())
