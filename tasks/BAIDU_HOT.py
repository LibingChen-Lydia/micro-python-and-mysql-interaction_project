#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
抓取百度热搜 TopN → 写入 MySQL。
- 建表：若不存在自动创建（InnoDB + utf8mb4）
- 幂等：同一天同标题唯一（重复则更新 url 与抓取时间）
- 解析：优先解析页面内注入的 JSON；失败再回退 HTML 解析
"""

import argparse
import datetime
import json
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from mysql_helper import MySqlHelper

BAIDU_REALTIME_URL = "https://top.baidu.com/board?platform=pc&tab=realtime"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36")
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS baidu_hotsearch (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  rank_no      INT UNSIGNED NOT NULL,
  title        VARCHAR(255)  NOT NULL,
  url          VARCHAR(500)  NULL,
  grabbed_at   DATETIME      NOT NULL,
  grabbed_date DATE          AS (DATE(grabbed_at)) STORED,
  UNIQUE KEY uk_title_day (title, grabbed_date),
  KEY idx_time (grabbed_at),
  KEY idx_rank_day (rank_no, grabbed_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

# ---------- 抓取与解析 ----------

def fetch_html(timeout: int = 15) -> str:
    resp = requests.get(BAIDU_REALTIME_URL, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text

def parse_from_initial_state(html: str) -> Optional[List[Dict[str, Any]]]:
    m = re.search(r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;\s*</script>", html, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
    except Exception:
        return None

    # 递归挖出 items
    def dig(obj):
        if isinstance(obj, dict):
            if "items" in obj and isinstance(obj["items"], list):
                return obj["items"]
            for v in obj.values():
                r = dig(v)
                if r is not None:
                    return r
        elif isinstance(obj, list):
            for v in obj:
                r = dig(v)
                if r is not None:
                    return r
        return None

    raw = dig(data)
    if not raw:
        return None

    items = []
    for i, it in enumerate(raw, start=1):
        title = (it.get("word") or it.get("query") or it.get("title") or it.get("content") or "").strip()
        if not title:
            continue
        url = it.get("url") or it.get("appUrl") or it.get("redirectUrl")
        items.append({"rank": i, "title": title, "url": url})
    return items

def parse_from_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(".category-wrap_iQLoo .content_1YWBm")
    out = []
    rank = 1
    for card in cards:
        title_el = card.select_one(".c-single-text-ellipsis") or card.find("a")
        title = (title_el.get_text(strip=True) if title_el else "").strip()
        if not title:
            continue
        link_el = card.find("a", href=True)
        url = urljoin(BAIDU_REALTIME_URL, link_el["href"]) if link_el else None
        out.append({"rank": rank, "title": title, "url": url})
        rank += 1
    return out

def get_top_n(n: int = 10) -> List[Dict[str, Any]]:
    html = fetch_html()
    items = parse_from_initial_state(html) or []
    if len(items) < n:
        items = parse_from_html(html) or []
    return items[:n]

# ---------- 入库 ----------

def ensure_table(db: MySqlHelper):
    db.execute_non_query(CREATE_TABLE_SQL)

def save_items(db: MySqlHelper, items: List[Dict[str, Any]]) -> int:
    now = datetime.datetime.now()
    sql = """
    INSERT INTO baidu_hotsearch
      (rank_no, title, url, grabbed_at)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      url        = VALUES(url),
      grabbed_at = VALUES(grabbed_at);
    """
    params = [(it["rank"], it["title"], it.get("url"), now)
              for it in items if it.get("title")]
    if not params:
        return 0
    return db.execute_many(sql, params)

# ---------- CLI ----------

def build_parser():
    p = argparse.ArgumentParser(description="Fetch Baidu Hot Search TopN and save to MySQL")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default=None)
    p.add_argument("--database", "-d", required=True)
    p.add_argument("--top", type=int, default=10, help="How many to save (default 10)")
    p.add_argument("--print-only", action="store_true", help="Only print results, do not write DB")
    return p

def main():
    args = build_parser().parse_args()

    # 拉取与展示
    items = get_top_n(args.top)
    if not items:
        print("❌ 未抓到数据（页面结构可能变动或网络失败）")
        return
    print(f"抓到 {len(items)} 条：")
    for it in items:
        print(f"{it['rank']:>2} | {it['title'][:50]} | url={it.get('url')}")

    if args.print_only:
        return

    # 写库
    db = MySqlHelper(
        host=args.host, port=args.port, user=args.user,
        password=args.password, database=args.database,
        charset="utf8mb4"
    )
    try:
        ensure_table(db)
        n = save_items(db, items)
        print(f"✅ 入库完成：{n} 条（幂等）")
    finally:
        db.close()

if __name__ == "__main__":
    main()
