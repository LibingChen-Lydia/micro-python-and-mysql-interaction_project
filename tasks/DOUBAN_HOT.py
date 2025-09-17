#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Douban Top100 → MySQL (单文件版)
- 第一次运行会自动在目标库里创建表（IF NOT EXISTS）
- 解析列表页（前4页=100条），提取：排名/标题/原名/年份/评分/投票/导演/详情URL
- 维表：类型/国家（可视化常用维度）；通过多对多映射表关联
- 幂等：主表以 douban_id 唯一，重复抓取会更新相关字段
"""

import argparse
import datetime
import re
import time
from typing import List, Dict, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from mysql_helper import MySqlHelper

BASE_URL = "https://movie.douban.com/top250"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Referer": "https://movie.douban.com/"
}

# -------------------- 建表 SQL（统一 douban_* 前缀） --------------------

CREATE_MOVIES = """
CREATE TABLE IF NOT EXISTS douban_movies (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  douban_id VARCHAR(20) NOT NULL,
  rank_no INT UNSIGNED NOT NULL,
  title VARCHAR(255) NOT NULL,
  original_title VARCHAR(255) NULL,
  year INT UNSIGNED NULL,
  rating DECIMAL(3,1) NULL,
  votes INT UNSIGNED NULL,
  director VARCHAR(255) NULL,
  url VARCHAR(500) NULL,
  poster_url VARCHAR(500) NULL,
  summary TEXT NULL,
  grabbed_at DATETIME NOT NULL,
  grabbed_date DATE AS (DATE(grabbed_at)) STORED,
  UNIQUE KEY uk_douban (douban_id),
  KEY idx_year (year),
  KEY idx_rating (rating),
  KEY idx_votes (votes)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

CREATE_GENRE = """
CREATE TABLE IF NOT EXISTS douban_genre (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

CREATE_MAP_MOVIE_GENRE = """
CREATE TABLE IF NOT EXISTS douban_movie_genre (
  movie_id BIGINT UNSIGNED NOT NULL,
  genre_id INT UNSIGNED NOT NULL,
  PRIMARY KEY (movie_id, genre_id),
  CONSTRAINT fk_mg_movie FOREIGN KEY (movie_id) REFERENCES douban_movies(id) ON DELETE CASCADE,
  CONSTRAINT fk_mg_genre FOREIGN KEY (genre_id) REFERENCES douban_genre(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

CREATE_COUNTRY = """
CREATE TABLE IF NOT EXISTS douban_country (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(80) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

CREATE_MAP_MOVIE_COUNTRY = """
CREATE TABLE IF NOT EXISTS douban_movie_country (
  movie_id BIGINT UNSIGNED NOT NULL,
  country_id INT UNSIGNED NOT NULL,
  PRIMARY KEY (movie_id, country_id),
  CONSTRAINT fk_mc_movie FOREIGN KEY (movie_id) REFERENCES douban_movies(id) ON DELETE CASCADE,
  CONSTRAINT fk_mc_country FOREIGN KEY (country_id) REFERENCES douban_country(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

def ensure_schema(db: MySqlHelper):
    """一次性确保5张表存在（与你的 ensure_table 配合）"""
    db.ensure_table(CREATE_MOVIES, table_name="douban_movies")
    db.ensure_table(CREATE_GENRE, table_name="douban_genre")
    db.ensure_table(CREATE_COUNTRY, table_name="douban_country")
    db.ensure_table(CREATE_MAP_MOVIE_GENRE, table_name="douban_movie_genre")
    db.ensure_table(CREATE_MAP_MOVIE_COUNTRY, table_name="douban_movie_country")
    print("✅ Schema ensured: douban_movies / douban_genre / douban_country / douban_movie_genre / douban_movie_country")

# -------------------- 抓取与解析 --------------------

def fetch_list_page(start: int, timeout: int = 15) -> str:
    resp = requests.get(BASE_URL, params={"start": start, "filter": ""}, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text

def parse_list(html: str) -> List[Dict]:
    """
    解析列表页的一项项电影卡片。
    返回字段：rank_no, douban_id, title, original_title, year, rating, votes, director, countries(list), genres(list), url
    """
    soup = BeautifulSoup(html, "lxml")
    items: List[Dict] = []

    for div in soup.select("div.item"):
        # rank
        em = div.select_one("em")
        rank_no = int(em.get_text(strip=True)) if em and em.get_text(strip=True).isdigit() else None

        # 标题与链接
        a = div.select_one("div.hd a")
        if not a:
            continue
        url = a["href"]
        m = re.search(r"/subject/(\d+)/", url)
        douban_id = m.group(1) if m else None

        title_spans = div.select("span.title")
        title = title_spans[0].get_text(strip=True) if title_spans else a.get_text(strip=True)

        # 原名（可选）
        original_title = None
        if len(title_spans) > 1:
            t2 = title_spans[1].get_text(strip=True)
            original_title = t2.strip(" /") if t2 else None
        else:
            other = div.select_one("span.other")
            if other:
                original_title = other.get_text(strip=True).strip(" /")

        # 评分与投票
        rating = None
        rn = div.select_one("span.rating_num")
        if rn:
            try:
                rating = float(rn.get_text(strip=True))
            except:
                rating = None

        votes = None
        pv = div.find("span", string=re.compile(r"评价"))
        if pv:
            mv = re.search(r"(\d[\d,]*)", pv.get_text())
            if mv:
                votes = int(mv.group(1).replace(",", ""))

        # 信息块（导演/主演；年份/国家/类型）
        info = div.select_one("div.bd p").get_text("\n", strip=True) if div.select_one("div.bd p") else ""
        parts = info.split("\n")
        director = None
        if parts:
            md = re.search(r"导演[:：]\s*([^/]+)", parts[0])
            director = (md.group(1).strip() if md else parts[0].split("主演")[0]).strip()
            director = re.sub(r"\s+", " ", director)

        # 第二行一般 "1994 / 美国 / 犯罪 剧情"
        countries, genres, year = [], [], None
        if len(parts) > 1:
            segs = [s.strip() for s in parts[1].split("/") if s.strip()]
            # 年份
            y = re.search(r"(\d{4})", " ".join(segs))
            if y:
                year = int(y.group(1))
            # 含空格的继续拆
            cands: List[str] = []
            for s in segs:
                if re.fullmatch(r"\d{4}", s):
                    continue
                if " " in s:
                    cands.extend([x for x in s.split() if x])
                else:
                    cands.append(s)

            # 粗略分拣：类型词 → genres；其余 → countries
            type_words = {
                "剧情","喜剧","动作","爱情","科幻","动画","悬疑","惊悚","恐怖","纪录片","短片",
                "情色","同性","音乐","歌舞","传记","历史","战争","西部","奇幻","冒险","灾难",
                "武侠","古装","犯罪","家庭","儿童","运动","真人秀","脱口秀"
            }
            for w in cands:
                if w in type_words or re.search(r"^(Animation|Comedy|Action|Romance|Sci[- ]?Fi|Mystery|Thriller|Horror|Documentary|Short|Biography|History|War|Western|Fantasy|Adventure|Crime|Family|Music|Musical)$", w, re.I):
                    genres.append(w)
                else:
                    countries.append(w)

        items.append({
            "rank_no": rank_no,
            "douban_id": douban_id,
            "title": title,
            "original_title": original_title,
            "year": year,
            "rating": rating,
            "votes": votes,
            "director": director,
            "countries": countries,
            "genres": genres,
            "url": url
        })

    return items

def crawl_top_n(n: int = 100, sleep_sec: Tuple[float, float] = (1.2, 2.5)) -> List[Dict]:
    """抓取前 n 条（Top100=4页，每页25条）"""
    pages = (n + 24) // 25
    all_items: List[Dict] = []
    for i in range(pages):
        start = i * 25
        html = fetch_list_page(start)
        page_items = parse_list(html)
        all_items.extend(page_items)
        time.sleep((sleep_sec[0] + sleep_sec[1]) / 2.0)

    # 去重（按 douban_id）
    seen, uniq = set(), []
    for it in all_items:
        key = it.get("douban_id") or (it["title"], it.get("year"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(it)

    uniq.sort(key=lambda x: x["rank_no"] or 9999)
    return uniq[:n]

# -------------------- 入库（Upsert + 维表映射） --------------------

UPSERT_MOVIE_SQL = """
INSERT INTO douban_movies
  (douban_id, rank_no, title, original_title, year, rating, votes, director, url, grabbed_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  rank_no=VALUES(rank_no),
  title=VALUES(title),
  original_title=VALUES(original_title),
  year=VALUES(year),
  rating=VALUES(rating),
  votes=VALUES(votes),
  director=VALUES(director),
  url=VALUES(url),
  grabbed_at=VALUES(grabbed_at);
""".strip()

def upsert_movie(db: MySqlHelper, m: Dict) -> int:
    """插/改 douban_movies，返回 movie_id"""
    now = datetime.datetime.now()
    db.execute_non_query(UPSERT_MOVIE_SQL, (
        m.get("douban_id"),
        m.get("rank_no"),
        m.get("title"),
        m.get("original_title"),
        m.get("year"),
        m.get("rating"),
        m.get("votes"),
        m.get("director"),
        m.get("url"),
        now
    ))
    row = db.execute_query("SELECT id FROM douban_movies WHERE douban_id=%s", (m.get("douban_id"),))
    return row[0]["id"]

def ensure_dim_and_map(
    db: MySqlHelper,
    movie_id: int,
    names: List[str],
    dim_table: str,
    map_table: str,
    map_fk_col: str,  # "genre_id" 或 "country_id"
) -> None:
    """把 names 写入维表，并与 movie 建映射（主键(movie_id, *_id) 保证幂等）"""
    # 去重 + 清洗
    clean_names = []
    seen = set()
    for n in names or []:
        n = (n or "").strip()
        if not n:
            continue
        if n in seen:
            continue
        seen.add(n)
        clean_names.append(n)

    if not clean_names:
        return

    with db._get_cursor() as cur:
        for name in clean_names:
            # 1) 维表 upsert，并把现有 id 写入 LAST_INSERT_ID()，这样无论插入还是命中唯一键，
            #    cur.lastrowid 都会得到该行的主键 id
            cur.execute(
                f"""
                INSERT INTO {dim_table}(name) VALUES (%s)
                ON DUPLICATE KEY UPDATE
                  id = LAST_INSERT_ID(id),   -- 关键点：复用已有 id
                  name = VALUES(name)
                """,
                (name,),
            )
            dim_id = cur.lastrowid  # 这里即可拿到 id

            # 2) 映射表去重插入
            cur.execute(
                f"INSERT IGNORE INTO {map_table}(movie_id, {map_fk_col}) VALUES (%s, %s)",
                (movie_id, dim_id),
            )

# -------------------- CLI --------------------

def build_parser():
    p = argparse.ArgumentParser(description="Crawl Douban Top100 and save to MySQL (auto-create tables)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default=None)
    p.add_argument("--database", "-d", required=True)
    p.add_argument("--top", type=int, default=100, help="How many to fetch (default 100)")
    p.add_argument("--print-only", action="store_true", help="Only print results, do not write DB")
    return p

def main():
    args = build_parser().parse_args()

    # 1) 抓取
    movies = crawl_top_n(args.top)
    print(f"抓到 {len(movies)} 条；预览前 5 条：")
    for m in movies[:5]:
        print(f"- #{m['rank_no']:>3} {m['title']} ({m.get('year')})  rating={m.get('rating')}  votes={m.get('votes')}  dir={m.get('director')}")

    if args.print_only:
        return

    # 2) 写库（自动建表）
    db = MySqlHelper(
        host=args.host, port=args.port, user=args.user,
        password=args.password, database=args.database,
        charset="utf8mb4"
    )
    try:
        ensure_schema(db)
        for m in movies:
            mid = upsert_movie(db, m)
            # 维表映射（已统一成 douban_* 表名）
            ensure_dim_and_map(db, mid, m.get("genres", []),    "douban_genre",   "douban_movie_genre",   "genre_id")
            ensure_dim_and_map(db, mid, m.get("countries", []), "douban_country", "douban_movie_country", "country_id")
        print("✅ Top100 入库完成（幂等）。")
    finally:
        db.close()

if __name__ == "__main__":
    main()