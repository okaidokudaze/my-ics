#!/usr/bin/env python3
"""
GitHub上のICS（source of truth）→ Notion Database へ自動同期するスクリプト（安全版）

狙い：
- ICSのDESCRIPTIONに入れたラベル（種別/大会/ラウンド/区分/確度/出典/対戦/スコア 等）を優先してNotionへ反映
- Notionの「カード（title）」は、可能なら必ず「対戦カード（ホーム vs アウェイ）」にする
- DBのプロパティ名が違っても落ちないように、DBスキーマを取得して「存在するプロパティだけ」更新する

環境変数:
- NOTION_TOKEN
- NOTION_DATABASE_ID
- ICS_PATH (任意, デフォルト: soccer_osaka_hs_boys.ics)
"""

import os
import sys
import re
import hashlib
from datetime import datetime, timezone, date
from zoneinfo import ZoneInfo
from notion_client import Client
from notion_client.errors import APIResponseError
from icalendar import Calendar

JST = ZoneInfo("Asia/Tokyo")

# ---------------------------
# Utility
# ---------------------------

def now_jst() -> datetime:
    return datetime.now(tz=JST)

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def norm_text(s: str) -> str:
    return (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()

def unescape_desc(s: str) -> str:
    """
    icalendarで読むと DESCRIPTION が '\\n' のまま入ってくることがあるので改行化。
    """
    s = norm_text(s)
    # まず literal "\n" を改行へ
    s = s.replace("\\n", "\n").replace("\\N", "\n")
    return s

def safe_dt(v) -> datetime | None:
    """
    icalendarは date or datetime を返す。
    """
    if v is None:
        return None
    if hasattr(v, "dt"):
        v = v.dt
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=JST)
        return v.astimezone(JST)
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, 0, 0, tzinfo=JST)
    return None

def to_notion_date(dt: datetime | None) -> dict:
    if dt is None:
        return None
    return {"start": dt.isoformat()}

# ---------------------------
# DESCRIPTION parsing
# ---------------------------

LABELS = {
    "種別": "type",
    "大会": "tournament",
    "ラウンド": "round",
    "節/ラウンド": "round",
    "節／ラウンド": "round",
    "区分": "kind",
    "確度": "certainty",
    "出典": "source",
    "対戦": "matchup",
    "ホーム": "home",
    "アウェイ": "away",
    "スコア": "score",
}

_vs_re = re.compile(r"\s*(.+?)\s*(?:vs\.?|VS\.?|ｖｓ|v|V)\s*(.+?)\s*$")

def parse_description(desc: str) -> dict:
    out = {k: "" for k in ["type","tournament","round","kind","certainty","source","matchup","home","away","score"]}
    txt = unescape_desc(desc)

    for raw in txt.split("\n"):
        line = raw.strip()
        if not line:
            continue

        # 「キー：値」「キー:値」両対応
        m = re.match(r"^([^：:]+)[：:]\s*(.*)$", line)
        if not m:
            continue
        key = m.group(1).strip()
        val = m.group(2).strip()

        if key in LABELS:
            out[LABELS[key]] = val

    # matchup から home/away 推定
    if out["matchup"] and (not out["home"] or not out["away"]):
        mm = _vs_re.match(out["matchup"].replace("　", " "))
        if mm:
            out["home"] = out["home"] or mm.group(1).strip()
            out["away"] = out["away"] or mm.group(2).strip()

    # home/away から matchup 補完
    if (out["home"] and out["away"]) and not out["matchup"]:
        out["matchup"] = f"{out['home']} vs {out['away']}"

    return out

def infer_from_summary(summary: str) -> dict:
    """
    SUMMARYから最低限の大会/ラウンド推定（大阪府／{大会}／{ラウンド} …想定）
    """
    out = {"tournament": "", "round": ""}
    s = (summary or "").strip()
    if "／" in s:
        parts = [p.strip() for p in s.split("／")]
        # parts[0] = 大阪府
        if len(parts) >= 2:
            out["tournament"] = parts[1]
        if len(parts) >= 3:
            out["round"] = parts[2]
    return out

_score_re = re.compile(r"(\d+\s*-\s*\d+)(?:\s*\(.*?\))?")

def clean_score(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # 「1-3」「2-2（PK4-5）」等を雑に許容（文字はそのままでもOK）
    return s

# ---------------------------
# Notion schema helpers
# ---------------------------

def get_db_schema(notion: Client, database_id: str) -> tuple[str, dict]:
    db = notion.databases.retrieve(database_id=database_id)
    props = db.get("properties", {}) or {}
    title_name = None
    for name, meta in props.items():
        if meta.get("type") == "title":
            title_name = name
            break
    if not title_name:
        raise RuntimeError("Notion DBに title プロパティが見つかりません。")
    return title_name, props

def rt(content: str) -> dict:
    content = (content or "").strip()
    if not content:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": content}}]}

def title_prop(name: str, content: str) -> dict:
    content = (content or "").strip()
    if not content:
        content = "（未入力）"
    return {name: {"title": [{"text": {"content": content}}]}}

def date_prop(dt: datetime | None) -> dict:
    if dt is None:
        return {"date": None}
    return {"date": {"start": dt.isoformat()}}

def select_prop(val: str) -> dict:
    val = (val or "").strip()
    if not val:
        return {"select": None}
    return {"select": {"name": val}}

# ---------------------------
# Build item from VEVENT
# ---------------------------

def build_item(vevent) -> dict:
    uid = str(vevent.get("UID", "")).strip()
    summary = str(vevent.get("SUMMARY", "")).strip()
    location = str(vevent.get("LOCATION", "")).strip()

    desc_val = vevent.get("DESCRIPTION")
    desc = str(desc_val) if desc_val is not None else ""
    parsed = parse_description(desc)

    dtstart = safe_dt(vevent.get("DTSTART"))
    dtend = safe_dt(vevent.get("DTEND"))

    if dtstart is None:
        raise ValueError("DTSTARTがありません: UID=" + uid)

    inferred = infer_from_summary(summary)

    tournament = parsed["tournament"] or inferred["tournament"] or summary
    round_name = parsed["round"] or inferred["round"] or ""

    # 区分
    kind = parsed["kind"].strip()
    if kind not in ("予定", "結果"):
        kind = "予定" if dtstart > now_jst() else "結果"

    # 確度
    certainty = parsed["certainty"].strip()
    if certainty not in ("確定", "暫定", "未定"):
        # locationが空/会場不明なら未定、そうでなければ暫定
        certainty = "未定" if (not location or "会場不明" in location) else "暫定"

    # 対戦/スコア
    matchup = parsed["matchup"].strip()
    home = parsed["home"].strip()
    away = parsed["away"].strip()
    score = clean_score(parsed["score"].strip())

    # タイトル（カード）
    card_title = ""
    if matchup:
        card_title = matchup
        if kind == "結果" and score:
            card_title = f"{card_title}／{score}"
    else:
        # 対戦が無い場合はSUMMARY（既存運用維持）
        card_title = summary or f"{tournament}／{round_name}"

    source = parsed["source"].strip()
    match_type = parsed["type"].strip()

    fingerprint = sha1("|".join([
        uid,
        card_title,
        summary,
        location,
        dtstart.isoformat(),
        dtend.isoformat() if dtend else "",
        tournament,
        round_name,
        kind,
        certainty,
        match_type,
        matchup,
        home,
        away,
        score,
        source,
    ]))

    return {
        "uid": uid,
        "summary": summary,
        "card_title": card_title,
        "dtstart": dtstart,
        "dtend": dtend,
        "location": location,
        "tournament": tournament,
        "round": round_name,
        "kind": kind,
        "certainty": certainty,
        "type": match_type,
        "matchup": matchup,
        "home": home,
        "away": away,
        "score": score,
        "source": source,
        "fingerprint": fingerprint,
    }

# ---------------------------
# Notion mapping
# ---------------------------

def build_notion_props(item: dict, title_prop_name: str, db_props: dict) -> dict:
    props_out = {}

    # title
    props_out.update(title_prop(title_prop_name, item["card_title"]))

    # helper to add if property exists & type matches roughly
    def put(name: str, payload: dict):
        if name not in db_props:
            return
        props_out[name] = payload

    put("開始", date_prop(item["dtstart"]))
    put("終了", date_prop(item["dtend"]))
    put("大会", rt(item["tournament"]))
    put("節／ラウンド", rt(item["round"]))
    put("節/ラウンド", rt(item["round"]))  # 表記違いの保険
    put("会場", rt(item["location"]))
    put("区分", select_prop(item["kind"]))
    put("確度", select_prop(item["certainty"]))
    put("更新日", date_prop(now_jst()))
    put("UID", rt(item["uid"]))
    put("出典", rt(item["source"]))
    put("fingerprint", rt(item["fingerprint"]))

    # 任意（DBに存在する時だけ）
    put("種別", select_prop(item["type"]))
    put("ホーム", rt(item["home"]))
    put("アウェイ", rt(item["away"]))
    put("スコア", rt(item["score"]))
    put("対戦", rt(item["matchup"]))

    return props_out

def page_uid(page: dict, uid_prop: str = "UID") -> str:
    try:
        rt = page["properties"][uid_prop]["rich_text"]
        if rt:
            return rt[0]["plain_text"].strip()
    except Exception:
        return ""
    return ""

def page_fp(page: dict, fp_prop: str = "fingerprint") -> str:
    try:
        rt = page["properties"][fp_prop]["rich_text"]
        if rt:
            return rt[0]["plain_text"].strip()
    except Exception:
        return ""
    return ""

def fetch_all_pages(notion: Client, database_id: str) -> list:
    pages = []
    cursor = None
    while True:
        if cursor:
            resp = notion.databases.query(database_id=database_id, start_cursor=cursor)
        else:
            resp = notion.databases.query(database_id=database_id)
        pages.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break
    return pages

# ---------------------------
# Main
# ---------------------------

def main() -> int:
    token = os.getenv("NOTION_TOKEN", "").strip()
    dbid = os.getenv("NOTION_DATABASE_ID", "").strip()
    ics_path = os.getenv("ICS_PATH", "soccer_osaka_hs_boys.ics").strip()

    if not token or not dbid:
        print("ERROR: NOTION_TOKEN と NOTION_DATABASE_ID が必要です。", file=sys.stderr)
        return 2

    if not os.path.exists(ics_path):
        print(f"ERROR: ICS_PATH が見つかりません: {ics_path}", file=sys.stderr)
        return 2

    with open(ics_path, "rb") as f:
        cal = Calendar.from_ical(f.read())

    items = []
    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue
        it = build_item(comp)
        if it["uid"]:
            items.append(it)

    notion = Client(auth=token)

    # DBスキーマ取得（title名の特定＆存在プロパティだけ更新するため）
    title_name, db_props = get_db_schema(notion, dbid)

    # existing pages
    pages = fetch_all_pages(notion, dbid)
    by_uid = {}
    for pg in pages:
        uid = page_uid(pg, "UID")
        if uid:
            by_uid[uid] = pg

    created = updated = skipped = 0

    for it in items:
        uid = it["uid"]
        props = build_notion_props(it, title_name, db_props)

        if uid not in by_uid:
            notion.pages.create(parent={"database_id": dbid}, properties=props)
            created += 1
            continue

        pg = by_uid[uid]
        old_fp = page_fp(pg, "fingerprint")
        if old_fp == it["fingerprint"]:
            skipped += 1
            continue

        notion.pages.update(page_id=pg["id"], properties=props)
        updated += 1

    print(f"OK: created={created}, updated={updated}, skipped={skipped}, total_ics_events={len(items)}, total_notion_pages={len(pages)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
