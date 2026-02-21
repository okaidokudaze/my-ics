#!/usr/bin/env python3
"""
GitHub上のICS（source of truth）→ Notion Database へ自動同期するスクリプト（改修版）

目的（表示崩れの修正）:
- Notionの「カード(title)」を対戦カード（ホーム vs アウェイ／結果ならスコア付き）にする
- Notionの「節／ラウンド」に、DESCRIPTIONの「ラウンド：」を入れる（旧仕様の「区分：」誤流し込みを廃止）
- Notionの「区分」はDESCRIPTIONの「区分：予定/結果」を優先（日時の未来/過去判定で強制しない）
- Notionの「確度」はDESCRIPTIONの「確度：確定/暫定/未定」を優先（会場不明判定だけにしない）
- Notionの「ホーム」「アウェイ」「スコア」も同期（存在するDBプロパティに書き込む）

後方互換:
- 旧DESCRIPTION（大会：... / 区分：予選リーグ（えグループ） / 出典：...）も読み取る
  ※旧「区分：」が 予定/結果 以外なら「ラウンド」扱いに回す

要件（Notion側のDBプロパティ名）:
- カード (title)
- 開始 (date)
- 終了 (date)
- 大会 (rich_text)
- 節／ラウンド (rich_text)
- 会場 (rich_text)
- 区分 (select) …「予定」「結果」
- 確度 (select) …「確定」「暫定」「未定」
- ホーム (rich_text) ※DBにある場合
- アウェイ (rich_text) ※DBにある場合
- スコア (rich_text) ※DBにある場合
- 更新日 (date)
- UID (rich_text)
- 出典 (rich_text)
- fingerprint (rich_text)  ※内部用（変更検知）

環境変数:
- NOTION_TOKEN
- NOTION_DATABASE_ID
- ICS_PATH (任意, デフォルト: soccer_osaka_hs_boys.ics)
"""

import os
import sys
import hashlib
from pathlib import Path
from datetime import datetime, date, time
from zoneinfo import ZoneInfo
from notion_client import Client

JST = ZoneInfo("Asia/Tokyo")

# -----------------------------
# utilities
# -----------------------------
def now_jst() -> datetime:
    return datetime.now(tz=JST)

def norm_text(s: str) -> str:
    return (s or "").replace("\r\n", "\n").replace("\r", "\n")

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def unfold_ics_lines(text: str) -> list[str]:
    """ICSの継続行（先頭が半角スペース）を連結して1行に戻す"""
    out: list[str] = []
    for line in norm_text(text).split("\n"):
        if not line:
            continue
        if line.startswith(" "):
            if out:
                out[-1] += line[1:]
            else:
                out.append(line[1:])
        else:
            out.append(line)
    return out

def parse_ics_datetime(value: str) -> datetime | None:
    """
    DTSTART/DTEND の値を JST の aware datetime に変換
    - 20251207T091500
    - 20251207T091500Z
    - 20251207（date-only）
    """
    value = (value or "").strip()
    if not value:
        return None
    # Zulu
    zulu = value.endswith("Z")
    if zulu:
        value = value[:-1]

    try:
        if "T" in value:
            d, t = value.split("T", 1)
            y = int(d[0:4]); m = int(d[4:6]); dd = int(d[6:8])
            hh = int(t[0:2]); mm = int(t[2:4]); ss = int(t[4:6]) if len(t) >= 6 else 0
            dt = datetime(y, m, dd, hh, mm, ss)
        else:
            y = int(value[0:4]); m = int(value[4:6]); dd = int(value[6:8])
            dt = datetime(y, m, dd, 0, 0, 0)
    except Exception:
        return None

    if zulu:
        # UTC → JST
        return dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(JST)
    return dt.replace(tzinfo=JST)

def to_notion_date(dt: datetime | None) -> dict | None:
    if dt is None:
        return None
    return {"start": dt.isoformat()}

# -----------------------------
# DESCRIPTION parsing (new + old)
# -----------------------------
def parse_desc(desc: str) -> dict:
    """
    新仕様（推奨）:
      種別：公式戦 / 練習試合 / TRM / 招待試合
      大会：...
      ラウンド：...
      区分：予定 or 結果
      確度：確定 / 暫定 / 未定
      出典：...
      スコア：...（結果のみ）
      対戦：ホーム vs アウェイ

    旧仕様（後方互換）:
      大会：...
      区分：予選リーグ（えグループ）   ← 予定/結果ではない場合はラウンド扱い
      出典：...
    """
    out = {
        "type": "",
        "tournament": "",
        "round": "",
        "kind": "",        # 予定/結果
        "certainty": "",   # 確定/暫定/未定
        "source": "",
        "score": "",
        "match": "",       # 対戦
        "home": "",
        "away": "",
    }

    txt = norm_text(desc)

    # DESCRIPTION内の "\\n" を改行に戻す（ICSのエスケープ形式対策）
    txt = txt.replace("\\n", "\n")
    lines = [ln.strip() for ln in txt.split("\n") if ln.strip()]

    # 一旦すべて拾う
    legacy_round_candidate = ""
    for line in lines:
        if line.startswith("種別："):
            out["type"] = line.replace("種別：", "", 1).strip()
        elif line.startswith("大会："):
            out["tournament"] = line.replace("大会：", "", 1).strip()
        elif line.startswith("ラウンド："):
            out["round"] = line.replace("ラウンド：", "", 1).strip()
        elif line.startswith("節／ラウンド：") or line.startswith("節/ラウンド："):
            out["round"] = line.split("：", 1)[1].strip()
        elif line.startswith("区分："):
            v = line.replace("区分：", "", 1).strip()
            if v in ("予定", "結果"):
                out["kind"] = v
            else:
                legacy_round_candidate = v
        elif line.startswith("確度："):
            out["certainty"] = line.replace("確度：", "", 1).strip()
        elif line.startswith("出典："):
            out["source"] = line.replace("出典：", "", 1).strip()
        elif line.startswith("スコア："):
            out["score"] = line.replace("スコア：", "", 1).strip()
        elif line.startswith("対戦："):
            out["match"] = line.replace("対戦：", "", 1).strip()
        elif line.startswith("ホーム："):
            out["home"] = line.replace("ホーム：", "", 1).strip()
        elif line.startswith("アウェイ："):
            out["away"] = line.replace("アウェイ：", "", 1).strip()

    # 旧仕様の区分（=ラウンド）救済
    if not out["round"] and legacy_round_candidate:
        out["round"] = legacy_round_candidate

    # 対戦 → home/away
    if (not out["home"] or not out["away"]) and out["match"]:
        h, a = split_vs(out["match"])
        if h and not out["home"]:
            out["home"] = h
        if a and not out["away"]:
            out["away"] = a

    return out

def split_vs(s: str) -> tuple[str, str]:
    """'A vs B' / 'A VS B' / 'A ｖｓ B' などを分割"""
    if not s:
        return "", ""
    # normalize separators
    for sep in [" vs ", " VS ", " Vs ", " ｖｓ ", " v ", " V ", "　vs　", "　VS　"]:
        if sep in s:
            parts = s.split(sep, 1)
            return parts[0].strip(), parts[1].strip()
    # fallback: "A vsB" など雑
    m = None
    for pat in [r"(.+?)\s*(?:vs|VS|Vs|ｖｓ)\s*(.+)"]:
        import re
        m = re.match(pat, s)
        if m:
            return m.group(1).strip(), m.group(2).strip()
    return "", ""

def parse_score_from_summary(summary: str) -> tuple[str, str, str]:
    """
    SUMMARY から (home, score, away) を可能なら抽出
    例:
      '清風 4 - 0 追手門学院'
      '星翔 0(1) - 0(0) 大商学園'
      '大阪学院大高 vs 神戸科学技術／5-2'
    """
    import re
    s = (summary or "").strip()

    # pattern 1: "A 4 - 0 B" (with optional PK like 0(1) - 0(0))
    m = re.match(r"^(.+?)\s+(\d+(?:\(\d+\))?\s*-\s*\d+(?:\(\d+\))?(?:\s*（PK\d+(?:-\d+)?）)?(?:\s*\(PK\d+(?:-\d+)?\))?)\s+(.+?)$", s)
    if m:
        return m.group(1).strip(), clean_score(m.group(2)), m.group(3).strip()

    # pattern 2: "A vs B／5-2" (or with slash)
    if "／" in s:
        left, right = s.split("／", 1)
        h, a = split_vs(left)
        if h and a:
            # score is first token
            sc = right.strip()
            return h, clean_score(sc), a

    # pattern 3: "A vs B"
    h, a = split_vs(s)
    if h and a:
        return h, "", a

    return "", "", ""

def clean_score(score: str) -> str:
    return (score or "").replace("　", " ").strip()

# -----------------------------
# ICS parsing (no external libs)
# -----------------------------
def parse_ics_events(ics_path: str) -> list[dict]:
    txt = Path(ics_path).read_text(encoding="utf-8", errors="replace")
    lines = unfold_ics_lines(txt)

    events: list[dict] = []
    cur: dict | None = None

    for line in lines:
        if line == "BEGIN:VEVENT":
            cur = {"raw": {}}
            continue
        if line == "END:VEVENT":
            if cur:
                events.append(cur)
            cur = None
            continue
        if cur is None:
            continue

        # key(;params):value
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        cur["raw"][k] = v

    normed = []
    for e in events:
        raw = e.get("raw", {})
        # DTSTART / DTEND may include TZID params
        dtstart = None
        dtend = None
        for k, v in raw.items():
            if k.startswith("DTSTART"):
                dtstart = parse_ics_datetime(v)
            elif k.startswith("DTEND"):
                dtend = parse_ics_datetime(v)

        normed.append({
            "uid": (raw.get("UID", "") or "").strip(),
            "summary": (raw.get("SUMMARY", "") or "").strip(),
            "location": (raw.get("LOCATION", "") or "").strip(),
            "description": (raw.get("DESCRIPTION", "") or ""),
            "dtstart": dtstart,
            "dtend": dtend,
        })

    return normed

# -----------------------------
# Notion mapping
# -----------------------------
def safe_rich_text(s: str) -> dict:
    if not s:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": s}}]}

def safe_title(s: str) -> dict:
    if not s:
        s = "(no title)"
    return {"title": [{"text": {"content": s}}]}

def build_item(ev: dict) -> dict:
    uid = ev["uid"]
    summary = ev["summary"]
    location = ev["location"]
    desc_raw = ev.get("description") or ""
    parsed = parse_desc(desc_raw)

    dtstart = ev.get("dtstart")
    dtend = ev.get("dtend")
    if dtstart is None:
        raise ValueError(f"DTSTARTがありません: UID={uid}")

    # kind（予定/結果）
    kind = parsed["kind"]
    if kind not in ("予定", "結果"):
        kind = "予定" if dtstart > now_jst() else "結果"

    # certainty（確定/暫定/未定）
    certainty = parsed["certainty"]
    if certainty not in ("確定", "暫定", "未定"):
        if (not location) or ("会場不明" in location):
            certainty = "未定"
        else:
            certainty = "暫定"

    # tournament
    tournament = parsed["tournament"]
    if not tournament:
        if "／" in summary:
            # 例: 大阪府／大会名／ラウンド
            tmp = summary.split("／", 1)[1]
            tournament = tmp.split("／", 1)[0].strip()
        else:
            tournament = summary

    # round
    round_name = parsed["round"]

    # home/away/score
    home = parsed["home"]
    away = parsed["away"]
    score = parsed["score"]

    if not (home and away):
        h2, sc2, a2 = parse_score_from_summary(summary)
        if h2 and not home:
            home = h2
        if a2 and not away:
            away = a2
        if sc2 and not score:
            score = sc2

    # title（カード）
    title = summary
    if home and away:
        if score:
            title = f"{home} {score} {away}"
        else:
            title = f"{home} vs {away}"
    else:
        # 対戦カードが取れない場合、カードに大会名が出てしまうのを避けるため
        # 「節／ラウンド」があるならそれをタイトルにする（例：予選リーグ（えグループ））
        if round_name:
            title = round_name

    # source
    source = parsed["source"]

    # fingerprint
    fingerprint = sha1("|".join([
        uid,
        summary,
        location,
        dtstart.isoformat(),
        (dtend.isoformat() if dtend else ""),
        tournament,
        round_name,
        kind,
        certainty,
        home,
        away,
        score,
        source,
    ]))

    return {
        "uid": uid,
        "summary": summary,
        "title": title,
        "location": location,
        "dtstart": dtstart,
        "dtend": dtend,
        "tournament": tournament,
        "round": round_name,
        "kind": kind,
        "certainty": certainty,
        "home": home,
        "away": away,
        "score": score,
        "source": source,
        "fingerprint": fingerprint,
    }

def build_notion_props(item: dict, db_prop_names: set[str]) -> dict:
    props: dict = {
        "カード": safe_title(item["title"]),
        "開始": {"date": to_notion_date(item["dtstart"])},
        "終了": {"date": to_notion_date(item["dtend"])} if item["dtend"] else {"date": None},
        "大会": safe_rich_text(item["tournament"]),
        "節／ラウンド": safe_rich_text(item["round"]),
        "会場": safe_rich_text(item["location"]),
        "区分": {"select": {"name": item["kind"]}},
        "確度": {"select": {"name": item["certainty"]}},
        "更新日": {"date": to_notion_date(now_jst())},
        "UID": safe_rich_text(item["uid"]),
        "出典": safe_rich_text(item["source"]),
        "fingerprint": safe_rich_text(item["fingerprint"]),
    }

    # 追加列（DBに存在する場合のみ）
    if "ホーム" in db_prop_names:
        props["ホーム"] = safe_rich_text(item["home"])
    if "アウェイ" in db_prop_names:
        props["アウェイ"] = safe_rich_text(item["away"])
    if "スコア" in db_prop_names:
        props["スコア"] = safe_rich_text(item["score"])

    # 種別列がある場合（任意）
    if "種別" in db_prop_names:
        # DESCRIPTIONからの種別は parse_desc に入っているが item に保持していないので summary fallback
        # 必要なら item にも追加してください
        pass

    return props

def fetch_db_prop_names(notion: Client, database_id: str) -> set[str]:
    db = notion.databases.retrieve(database_id=database_id)
    props = db.get("properties", {}) or {}
    return set(props.keys())

def fetch_all_pages(notion: Client, database_id: str) -> list:
    pages = []
    cursor = None
    while True:
        resp = notion.databases.query(database_id=database_id, start_cursor=cursor) if cursor else notion.databases.query(database_id=database_id)
        pages.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break
    return pages

def page_uid_from_props(page: dict) -> str:
    try:
        rt = page["properties"]["UID"]["rich_text"]
        if rt:
            return rt[0]["plain_text"].strip()
    except Exception:
        return ""
    return ""

def page_fp_from_props(page: dict) -> str:
    try:
        rt = page["properties"]["fingerprint"]["rich_text"]
        if rt:
            return rt[0]["plain_text"].strip()
    except Exception:
        return ""
    return ""

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

    notion = Client(auth=token)
    db_prop_names = fetch_db_prop_names(notion, dbid)

    events = parse_ics_events(ics_path)
    items = []
    for ev in events:
        if not ev.get("uid"):
            continue
        items.append(build_item(ev))

    # 既存ページ取得
    pages = fetch_all_pages(notion, dbid)
    by_uid = {}
    for pg in pages:
        uid = page_uid_from_props(pg)
        if uid:
            by_uid[uid] = pg

    created = updated = skipped = 0
    for it in items:
        uid = it["uid"]
        props = build_notion_props(it, db_prop_names)

        if uid not in by_uid:
            notion.pages.create(parent={"database_id": dbid}, properties=props)
            created += 1
        else:
            pg = by_uid[uid]
            old_fp = page_fp_from_props(pg)
            if old_fp == it["fingerprint"]:
                skipped += 1
            else:
                notion.pages.update(page_id=pg["id"], properties=props)
                updated += 1

    print(f"OK: created={created}, updated={updated}, skipped={skipped}, total_ics_events={len(items)}, total_notion_pages={len(pages)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())