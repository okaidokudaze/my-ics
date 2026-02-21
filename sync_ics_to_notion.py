#!/usr/bin/env python3
"""
GitHub上のICS（source of truth）→ Notion Database へ自動同期するスクリプト

Notion側のDBプロパティ名（前提）:
- カード (title)            : 対戦カード（例：A vs B）
- 開始 (date)
- 終了 (date)
- 大会 (rich_text)          : 大会名
- 節／ラウンド (rich_text)  : 節/ラウンド（例：2A90、準々決勝 など）
- 会場 (rich_text)
- 区分 (select)             : 予定 / 結果
- 確度 (select)             : 確定 / 暫定 / 未定
- ホーム (rich_text)
- アウェイ (rich_text)
- スコア (rich_text)
- 更新日 (date)             : 同期実行日時
- UID (rich_text)           : VEVENTのUID（ユニークキー）
- 出典 (rich_text)          : DESCRIPTIONの「出典：…」
- fingerprint (rich_text)   : 変更検知用（非表示推奨）

環境変数:
- NOTION_TOKEN
- NOTION_DATABASE_ID
- ICS_PATH (任意, デフォルト: soccer_osaka_hs_boys.ics)
"""
import os
import sys
import hashlib
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from notion_client import Client
from notion_client.errors import APIResponseError
from icalendar import Calendar

JST = ZoneInfo("Asia/Tokyo")


def now_jst() -> datetime:
    return datetime.now(tz=JST)


def norm_text(s: str) -> str:
    return (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def _unescape_desc(desc: str) -> str:
    """
    icalendarが返すDESCRIPTIONは '\\n' が含まれることがある。
    それを実改行に戻してからパースする。
    """
    txt = norm_text(desc)
    # まず literal \n を改行へ
    txt = txt.replace("\\n", "\n")
    # まれに \\n（バックスラッシュが2個）になってるケースも救う
    txt = txt.replace("\\\\n", "\n")
    return txt


def parse_desc(desc: str) -> dict:
    """
    DESCRIPTION例（表記ゆれに強くする）:
      種別：公式戦
      大会：...
      ラウンド：...
      節/ラウンド：...
      区分：予定
      確度：確定
      出典：https://...
      対戦：A vs B
      スコア：1-0
    """
    out = {
        "tournament": "",
        "round": "",
        "kind": "",
        "certainty": "",
        "source": "",
        "matchup": "",
        "home": "",
        "away": "",
        "score": "",
        "type": "",
    }

    txt = _unescape_desc(desc)

    for raw in txt.split("\n"):
        line = raw.strip()
        if not line:
            continue

        # 区切りは「：」または「:」
        if "：" in line:
            k, v = line.split("：", 1)
        elif ":" in line:
            k, v = line.split(":", 1)
        else:
            continue

        key = k.strip()
        val = v.strip()

        if key in ("大会",):
            out["tournament"] = val
        elif key in ("ラウンド", "節/ラウンド", "節／ラウンド", "節/ラウンド ", "節／ラウンド "):
            out["round"] = val
        elif key in ("区分",):
            out["kind"] = val
        elif key in ("確度",):
            out["certainty"] = val
        elif key in ("出典",):
            out["source"] = val
        elif key in ("対戦",):
            out["matchup"] = val
        elif key in ("ホーム",):
            out["home"] = val
        elif key in ("アウェイ", "アウェー"):
            out["away"] = val
        elif key in ("スコア",):
            out["score"] = val
        elif key in ("種別",):
            out["type"] = val

    # 対戦からホーム/アウェイを補完
    if out["matchup"] and (not out["home"] or not out["away"]):
        h, a = split_matchup(out["matchup"])
        if h and not out["home"]:
            out["home"] = h
        if a and not out["away"]:
            out["away"] = a

    return out


def to_notion_date(dt: datetime) -> dict:
    return {"start": dt.isoformat()}


def safe_dt(v) -> datetime | None:
    if v is None:
        return None
    if hasattr(v, "dt"):
        v = v.dt
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=JST)
        return v.astimezone(JST)
    # date only
    try:
        return datetime(v.year, v.month, v.day, 0, 0, tzinfo=JST)
    except Exception:
        return None


VS_SEP_RE = re.compile(r"\s*(?:vs\.?|VS|Vs|ｖｓ|ｖｓ\.?|対)\s*", re.IGNORECASE)


def split_matchup(text: str) -> tuple[str, str]:
    t = norm_text(text)
    if not t:
        return "", ""
    parts = VS_SEP_RE.split(t)
    if len(parts) >= 2:
        return parts[0].strip(), parts[1].strip()
    return "", ""


SCORE_RE = re.compile(r"(\d+(?:\(\d+\))?)\s*[-−–ー]\s*(\d+(?:\(\d+\))?)")


def normalize_score(score_text: str, home: str = "", away: str = "") -> str:
    s = norm_text(score_text)
    if not s:
        return ""
    # チーム名が混ざってる場合は除去
    for name in [home, away]:
        if name:
            s = s.replace(name, " ")
    s = re.sub(r"\s+", " ", s).strip()

    # PK表記がある場合は残す（例：1-1 PK3-1）
    # まず主要スコアの抽出を試す
    m = SCORE_RE.search(s)
    if not m:
        return s  # 抽出できなければそのまま
    main = f"{m.group(1)}-{m.group(2)}"
    # PKが続くケース
    pk = ""
    m2 = re.search(r"(PK\s*\d+\s*[-−–ー]\s*\d+)", s, flags=re.IGNORECASE)
    if m2:
        pk = " " + m2.group(1).replace(" ", "")
    return (main + pk).strip()


def extract_score_from_summary(summary: str) -> str:
    s = norm_text(summary)
    m = SCORE_RE.search(s)
    if not m:
        # 例：0-0 PK3-5 のようなパターンもここで拾う
        m = re.search(r"(\d+)\s*[-−–ー]\s*(\d+)", s)
        if not m:
            return ""
        main = f"{m.group(1)}-{m.group(2)}"
    else:
        main = f"{m.group(1)}-{m.group(2)}"
    pk = ""
    m2 = re.search(r"(PK\s*\d+\s*[-−–ー]\s*\d+)", s, flags=re.IGNORECASE)
    if m2:
        pk = " " + m2.group(1).replace(" ", "")
    return (main + pk).strip()


def build_item(vevent) -> dict:
    uid = str(vevent.get("UID", "")).strip()
    summary = str(vevent.get("SUMMARY", "")).strip()
    location = str(vevent.get("LOCATION", "")).strip()
    desc_v = vevent.get("DESCRIPTION")
    desc = str(desc_v) if desc_v is not None else ""

    parsed = parse_desc(desc)

    dtstart = safe_dt(vevent.get("DTSTART"))
    dtend = safe_dt(vevent.get("DTEND"))
    if dtstart is None:
        raise ValueError("DTSTARTがありません: UID=" + uid)

    # 大会名
    tournament = parsed["tournament"]
    if not tournament:
        # 旧SUMMARY形式「大阪府／大会名／ラウンド…」の救済
        if "／" in summary:
            parts = summary.split("／")
            tournament = parts[1].strip() if len(parts) >= 2 else summary
        else:
            tournament = summary

    # 節/ラウンド
    round_name = parsed["round"]

    # 対戦（カード）とホーム/アウェイ
    matchup = parsed["matchup"]
    home = parsed["home"]
    away = parsed["away"]

    if not matchup:
        # SUMMARYが「A vs B」なら拾う
        h, a = split_matchup(summary)
        if h and a:
            matchup = f"{h} vs {a}"
            home, away = h, a

    # スコア
    score = parsed["score"]
    if not score:
        score = extract_score_from_summary(summary)
    score = normalize_score(score, home, away)

    # 区分（予定/結果）はDESCRIPTION優先（日時で上書きしない）
    kind = parsed["kind"].strip()
    if kind not in ("予定", "結果"):
        kind = "結果" if score else "予定"

    # 確度もDESCRIPTION優先（無ければ場所から推定）
    certainty = parsed["certainty"].strip()
    if certainty not in ("確定", "暫定", "未定"):
        certainty = "未定" if (not location or "会場不明" in location) else "暫定"

    source = parsed["source"].strip()

    fingerprint = sha1(
        "|".join(
            [
                uid,
                summary,
                location,
                dtstart.isoformat(),
                (dtend.isoformat() if dtend else ""),
                tournament,
                round_name,
                matchup,
                home,
                away,
                score,
                source,
                kind,
                certainty,
            ]
        )
    )

    return {
        "uid": uid,
        "summary": summary,
        "location": location,
        "dtstart": dtstart,
        "dtend": dtend,
        "tournament": tournament,
        "round": round_name,
        "matchup": matchup,
        "home": home,
        "away": away,
        "score": score,
        "source": source,
        "kind": kind,
        "certainty": certainty,
        "fingerprint": fingerprint,
    }


def _rt(text: str) -> dict:
    t = norm_text(text)
    return {"rich_text": [{"text": {"content": t}}]} if t else {"rich_text": []}


def notion_props(item: dict) -> dict:
    # title は「対戦カード」優先（無ければSUMMARY）
    title = item["matchup"] or item["summary"]
    props = {
        "カード": {"title": [{"text": {"content": title}}]},
        "開始": {"date": to_notion_date(item["dtstart"])},
        "終了": {"date": to_notion_date(item["dtend"])} if item["dtend"] else {"date": None},
        "大会": _rt(item["tournament"]),
        "節／ラウンド": _rt(item["round"]),
        "会場": _rt(item["location"]),
        "区分": {"select": {"name": item["kind"]}},
        "確度": {"select": {"name": item["certainty"]}},
        "ホーム": _rt(item["home"]),
        "アウェイ": _rt(item["away"]),
        "スコア": _rt(item["score"]),
        "更新日": {"date": to_notion_date(now_jst())},
        "UID": _rt(item["uid"]),
        "出典": _rt(item["source"]),
        # 内部用（変更検知）
        "fingerprint": _rt(item["fingerprint"]),
    }
    return props


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


def fetch_all_pages(notion: Client, database_id: str) -> list:
    pages = []
    cursor = None
    while True:
        resp = (
            notion.databases.query(database_id=database_id, start_cursor=cursor)
            if cursor
            else notion.databases.query(database_id=database_id)
        )
        pages.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break
    return pages


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
        items.append(build_item(comp))

    notion = Client(auth=token)

    pages = fetch_all_pages(notion, dbid)
    by_uid = {}
    for pg in pages:
        uid = page_uid_from_props(pg)
        if uid:
            by_uid[uid] = pg

    created = updated = skipped = 0

    for it in items:
        uid = it["uid"]
        if not uid:
            continue
        props = notion_props(it)
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

    print(
        f"OK: created={created}, updated={updated}, skipped={skipped}, "
        f"total_ics_events={len(items)}, total_notion_pages={len(pages)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
