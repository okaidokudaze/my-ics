#!/usr/bin/env python3
"""
GitHub上のICS（source of truth）→ Notion Database へ自動同期するスクリプト

要件（Notion側のDBプロパティ名）:
- カード (title)
- 開始 (date)
- 終了 (date)
- 大会 (rich_text)
- 節／ラウンド (rich_text)   ※ICSのDESCRIPTIONの「区分：」を入れる
- 会場 (rich_text)
- 区分 (select)              ※「予定」「結果」(DTSTARTが現在より未来=予定)
- 確度 (select)              ※「確定」「暫定」「未定」(会場不明=未定、それ以外=暫定)
- 更新日 (date)              ※同期実行日時
- UID (rich_text)            ※VEVENTのUID（ユニークキー）
- 出典 (rich_text)           ※ICSのDESCRIPTIONの「出典：」(公開側では非表示推奨)

環境変数:
- NOTION_TOKEN
- NOTION_DATABASE_ID
- ICS_PATH (任意, デフォルト: soccer_osaka_hs_boys.ics)
"""
import os
import sys
import hashlib
from datetime import datetime, timezone
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


def parse_desc(desc: str) -> dict:
    """
    DESCRIPTION例:
    大会：...\\n区分：...\\n出典：https://...
    """
    out = {"tournament": "", "round": "", "source": ""}
    txt = norm_text(desc)
    for line in txt.split("\n"):
        line = line.strip()
        if line.startswith("大会："):
            out["tournament"] = line.replace("大会：", "", 1).strip()
        elif line.startswith("区分："):
            out["round"] = line.replace("区分：", "", 1).strip()
        elif line.startswith("出典："):
            out["source"] = line.replace("出典：", "", 1).strip()
    return out


def to_notion_date(dt: datetime) -> dict:
    # Notion date expects ISO 8601 with timezone
    return {"start": dt.isoformat()}


def safe_dt(v) -> datetime | None:
    if v is None:
        return None
    # icalendar can return date or datetime
    if hasattr(v, "dt"):
        v = v.dt
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=JST)
        return v.astimezone(JST)
    # date only
    try:
        from datetime import date, time
        if hasattr(v, "year"):
            return datetime(v.year, v.month, v.day, 0, 0, tzinfo=JST)
    except Exception:
        pass
    return None


def build_item(vevent) -> dict:
    uid = str(vevent.get("UID", "")).strip()
    summary = str(vevent.get("SUMMARY", "")).strip()
    location = str(vevent.get("LOCATION", "")).strip()
    desc = vevent.get("DESCRIPTION")
    desc = str(desc) if desc is not None else ""
    parsed = parse_desc(desc)

    dtstart = safe_dt(vevent.get("DTSTART"))
    dtend = safe_dt(vevent.get("DTEND"))
    if dtstart is None:
        raise ValueError("DTSTARTがありません: UID=" + uid)

    # 区分（予定/結果）
    kind = "予定" if dtstart > now_jst() else "結果"

    # 確度
    certainty = "未定" if "会場不明" in location else "暫定"

    # 大会はDESCRIPTION優先、無ければSUMMARYから推定（大阪府／xxxx のxxxx部分）
    tournament = parsed["tournament"]
    if not tournament:
        if "／" in summary:
            tournament = summary.split("／", 1)[1].strip()
        else:
            tournament = summary

    round_name = parsed["round"]
    source = parsed["source"]

    # 変更検知用ハッシュ
    fingerprint = sha1("|".join([
        uid, summary, location,
        dtstart.isoformat(),
        (dtend.isoformat() if dtend else ""),
        tournament, round_name, source,
        kind, certainty,
    ]))

    return {
        "uid": uid,
        "summary": summary,
        "location": location,
        "dtstart": dtstart,
        "dtend": dtend,
        "tournament": tournament,
        "round": round_name,
        "source": source,
        "kind": kind,
        "certainty": certainty,
        "fingerprint": fingerprint,
    }


def notion_props(item: dict) -> dict:
    props = {
        "カード": {"title": [{"text": {"content": item["summary"]}}]},
        "開始": {"date": to_notion_date(item["dtstart"])},
        "終了": {"date": to_notion_date(item["dtend"])} if item["dtend"] else {"date": None},
        "大会": {"rich_text": [{"text": {"content": item["tournament"]}}]} if item["tournament"] else {"rich_text": []},
        "節／ラウンド": {"rich_text": [{"text": {"content": item["round"]}}]} if item["round"] else {"rich_text": []},
        "会場": {"rich_text": [{"text": {"content": item["location"]}}]} if item["location"] else {"rich_text": []},
        "区分": {"select": {"name": item["kind"]}},
        "確度": {"select": {"name": item["certainty"]}},
        "更新日": {"date": to_notion_date(now_jst())},
        "UID": {"rich_text": [{"text": {"content": item["uid"]}}]},
        "出典": {"rich_text": [{"text": {"content": item["source"]}}]} if item["source"] else {"rich_text": []},
        # 内部用（変更検知）
        "fingerprint": {"rich_text": [{"text": {"content": item["fingerprint"]}}]},
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
        resp = notion.databases.query(database_id=database_id, start_cursor=cursor) if cursor else notion.databases.query(database_id=database_id)
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

    # 削除（ICS側に無いUID）はアーカイブしたい場合、ここで実装可能
    # ただし事故防止のためデフォルトでは何もしない

    print(f"OK: created={created}, updated={updated}, skipped={skipped}, total_ics_events={len(items)}, total_notion_pages={len(pages)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
