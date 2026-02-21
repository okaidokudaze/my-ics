#!/usr/bin/env python3
"""
GitHub上のICS（source of truth）→ Notion Database へ自動同期するスクリプト

想定（Notion側のDBプロパティ名・例）
- カード (title)              : 表示用タイトル（=対戦カードを入れるのが推奨）
- 開始 (date)
- 終了 (date)
- 大会 (rich_text)
- 節／ラウンド (rich_text)
- 会場 (rich_text)
- 区分 (select)              : 「予定」「結果」
- 確度 (select)              : 「確定」「暫定」「未定」
- 更新日 (date)              : 同期実行日時
- UID (rich_text)            : VEVENTのUID（ユニークキー）
- 出典 (rich_text)            : DESCRIPTIONの「出典：」（公開側では非表示推奨）

（任意・DBに存在すれば自動で入れる）
- 種別 (select)              : 「公式戦」「練習試合」「TRM」「招待試合」
- 対戦 (rich_text)
- スコア (rich_text)
- fingerprint (rich_text)    : 変更検知用（無くても動くが毎回更新になりやすい）

環境変数:
- NOTION_TOKEN
- NOTION_DATABASE_ID
- ICS_PATH (任意, デフォルト: soccer_osaka_hs_boys.ics)
"""

import os
import sys
import hashlib
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


def parse_desc(desc: str) -> dict:
    """
    DESCRIPTION例（ラベル付き・表記ゆれ禁止の想定）:
      種別：公式戦
      大会：高円宮杯...
      ラウンド：第1節
      区分：予定
      確度：確定
      出典：https://...
      スコア：1-0
      対戦：A vs B

    ※ICS生成側の都合で「\\n」が文字として入っている場合があるため、ここで改めて改行に変換する。
    """
    out = {
        "type": "",
        "tournament": "",
        "round": "",
        "kind": "",
        "certainty": "",
        "source": "",
        "score": "",
        "matchup": "",
    }

    txt = norm_text(desc)

    # 「\n」(実改行) と 「\\n」(文字列) の両方を改行として扱う
    txt = txt.replace("\\n", "\n")

    for raw in txt.split("\n"):
        line = raw.strip()
        if not line:
            continue

        if line.startswith("種別："):
            out["type"] = line.replace("種別：", "", 1).strip()

        elif line.startswith("大会："):
            out["tournament"] = line.replace("大会：", "", 1).strip()

        elif line.startswith("ラウンド："):
            out["round"] = line.replace("ラウンド：", "", 1).strip()

        elif line.startswith("節/ラウンド："):
            out["round"] = line.replace("節/ラウンド：", "", 1).strip()

        elif line.startswith("節／ラウンド："):
            out["round"] = line.replace("節／ラウンド：", "", 1).strip()

        elif line.startswith("区分："):
            out["kind"] = line.replace("区分：", "", 1).strip()

        elif line.startswith("確度："):
            out["certainty"] = line.replace("確度：", "", 1).strip()

        elif line.startswith("出典："):
            out["source"] = line.replace("出典：", "", 1).strip()

        elif line.startswith("スコア："):
            out["score"] = line.replace("スコア：", "", 1).strip()

        elif line.startswith("対戦："):
            out["matchup"] = line.replace("対戦：", "", 1).strip()

    return out


def to_notion_date(dt: datetime) -> dict:
    # Notion date expects ISO 8601 with timezone
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


def get_db_property_names(notion: Client, database_id: str) -> set[str]:
    db = notion.databases.retrieve(database_id=database_id)
    props = db.get("properties", {}) or {}
    return set(props.keys())


def choose_prop_key(db_props: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in db_props:
            return c
    return None


def build_item(vevent, db_props: set[str]) -> dict:
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

    # 区分（予定/結果）はDESCRIPTIONを優先（無ければ日時で推定）
    kind = parsed["kind"] if parsed["kind"] in ("予定", "結果") else ("予定" if dtstart > now_jst() else "結果")

    # 確度はDESCRIPTIONを優先（無ければ簡易推定）
    if parsed["certainty"] in ("確定", "暫定", "未定"):
        certainty = parsed["certainty"]
    else:
        certainty = "未定" if (not location or "会場不明" in location) else "暫定"

    # 大会はDESCRIPTION優先、無ければSUMMARYから推定
    tournament = parsed["tournament"]
    if not tournament:
        if "／" in summary:
            tournament = summary.split("／", 1)[1].strip()
        else:
            tournament = ""

    round_name = parsed["round"]

    source = parsed["source"]
    score = parsed["score"]
    matchup = parsed["matchup"]

    # カード（title）は「対戦」を優先。無ければSUMMARY。
    card_title = matchup or summary
    if matchup and kind == "結果" and score:
        # タイトルにスコアも付けたい場合（任意）
        card_title = f"{matchup}／{score}"

    # 変更検知用ハッシュ
    fingerprint = sha1(
        "|".join(
            [
                uid,
                card_title,
                summary,
                location,
                dtstart.isoformat(),
                (dtend.isoformat() if dtend else ""),
                tournament,
                round_name,
                kind,
                certainty,
                source,
                score,
                matchup,
                parsed["type"],
            ]
        )
    )

    return {
        "uid": uid,
        "card_title": card_title,
        "summary": summary,
        "location": location,
        "dtstart": dtstart,
        "dtend": dtend,
        "tournament": tournament,
        "round": round_name,
        "source": source,
        "kind": kind,
        "certainty": certainty,
        "type": parsed["type"],
        "score": score,
        "matchup": matchup,
        "fingerprint": fingerprint,
    }


def notion_props(item: dict, db_props: set[str]) -> dict:
    props: dict = {}

    # 必須（存在前提）
    props["カード"] = {"title": [{"text": {"content": item["card_title"]}}]}
    props["開始"] = {"date": to_notion_date(item["dtstart"])}
    props["終了"] = {"date": to_notion_date(item["dtend"])} if item["dtend"] else {"date": None}

    # 大会
    if "大会" in db_props:
        props["大会"] = {"rich_text": [{"text": {"content": item["tournament"]}}]} if item["tournament"] else {"rich_text": []}

    # 節／ラウンド（DB側の表記が「節/ラウンド」でも動くように両対応）
    round_key = choose_prop_key(db_props, ["節／ラウンド", "節/ラウンド", "ラウンド"])
    if round_key:
        props[round_key] = {"rich_text": [{"text": {"content": item["round"]}}]} if item["round"] else {"rich_text": []}

    # 会場
    if "会場" in db_props:
        props["会場"] = {"rich_text": [{"text": {"content": item["location"]}}]} if item["location"] else {"rich_text": []}

    # 区分・確度
    if "区分" in db_props:
        props["区分"] = {"select": {"name": item["kind"]}}
    if "確度" in db_props:
        props["確度"] = {"select": {"name": item["certainty"]}}

    # 更新日
    if "更新日" in db_props:
        props["更新日"] = {"date": to_notion_date(now_jst())}

    # UID（キー）
    props["UID"] = {"rich_text": [{"text": {"content": item["uid"]}}]}

    # 出典
    if "出典" in db_props:
        props["出典"] = {"rich_text": [{"text": {"content": item["source"]}}]} if item["source"] else {"rich_text": []}

    # 任意
    if "種別" in db_props and item["type"]:
        props["種別"] = {"select": {"name": item["type"]}}

    if "対戦" in db_props and item["matchup"]:
        props["対戦"] = {"rich_text": [{"text": {"content": item["matchup"]}}]}

    if "スコア" in db_props and item["score"]:
        props["スコア"] = {"rich_text": [{"text": {"content": item["score"]}}]}

    if "fingerprint" in db_props:
        props["fingerprint"] = {"rich_text": [{"text": {"content": item["fingerprint"]}}]}

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

    notion = Client(auth=token)
    db_props = get_db_property_names(notion, dbid)

    with open(ics_path, "rb") as f:
        cal = Calendar.from_ical(f.read())

    items = []
    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue
        items.append(build_item(comp, db_props))

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

        props = notion_props(it, db_props)

        if uid not in by_uid:
            notion.pages.create(parent={"database_id": dbid}, properties=props)
            created += 1
        else:
            pg = by_uid[uid]
            old_fp = page_fp_from_props(pg)
            if old_fp and old_fp == it["fingerprint"]:
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
