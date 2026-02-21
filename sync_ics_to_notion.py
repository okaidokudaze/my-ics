#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_ics_to_notion.py (v4)
- Notionの「カード(タイトル)」を可能な限り「ホーム vs アウェイ」にする
- DESCRIPTIONのラベル（種別/大会/ラウンド/区分/確度/出典/スコア/対戦/ホーム/アウェイ）を優先
- ただし「スコアがある」または「終了時刻が過去」の場合は区分を結果に寄せる（過去なのに予定を防ぐ）

環境変数（既存運用に合わせて想定）:
  NOTION_TOKEN          : Notion Integration token
  NOTION_DATABASE_ID    : Notion database id
  ICS_PATH              : 読み込むICSファイルパス（省略時 soccer_osaka_hs_boys.ics）
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from icalendar import Calendar
from notion_client import Client
from zoneinfo import ZoneInfo


JST = ZoneInfo("Asia/Tokyo")


# --------------------------
# Parsing helpers
# --------------------------
LABEL_SPLIT_RE = re.compile(r"^\s*([^:：]+)\s*[:：]\s*(.*)\s*$")
SCORE_RE = re.compile(r"(\d+)\s*(?:\(|（)?\d*(?:\)|）)?\s*-\s*(\d+)(?:\s*(?:\(|（)?\d*(?:\)|）)?)?")
VS_RE = re.compile(r"\s*(.+?)\s*(?:vs|VS|Vs|ｖｓ|ＶＳ|対)\s*(.+?)\s*$")

def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", "replace")
        except Exception:
            return v.decode(errors="replace")
    return str(v)

def _normalize_desc(desc: str) -> str:
    # Notion同期用に DESCRIPTION に "\n" が文字列で入ってくるケースを救う
    desc = desc.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
    return desc

def parse_labeled_description(desc: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in _normalize_desc(desc).split("\n"):
        m = LABEL_SPLIT_RE.match(line)
        if not m:
            continue
        k = m.group(1).strip()
        v = m.group(2).strip()
        if not k:
            continue
        out[k] = v
    return out

def split_summary(summary: str) -> Tuple[str, str, str, str]:
    """
    SUMMARYが "大阪府／大会名／ラウンド／スコア" 形式の場合に分解
    """
    parts = [p.strip() for p in summary.split("／")]
    pref = parts[0] if len(parts) >= 1 else ""
    tournament = parts[1] if len(parts) >= 2 else ""
    rnd = parts[2] if len(parts) >= 3 else ""
    extra = "／".join(parts[3:]) if len(parts) >= 4 else ""
    return pref, tournament, rnd, extra

def parse_vs(text: str) -> Tuple[str, str]:
    t = text.strip()
    m = VS_RE.match(t)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    # "A 1-0 B" 形式
    if " " in t:
        # ざっくり、スコアの前後を切る
        sm = SCORE_RE.search(t)
        if sm:
            left = t[:sm.start()].strip(" 　-－—–—")
            right = t[sm.end():].strip(" 　-－—–—")
            if left and right:
                return left, right
    return "", ""

def parse_score(text: str) -> str:
    t = text.strip()
    sm = SCORE_RE.search(t)
    if not sm:
        return ""
    return f"{sm.group(1)}-{sm.group(2)}"

def coerce_status(meta_status: str, has_score: bool, dtend_jst: Optional[datetime], now_jst: datetime) -> str:
    """
    区分（予定/結果）を決める。
    - DESCRIPTIONに区分があれば基本それ
    - ただし「スコアあり」または「終了時刻が過去」なら結果に寄せる
    """
    s = (meta_status or "").strip()
    if s not in ("予定", "結果"):
        s = "予定"

    if has_score:
        return "結果"

    if dtend_jst is not None and dtend_jst <= (now_jst - timedelta(minutes=1)):
        # 過去なのに「予定」になってしまうのを防ぐ
        return "結果"

    return s

def to_jst_datetime(dt: Any) -> Optional[datetime]:
    """
    icalendarから取得したdtstart/dtendは date or datetime の可能性。
    JSTのdatetimeに統一して返す（dateなら00:00/23:59扱いは呼び出し側で）。
    """
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=JST)
        return dt.astimezone(JST)
    if isinstance(dt, date):
        return datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=JST)
    return None


@dataclass
class IcsItem:
    uid: str
    dtstart: Optional[datetime]
    dtend: Optional[datetime]
    summary: str
    location: str
    description: str
    meta: Dict[str, str]

    @property
    def start_iso(self) -> Optional[str]:
        if self.dtstart is None:
            return None
        return self.dtstart.isoformat()

    @property
    def end_iso(self) -> Optional[str]:
        if self.dtend is None:
            return None
        return self.dtend.isoformat()


def parse_ics(ics_path: str) -> List[IcsItem]:
    txt = Path(ics_path).read_text(encoding="utf-8", errors="replace")
    cal = Calendar.from_ical(txt)
    items: List[IcsItem] = []
    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue
        uid = _to_str(comp.get("UID")).strip()
        summary = _to_str(comp.get("SUMMARY")).strip()
        location = _to_str(comp.get("LOCATION")).strip()
        desc = _to_str(comp.get("DESCRIPTION")).strip()
        meta = parse_labeled_description(desc)

        dtstart_raw = comp.get("DTSTART")
        dtend_raw = comp.get("DTEND")

        dtstart = to_jst_datetime(getattr(dtstart_raw, "dt", dtstart_raw))
        dtend = to_jst_datetime(getattr(dtend_raw, "dt", dtend_raw))

        # DTENDが無い場合は+2h
        if dtstart and dtend is None:
            dtend = dtstart + timedelta(hours=2)

        items.append(IcsItem(uid=uid, dtstart=dtstart, dtend=dtend, summary=summary, location=location, description=desc, meta=meta))
    return items


# --------------------------
# Notion helpers
# --------------------------
def get_db_schema(notion: Client, db_id: str) -> Dict[str, Any]:
    db = notion.databases.retrieve(database_id=db_id)
    return db.get("properties", {})

def find_title_prop(props: Dict[str, Any]) -> str:
    for name, p in props.items():
        if p.get("type") == "title":
            return name
    # fall back
    return "Name"

def pick_prop(props: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    # 完全一致優先
    for c in candidates:
        if c in props:
            return c
    # ゆるい一致（全角/半角違いなど）
    normalized = {re.sub(r"\s+", "", k): k for k in props.keys()}
    for c in candidates:
        key = re.sub(r"\s+", "", c)
        if key in normalized:
            return normalized[key]
    return None

def build_props_map(props: Dict[str, Any]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    m["title"] = find_title_prop(props)
    m["tournament"] = pick_prop(props, ["大会", "Tournament"])
    m["round"] = pick_prop(props, ["節／ラウンド", "節/ラウンド", "ラウンド", "Round"])
    m["start"] = pick_prop(props, ["開始", "Start", "開始日時"])
    m["end"] = pick_prop(props, ["終了", "End", "終了日時"])
    m["status"] = pick_prop(props, ["区分", "Status"])
    m["home"] = pick_prop(props, ["ホーム", "Home"])
    m["away"] = pick_prop(props, ["アウェイ", "アウェー", "Away"])
    m["score"] = pick_prop(props, ["スコア", "Score"])
    m["kind"] = pick_prop(props, ["種別", "Kind"])
    m["confidence"] = pick_prop(props, ["確度", "Confidence"])
    m["source"] = pick_prop(props, ["出典", "Source"])
    m["place"] = pick_prop(props, ["会場", "場所", "LOCATION", "Place"])
    m["uid"] = pick_prop(props, ["UID", "uid"])
    return {k: v for k, v in m.items() if v}

def rich_text(val: str) -> Dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": val}}]} if val else {"rich_text": []}

def title_text(val: str) -> Dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": val}}]} if val else {"title": []}

def select_val(val: str) -> Dict[str, Any]:
    return {"select": {"name": val}} if val else {"select": None}

def date_val(start_iso: Optional[str], end_iso: Optional[str]) -> Dict[str, Any]:
    if not start_iso:
        return {"date": None}
    return {"date": {"start": start_iso, "end": end_iso}}

def upsert_page(notion: Client, db_id: str, props_map: Dict[str, str], item: IcsItem, now_jst: datetime) -> None:
    # meta fields
    meta = item.meta

    # tournament/round from DESCRIPTION優先、無ければSUMMARYから
    _, t_from_sum, r_from_sum, extra = split_summary(item.summary)
    tournament = meta.get("大会", "") or t_from_sum
    rnd = meta.get("ラウンド", "") or meta.get("節／ラウンド", "") or meta.get("節/ラウンド", "") or r_from_sum

    # teams
    home = meta.get("ホーム", "") or ""
    away = meta.get("アウェイ", "") or meta.get("アウェー", "") or ""

    if not (home and away):
        vs = meta.get("対戦", "") or ""
        if vs:
            h, a = parse_vs(vs)
            home = home or h
            away = away or a

    # score
    score = meta.get("スコア", "") or ""
    if not score:
        # 対戦行にスコアが混ざってる場合
        score = parse_score(meta.get("対戦", "") or "")
    if not score:
        # SUMMARY末尾の「／1-0」など
        score = parse_score(item.summary)

    has_score = bool(score)

    # status
    status = coerce_status(meta.get("区分", ""), has_score, item.dtend, now_jst)

    # kind/confidence/source/place
    kind = meta.get("種別", "")
    confidence = meta.get("確度", "")
    source = meta.get("出典", "")
    place = meta.get("会場", "") or item.location

    # title (card)
    if home and away:
        card = f"{home} vs {away}"
        if has_score:
            card += f"／{score}"
    else:
        # 対戦が無い場合は大会名を残す（箱イベント扱い）
        # ただし「大阪府／」などのプレフィックスは落とす
        card = item.summary.replace("大阪府／", "").strip()

    # build notion properties
    notion_props: Dict[str, Any] = {}

    title_prop = props_map.get("title")
    if title_prop:
        notion_props[title_prop] = title_text(card)

    if props_map.get("tournament"):
        notion_props[props_map["tournament"]] = rich_text(tournament)

    if props_map.get("round"):
        notion_props[props_map["round"]] = rich_text(rnd)

    if props_map.get("start"):
        notion_props[props_map["start"]] = date_val(item.start_iso, None)

    if props_map.get("end"):
        notion_props[props_map["end"]] = date_val(item.end_iso, None)

    if props_map.get("status"):
        notion_props[props_map["status"]] = select_val(status)

    if props_map.get("home"):
        notion_props[props_map["home"]] = rich_text(home)

    if props_map.get("away"):
        notion_props[props_map["away"]] = rich_text(away)

    if props_map.get("score"):
        notion_props[props_map["score"]] = rich_text(score)

    if props_map.get("kind"):
        notion_props[props_map["kind"]] = select_val(kind) if kind else rich_text(kind)

    if props_map.get("confidence"):
        notion_props[props_map["confidence"]] = select_val(confidence) if confidence else rich_text(confidence)

    if props_map.get("source"):
        notion_props[props_map["source"]] = rich_text(source)

    if props_map.get("place"):
        notion_props[props_map["place"]] = rich_text(place)

    if props_map.get("uid"):
        notion_props[props_map["uid"]] = rich_text(item.uid)

    # --- find existing page by UID ---
    # UIDプロパティが無い場合は作成だけ（既存と紐づかないので注意）
    existing_page_id: Optional[str] = None
    uid_prop_name = props_map.get("uid")

    if uid_prop_name:
        q = notion.databases.query(
            database_id=db_id,
            filter={
                "property": uid_prop_name,
                "rich_text": {"equals": item.uid}
            },
            page_size=1
        )
        results = q.get("results", [])
        if results:
            existing_page_id = results[0]["id"]

    if existing_page_id:
        notion.pages.update(page_id=existing_page_id, properties=notion_props)
    else:
        notion.pages.create(parent={"database_id": db_id}, properties=notion_props)


def main() -> int:
    token = os.environ.get("NOTION_TOKEN", "").strip()
    db_id = os.environ.get("NOTION_DATABASE_ID", "").strip()
    ics_path = os.environ.get("ICS_PATH", "soccer_osaka_hs_boys.ics").strip()

    if not token or not db_id:
        print("ERROR: NOTION_TOKEN / NOTION_DATABASE_ID が未設定です。")
        return 2

    notion = Client(auth=token)
    props = get_db_schema(notion, db_id)
    props_map = build_props_map(props)

    now_jst = datetime.now(tz=timezone.utc).astimezone(JST)

    items = parse_ics(ics_path)
    # UIDが空のイベントは無視
    items = [it for it in items if it.uid]

    ok = 0
    for it in items:
        try:
            upsert_page(notion, db_id, props_map, it, now_jst)
            ok += 1
        except Exception as e:
            print(f"[WARN] uid={it.uid} summary={it.summary} err={e}")

    print(f"Done. upserted={ok} events={len(items)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
