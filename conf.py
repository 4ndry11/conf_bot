# -*- coding: utf-8 -*-
import os
import re
import uuid
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from gspread.utils import rowcol_to_a1

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,  # <-- добавь эти два
)
# =============================== CONFIG =======================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", "-1003053461710"))
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "f7T9vQ1111wLp2Gx8Z")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Conference ZVILNYMO")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
GOOGLE_SA_PATH = os.getenv("GOOGLE_SA_PATH", "/etc/secrets/gsheets.json")
TZ = ZoneInfo(TIMEZONE)

# =========================== SHEETS CONNECTION =================================

def _open_gsheet() -> gspread.Spreadsheet:
    if not os.path.exists(GOOGLE_SA_PATH):
        raise RuntimeError(f"Google SA file not found: {GOOGLE_SA_PATH}")
    sa = gspread.service_account(filename=GOOGLE_SA_PATH)
    return sa.open(SPREADSHEET_NAME)

GS = _open_gsheet()

def ws(name: str) -> gspread.Worksheet:
    return GS.worksheet(name)

def ws_headers(w: gspread.Worksheet) -> List[str]:
    row = w.row_values(1)
    return [h.strip() for h in row]

def get_all_records(w: gspread.Worksheet) -> List[Dict[str, Any]]:
    return w.get_all_records(expected_headers=ws_headers(w), default_blank="")

def find_row_by_value(w: gspread.Worksheet, column_name: str, value: Any) -> Optional[int]:
    headers = ws_headers(w)
    if column_name not in headers:
        return None
    col_idx = headers.index(column_name) + 1
    col_vals = w.col_values(col_idx)
    for i, v in enumerate(col_vals, start=1):
        if i == 1:
            continue
        if str(v).strip() == str(value).strip():
            return i
    return None

def append_dict(w: gspread.Worksheet, data: Dict[str, Any]) -> None:
    headers = ws_headers(w)
    row = [str(data.get(h, "")) if data.get(h, "") is not None else "" for h in headers]
    w.append_row(row, value_input_option="USER_ENTERED")

def update_row_dict(w: gspread.Worksheet, row_idx: int, data: Dict[str, Any]) -> None:
    headers = ws_headers(w)
    row = [str(data.get(h, "")) if data.get(h, "") is not None else "" for h in headers]
    start_a1 = rowcol_to_a1(row_idx, 1)
    end_a1 = rowcol_to_a1(row_idx, len(headers))
    rng = f"{start_a1}:{end_a1}"
    w.update(rng, [row], value_input_option="USER_ENTERED")

def update_cell(w: gspread.Worksheet, row_idx: int, column_name: str, value: Any) -> None:
    headers = ws_headers(w)
    if column_name not in headers:
        return
    col_idx = headers.index(column_name) + 1
    w.update_cell(row_idx, col_idx, str(value) if value is not None else "")

def delete_row(w: gspread.Worksheet, row_idx: int) -> None:
    w.delete_rows(row_idx)

# =============================== HELPERS =======================================

def now_kyiv() -> datetime:
    return datetime.now(TZ)

def iso_dt(dt: Optional[datetime] = None) -> str:
    dt = dt or now_kyiv()
    return dt.strftime("%Y-%m-%d %H:%M")

def parse_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    except Exception:
        return None

def fmt_date(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y")

def fmt_time(dt: datetime) -> str:
    return dt.strftime("%H:%M")

def short_uuid(n: int = 8) -> str:
    return uuid.uuid4().hex[:n]

PHONE_RE = re.compile(r"^(?:\+?38)?0?\d{9}$|^380\d{9}$")

def normalize_phone(raw: str) -> Optional[str]:
    digits = re.sub(r"\D", "", raw or "")
    if digits.startswith("380") and len(digits) == 12:
        return digits
    if digits.startswith("0") and len(digits) == 10:
        return "38" + digits
    if len(digits) == 9:
        return "380" + digits
    return None

def rsvp_get_for_event_ids_for_client(client_id: str) -> List[Dict[str, Any]]:
    w = ws(SHEET_RSVP)
    return [r for r in get_all_records(w) if str(r.get("client_id")) == str(client_id)]

def a2i(v: Any, default: int = 0) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default
def is_earliest_upcoming_event_of_type(event: Dict[str, Any]) -> bool:
    """True, если это ближайшее (по времени) будущее событие данного type."""
    now = now_kyiv()
    etype = a2i(event.get("type"))
    dt_this = event_start_dt(event)
    if not dt_this:
        return False
    # Собираем все будущие события этого типа и проверяем, что текущее — самое раннее.
    cands: List[Tuple[datetime, Dict[str, Any]]] = []
    for e in get_all_events():
        if a2i(e.get("type")) != etype:
            continue
        dt = event_start_dt(e)
        if dt and dt >= now:
            cands.append((dt, e))
    if not cands:
        return False
    cands.sort(key=lambda x: x[0])
    return cands[0][1].get("event_id") == event.get("event_id")


def client_has_active_invite_for_type(client_id: str, type_code: int) -> bool:
    """
    Есть ли у клиента 'активная' запись на событие этого типа в будущем:
    - запись в RSVP по событию этого типа и
    - RSVP в состоянии "" (ещё не ответил) или "going".
    """
    now = now_kyiv()
    # Соберём события по id
    events_by_id = {e.get("event_id"): e for e in get_all_events()}
    for r in rsvp_get_for_event_ids_for_client(client_id):
        ev = events_by_id.get(str(r.get("event_id")))
        if not ev:
            continue
        if a2i(ev.get("type")) != int(type_code):
            continue
        dt = event_start_dt(ev)
        if not dt or dt < now:
            continue
        rsvp_val = str(r.get("rsvp") or "")
        if rsvp_val in {"", "going"}:
            return True
    return False

def build_types_overview_text(cli: Dict[str, Any]) -> str:
    text = (
        "✅ Ви підключені до розсилки на конференції.\n"
        "Надсилатимемо інвайти на найближчі події.\n\n"
        "Доступні типи конференцій:\n"
    )
    rows = get_eventtypes_active()
    if not rows:
        return text + "Наразі немає активних типів."
    lines = []
    for rt in rows:
        tcode = a2i(rt.get("type_code"))
        title = str(rt.get("title"))
        attended = client_has_attended_type(cli["client_id"], tcode)
        flag = "✅ Був(ла)" if attended else "⭕️ Ще не був(ла)"
        lines.append(f"• {title} — {flag}")
    return text + "\n".join(lines)

# =============================== SHEET NAMES ===================================

SHEET_EVENTTYPES = "EventTypes"
SHEET_CLIENTS    = "Clients"
SHEET_EVENTS     = "Events"
SHEET_ATTEND     = "Attendance"
SHEET_LOG        = "DeliveryLog"
SHEET_FEEDBACK   = "Feedback"
SHEET_MSG        = "Messages"
SHEET_RSVP       = "RSVP"

# =============================== DOMAIN LAYER ==================================

def messages_get(key: str, lang: str = "uk") -> str:
    try:
        w = ws(SHEET_MSG)
        rows = get_all_records(w)
        for r in rows:
            if str(r.get("key")).strip() == key and str(r.get("lang", "uk")).strip() == lang:
                return str(r.get("text", "")).replace("\\n", "\n")
    except Exception:
        pass
    FALLBACKS = {
        "invite.title": "Запрошення на зустріч: {title}",
        "invite.body": "{name}, запрошуємо на зустріч: {title}\n🗓 {date} о {time} (Київ)\nℹ️ {description}\nВиберіть варіант нижче:\n[✅ Так, буду] [🚫 Не зможу] [🔔 Нагадати за 24 год]",
        "reminder.60m": "⏰ Нагадуємо: через 1 год почнеться {title}. Посилання: {link}",
        "feedback.ask": "Дякуємо за участь у *{title}*.\nОцініть, будь ласка:\n1) Корисність: ⭐️1–5\n2) Чи зрозумілі наступні кроки? ✅ Так / ⚠️ Частково / ❌ Ні\nМожна додати коментар: [✍️ Написати відгук]",
        "reminder.24h": "🔔 Нагадуємо: завтра о {time} відбудеться {title}.\nПосилання: {link}",
        "update.notice": "🛠 Оновлення зустрічі {title}.\nЗверніть увагу: {what}",
        "cancel.notice": "❌ Зустріч {title} скасовано. Ми надішлемо нову дату найближчим часом.",
        "help.body": "👋 Це бот для запрошень на наші онлайн-зустрічі.\n\nВи отримуватимете інвайти та нагадування. Кнопки під повідомленням:\n• ✅ Так, буду — підтвердити участь (ми нагадаємо за 24 год і за 1 год)\n• 🚫 Не зможу — пропустити цю дату (ми запропонуємо іншу)\n• 🔔 Нагадати за 24 год — якщо ще не вирішили.",
    }
    return FALLBACKS.get(key, "")

def log_action(action: str, client_id: Optional[str] = None,
               event_id: Optional[str] = None, details: str = "") -> None:
    try:
        w = ws(SHEET_LOG)
        append_dict(w, {
            "ts": now_kyiv().strftime("%Y-%m-%d %H:%M:%S"),
            "client_id": client_id or "",
            "event_id": event_id or "",
            "action": action,
            "details": details or "",
        })
    except Exception:
        pass

def has_log(action: str, client_id: str, event_id: str) -> bool:
    try:
        w = ws(SHEET_LOG)
        rows = get_all_records(w)
        for r in rows:
            if str(r.get("action")) == action and str(r.get("client_id")) == client_id and str(r.get("event_id")) == event_id:
                return True
    except Exception:
        return False
    return False

def get_eventtypes_active() -> List[Dict[str, Any]]:
    w = ws(SHEET_EVENTTYPES)
    rows = get_all_records(w)
    return [r for r in rows if a2i(r.get("active"), 0) == 1]

def get_eventtype_by_code(type_code: int) -> Optional[Dict[str, Any]]:
    for r in get_eventtypes_active():
        if a2i(r.get("type_code"), -1) == int(type_code):
            return r
    return None

def client_id_for_tg(tg_user_id: int) -> str:
    return f"cl_{tg_user_id}"

def get_client_by_tg(tg_user_id: int) -> Optional[Dict[str, Any]]:
    w = ws(SHEET_CLIENTS)
    rows = get_all_records(w)
    for r in rows:
        if str(r.get("tg_user_id")).strip() == str(tg_user_id):
            return r
    return None

def upsert_client(tg_user_id: int, full_name: str, phone: str, status: str = "active") -> Dict[str, Any]:
    w = ws(SHEET_CLIENTS)
    cid = client_id_for_tg(tg_user_id)
    now = iso_dt()
    payload = {
        "client_id": cid,
        "tg_user_id": tg_user_id,
        "phone": phone,
        "full_name": full_name,
        "status": status,
        "created_at": now,
        "last_seen_at": now,
    }
    existing_row = find_row_by_value(w, "tg_user_id", tg_user_id)
    if existing_row:
        old_vals = w.row_values(existing_row)
        headers = ws_headers(w)
        old_map = {headers[i]: old_vals[i] if i < len(old_vals) else "" for i in range(len(headers))}
        payload["created_at"] = old_map.get("created_at", now)
        update_row_dict(w, existing_row, payload)
    else:
        append_dict(w, payload)
    log_action("client_registered", client_id=cid, event_id=None, details=f"tg={tg_user_id}")
    return payload

def touch_client_seen(tg_user_id: int) -> None:
    w = ws(SHEET_CLIENTS)
    row = find_row_by_value(w, "tg_user_id", tg_user_id)
    if row:
        update_cell(w, row, "last_seen_at", iso_dt())

def list_active_clients() -> List[Dict[str, Any]]:
    w = ws(SHEET_CLIENTS)
    rows = get_all_records(w)
    return [r for r in rows if str(r.get("status", "")).strip().lower() == "active"]

def create_event(type_code: int, title: str, description: str, start_at: str,
                 duration_min: int, link: str, created_by: str) -> Dict[str, Any]:
    w = ws(SHEET_EVENTS)
    event_id = f"ev_{short_uuid(10)}"
    payload = {
        "event_id": event_id,
        "type": int(type_code),
        "title": title,
        "description": description,
        "start_at": start_at,
        "duration_min": int(duration_min),
        "link": link,
        "created_by": created_by,
        "created_at": iso_dt(),
    }
    append_dict(w, payload)
    log_action("event_created", client_id=None, event_id=event_id, details=f"type={type_code}")
    return payload

def get_all_events() -> List[Dict[str, Any]]:
    w = ws(SHEET_EVENTS)
    return get_all_records(w)

def get_event_by_id(event_id: str) -> Optional[Dict[str, Any]]:
    for r in get_all_events():
        if str(r.get("event_id")).strip() == event_id:
            return r
    return None

def update_event_field(event_id: str, field: str, value: Any) -> None:
    w = ws(SHEET_EVENTS)
    row = find_row_by_value(w, "event_id", event_id)
    if row:
        update_cell(w, row, field, value)
        log_action("event_updated", client_id=None, event_id=event_id, details=f"{field}={value}")

def delete_event(event_id: str) -> None:
    w = ws(SHEET_EVENTS)
    row = find_row_by_value(w, "event_id", event_id)
    if row:
        delete_row(w, row)
        log_action("event_canceled", client_id=None, event_id=event_id, details="deleted")

def event_start_dt(event: Dict[str, Any]) -> Optional[datetime]:
    return parse_dt(str(event.get("start_at", "")).strip())

def list_future_events_sorted() -> List[Dict[str, Any]]:
    now = now_kyiv()
    events = []
    for e in get_all_events():
        dt = event_start_dt(e)
        if dt and dt >= now - timedelta(days=1):
            events.append((dt, e))
    events.sort(key=lambda x: x[0])
    return [e for _, e in events]

def list_alternative_events_same_type(type_code: int, exclude_event_id: str) -> List[Dict[str, Any]]:
    out = []
    now = now_kyiv()
    for e in get_all_events():
        if a2i(e.get("type")) == int(type_code) and str(e.get("event_id")) != exclude_event_id:
            dt = event_start_dt(e)
            if dt and dt >= now:
                out.append((dt, e))
    out.sort(key=lambda x: x[0])
    return [e for _, e in out]

def mark_attendance(event_id: str, client_id: str, attended: int = 1) -> None:
    w = ws(SHEET_ATTEND)
    rows = get_all_records(w)
    row = None
    for i, r in enumerate(rows, start=2):
        if str(r.get("event_id")) == event_id and str(r.get("client_id")) == client_id:
            row = i
            break
    payload = {
        "event_id": event_id,
        "client_id": client_id,
        "attended": int(attended),
        "marked_at": iso_dt(),
    }
    if row:
        update_row_dict(w, row, payload)
    else:
        append_dict(w, payload)
    log_action("attendance_marked", client_id=client_id, event_id=event_id, details=f"attended={attended}")

def attendance_clear_for_event(event_id: str, mode: str = "zero") -> int:
    """
    mode="zero"  — проставить attended=0 всем по этому event_id (и обновить marked_at)
    mode="delete" — удалить строки Attendance для этого event_id
    Возвращает количество затронутых строк.
    """
    w = ws(SHEET_ATTEND)
    rows = get_all_records(w)
    touched = 0

    if mode == "delete":
        # соберём индексы и удалим снизу, чтобы не сдвигались
        idxs = [i for i, r in enumerate(rows, start=2) if str(r.get("event_id")) == event_id]
        for i in reversed(idxs):
            delete_row(w, i)
            touched += 1
    else:
        # zero: ставим attended=0
        for i, r in enumerate(rows, start=2):
            if str(r.get("event_id")) == event_id:
                update_cell(w, i, "attended", 0)
                update_cell(w, i, "marked_at", iso_dt())
                touched += 1

    log_action("attendance_cleared_on_cancel", client_id="", event_id=event_id, details=f"mode={mode}; rows={touched}")
    return touched


def client_has_attended_type(client_id: str, type_code: int) -> bool:
    events_by_id = {e.get("event_id"): e for e in get_all_events()}
    w = ws(SHEET_ATTEND)
    rows = get_all_records(w)
    for r in rows:
        if str(r.get("client_id")) == client_id and a2i(r.get("attended")) == 1:
            ev = events_by_id.get(str(r.get("event_id")))
            if ev and a2i(ev.get("type")) == int(type_code):
                return True
    return False

def rsvp_upsert(event_id: str, client_id: str, rsvp: Optional[str] = None,
                remind_24h: Optional[int] = None,
                reminded_24h: Optional[int] = None,
                reminded_60m: Optional[int] = None) -> None:
    w = ws(SHEET_RSVP)
    rows = get_all_records(w)
    row_idx = None
    base = {}
    for i, r in enumerate(rows, start=2):
        if str(r.get("event_id")) == event_id and str(r.get("client_id")) == client_id:
            row_idx = i
            base = r
            break
    payload = {
        "event_id": event_id,
        "client_id": client_id,
        "rsvp": rsvp if rsvp is not None else base.get("rsvp", ""),
        "remind_24h": int(remind_24h) if remind_24h is not None else a2i(base.get("remind_24h"), 0),
        "reminded_24h": int(reminded_24h) if reminded_24h is not None else a2i(base.get("reminded_24h"), 0),
        "reminded_60m": int(reminded_60m) if reminded_60m is not None else a2i(base.get("reminded_60m"), 0),
        "rsvp_at": iso_dt(),
    }
    if row_idx:
        update_row_dict(w, row_idx, payload)
    else:
        append_dict(w, payload)

def rsvp_get_for_event(event_id: str) -> List[Dict[str, Any]]:
    w = ws(SHEET_RSVP)
    return [r for r in get_all_records(w) if str(r.get("event_id")) == event_id]

def feedback_get(event_id: str, client_id: str) -> Optional[Dict[str, Any]]:
    w = ws(SHEET_FEEDBACK)
    rows = get_all_records(w)
    for r in rows:
        if str(r.get("event_id")) == event_id and str(r.get("client_id")) == client_id:
            return r
    return None


def feedback_upsert(event_id: str, client_id: str, stars: Optional[int] = None, comment: Optional[str] = None) -> Dict[str, Any]:
    """Создаёт или обновляет запись фидбэка для пары (event_id, client_id)."""
    w = ws(SHEET_FEEDBACK)
    rows = get_all_records(w)
    row_idx = None
    current = {}
    for i, r in enumerate(rows, start=2):
        if str(r.get("event_id")) == event_id and str(r.get("client_id")) == client_id:
            row_idx = i
            current = r
            break

    payload = {
        "event_id": event_id,
        "client_id": client_id,
        "stars": int(stars) if stars is not None else a2i(current.get("stars"), 0),
        "comment": (comment if comment is not None else current.get("comment", "")) or "",
        "owner": current.get("owner", ""),
    }

    if row_idx:
        update_row_dict(w, row_idx, payload)
    else:
        append_dict(w, payload)

    return payload


def feedback_assign_owner(event_id: str, client_id: str, owner: str) -> None:
    w = ws(SHEET_FEEDBACK)
    rows = get_all_records(w)
    last_idx = None
    for i, r in enumerate(rows, start=2):
        if str(r.get("event_id")) == event_id and str(r.get("client_id")) == client_id:
            last_idx = i
    if last_idx:
        update_cell(w, last_idx, "owner", owner)

def try_get_tg_from_client_id(client_id: str) -> Optional[int]:
    w = ws(SHEET_CLIENTS)
    rows = get_all_records(w)
    for r in rows:
        if str(r.get("client_id")) == str(client_id):
            return int(r.get("tg_user_id"))
    return None

# ============================== KEYBOARDS ======================================

def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати конференцію", callback_data="admin:add")],
        [InlineKeyboardButton(text="📋 Список конференцій", callback_data="admin:list:0")],
    ])

def kb_rsvp(event_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Так, буду", callback_data=f"rsvp:{event_id}:going"),
            InlineKeyboardButton(text="🚫 Не зможу", callback_data=f"rsvp:{event_id}:declined"),
        ]
    ])


def kb_event_actions(event_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Змінити", callback_data=f"admin:edit:{event_id}")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data=f"admin:cancel:{event_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:list:0")],
    ])

def kb_edit_event_menu(event_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Назва", callback_data=f"admin:edit:{event_id}:field:title")],
        [InlineKeyboardButton(text="✏️ Опис", callback_data=f"admin:edit:{event_id}:field:description")],
        [InlineKeyboardButton(text="🗓 Дата/час", callback_data=f"admin:edit:{event_id}:field:start_at")],
        [InlineKeyboardButton(text="⏱ Тривалість (хв)", callback_data=f"admin:edit:{event_id}:field:duration_min")],
        [InlineKeyboardButton(text="🔗 Посилання", callback_data=f"admin:edit:{event_id}:field:link")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:list:0")],
    ])

def kb_cancel_confirm(event_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Так, скасувати", callback_data=f"admin:cancel:{event_id}:yes")],
        [InlineKeyboardButton(text="⬅️ Ні, назад", callback_data=f"admin:edit:{event_id}")],
    ])

def kb_claim_feedback(event_id: str, client_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛠 Беру в роботу", callback_data=f"claim:{event_id}:{client_id}")],
    ])
def kb_client_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📋 Мої конференції")]],
        resize_keyboard=True
    )
# ============================== STATE / MEMORY =================================

ADMINS: set[int] = set()

class RegisterSG(StatesGroup):
    wait_name = State()
    wait_phone = State()

class AdminAddSG(StatesGroup):
    menu = State()
    wait_title = State()
    wait_desc = State()
    wait_start_at = State()
    wait_duration = State()
    wait_link = State()

class AdminEditFieldSG(StatesGroup):
    wait_value = State()

class FeedbackSG(StatesGroup):
    wait_comment = State()

# ================================ BOT/DP =======================================

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone=str(TZ))

# =============================== HANDLERS ======================================

@dp.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    touch_client_seen(m.from_user.id)
    args = (m.text or "").split(maxsplit=1)
    arg = ""
    if len(args) > 1:
        arg = args[1].strip()

    # Адмін-режим
    if arg.startswith("admin_"):
        pwd = arg.split("admin_", 1)[1]
        if pwd == ADMIN_PASSWORD:
            ADMINS.add(m.from_user.id)
            await m.answer("Вітаю в адмін-панелі.", reply_markup=kb_admin_main())
            return
        else:
            await m.answer("Невірний пароль для адмін-панелі.")
            return

    # Клієнтський режим
    cli = get_client_by_tg(m.from_user.id)
    if not cli or not cli.get("full_name") or not cli.get("phone"):
        await state.set_state(RegisterSG.wait_name)
        await m.answer("👋 Привіт! Вкажіть, будь ласка, Ваше ПІБ (українською).")
        return

    await send_welcome_and_types_list(m, cli)

async def send_welcome_and_types_list(m: Message, cli: Dict[str, Any]):
    await m.answer(build_types_overview_text(cli), reply_markup=kb_client_main())


@dp.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(messages_get("help.body"))

# ---------- Реєстрація клієнта ----------

@dp.message(RegisterSG.wait_name)
async def reg_wait_name(m: Message, state: FSMContext):
    full_name = (m.text or "").strip()
    if len(full_name) < 3:
        await m.answer("Будь ласка, введіть коректне ПІБ (не менше 3 символів).")
        return
    await state.update_data(full_name=full_name)
    await state.set_state(RegisterSG.wait_phone)
    await m.answer("Вкажіть номер телефону у форматі 380XXXXXXXXX:")

@dp.message(RegisterSG.wait_phone)
async def reg_wait_phone(m: Message, state: FSMContext):
    phone = normalize_phone(m.text or "")
    if not phone:
        await m.answer("Невірний формат. Приклад: 380671234567. Спробуйте ще раз:")
        return
    data = await state.get_data()
    cli = upsert_client(m.from_user.id, data["full_name"], phone)
    await state.clear()
    await send_welcome_and_types_list(m, cli)
    
@dp.message(F.text == "📋 Мої конференції")
async def show_my_conferences(m: Message):
    cli = get_client_by_tg(m.from_user.id)
    if not cli:
        await m.answer("Будь ласка, зареєструйтесь командою /start.", reply_markup=kb_client_main())
        return
    await m.answer(build_types_overview_text(cli), reply_markup=kb_client_main())

# ---------- Адмін меню / додати / список / редагування ----------

@dp.callback_query(F.data == "admin:add")
async def admin_add(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    types = get_eventtypes_active()
    if not types:
        await q.message.edit_text("Немає активних типів конференцій.", reply_markup=kb_admin_main())
        await q.answer()
        return
    buttons = [[InlineKeyboardButton(text=t["title"], callback_data=f"admin:add:type:{t['type_code']}")] for t in types]
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:home")])
    await q.message.edit_text("Оберіть тип конференції:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await q.answer()

@dp.callback_query(F.data.startswith("admin:add:type:"))
async def admin_add_select_type(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    type_code = int(q.data.split(":")[-1])
    et = get_eventtype_by_code(type_code)
    if not et:
        await q.message.edit_text("Тип не знайдено.", reply_markup=kb_admin_main())
        await q.answer()
        return
    payload = {
        "type_code": type_code,
        "type_title": et["title"],
        "title": et["title"],
        "description": et["description"],
    }
    await state.set_state(AdminAddSG.menu)
    await state.update_data(**payload)
    await q.message.edit_text(
        f"Базові дані підставлено з довідника:\n"
        f"• Тип: {payload['type_title']}\n• Назва: {payload['title']}\n• Опис: {payload['description']}\n\n"
        f"Можете підправити та натиснути «➡️ Далі».",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Змінити назву", callback_data="admin:add:edit_title")],
            [InlineKeyboardButton(text="✏️ Змінити опис", callback_data="admin:add:edit_desc")],
            [InlineKeyboardButton(text="➡️ Далі", callback_data="admin:add:next")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:home")],
        ])
    )
    await q.answer()

@dp.callback_query(F.data == "admin:add:edit_title")
async def admin_add_edit_title(q: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAddSG.wait_title)
    await q.message.edit_text("Надішліть нову назву конференції:")
    await q.answer()

@dp.message(AdminAddSG.wait_title)
async def admin_add_wait_title(m: Message, state: FSMContext):
    title = (m.text or "").strip()
    await state.update_data(title=title)
    data = await state.get_data()
    await state.set_state(AdminAddSG.menu)
    await m.answer(
        f"Назву оновлено.\n\nПоточні дані:\n• Тип: {data['type_title']}\n• Назва: {data['title']}\n• Опис: {data['description']}\n\n"
        f"Натисніть «➡️ Далі» або змініть інше поле.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Змінити назву", callback_data="admin:add:edit_title")],
            [InlineKeyboardButton(text="✏️ Змінити опис", callback_data="admin:add:edit_desc")],
            [InlineKeyboardButton(text="➡️ Далі", callback_data="admin:add:next")],
        ])
    )

@dp.callback_query(F.data == "admin:add:edit_desc")
async def admin_add_edit_desc(q: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAddSG.wait_desc)
    await q.message.edit_text("Надішліть новий опис конференції:")
    await q.answer()

@dp.message(AdminAddSG.wait_desc)
async def admin_add_wait_desc(m: Message, state: FSMContext):
    desc = (m.text or "").strip()
    await state.update_data(description=desc)
    data = await state.get_data()
    await state.set_state(AdminAddSG.menu)
    await m.answer(
        f"Опис оновлено.\n\nПоточні дані:\n• Тип: {data['type_title']}\n• Назва: {data['title']}\n• Опис: {data['description']}\n\n"
        f"Натисніть «➡️ Далі» або змініть інше поле.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Змінити назву", callback_data="admin:add:edit_title")],
            [InlineKeyboardButton(text="✏️ Змінити опис", callback_data="admin:add:edit_desc")],
            [InlineKeyboardButton(text="➡️ Далі", callback_data="admin:add:next")],
        ])
    )

@dp.callback_query(F.data == "admin:add:next")
async def admin_add_next(q: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAddSG.wait_start_at)
    await q.message.edit_text("Вкажіть дату та час початку у форматі: YYYY-MM-DD HH:MM (Київ). Напр.: 2025-10-05 15:00")
    await q.answer()

@dp.message(AdminAddSG.wait_start_at)
async def admin_add_wait_start_at(m: Message, state: FSMContext):
    dt = parse_dt(m.text or "")
    if not dt:
        await m.answer("Невірний формат. Приклад: 2025-10-05 15:00 (Київ). Спробуйте ще раз:")
        return
    await state.update_data(start_at=iso_dt(dt))
    await state.set_state(AdminAddSG.wait_duration)
    await m.answer("Вкажіть тривалість у хвилинах (ціле число):")

@dp.message(AdminAddSG.wait_duration)
async def admin_add_wait_duration(m: Message, state: FSMContext):
    try:
        dur = int((m.text or "").strip())
        if dur <= 0:
            raise ValueError()
    except Exception:
        await m.answer("Вкажіть додатне ціле число хвилин. Спробуйте ще раз:")
        return
    await state.update_data(duration_min=dur)
    await state.set_state(AdminAddSG.wait_link)
    await m.answer("Вставте посилання на конференцію (URL):")

@dp.message(AdminAddSG.wait_link)
async def admin_add_wait_link(m: Message, state: FSMContext):
    link = (m.text or "").strip()
    data = await state.get_data()
    created = create_event(
        type_code=int(data["type_code"]),
        title=data["title"],
        description=data["description"],
        start_at=data["start_at"],
        duration_min=int(data["duration_min"]),
        link=link,
        created_by=f"admin:{m.from_user.id}",
    )
    await send_initial_invites_for_event(created)
    await state.clear()
    await m.answer(
        f"✅ Подію створено:\n"
        f"• {created['title']}\n"
        f"• Дата/час: {created['start_at']} (Київ)\n"
        f"• Тривалість: {created['duration_min']} хв\n"
        f"• Посилання: {created['link']}\n",
        reply_markup=kb_admin_main()
    )

@dp.callback_query(F.data == "admin:home")
async def admin_home(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    await q.message.edit_text("Адмін-панель:", reply_markup=kb_admin_main())
    await q.answer()

@dp.callback_query(F.data.startswith("admin:list:"))
async def admin_list(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    page = int(q.data.split(":")[-1])
    events = list_future_events_sorted()
    per = 10
    total = len(events)
    start = page * per
    end = start + per
    subset = events[start:end]
    if not subset and page != 0:
        page = 0
        start, end = 0, per
        subset = events[start:end]
    buttons = []
    for e in subset:
        dt = event_start_dt(e)
        dt_str = dt.strftime("%Y-%m-%d %H:%M") if dt else "—"
        buttons.append([InlineKeyboardButton(text=f"{e['title']} — {dt_str}", callback_data=f"admin:event:{e['event_id']}")])
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin:list:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin:list:{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🏠 Головне меню", callback_data="admin:home")])
    await q.message.edit_text(f"Список конференцій (усього: {total}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await q.answer()

@dp.callback_query(F.data.startswith("admin:event:"))
async def admin_event_open(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return
    event_id = parts[-1]
    e = get_event_by_id(event_id)
    if not e:
        await q.message.edit_text("Подію не знайдено.", reply_markup=kb_admin_main())
        await q.answer()
        return
    await q.message.edit_text(
        f"Подія:\n• {e['title']}\n• Опис: {e['description']}\n• Початок: {e['start_at']}\n"
        f"• Тривалість: {e['duration_min']} хв\n• Посилання: {e['link']}",
        reply_markup=kb_event_actions(event_id)
    )
    await q.answer()

@dp.callback_query(F.data.startswith("admin:edit:"))
async def admin_edit(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) == 3:
        event_id = parts[-1]
        await q.message.edit_text("Оберіть поле для редагування:", reply_markup=kb_edit_event_menu(event_id))
        await q.answer()
        return
    if len(parts) == 5 and parts[3] == "field":
        event_id = parts[2]
        field = parts[4]
        await state.set_state(AdminEditFieldSG.wait_value)
        await state.update_data(event_id=event_id, field=field)
        prompts = {
            "title": "Введіть нову назву:",
            "description": "Введіть новий опис:",
            "start_at": "Введіть нову дату/час у форматі YYYY-MM-DD HH:MM:",
            "duration_min": "Введіть нову тривалість у хвилинах:",
            "link": "Вставте нове посилання на конференцію:",
        }
        await q.message.edit_text(prompts.get(field, "Введіть значення:"))
        await q.answer()

@dp.message(AdminEditFieldSG.wait_value)
@dp.message(AdminEditFieldSG.wait_value)
async def admin_edit_field_value(m: Message, state: FSMContext):
    data = await state.get_data()
    event_id = data.get("event_id")
    field = data.get("field")

    # text-поля
    if field in {"title", "description", "link"}:
        val = (m.text or "").strip()
        update_event_field(event_id, field, val)
        await m.answer("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
        await state.clear()

        # подробные уведомления по каждому полю
        if field == "title":
            await notify_event_update(event_id, f"Оновлено назву: {val}")
        elif field == "description":
            await notify_event_update(event_id, "Оновлено опис.")
        elif field == "link":
            await notify_event_update(event_id, f"Оновлено посилання: {val}")
        return

    # дата/час
    if field == "start_at":
        dt = parse_dt(m.text or "")
        if not dt:
            await m.answer("Невірний формат. Приклад: 2025-10-05 15:00. Спробуйте ще раз:")
            return
        update_event_field(event_id, "start_at", iso_dt(dt))
        await m.answer("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
        await state.clear()
        # <-- ключ: в уведомлении шлём новое время
        await notify_event_update(event_id, f"Змінено дату/час: {fmt_date(dt)} о {fmt_time(dt)} (Київ)")
        return

    # тривалість
    if field == "duration_min":
        try:
            dur = int((m.text or "").strip())
            if dur <= 0:
                raise ValueError()
        except Exception:
            await m.answer("Введіть додатне ціле число. Спробуйте ще раз:")
            return
        update_event_field(event_id, "duration_min", dur)
        await m.answer("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
        await state.clear()
        await notify_event_update(event_id, f"Змінено тривалість: {dur} хв")
        return


@dp.callback_query(F.data.startswith("admin:cancel:"))
async def admin_cancel(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) == 3:
        event_id = parts[-1]
        await q.message.edit_text("Підтвердити скасування події?", reply_markup=kb_cancel_confirm(event_id))
        await q.answer()
        return
    if len(parts) == 4 and parts[-1] == "yes":
        event_id = parts[2]
        # 1) уведомляем участников
        await notify_event_cancel(event_id)
        # 2) чистим Attendance (ставим attended=0) — можно mode="delete", если хочешь удалять строки полностью
        attendance_clear_for_event(event_id, mode="zero")
        # 3) удаляем сам ивент
        delete_event(event_id)
        await q.message.edit_text("✅ Подію скасовано, відмітки відвідування скинуто.", reply_markup=kb_admin_main())
        await q.answer()
        return


# ---------- RSVP ----------

@dp.callback_query(F.data.startswith("rsvp:"))
async def cb_rsvp(q: CallbackQuery):
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return
    _, event_id, action = parts
    cli = get_client_by_tg(q.from_user.id)
    if not cli:
        await q.message.edit_text("Будь ласка, зареєструйтесь командою /start.")
        await q.answer()
        return
    client_id = cli["client_id"]
    event = get_event_by_id(event_id)
    if not event:
        await q.message.edit_text("Подію не знайдено.")
        await q.answer()
        return

    if action == "going":
        rsvp_upsert(event_id, client_id, rsvp="going")
        mark_attendance(event_id, client_id, 1)
        log_action("rsvp_yes", client_id=client_id, event_id=event_id, details="")
        await q.message.edit_text("Дякуємо! Участь підтверджено ✅")
        await q.answer()
        return

    if action == "declined":
        rsvp_upsert(event_id, client_id, rsvp="declined")
        log_action("rsvp_no", client_id=client_id, event_id=event_id, details="")
    
        alt = list_alternative_events_same_type(a2i(event.get("type")), event_id)
        if not alt:
            await q.message.edit_text("Добре! Тоді очікуйте нове запрошення на іншу дату.")
        else:
            rows = []
            for a in alt[:8]:  # не больше 8 кнопок
                dt = event_start_dt(a)
                when = f"{fmt_date(dt)} о {fmt_time(dt)}" if dt else a.get('start_at', '')
                # Кнопка выбирает альтернативную дату
                rows.append([InlineKeyboardButton(text=when, callback_data=f"alt:pick:{a['event_id']}")])
            rows.append([InlineKeyboardButton(text="❌ Закрити", callback_data="noop")])
    
            title_for_info = event.get("title", "подія")
            await q.message.edit_text(
                f"Можливі альтернативні дати за темою «{title_for_info}»:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
    await q.answer()
    return


@dp.callback_query(F.data.startswith("claim:"))
async def claim_feedback(q: CallbackQuery):
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return
    _, event_id, client_id = parts
    owner = f"@{q.from_user.username}" if q.from_user and q.from_user.username else f"id:{q.from_user.id}"
    feedback_assign_owner(event_id, client_id, owner)
    log_action("complaint_taken", client_id=client_id, event_id=event_id, details=f"owner={owner}")
    await q.message.edit_text(f"✅ Взято в роботу ({owner})")
    await q.answer()
    
@dp.callback_query(F.data.startswith("alt:pick:"))
async def alt_pick(q: CallbackQuery):
    # alt:pick:<alt_event_id>
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return

    alt_event_id = parts[2]
    cli = get_client_by_tg(q.from_user.id)
    if not cli:
        await q.message.edit_text("Будь ласка, зареєструйтесь командою /start.")
        await q.answer()
        return

    client_id = cli["client_id"]
    alt_event = get_event_by_id(alt_event_id)
    if not alt_event:
        await q.message.edit_text("Альтернативну дату не знайдено.")
        await q.answer()
        return

    # подтверждаем участие на выбранной дате
    rsvp_upsert(alt_event_id, client_id, rsvp="going")
    mark_attendance(alt_event_id, client_id, 1)  # как и в обычном 'going' сейчас
    log_action("rsvp_alt_yes", client_id=client_id, event_id=alt_event_id, details="picked_alternative")

    dt = event_start_dt(alt_event)
    when = f"{fmt_date(dt)} о {fmt_time(dt)}" if dt else alt_event.get("start_at", "")
    await q.message.edit_text(
        f"✅ Участь підтверджено на альтернативну дату:\n"
        f"• {alt_event.get('title','')}\n"
        f"• 🗓 {when}\n"
        f"• 🔗 {alt_event.get('link','')}"
    )
    await q.answer()

@dp.callback_query(F.data == "noop")
async def noop(q: CallbackQuery):
    await q.answer()

# ---------- FEEDBACK (зірки + коментар) ----------

async def route_low_feedback(event_id: str, client_id: str, stars: int, comment: str):
    cli_tg = try_get_tg_from_client_id(client_id)
    cli_row = get_client_by_tg(cli_tg) if cli_tg else None
    full_name = cli_row["full_name"] if cli_row else client_id
    phone = cli_row["phone"] if cli_row else "—"
    event = get_event_by_id(event_id) or {}

    text = (
        f"⚠️ Низька оцінка події\n"
        f"• Подія: {event.get('title','')}\n"
        f"• Клієнт: {full_name} (tg_id={cli_tg})\n"
        f"• Телефон: {phone}\n"
        f"• Оцінка: {stars}\n"
        f"• Коментар: {comment or '—'}"
    )
    kb = kb_claim_feedback(event_id, client_id)

    # 1) Пытаемся в SUPPORT_CHAT_ID
    try:
        msg = await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text, reply_markup=kb)
        log_action("feedback_low_notified", client_id=client_id, event_id=event_id, details=f"support_chat:{SUPPORT_CHAT_ID}")
        return
    except TelegramRetryAfter as ex:
        await asyncio.sleep(ex.retry_after + 1)
        try:
            msg = await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text, reply_markup=kb)
            log_action("feedback_low_notified", client_id=client_id, event_id=event_id, details=f"support_chat:{SUPPORT_CHAT_ID}/after_retry")
            return
        except Exception as ex2:
            log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"retry_fail:{type(ex2).__name__}")

    except (TelegramForbiddenError, TelegramBadRequest) as ex:
        # Бот не может писать в этот чат (не добавлен/не админ/неверный ID/канал закрыт и т.п.)
        log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"{type(ex).__name__}:{ex}")

    except Exception as ex:
        log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"unknown:{type(ex).__name__}")

    # 2) Фолбэк: личкой всем активным админам, если чат поддержки недоступен
    if ADMINS:
        for admin_id in list(ADMINS):
            try:
                await bot.send_message(chat_id=admin_id, text="(фолбэк) " + text, reply_markup=kb)
                log_action("feedback_low_notified_admin_dm", client_id=client_id, event_id=event_id, details=f"to_admin:{admin_id}")
            except Exception as ex:
                log_action("feedback_low_admin_dm_fail", client_id=client_id, event_id=event_id, details=f"{admin_id}:{type(ex).__name__}")


async def route_low_feedback_comment_update(event_id: str, client_id: str, comment: str):
    # короткая “добавка” к уже отправленной скарге
    cli_tg = try_get_tg_from_client_id(client_id)
    event = get_event_by_id(event_id) or {}
    text = (
        f"📝 Доповнення до скарги\n"
        f"• Подія: {event.get('title','')}\n"
        f"• Клієнт: {client_id} (tg_id={cli_tg})\n"
        f"• Коментар: {comment or '—'}"
    )
    try:
        await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text)
        log_action("low_fb_comment_update_sent", client_id=client_id, event_id=event_id, details="")
    except Exception as e:
        log_action("support_send_error", client_id=client_id, event_id=event_id, details=f"{e!r}")


@dp.callback_query(F.data.startswith("fb:"))
async def fb_callbacks(q: CallbackQuery, state: FSMContext):
    data = q.data or ""

    # Выбор звёзд: fb:<event_id>:<client_id>:<stars>
    if data.startswith("fb:") and data.count(":") == 3 and not data.startswith("fb:comment:") and not data.startswith("fb:skip:"):
        _, event_id, client_id, stars = data.split(":")
        stars = int(stars)
    
        # 1) сохраняем оценку
        feedback_upsert(event_id, client_id, stars=stars)
    
        # 2) СРАЗУ пингуем саппорт, если <4
        if stars < 4:
            try:
                await route_low_feedback(event_id, client_id, stars, "")
                log_action("low_fb_alert_sent", client_id=client_id, event_id=event_id, details=f"stars={stars}")
            except Exception as e:
                log_action("support_send_error", client_id=client_id, event_id=event_id, details=f"{e!r}")
    
        # 3) предлагаем комментарий или пропустить
        prompt = f"Дякуємо! Оцінка {stars}⭐️ збережена.\нБажаєте додати короткий коментар?"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Написати коментар", callback_data=f"fb:comment:{event_id}:{client_id}")],
            [InlineKeyboardButton(text="⏭ Пропустити", callback_data=f"fb:skip:{event_id}:{client_id}")]
        ])
        await q.message.edit_text(prompt, reply_markup=kb)
        await q.answer()
        return


    # Нажали «Пропустити»: fb:skip:<event_id>:<client_id>
    if data.startswith("fb:skip:"):
        _, _, event_id, client_id = data.split(":")
        await q.message.edit_text("Дякуємо за ваш відгук! ✅")
        await q.answer()
        return


    # Запросили ввод комментария: fb:comment:<event_id>:<client_id>
    if data.startswith("fb:comment:"):
        _, _, event_id, client_id = data.split(":")
        tg_id = try_get_tg_from_client_id(client_id)
        if not tg_id or not q.from_user or q.from_user.id != int(tg_id):
            await q.message.edit_text("Введіть коментар у приватному діалозі з ботом.")
            await q.answer()
            return
        await state.set_state(FeedbackSG.wait_comment)
        await state.update_data(event_id=event_id, client_id=client_id)
        await q.message.edit_text("Надішліть, будь ласка, текстовий коментар одним повідомленням.\nАбо надішліть «-», щоб пропустити.")
        await q.answer()
        return


@dp.message(FeedbackSG.wait_comment)
async def fb_wait_comment(m: Message, state: FSMContext):
    data = await state.get_data()
    event_id = data["event_id"]
    client_id = data["client_id"]

    comment = (m.text or "").strip()
    if comment == "-":
        comment = ""

    saved = feedback_upsert(event_id, client_id, comment=comment)
    stars = a2i(saved.get("stars"), 0)

    await m.answer("Дякуємо! Відгук збережено. ✅")
    await state.clear()

    # если оценка была низкой — досылаем апдейт коммента
    if stars and stars < 4 and comment:
        await route_low_feedback_comment_update(event_id, client_id, comment)



# =============================== NOTIFY HELPERS ================================

async def notify_event_update(event_id: str, what: str):
    event = get_event_by_id(event_id)
    if not event:
        return
    templ = messages_get("update.notice")
    body = templ.format(title=event["title"], what=what)
    for r in rsvp_get_for_event(event_id):
        if str(r.get("rsvp")) == "going":
            tg_id = try_get_tg_from_client_id(r.get("client_id"))
            if tg_id:
                try:
                    await bot.send_message(chat_id=int(tg_id), text=body)
                except Exception:
                    pass

async def notify_event_cancel(event_id: str):
    event = get_event_by_id(event_id)
    if not event:
        return
    templ = messages_get("cancel.notice")
    body = templ.format(title=event["title"])
    for r in rsvp_get_for_event(event_id):
        if str(r.get("rsvp")) == "going":
            tg_id = try_get_tg_from_client_id(r.get("client_id"))
            if tg_id:
                try:
                    await bot.send_message(chat_id=int(tg_id), text=body)
                except Exception:
                    pass
async def send_initial_invites_for_event(event: Dict[str, Any]):
    """Сразу рассылаем інвайт всем активным клиентам, кто не был на этом типе и не получал інвайт по этому event_id.
       Плюс антиспам: шлём только для ближайшего события этого типа и только если у клиента нет активного інвайта по типу.
    """
    dt = event_start_dt(event)
    if not dt:
        return

    # 1) Шлём інвайты только для ближайшей даты этого типа
    if not is_earliest_upcoming_event_of_type(event):
        return

    type_code = a2i(event.get("type"))
    for cli in list_active_clients():
        cid = cli.get("client_id"); tg_id = cli.get("tg_user_id")
        if not cid or not tg_id:
            continue
        if client_has_attended_type(cid, type_code):
            continue
        # 2) Если у клиента уже есть "активный" інвайт по этому типу — не дублируем
        if client_has_active_invite_for_type(cid, type_code):
            continue
        if has_log("invite_sent", cid, event["event_id"]):
            continue

        body = messages_get("invite.body").format(
            name=cli.get("full_name","Клієнт"),
            title=event["title"],
            date=fmt_date(dt),
            time=fmt_time(dt),
            description=event["description"]
        )
        try:
            await bot.send_message(chat_id=int(tg_id),
                                   text=messages_get("invite.title").format(title=event["title"]))
            await bot.send_message(chat_id=int(tg_id), text=body, reply_markup=kb_rsvp(event["event_id"]))
            # создаём/обновляем строку RSVP (пока без ответа)
            rsvp_upsert(event["event_id"], cid, rsvp="")
            log_action("invite_sent", client_id=cid, event_id=event["event_id"], details="immediate")
        except Exception as e:
            log_action("invite_immediate_error", client_id=cid, event_id=event["event_id"], details=f"{e!r}")


# =============================== SCHEDULER TICK ================================

async def scheduler_tick():
    now = now_kyiv()
    for e in list_future_events_sorted():
        dt = event_start_dt(e)
        if not dt:
            continue

        # Сколько осталось до старта (в секундах)
        diff = (dt - now).total_seconds()

        # ============= 2) НАГАДУВАННЯ за ~3 хв (эмулируем 24h) ==================
        # (было: 24*3600 ± 60)
        if 3*60 - 60 <= diff <= 3*60 + 60:
            for r in rsvp_get_for_event(e["event_id"]):
                cid = r.get("client_id")
                tg_id = try_get_tg_from_client_id(cid)
                if not tg_id:
                    continue
                if a2i(r.get("reminded_24h"), 0) == 1:
                    continue
                # Только для тех, кто подтвердил участие
                if str(r.get("rsvp")) == "going":
                    body = messages_get("reminder.24h").format(
                        title=e["title"], time=fmt_time(dt), link=e["link"]
                    )
                    try:
                        await bot.send_message(chat_id=int(tg_id), text=body)
                        rsvp_upsert(e["event_id"], cid, reminded_24h=1)
                        log_action("remind_24h_sent", client_id=cid, event_id=e["event_id"], details="test_3min")
                    except Exception:
                        pass

        # ============= 3) НАГАДУВАННЯ за ~2 хв (эмулируем 60m) ==================
        # (было: 60*60 ± 60, потом 5*60 ± 60)
        if 2*60 - 60 <= diff <= 2*60 + 60:
            for r in rsvp_get_for_event(e["event_id"]):
                cid = r.get("client_id")
                tg_id = try_get_tg_from_client_id(cid)
                if not tg_id:
                    continue
                if a2i(r.get("reminded_60m"), 0) == 1:
                    continue
                if str(r.get("rsvp")) == "going":
                    body = messages_get("reminder.60m").format(title=e["title"], link=e["link"])
                    try:
                        await bot.send_message(chat_id=int(tg_id), text=body)
                        rsvp_upsert(e["event_id"], cid, reminded_60m=1)
                        log_action("remind_60m_sent", client_id=cid, event_id=e["event_id"], details="test_2min")
                    except Exception:
                        pass

        # ============= 4) ФІДБЕК через ~2 хв після завершення ====================
        # (было: +3 часа, потом +5 мин; делаем +2 минуты от конца)
        end_dt = dt + timedelta(minutes=a2i(e.get("duration_min")))
        if -60 <= (now - end_dt - timedelta(minutes=2)).total_seconds() <= 60:
            if has_log("feedback_requested", client_id="", event_id=e["event_id"]):
                continue

            # Всем, у кого attended=1 по этому событию
            w_att = ws(SHEET_ATTEND)
            rows_att = get_all_records(w_att)
            for r in rows_att:
                if str(r.get("event_id")) == e["event_id"] and a2i(r.get("attended")) == 1:
                    cid = r.get("client_id")
                    tg_id = try_get_tg_from_client_id(cid)
                    if not tg_id:
                        continue

                    text = messages_get("feedback.ask").format(title=e["title"])
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text="⭐️1", callback_data=f"fb:{e['event_id']}:{cid}:1"),
                            InlineKeyboardButton(text="⭐️2", callback_data=f"fb:{e['event_id']}:{cid}:2"),
                            InlineKeyboardButton(text="⭐️3", callback_data=f"fb:{e['event_id']}:{cid}:3"),
                            InlineKeyboardButton(text="⭐️4", callback_data=f"fb:{e['event_id']}:{cid}:4"),
                            InlineKeyboardButton(text="⭐️5", callback_data=f"fb:{e['event_id']}:{cid}:5"),
                        ]
                    ])
                    try:
                        await bot.send_message(chat_id=int(tg_id), text=text, reply_markup=kb)
                    except Exception:
                        pass

            log_action("feedback_requested", client_id="", event_id=e["event_id"], details="test_plus2min")




# ================================ STARTUP ======================================

async def on_startup():
    scheduler.add_job(scheduler_tick, "interval", seconds=60, id="tick", replace_existing=True)
    scheduler.start()

async def main():
    # на всякий случай: снимаем webhook, чтобы не было конфликта с polling
    await bot.delete_webhook(drop_pending_updates=True)
    await on_startup()
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    asyncio.run(main())
