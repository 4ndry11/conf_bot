# -*- coding: utf-8 -*-

import os
import re
import uuid
import json
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from gspread.utils import rowcol_to_a1
from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, MessageEntity
)
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, JobQueue
)

# =============================== CONFIG ======================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", "-1003053461710"))
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "f7T9vQ1111wLp2Gx8Z")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Conference ZVILNYMO")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
GOOGLE_SA_PATH = os.getenv("GOOGLE_SA_PATH", "/etc/secrets/gsheets.json")

TZ = ZoneInfo(TIMEZONE)

# =========================== SHEETS CONNECTION ================================

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
    rng = f"A{row_idx}:{rowcol_to_a1(1, len(headers))[:-1]}{row_idx}"
    w.update(rng, [row], value_input_option="USER_ENTERED")

def update_cell(w: gspread.Worksheet, row_idx: int, column_name: str, value: Any) -> None:
    headers = ws_headers(w)
    if column_name not in headers:
        return
    col_idx = headers.index(column_name) + 1
    a1 = f"{rowcol_to_a1(row_idx, col_idx)}"
    w.update(a1, str(value) if value is not None else "", value_input_option="USER_ENTERED")

def delete_row(w: gspread.Worksheet, row_idx: int) -> None:
    w.delete_rows(row_idx)

# =============================== HELPERS ======================================

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

def a2i(v: Any, default: int = 0) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default

# =========================== DOMAIN READ/WRITE ================================

# Sheets names (fixed by spec)
SHEET_EVENTTYPES = "EventTypes"
SHEET_CLIENTS    = "Clients"
SHEET_EVENTS     = "Events"
SHEET_ATTEND     = "Attendance"
SHEET_LOG        = "DeliveryLog"
SHEET_FEEDBACK   = "Feedback"
SHEET_MSG        = "Messages"
SHEET_RSVP       = "RSVP"

def messages_get(key: str, lang: str = "uk") -> str:
    try:
        w = ws(SHEET_MSG)
        rows = get_all_records(w)
        for r in rows:
            if str(r.get("key")).strip() == key and str(r.get("lang", "uk")).strip() == lang:
                return str(r.get("text", "")).replace("\\n", "\n")
    except Exception:
        pass
    # fallback невеликий, щоби не падати
    FALLBACKS = {
        "invite.title": "Запрошення на зустріч: {title}",
        "invite.body": "{name}, запрошуємо на зустріч: {title}\n🗓 {date} о {time} (Київ)\nℹ️ {description}\nОберіть варіант нижче:",
        "reminder.60m": "⏰ Нагадуємо: через 1 год почнеться {title}. Посилання: {link}",
        "reminder.24h": "🔔 Нагадуємо: завтра о {time} відбудеться {title}.\nПосилання: {link}",
        "feedback.ask": "Дякуємо за участь у *{title}*.\nОцініть, будь ласка (1–5 ⭐️) та додайте коментар.",
        "update.notice": "🛠 Оновлення зустрічі {title}.\nЗверніть увагу: {what}",
        "cancel.notice": "❌ Зустріч {title} скасовано. Ми надішлемо нову дату найближчим часом.",
        "help.body": "👋 Це бот для запрошень на наші онлайн-зустрічі.",
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
        # зберігаємо created_at зі старого
        old = w.row_values(existing_row)
        headers = ws_headers(w)
        try:
            old_map = {headers[i]: old[i] if i < len(old) else "" for i in range(len(headers))}
            payload["created_at"] = old_map.get("created_at", now)
        except Exception:
            pass
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
    row = None
    # унікально по (event_id, client_id)
    rows = get_all_records(w)
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
    for i, r in enumerate(rows, start=2):
        if str(r.get("event_id")) == event_id and str(r.get("client_id")) == client_id:
            row_idx = i
            base = r
            break
    base = base if row_idx else {}
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

def feedback_save(event_id: str, client_id: str, stars: int, comment: str = "") -> None:
    w = ws(SHEET_FEEDBACK)
    payload = {
        "event_id": event_id,
        "client_id": client_id,
        "stars": int(stars),
        "comment": comment or "",
        "owner": "",
    }
    # перезапис по унікальному ключу (event_id, client_id) — простий варіант: add новий рядок
    append_dict(w, payload)
    if stars < 4:
        log_action("feedback_low_routed", client_id=client_id, event_id=event_id, details=f"stars={stars}")

def feedback_assign_owner(event_id: str, client_id: str, owner: str) -> None:
    w = ws(SHEET_FEEDBACK)
    rows = get_all_records(w)
    # знаходимо останній запис для пари
    last_idx = None
    for i, r in enumerate(rows, start=2):
        if str(r.get("event_id")) == event_id and str(r.get("client_id")) == client_id:
            last_idx = i
    if last_idx:
        update_cell(w, last_idx, "owner", owner)

# ============================== UI BUILDERS ===================================

def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Додати конференцію", callback_data="admin:add")],
        [InlineKeyboardButton("📋 Список конференцій", callback_data="admin:list:0")]
    ])

def kb_rsvp(event_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Так, буду", callback_data=f"rsvp:{event_id}:going"),
            InlineKeyboardButton("🚫 Не зможу", callback_data=f"rsvp:{event_id}:declined"),
        ],
        [InlineKeyboardButton("🔔 Нагадати за 24 год", callback_data=f"rsvp:{event_id}:remind")],
    ])

def kb_edit_event_menu(event_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Назва", callback_data=f"admin:edit:{event_id}:field:title")],
        [InlineKeyboardButton("✏️ Опис", callback_data=f"admin:edit:{event_id}:field:description")],
        [InlineKeyboardButton("🗓 Дата/час", callback_data=f"admin:edit:{event_id}:field:start_at")],
        [InlineKeyboardButton("⏱ Тривалість (хв)", callback_data=f"admin:edit:{event_id}:field:duration_min")],
        [InlineKeyboardButton("🔗 Посилання", callback_data=f"admin:edit:{event_id}:field:link")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"admin:list:0")]
    ])

def kb_event_actions(event_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Змінити", callback_data=f"admin:edit:{event_id}")],
        [InlineKeyboardButton("❌ Скасувати", callback_data=f"admin:cancel:{event_id}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="admin:list:0")]
    ])

def kb_cancel_confirm(event_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Так, скасувати", callback_data=f"admin:cancel:{event_id}:yes")],
        [InlineKeyboardButton("⬅️ Ні, назад", callback_data=f"admin:edit:{event_id}")]
    ])

def kb_claim_feedback(event_id: str, client_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛠 Беру в роботу", callback_data=f"claim:{event_id}:{client_id}")]
    ])

# ============================== STATE STORAGE =================================

ADMINS: set[int] = set()
USER_STATE: Dict[int, Dict[str, Any]] = {}   # простий FSM у пам'яті

def is_admin(user_id: int) -> bool:
    return user_id in ADMINS

def require_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else 0
    return is_admin(uid)

def set_state(user_id: int, mode: str, step: str, data: Optional[Dict[str, Any]] = None):
    USER_STATE[user_id] = {"mode": mode, "step": step, "data": data or {}}

def get_state(user_id: int) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    s = USER_STATE.get(user_id, {})
    return s.get("mode"), s.get("step"), s.get("data", {})

def clear_state(user_id: int):
    USER_STATE.pop(user_id, None)

# ================================ HANDLERS ====================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    args = context.args or []
    arg = args[0] if args else ""
    touch_client_seen(user.id)

    # Адмін-режим через deep-link: /start admin_<password>
    if arg.startswith("admin_"):
        pwd = arg.split("admin_", 1)[1]
        if pwd == ADMIN_PASSWORD:
            ADMINS.add(user.id)
            await msg.reply_text("Вітаю в адмін-панелі.", reply_markup=kb_admin_main())
            return
        else:
            await msg.reply_text("Невірний пароль для адмін-панелі.")
            return

    # Клієнтський старт
    cli = get_client_by_tg(user.id)
    if not cli or not cli.get("full_name") or not cli.get("phone"):
        await msg.reply_text("👋 Привіт! Вкажіть, будь ласка, Ваше ПІБ (українською).")
        set_state(user.id, "register", "wait_name", {})
        return

    # Уже зареєстрований — показ переліку типів з відмітками
    await send_welcome_and_types_list(update, context, cli)

async def send_welcome_and_types_list(update: Update, context: ContextTypes.DEFAULT_TYPE, cli: Dict[str, Any]):
    user = update.effective_user
    msg = update.effective_message
    text = (
        "✅ Ви підключені до розсилки на конференції.\n"
        "Надсилатимемо інвайти на найближчі події.\n\n"
        "Доступні типи конференцій:\n"
    )
    rows = get_eventtypes_active()
    # Визначаємо відвідані по типу
    lines = []
    for rt in rows:
        tcode = a2i(rt.get("type_code"))
        title = str(rt.get("title"))
        attended = client_has_attended_type(cli["client_id"], tcode)
        flag = "✅ Був(ла)" if attended else "⭕️ Ще не був(ла)"
        lines.append(f"• {title} — {flag}")
    text += "\n".join(lines) if lines else "Наразі немає активних типів."
    await msg.reply_text(text)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(messages_get("help.body"))

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    mode, step, data = get_state(user.id)

    # Реєстрація клієнта: ПІБ -> телефон
    if mode == "register" and step == "wait_name":
        full_name = (msg.text or "").strip()
        if len(full_name) < 3:
            await msg.reply_text("Будь ласка, введіть коректне ПІБ (не менше 3 символів).")
            return
        data["full_name"] = full_name
        set_state(user.id, "register", "wait_phone", data)
        await msg.reply_text("Вкажіть номер телефону у форматі 380XXXXXXXXX:")
        return

    if mode == "register" and step == "wait_phone":
        phone = normalize_phone(msg.text or "")
        if not phone:
            await msg.reply_text("Невірний формат. Приклад: 380671234567. Спробуйте ще раз:")
            return
        cli = upsert_client(user.id, data["full_name"], phone)
        clear_state(user.id)
        await send_welcome_and_types_list(update, context, cli)
        return

    # Адмін: майстер створення — редагування титулу/опису/дат/тривалості/лінку
    if mode == "admin_add":
        if step == "await_title":
            data["title"] = (msg.text or "").strip()
            set_state(user.id, "admin_add", "menu", data)
            await msg.reply_text(f"Назву оновлено.\n\nПоточні дані:\n• Тип: {data['type_title']}\n• Назва: {data['title']}\n• Опис: {data['description']}\n\nНатисніть «➡️ Далі» або змініть інше поле.",
                                 reply_markup=InlineKeyboardMarkup([
                                     [InlineKeyboardButton("✏️ Змінити назву", callback_data="admin:add:edit_title")],
                                     [InlineKeyboardButton("✏️ Змінити опис", callback_data="admin:add:edit_desc")],
                                     [InlineKeyboardButton("➡️ Далі", callback_data="admin:add:next")]
                                 ]))
            return
        if step == "await_desc":
            data["description"] = (msg.text or "").strip()
            set_state(user.id, "admin_add", "menu", data)
            await msg.reply_text(f"Опис оновлено.\n\nПоточні дані:\n• Тип: {data['type_title']}\n• Назва: {data['title']}\n• Опис: {data['description']}\n\nНатисніть «➡️ Далі» або змініть інше поле.",
                                 reply_markup=InlineKeyboardMarkup([
                                     [InlineKeyboardButton("✏️ Змінити назву", callback_data="admin:add:edit_title")],
                                     [InlineKeyboardButton("✏️ Змінити опис", callback_data="admin:add:edit_desc")],
                                     [InlineKeyboardButton("➡️ Далі", callback_data="admin:add:next")]
                                 ]))
            return
        if step == "await_start_at":
            dt = parse_dt(msg.text or "")
            if not dt:
                await msg.reply_text("Невірний формат. Приклад: 2025-10-05 15:00 (Київ). Спробуйте ще раз:")
                return
            data["start_at"] = iso_dt(dt)
            set_state(user.id, "admin_add", "await_duration", data)
            await msg.reply_text("Вкажіть тривалість у хвилинах (ціле число):")
            return
        if step == "await_duration":
            try:
                dur = int((msg.text or "").strip())
                if dur <= 0:
                    raise ValueError()
            except Exception:
                await msg.reply_text("Вкажіть додатне ціле число хвилин. Спробуйте ще раз:")
                return
            data["duration_min"] = dur
            set_state(user.id, "admin_add", "await_link", data)
            await msg.reply_text("Вставте посилання на конференцію (URL):")
            return
        if step == "await_link":
            link = (msg.text or "").strip()
            data["link"] = link
            # створення події
            created = create_event(
                type_code=int(data["type_code"]),
                title=data["title"],
                description=data["description"],
                start_at=data["start_at"],
                duration_min=int(data["duration_min"]),
                link=data["link"],
                created_by=f"admin:{user.id}"
            )
            clear_state(user.id)
            await msg.reply_text(
                f"✅ Подію створено:\n"
                f"• {created['title']}\n"
                f"• Дата/час: {created['start_at']} (Київ)\n"
                f"• Тривалість: {created['duration_min']} хв\n"
                f"• Посилання: {created['link']}\n",
                reply_markup=kb_admin_main()
            )
            return

    if mode == "admin_edit_field":
        event_id = data.get("event_id")
        field = data.get("field")
        if field in {"title", "description", "link"}:
            val = (msg.text or "").strip()
            update_event_field(event_id, field, val)
            await msg.reply_text("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
            clear_state(user.id)
            # повідомлення учасникам про оновлення (якщо треба)
            await notify_event_update(context, event_id, f"Змінено поле: {field}")
            return
        elif field == "start_at":
            dt = parse_dt(msg.text or "")
            if not dt:
                await msg.reply_text("Невірний формат. Приклад: 2025-10-05 15:00. Спробуйте ще раз:")
                return
            update_event_field(event_id, "start_at", iso_dt(dt))
            await msg.reply_text("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
            clear_state(user.id)
            await notify_event_update(context, event_id, "Змінено дату/час")
            return
        elif field == "duration_min":
            try:
                dur = int((msg.text or "").strip())
                if dur <= 0:
                    raise ValueError()
            except Exception:
                await msg.reply_text("Введіть додатне ціле число. Спробуйте ще раз:")
                return
            update_event_field(event_id, "duration_min", dur)
            await msg.reply_text("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
            clear_state(user.id)
            await notify_event_update(context, event_id, "Змінено тривалість")
            return

    if mode == "feedback_comment":
        event_id = data.get("event_id")
        client_id = data.get("client_id")
        stars = int(data.get("stars", 0))
        comment = (msg.text or "").strip()
        feedback_save(event_id, client_id, stars, comment)
        clear_state(user.id)
        await msg.reply_text("Дякуємо! Відгук збережено.")
        if stars < 4:
            await route_low_feedback(context, event_id, client_id, stars, comment)
        return

# ============================== CALLBACKS =====================================

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    data = query.data or ""
    await query.answer()

    # Адмін навігація
    if data == "admin:add":
        if not is_admin(user.id):
            return
        # Показ списку типів (за тайтлами)
        types = get_eventtypes_active()
        if not types:
            await query.edit_message_text("Немає активних типів конференцій.", reply_markup=kb_admin_main())
            return
        buttons = [[InlineKeyboardButton(t["title"], callback_data=f"admin:add:type:{t['type_code']}")] for t in types]
        buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="admin:home")])
        await query.edit_message_text("Оберіть тип конференції:", reply_markup=InlineKeyboardMarkup(buttons))
        return

    if data.startswith("admin:add:type:"):
        if not is_admin(user.id):
            return
        type_code = int(data.split(":")[-1])
        et = get_eventtype_by_code(type_code)
        if not et:
            await query.edit_message_text("Тип не знайдено.", reply_markup=kb_admin_main())
            return
        st = {
            "type_code": type_code,
            "type_title": et["title"],
            "title": et["title"],
            "description": et["description"],
        }
        set_state(user.id, "admin_add", "menu", st)
        await query.edit_message_text(
            f"Базові дані підставлено з довідника:\n"
            f"• Тип: {st['type_title']}\n• Назва: {st['title']}\n• Опис: {st['description']}\n\n"
            f"Можете підправити та натиснути «➡️ Далі».",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Змінити назву", callback_data="admin:add:edit_title")],
                [InlineKeyboardButton("✏️ Змінити опис", callback_data="admin:add:edit_desc")],
                [InlineKeyboardButton("➡️ Далі", callback_data="admin:add:next")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="admin:home")],
            ])
        )
        return

    if data == "admin:add:edit_title":
        set_state(user.id, "admin_add", "await_title", USER_STATE[user.id]["data"])
        await query.edit_message_text("Надішліть нову назву конференції:")
        return

    if data == "admin:add:edit_desc":
        set_state(user.id, "admin_add", "await_desc", USER_STATE[user.id]["data"])
        await query.edit_message_text("Надішліть новий опис конференції:")
        return

    if data == "admin:add:next":
        _, step, st = get_state(user.id)
        if not st:
            await query.edit_message_text("Сесію створення перервано. Спробуйте ще раз.", reply_markup=kb_admin_main())
            return
        set_state(user.id, "admin_add", "await_start_at", st)
        await query.edit_message_text("Вкажіть дату та час початку у форматі: YYYY-MM-DD HH:MM (Київ). Напр.: 2025-10-05 15:00")
        return

    if data.startswith("admin:list:"):
        if not is_admin(user.id):
            return
        page = int(data.split(":")[-1])
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
            buttons.append([InlineKeyboardButton(f"{e['title']} — {dt_str}", callback_data=f"admin:event:{e['event_id']}")])
        nav = []
        if start > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"admin:list:{page-1}"))
        if end < total:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"admin:list:{page+1}"))
        buttons.append(nav or [InlineKeyboardButton("—", callback_data="noop")])
        buttons.append([InlineKeyboardButton("🏠 Головне меню", callback_data="admin:home")])
        await query.edit_message_text(f"Список конференцій (усього: {total}):", reply_markup=InlineKeyboardMarkup(buttons))
        return

    if data == "admin:home":
        if not is_admin(user.id):
            return
        await query.edit_message_text("Адмін-панель:", reply_markup=kb_admin_main())
        return

    if data.startswith("admin:event:") and data.count(":") == 2:
        if not is_admin(user.id):
            return
        event_id = data.split(":")[-1]
        e = get_event_by_id(event_id)
        if not e:
            await query.edit_message_text("Подію не знайдено.", reply_markup=kb_admin_main())
            return
        await query.edit_message_text(
            f"Подія:\n• {e['title']}\n• Опис: {e['description']}\n• Початок: {e['start_at']}\n"
            f"• Тривалість: {e['duration_min']} хв\n• Посилання: {e['link']}",
            reply_markup=kb_event_actions(event_id)
        )
        return

    if data.startswith("admin:edit:") and data.count(":") == 2:
        if not is_admin(user.id):
            return
        event_id = data.split(":")[-1]
        await query.edit_message_text("Оберіть поле для редагування:", reply_markup=kb_edit_event_menu(event_id))
        return

    if data.startswith("admin:edit:") and data.count(":") == 4 and ":field:" in data:
        if not is_admin(user.id):
            return
        _, _, event_id, _, field = data.split(":")
        set_state(user.id, "admin_edit_field", "await", {"event_id": event_id, "field": field})
        prompts = {
            "title": "Введіть нову назву:",
            "description": "Введіть новий опис:",
            "start_at": "Введіть нову дату/час у форматі YYYY-MM-DD HH:MM:",
            "duration_min": "Введіть нову тривалість у хвилинах:",
            "link": "Вставте нове посилання на конференцію:",
        }
        await query.edit_message_text(prompts.get(field, "Введіть значення:"))
        return

    if data.startswith("admin:cancel:") and data.count(":") == 2:
        if not is_admin(user.id):
            return
        event_id = data.split(":")[-1]
        await query.edit_message_text("Підтвердити скасування події?", reply_markup=kb_cancel_confirm(event_id))
        return

    if data.startswith("admin:cancel:") and data.endswith(":yes"):
        if not is_admin(user.id):
            return
        event_id = data.split(":")[2]
        # повідомляємо учасникам з RSVP=going
        await notify_event_cancel(context, event_id)
        delete_event(event_id)
        await query.edit_message_text("✅ Подію скасовано та видалено.", reply_markup=kb_admin_main())
        return

    # Клієнтські RSVP
    if data.startswith("rsvp:"):
        _, event_id, action = data.split(":")
        cli = get_client_by_tg(user.id)
        if not cli:
            await query.edit_message_text("Будь ласка, зареєструйтесь командою /start.")
            return
        client_id = cli["client_id"]
        event = get_event_by_id(event_id)
        if not event:
            await query.edit_message_text("Подію не знайдено.")
            return

        if action == "going":
            rsvp_upsert(event_id, client_id, rsvp="going")
            # Фіксуємо відвідування як attended=1 (для логіки відміток та фідбеку)
            mark_attendance(event_id, client_id, 1)
            log_action("rsvp_yes", client_id=client_id, event_id=event_id, details="")
            await query.edit_message_text("Дякуємо! Участь підтверджено ✅")
            return

        if action == "declined":
            rsvp_upsert(event_id, client_id, rsvp="declined")
            log_action("rsvp_no", client_id=client_id, event_id=event_id, details="")
            # пропонуємо альтернативи цього самого типу
            alt = list_alternative_events_same_type(a2i(event.get("type")), event_id)
            if not alt:
                await query.edit_message_text("Добре! Тоді очікуйте нове запрошення на іншу дату.")
            else:
                btns = [[InlineKeyboardButton(f"{a['title']} — {a['start_at']}", callback_data="noop")] for a in alt]
                await query.edit_message_text("Можливі альтернативи:", reply_markup=InlineKeyboardMarkup(btns))
            return

        if action == "remind":
            rsvp_upsert(event_id, client_id, rsvp="remind_24h", remind_24h=1)
            log_action("rsvp_remind_24h", client_id=client_id, event_id=event_id, details="")
            await query.edit_message_text("Гаразд! Нагадаємо за 24 години 🔔")
            return

    # Претензію взято у роботу (низький відгук)
    if data.startswith("claim:"):
        _, event_id, client_id = data.split(":")
        owner = f"@{update.effective_user.username}" if update.effective_user and update.effective_user.username else f"id:{update.effective_user.id}"
        feedback_assign_owner(event_id, client_id, owner)
        log_action("complaint_taken", client_id=client_id, event_id=event_id, details=f"owner={owner}")
        await query.edit_message_text(f"✅ Взято в роботу ({owner})")
        return

# ================================= NOTIFY =====================================

async def notify_event_update(context: ContextTypes.DEFAULT_TYPE, event_id: str, what: str):
    # Розсилка оновлення тим, хто RSVP=going
    event = get_event_by_id(event_id)
    if not event:
        return
    templ = messages_get("update.notice")
    body = templ.format(title=event["title"], what=what)
    # пробігаємо по RSVP
    for r in rsvp_get_for_event(event_id):
        if str(r.get("rsvp")) == "going":
            tg_id = try_get_tg_from_client_id(r.get("client_id"))
            if tg_id:
                try:
                    await context.bot.send_message(chat_id=int(tg_id), text=body)
                except Exception:
                    pass

async def notify_event_cancel(context: ContextTypes.DEFAULT_TYPE, event_id: str):
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
                    await context.bot.send_message(chat_id=int(tg_id), text=body)
                except Exception:
                    pass

def try_get_tg_from_client_id(client_id: str) -> Optional[int]:
    w = ws(SHEET_CLIENTS)
    rows = get_all_records(w)
    for r in rows:
        if str(r.get("client_id")) == str(client_id):
            return int(r.get("tg_user_id"))
    return None

async def route_low_feedback(context: ContextTypes.DEFAULT_TYPE, event_id: str, client_id: str, stars: int, comment: str):
    # повідомлення в чат підтримки з кнопкою «Беру в роботу»
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
    try:
        await context.bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text,
                                       reply_markup=kb_claim_feedback(event_id, client_id))
    except Exception:
        pass

# =============================== SCHEDULER ====================================

async def scheduler_tick(context: ContextTypes.DEFAULT_TYPE):
    now = now_kyiv()
    # 1) INVITES (-24h) -> усім активним, хто ще не був на типі
    for e in list_future_events_sorted():
        dt = event_start_dt(e)
        if not dt:
            continue
        # Вікно для відправки інвайтів: коли настав момент -24h (±60с)
        diff = (dt - now).total_seconds()
        if 0 <= diff <= 60 + 5 or (24*3600 - 60) <= diff <= (24*3600 + 60):
            # (покриваємо обидва варіанти у випадку дрібних зсувів)
            # аудиторія
            type_code = a2i(e.get("type"))
            for cli in list_active_clients():
                cid = cli.get("client_id")
                tg_id = cli.get("tg_user_id")
                if not cid or not tg_id:
                    continue
                # пропускаємо, якщо вже був на цьому типі
                if client_has_attended_type(cid, type_code):
                    continue
                # не дублюємо інвайт
                if has_log("invite_sent", cid, e["event_id"]):
                    continue
                # надсилаємо інвайт
                title = e["title"]
                descr = e["description"]
                body = messages_get("invite.body").format(
                    name=cli.get("full_name","Клієнт"),
                    title=title,
                    date=fmt_date(dt),
                    time=fmt_time(dt),
                    description=descr
                )
                try:
                    await context.bot.send_message(chat_id=int(tg_id),
                                                   text=messages_get("invite.title").format(title=title))
                    await context.bot.send_message(chat_id=int(tg_id), text=body,
                                                   reply_markup=kb_rsvp(e["event_id"]))
                    log_action("invite_sent", client_id=cid, event_id=e["event_id"], details="")
                except Exception:
                    pass

        # 2) REMINDER -24h: тим, хто going або обрав remind_24h (щоб не плутатись з інвайтом — перевіряємо прапор reminded_24h)
        if 24*3600 - 60 <= diff <= 24*3600 + 60:
            for r in rsvp_get_for_event(e["event_id"]):
                cid = r.get("client_id")
                tg_id = try_get_tg_from_client_id(cid)
                if not tg_id:
                    continue
                if a2i(r.get("reminded_24h"), 0) == 1:
                    continue
                if str(r.get("rsvp")) in {"going", "remind_24h"}:
                    body = messages_get("reminder.24h").format(title=e["title"], time=fmt_time(dt), link=e["link"])
                    try:
                        await context.bot.send_message(chat_id=int(tg_id), text=body)
                        rsvp_upsert(e["event_id"], cid, reminded_24h=1)
                        log_action("remind_24h_sent", client_id=cid, event_id=e["event_id"], details="")
                    except Exception:
                        pass

        # 3) REMINDER -60m: тим, хто going
        if 60*60 - 60 <= diff <= 60*60 + 60:
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
                        await context.bot.send_message(chat_id=int(tg_id), text=body)
                        rsvp_upsert(e["event_id"], cid, reminded_60m=1)
                        log_action("remind_60m_sent", client_id=cid, event_id=e["event_id"], details="")
                    except Exception:
                        pass

        # 4) FEEDBACK +3h: тим, хто attended=1
        if -60 <= (now - dt - timedelta(hours=3)).total_seconds() <= 60:
            # збираємо фідбек лише раз
            if has_log("feedback_requested", client_id="", event_id=e["event_id"]):
                continue
            w_att = ws(SHEET_ATTEND)
            rows_att = get_all_records(w_att)
            for r in rows_att:
                if str(r.get("event_id")) == e["event_id"] and a2i(r.get("attended")) == 1:
                    cid = r.get("client_id")
                    tg_id = try_get_tg_from_client_id(cid)
                    if not tg_id:
                        continue
                    text = messages_get("feedback.ask").format(title=e["title"])
                    # кнопки зі зірками 1..5 + комент
                    kb = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("⭐️1", callback_data=f"fb:{e['event_id']}:{cid}:1"),
                            InlineKeyboardButton("⭐️2", callback_data=f"fb:{e['event_id']}:{cid}:2"),
                            InlineKeyboardButton("⭐️3", callback_data=f"fb:{e['event_id']}:{cid}:3"),
                            InlineKeyboardButton("⭐️4", callback_data=f"fb:{e['event_id']}:{cid}:4"),
                            InlineKeyboardButton("⭐️5", callback_data=f"fb:{e['event_id']}:{cid}:5"),
                        ],
                        [InlineKeyboardButton("✍️ Написати відгук", callback_data=f"fb:comment:{e['event_id']}:{cid}")]
                    ])
                    try:
                        await context.bot.send_message(chat_id=int(tg_id), text=text, reply_markup=kb)
                    except Exception:
                        pass
            log_action("feedback_requested", client_id="", event_id=e["event_id"], details="")

async def feedback_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    # fb:<event_id>:<client_id>:<stars>
    if data.startswith("fb:") and data.count(":") == 3:
        _, event_id, client_id, stars = data.split(":")
        stars = int(stars)
        feedback_save(event_id, client_id, stars, "")
        await query.edit_message_text(f"Дякуємо! Оцінка {stars}⭐️ збережена.")
        if stars < 4:
            await route_low_feedback(context, event_id, client_id, stars, "")
        return

    # fb:comment:<event_id>:<client_id>
    if data.startswith("fb:comment:"):
        _, _, event_id, client_id = data.split(":")
        # шукаємо tg користувача
        tg_id = try_get_tg_from_client_id(client_id)
        if not tg_id or not update.effective_user or update.effective_user.id != int(tg_id):
            await query.edit_message_text("Введіть коментар у приватному діалозі з ботом.")
            return
        set_state(tg_id, "feedback_comment", "await", {"event_id": event_id, "client_id": client_id, "stars": 0})
        await query.edit_message_text("Надішліть, будь ласка, текстовий відгук повідомленням.")
        return

# ================================ MAIN ========================================

async def post_init(app):
    # Планувальник: 1 раз на 60 секунд
    app.job_queue.run_repeating(scheduler_tick, interval=60, first=5)

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    # commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))

    # callbacks
    app.add_handler(CallbackQueryHandler(callbacks, pattern="^(admin:|rsvp:|claim:|noop$)"))
    app.add_handler(CallbackQueryHandler(feedback_callbacks, pattern="^(fb:)"))

    # text messages (states)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
