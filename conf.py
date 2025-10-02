# -*- coding: utf-8 -*-
"""
Conference ZV Bot
- UA-мова скрізь
- Адмін-панель через deep-link: https://t.me/<bot_username>?start=admin_<TOKEN>
- Створення/оновлення/скасування/ребродкаст івентів
- Інвайти, RSVP, обов'язкові нагадування -24h і -60m
- Після події: фідбек (+~3h), ескалація при оцінці <4 або не «так»
- Дані в Google Sheets (схема за твоїми листами)
"""

import asyncio
import hmac, hashlib
import json, os, re, uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote

import pytz
from dateutil import parser as dateparser

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from apscheduler.schedulers.asyncio import AsyncIOScheduler

import gspread
from google.oauth2.service_account import Credentials

# ======================= ENV & CONFIG =======================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x]
ADMIN_DEEPLINK_TOKEN = os.getenv("ADMIN_DEEPLINK_TOKEN", "changeme_admin_token")
SECRET_HMAC_KEY = os.getenv("SECRET_HMAC_KEY", "replace_with_strong_random_string")
SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", "0") or "0")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")

# Вікна перевірки (щоб не пропустити момент)
REMINDER24_WINDOW_MIN = (23*60-10, 24*60+10)  # 23:50..24:10 до старту
REMINDER60_WINDOW_MIN = (55, 65)              # 55..65 хв до старту
FEEDBACK_WINDOW_MIN   = (175, 185)            # 2:55..3:05 після закінчення (для 60 хв)

# Quiet hours можна вимкнути, якщо не потрібно притримувати інвайти ночами
QUIET_HOURS = None  # наприклад, (22, 8) або None

# Sheet names
SHEET_CLIENTS    = "Clients"
SHEET_EVENTS     = "Events"
SHEET_EVENTTYPES = "EventTypes"
SHEET_RSVP       = "RSVP"
SHEET_ATTENDANCE = "Attendance"
SHEET_FEEDBACK   = "Feedback"
SHEET_DELIVERY   = "DeliveryLog"
SHEET_MESSAGES   = "Messages"

tz = pytz.timezone(TIMEZONE)

# ======================= Google Sheets =======================
def _get_gspread_client():
    sa_path = os.getenv("GOOGLE_SA_PATH", "/etc/secrets/gsheets.json")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)


_gc = None
def gs():
    global _gc
    if _gc is None:
        _gc = _get_gspread_client()
    return _gc.open_by_key(SPREADSHEET_ID)

def _headers(ws) -> List[str]:
    return [h.strip() for h in ws.row_values(1)]

def _col_index(ws, col_name: str) -> int:
    return _headers(ws).index(col_name) + 1

def _read_all(ws) -> List[Dict[str, str]]:
    return ws.get_all_records(numeric_value=False)

def _append(ws, row_dict: Dict[str, str]):
    hdr = _headers(ws)
    ws.append_row([row_dict.get(h, "") for h in hdr], value_input_option="USER_ENTERED")

def _find_rows(ws, predicate) -> List[Tuple[int, Dict[str, str]]]:
    hdr = _headers(ws)
    values = ws.get_all_values()
    out = []
    for i in range(1, len(values)):
        d = {hdr[j]: values[i][j] if j < len(values[i]) else "" for j in range(len(hdr))}
        if predicate(d):
            out.append((i + 1, d))
    return out

def _update_row_by_key(ws, key_col: str, key_val: str, patch: Dict[str, str]) -> bool:
    vals = ws.get_all_values()
    col_map = {name: i for i, name in enumerate(vals[0])}
    for idx in range(1, len(vals)):
        row = vals[idx]
        if col_map.get(key_col) is None:
            continue
        if row[col_map[key_col]] == key_val:
            for k, v in patch.items():
                if k in col_map:
                    ws.update_cell(idx + 1, col_map[k] + 1, v)
            return True
    return False

def _upsert_rsvp(event_id: str, client_id: str, patch: Dict[str, str]):
    sh = gs().worksheet(SHEET_RSVP)
    rows = _find_rows(sh, lambda r: r.get("event_id")==event_id and r.get("client_id")==client_id)
    if rows:
        rownum, _ = rows[0]
        for k, v in patch.items():
            sh.update_cell(rownum, _col_index(sh, k), v)
    else:
        row = {"event_id": event_id, "client_id": client_id,
               "rsvp":"", "remind_24h":"", "reminded_24h":"", "reminded_60m":"", "rsvp_at":""}
        row.update(patch)
        _append(sh, row)

def _delivery_log(action: str, client_id: str, event_id: str, details: str=""):
    sh = gs().worksheet(SHEET_DELIVERY)
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M")
    _append(sh, {"ts": now, "client_id": client_id, "event_id": event_id, "action": action, "details": details})

# ======================= Bot =======================
bot = Bot(token=BOT_TOKEN, parse_mode="Markdown")
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ======================= Helpers =======================
def now_local() -> datetime:
    return datetime.now(tz)

def parse_dt_local(s: str) -> Optional[datetime]:
    s = s.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$", s):
        return tz.localize(datetime.strptime(s, "%Y-%m-%d %H:%M"))
    try:
        dt = dateparser.parse(s, dayfirst=True)
        if not dt: return None
        dt = dt.astimezone(tz) if dt.tzinfo else tz.localize(dt)
        return dt
    except Exception:
        return None

def fmt_date_time(dt: datetime) -> Tuple[str, str]:
    return dt.strftime("%d.%m.%Y"), dt.strftime("%H:%M")

def within_quiet_hours(dt: datetime) -> bool:
    if not QUIET_HOURS: return False
    start, end = QUIET_HOURS
    h = dt.hour
    if start < end:  # 22..8? ні
        return start <= h < end
    else:           # через північ
        return h >= start or h < end

def sign_payload(payload: str) -> str:
    return hmac.new(SECRET_HMAC_KEY.encode("utf-8"), payload.encode("utf-8"),
                    hashlib.sha256).hexdigest()[:16]

def make_cb(action: str, event_id: str, client_id: str) -> str:
    ts = int(now_local().timestamp())
    raw = f"{action}|{event_id}|{client_id}|{ts}"
    sig = sign_payload(raw)
    return f"{raw}|{sig}"

def verify_cb(data: str) -> Tuple[bool, Dict[str, str]]:
    try:
        action, event_id, client_id, ts, sig = data.split("|")
        raw = f"{action}|{event_id}|{client_id}|{ts}"
        ok = (sign_payload(raw) == sig)
        return ok, {"action": action, "event_id": event_id, "client_id": client_id, "ts": ts}
    except Exception:
        return False, {}

def tmpl(key: str, lang="uk") -> Optional[str]:
    try:
        ws = gs().worksheet(SHEET_MESSAGES)
        for r in _read_all(ws):
            if r.get("key") == key and r.get("lang") == lang:
                return r.get("text") or ""
    except Exception:
        pass
    return None

def default_invite_text(name: str, title: str, date_str: str, time_str: str, description: str) -> str:
    return (f"{name}, запрошуємо на зустріч: *{title}*\n"
            f"🗓 {date_str} о {time_str} (Київ)\n"
            f"ℹ️ {description}\n"
            "Виберіть варіант нижче:")

# ======================= Admin deep-link & panel =======================
ADMIN_PANEL_TEXT = "🔐 Адмін-панель: оберіть дію"

class CreateEventStates(StatesGroup):
    choosing_type = State()
    entering_datetime = State()
    entering_duration = State()
    entering_link = State()
    entering_title = State()
    entering_description = State()
    confirming = State()

class UpdateEventStates(StatesGroup):
    picking_event = State()
    picking_field = State()
    entering_value = State()

@router.message(CommandStart())
async def on_start(message: Message, state: FSMContext):
    payload = message.text.split(" ", 1)
    if len(payload) > 1 and payload[1].startswith("admin_"):
        # deep-link: ?start=admin_<TOKEN>  (TOKEN з # треба URL-кодувати як %23!)
        token = unquote(payload[1][len("admin_"):].strip())
        if (message.from_user.id in ADMIN_IDS) and (token == ADMIN_DEEPLINK_TOKEN):
            await show_admin_panel(message); return
        else:
            await message.answer("Команда недоступна."); return

    # Прив'язка client_id: /start <client_id>
    client_id = payload[1].strip() if len(payload) > 1 and not payload[1].startswith("admin_") else None
    try:
        ws = gs().worksheet(SHEET_CLIENTS)
        if client_id:
            updated = _update_row_by_key(ws, "client_id", client_id, {
                "tg_user_id": str(message.from_user.id),
                "last_seen_at": now_local().strftime("%Y-%m-%d %H:%M")
            })
            if updated:
                await message.answer("Вас підключено до сповіщень. Дякуємо!")
            else:
                await message.answer("Не знайшов клієнта за цим ID. Будь ласка, надішліть ваш телефон та ПІБ.")
        else:
            await message.answer("Вітаємо! Надішліть, будь ласка, ваш телефон і ПІБ одним повідомленням.")
    except Exception:
        await message.answer("Сталася помилка доступу до бази. Спробуйте пізніше.")

@router.message(Command("admin"))
async def on_admin_cmd(message: Message):
    if message.chat.type != "private": return
    if message.from_user.id not in ADMIN_IDS: return
    await show_admin_panel(message)

async def show_admin_panel(message: Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Створити івент", callback_data="admin:create")
    kb.button(text="✏️ Оновити івент", callback_data="admin:update")
    kb.button(text="❌ Скасувати івент", callback_data="admin:cancel")
    kb.button(text="🔁 Ребродкаст", callback_data="admin:rebroadcast")
    kb.button(text="📋 Ближчі івенти", callback_data="admin:list")
    kb.adjust(2,2,1)
    await message.answer(ADMIN_PANEL_TEXT, reply_markup=kb.as_markup())

def admin_only(func):
    async def wrapper(callback: CallbackQuery, state: FSMContext):
        if callback.message.chat.type != "private" or callback.from_user.id not in ADMIN_IDS:
            await callback.answer("Недоступно", show_alert=True); return
        return await func(callback, state)
    return wrapper

# -------- Create Event flow --------
@router.callback_query(F.data == "admin:create")
@admin_only
async def admin_create_start(callback: CallbackQuery, state: FSMContext):
    try:
        ws = gs().worksheet(SHEET_EVENTTYPES)
        types = [r for r in _read_all(ws) if str(r.get("active","")).strip().lower() in ("1","true","yes")]
        if not types:
            await callback.message.answer("Немає активних типів подій."); return
        await state.update_data(types=types, page=0)
        await send_types_page(callback.message, types, 0)
        await state.set_state(CreateEventStates.choosing_type)
    except Exception:
        await callback.message.answer("Помилка читання EventTypes.")

async def send_types_page(message: Message, types: List[Dict], page: int):
    PAGE = 8; start = page*PAGE; part = types[start:start+PAGE]
    kb = InlineKeyboardBuilder()
    for r in part:
        kb.button(text=f"{r.get('title','')} ({r.get('type_code')})",
                  callback_data=f"admin:create:type:{r.get('type_code')}")
    if start>0: kb.button(text="◀️", callback_data="admin:create:page:prev")
    if start+PAGE<len(types): kb.button(text="▶️", callback_data="admin:create:page:next")
    kb.button(text="✖️ Відміна", callback_data="admin:create:cancel")
    kb.adjust(1)
    await message.answer("Оберіть тип івенту:", reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("admin:create:page:"))
@admin_only
async def admin_create_page(callback: CallbackQuery, state: FSMContext):
    d = await state.get_data(); types = d.get("types", []); page = d.get("page", 0)
    if callback.data.endswith("prev") and page>0: page -= 1
    elif callback.data.endswith("next"): page += 1
    await state.update_data(page=page); await send_types_page(callback.message, types, page)

@router.callback_query(F.data.startswith("admin:create:type:"))
@admin_only
async def admin_create_type_selected(callback: CallbackQuery, state: FSMContext):
    type_code = callback.data.split(":")[-1]
    await state.update_data(new_event={"type": type_code})
    await callback.message.answer("Введіть дату і час у форматі *YYYY-MM-DD HH:MM* (Київ). Напр.: 2025-10-05 18:00")
    await state.set_state(CreateEventStates.entering_datetime)

@router.message(CreateEventStates.entering_datetime)
async def admin_create_enter_dt(message: Message, state: FSMContext):
    dt = parse_dt_local(message.text)
    if not dt:
        await message.answer("Не вдалося розпізнати дату/час. Приклад: 2025-10-05 18:00"); return
    d = await state.get_data(); ev = d.get("new_event", {}); ev["start_at"] = dt.strftime("%Y-%m-%d %H:%M")
    await state.update_data(new_event=ev)
    await message.answer("Тривалість у хвилинах? (30/45/60/90). За замовчуванням 60.")
    await state.set_state(CreateEventStates.entering_duration)

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# Додаємо клавіатуру в on_start (або після реєстрації клієнта)
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📋 Мої конференції")],
    ],
    resize_keyboard=True
)

@router.message(CommandStart())
async def on_start(message: Message, state: FSMContext):
    # ... твоя логіка прив’язки клієнта ...
    await message.answer("Вітаємо у боті! Використовуйте меню нижче 👇", reply_markup=main_kb)

# Обробник натискання кнопки
@router.message(F.text == "📋 Мої конференції")
async def on_my_events_button(message: Message):
    await send_my_events(message)


async def send_my_events(message: Message):
    try:
        # ==== вся логіка з попереднього прикладу ====
        ws_clients = gs().worksheet(SHEET_CLIENTS)
        clients = _read_all(ws_clients)
        me = next((c for c in clients if c.get("tg_user_id") == str(message.from_user.id)), None)
        if not me:
            await message.answer("Вас не знайдено в базі. Зареєструйтесь через /start <client_id>.")
            return
        client_id = me.get("client_id")

        ws_types = gs().worksheet(SHEET_EVENTTYPES)
        types = [r for r in _read_all(ws_types) if str(r.get("active","")).lower() in ("1","true","yes")]

        ws_att = gs().worksheet(SHEET_ATTENDANCE)
        attendance = [r for r in _read_all(ws_att) if r.get("client_id") == client_id and str(r.get("attended","")).lower() in ("true","1","yes")]

        ws_rsvp = gs().worksheet(SHEET_RSVP)
        rsvps = [r for r in _read_all(ws_rsvp) if r.get("client_id") == client_id and r.get("rsvp") == "going"]

        ws_events = gs().worksheet(SHEET_EVENTS)
        events = _read_all(ws_events)
        event_by_id = {e.get("event_id"): e for e in events}

        visited_types = set()
        for a in attendance:
            ev = event_by_id.get(a.get("event_id"))
            if ev: visited_types.add(ev.get("type"))
        for r in rsvps:
            ev = event_by_id.get(r.get("event_id"))
            if ev:
                start = parse_dt_local(ev.get("start_at",""))
                if start and start < now_local():
                    visited_types.add(ev.get("type"))

        text = "📋 *Ваш прогрес по конференціях:*\n\n"
        for t in types:
            mark = "✅" if t.get("type_code") in visited_types else "❌"
            text += f"{mark} {t.get('title') or t.get('type_code')}\n"

        await message.answer(text, parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"Помилка отримання списку конференцій: {e}")

    
@router.message(CreateEventStates.entering_duration)
async def admin_create_enter_duration(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text: dur = 60
    else:
        try:
            dur = int(text); assert 1 <= dur <= 360
        except Exception:
            await message.answer("Вкажіть число хвилин (1..360), напр. 60"); return
    d = await state.get_data(); ev = d.get("new_event", {}); ev["duration_min"] = str(dur)
    await state.update_data(new_event=ev)
    await message.answer("Вставте посилання на конференцію (https://...)")
    await state.set_state(CreateEventStates.entering_link)

@router.message(CreateEventStates.entering_link)
async def admin_create_enter_link(message: Message, state: FSMContext):
    link = (message.text or "").strip()
    if not (link.startswith("http://") or link.startswith("https://")):
        await message.answer("Будь ласка, повний URL (https://...)"); return
    d = await state.get_data(); ev = d.get("new_event", {}); ev["link"] = link
    await state.update_data(new_event=ev)
    await message.answer("Заголовок (можна пропустити, надішліть «-»):")
    await state.set_state(CreateEventStates.entering_title)

@router.message(CreateEventStates.entering_title)
async def admin_create_enter_title(message: Message, state: FSMContext):
    title = (message.text or "").strip()
    d = await state.get_data(); ev = d.get("new_event", {}); ev["title"] = "" if title == "-" else title
    await state.update_data(new_event=ev)
    await message.answer("Опис (можна пропустити, надішліть «-»):")
    await state.set_state(CreateEventStates.entering_description)

@router.message(CreateEventStates.entering_description)
async def admin_create_enter_desc(message: Message, state: FSMContext):
    desc = (message.text or "").strip()
    d = await state.get_data(); ev = d.get("new_event", {}); ev["description"] = "" if desc == "-" else desc
    await state.update_data(new_event=ev)
    ev = (await state.get_data()).get("new_event", {})
    preview = (f"*Попередній перегляд івенту:*\nТип: `{ev.get('type')}`\nЧас: `{ev.get('start_at')}` (Київ)\n"
               f"Тривалість: `{ev.get('duration_min')} хв`\nЛінк: {ev.get('link')}\n"
               f"Заголовок: {ev.get('title') or '(із EventTypes)'}\n"
               f"Опис: {ev.get('description') or '(із EventTypes)'}\n")
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Зберегти", callback_data="admin:create:confirm")
    kb.button(text="↩️ Відміна", callback_data="admin:create:cancel")
    kb.adjust(1,1)
    await message.answer(preview, reply_markup=kb.as_markup())
    await state.set_state(CreateEventStates.confirming)

@router.callback_query(F.data == "admin:create:cancel")
async def admin_create_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear(); await callback.message.answer("Створення скасовано.")

@router.callback_query(F.data == "admin:create:confirm")
async def admin_create_confirm(callback: CallbackQuery, state: FSMContext):
    ev = (await state.get_data()).get("new_event", {})
    if not ev: await callback.message.answer("Немає даних івенту."); return
    # Підставити title/description із EventTypes, якщо порожні
    try:
        wst = gs().worksheet(SHEET_EVENTTYPES); types = _read_all(wst)
        trow = next((r for r in types if r.get("type_code") == ev["type"]), None)
        if trow:
            ev.setdefault("title", ""); ev.setdefault("description", "")
            if not ev["title"]: ev["title"] = trow.get("title","")
            if not ev["description"]: ev["description"] = trow.get("description","")
    except Exception:
        pass
    # Зберегти в Events
    try:
        wse = gs().worksheet(SHEET_EVENTS)
        event_id = f"evt_{uuid.uuid4().hex[:8]}"
        _append(wse, {
            "event_id": event_id, "type": ev["type"],
            "title": ev.get("title",""), "description": ev.get("description",""),
            "start_at": ev["start_at"], "duration_min": ev["duration_min"], "link": ev["link"],
            "created_by": callback.from_user.username or str(callback.from_user.id),
            "created_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M"),
            "broadcasted_at": ""
        })
        await callback.message.answer(
            f"✅ Івент збережено: `{event_id}`\nРозіслати зараз?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Так, розіслати", callback_data=f"admin:broadcast:{event_id}")],
                [InlineKeyboardButton(text="Пізніше", callback_data="admin:noop")]
            ])
        )
        await state.clear()
    except Exception:
        await callback.message.answer("Помилка збереження івенту.")

@router.callback_query(F.data == "admin:noop")
async def admin_noop(callback: CallbackQuery):
    await callback.answer("Ок")

def _event_by_id(event_id: str) -> Optional[Dict[str,str]]:
    try:
        wse = gs().worksheet(SHEET_EVENTS)
        rows = _read_all(wse)
        return next((r for r in rows if r.get("event_id") == event_id), None)
    except Exception:
        return None

@router.callback_query(F.data.startswith("admin:broadcast:"))
async def admin_broadcast_now(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    event_id = callback.data.split(":")[-1]
    await do_broadcast(event_id, manual=True)
    await callback.message.answer("🚀 Розсилка запущена.")

@router.callback_query(F.data == "admin:list")
async def admin_list_upcoming(callback: CallbackQuery, state: FSMContext):
    try:
        wse = gs().worksheet(SHEET_EVENTS); rows = _read_all(wse); now = now_local()
        up = []
        for r in rows:
            dt = parse_dt_local(r.get("start_at",""))
            if not dt: continue
            if now <= dt <= now + timedelta(days=14): up.append(r)
        if not up:
            await callback.message.answer("Найближчих подій не знайдено."); return
        text = "*Ближчі події (14 днів):*\n"
        for r in sorted(up, key=lambda x: x.get("start_at")):
            text += f"- `{r.get('event_id')}` {r.get('title') or r.get('type')} — {r.get('start_at')}\n"
        await callback.message.answer(text)
    except Exception:
        await callback.message.answer("Помилка читання Events.")

# -------- Update / Cancel / Rebroadcast --------
class AdminAct(StatesGroup):
    picking_event = State()
    picking_field = State()
    entering_value = State()

@router.callback_query(F.data == "admin:rebroadcast")
async def admin_rebroadcast_pick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer("Введіть `event_id` для ребродкасту:")
    await state.set_state(AdminAct.picking_event); await state.update_data(action="rebroadcast")

@router.callback_query(F.data == "admin:update")
async def admin_update_pick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer("Введіть `event_id` для оновлення:")
    await state.set_state(AdminAct.picking_event); await state.update_data(action="update")

@router.callback_query(F.data == "admin:cancel")
async def admin_cancel_pick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    await callback.message.answer("Введіть `event_id` для скасування:")
    await state.set_state(AdminAct.picking_event); await state.update_data(action="cancel")

@router.message(AdminAct.picking_event)
async def admin_event_action_choose(message: Message, state: FSMContext):
    d = await state.get_data(); action = d.get("action"); event_id = message.text.strip()
    await state.update_data(event_id=event_id)
    if action == "rebroadcast":
        await do_broadcast(event_id, manual=True)
        await message.answer("🔁 Ребродкаст виконано (за наявності цільової аудиторії).")
        await state.clear(); return
    if action == "cancel":
        ev = _event_by_id(event_id)
        if not ev: await message.answer("Не знайшов подію."); await state.clear(); return
        try:
            wse = gs().worksheet(SHEET_EVENTS)
            rows = _find_rows(wse, lambda r: r.get("event_id")==event_id); rownum, r = rows[0]
            new_title = f"[СКАСОВАНО] {r.get('title') or r.get('type')}"
            wse.update_cell(rownum, _col_index(wse, "title"), new_title)
            await notify_going_update(event_id, "Зустріч скасовано. Надішлемо нову дату.")
            await message.answer("❌ Івент позначено як скасований. Учасників повідомлено.")
        except Exception:
            await message.answer("Помилка скасування.")
        await state.clear(); return
    if action == "update":
        kb = InlineKeyboardBuilder()
        for fld in ("start_at","link","duration_min","title","description"):
            kb.button(text=fld, callback_data=f"admin:update:field:{fld}")
        kb.adjust(3,2)
        await message.answer("Оберіть поле для оновлення:", reply_markup=kb.as_markup())
        await state.set_state(AdminAct.picking_field)

@router.callback_query(F.data.startswith("admin:update:field:"))
async def admin_update_field_selected(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS: return
    fld = callback.data.split(":")[-1]
    await state.update_data(field=fld)
    await callback.message.answer(f"Введіть нове значення для `{fld}`:")
    await state.set_state(AdminAct.entering_value)

@router.message(AdminAct.entering_value)
async def admin_update_enter_value(message: Message, state: FSMContext):
    d = await state.get_data(); event_id = d.get("event_id"); field = d.get("field"); val = message.text.strip()
    # Валідація
    if field == "start_at" and not parse_dt_local(val):
        await message.answer("Невалідна дата. Приклад: 2025-10-05 18:00"); return
    if field == "duration_min":
        try: assert int(val) > 0
        except Exception: await message.answer("Вкажіть число хвилин, напр. 60"); return
    if field == "link" and not (val.startswith("http://") or val.startswith("https://")):
        await message.answer("Повний URL (https://...)"); return
    try:
        wse = gs().worksheet(SHEET_EVENTS)
        ok = _update_row_by_key(wse, "event_id", event_id, {field: val})
        if not ok: await message.answer("Не знайшов подію для оновлення.")
        else:
            await notify_going_update(event_id, f"Оновлення події: змінено `{field}`.")
            await message.answer("✅ Оновлено і повідомлено підтверджених.")
    except Exception:
        await message.answer("Помилка оновлення.")
    await state.clear()

# ======================= Broadcast & targeting =======================
async def do_broadcast(event_id: str, manual: bool=False):
    try:
        wse = gs().worksheet(SHEET_EVENTS); events = _read_all(wse)
        ev = next((r for r in events if r.get("event_id") == event_id), None)
        if not ev: return
        if ev.get("broadcasted_at") and not manual: return

        # Defaults from EventTypes
        title = ev.get("title") or ""
        description = ev.get("description") or ""
        try:
            wst = gs().worksheet(SHEET_EVENTTYPES); types = _read_all(wst)
            trow = next((r for r in types if r.get("type_code") == ev.get("type")), None)
            if trow:
                if not title: title = trow.get("title","")
                if not description: description = trow.get("description","")
        except Exception:
            pass

        start = parse_dt_local(ev.get("start_at",""))
        if not start: return
        date_str, time_str = fmt_date_time(start)

        # Аудиторія: усі активні клієнти з tg_user_id, що ще не пройшли цей type
        wsc = gs().worksheet(SHEET_CLIENTS); clients = _read_all(wsc)
        wsr = gs().worksheet(SHEET_RSVP); rsvps = _read_all(wsr)
        wsa = gs().worksheet(SHEET_ATTENDANCE); attendance = _read_all(wsa)

        # event_id->type
        event_type_map = {x.get("event_id"): x.get("type") for x in events}
        passed = set()
        for r in rsvps:
            if r.get("rsvp") == "going" and event_type_map.get(r.get("event_id")) == ev.get("type"):
                passed.add(r.get("client_id"))
        for a in attendance:
            if str(a.get("attended","")).strip().lower() in ("true","1","yes") and event_type_map.get(a.get("event_id")) == ev.get("type"):
                passed.add(a.get("client_id"))

        def invite_kb(cid: str) -> InlineKeyboardMarkup:
            return InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Так, буду", callback_data=make_cb("going", event_id, cid)),
                InlineKeyboardButton(text="🚫 Не зможу", callback_data=make_cb("not", event_id, cid)),
            ], [
                InlineKeyboardButton(text="🔔 Нагадати за 24 год", callback_data=make_cb("rem24", event_id, cid))
            ]])

        text_template = tmpl("invite.body","uk") or default_invite_text("{name}", "{title}", "{date}", "{time}", "{description}")
        sent = 0
        for c in clients:
            if c.get("status") != "active" or not c.get("tg_user_id"): continue
            cid = c.get("client_id")
            if cid in passed: continue
            if QUIET_HOURS and within_quiet_hours(now_local()):
                # приглушити інвайти в тихі години (за потреби)
                continue
            name = c.get("full_name") or "Клієнт"
            text = (text_template
                    .replace("{name}", name)
                    .replace("{title}", title or ev.get("type"))
                    .replace("{date}", date_str)
                    .replace("{time}", time_str)
                    .replace("{description}", description or ""))
            try:
                await bot.send_message(chat_id=int(c.get("tg_user_id")), text=text, reply_markup=invite_kb(cid))
                _delivery_log("invite_sent", cid, event_id, ""); sent += 1
                await asyncio.sleep(0.04)
            except Exception as e:
                _delivery_log("fail", cid, event_id, f"invite: {e}")
        if sent>0 or manual:
            _update_row_by_key(wse, "event_id", event_id, {"broadcasted_at": now_local().strftime("%Y-%m-%d %H:%M")})
    except Exception:
        pass

async def notify_going_update(event_id: str, text: str):
    try:
        wsr = gs().worksheet(SHEET_RSVP); rows = _read_all(wsr)
        users = [r.get("client_id") for r in rows if r.get("event_id")==event_id and r.get("rsvp")=="going"]
        wsc = gs().worksheet(SHEET_CLIENTS); clients = _read_all(wsc)
        tg = {c.get("client_id"): c.get("tg_user_id") for c in clients}
        for cid in users:
            chat = tg.get(cid)
            if not chat: continue
            try:
                await bot.send_message(chat_id=int(chat), text=text)
                _delivery_log("update_sent", cid, event_id, text); await asyncio.sleep(0.04)
            except Exception as e:
                _delivery_log("fail", cid, event_id, f"update: {e}")
    except Exception:
        pass

# ======================= RSVP =======================
@router.callback_query(F.data.regexp(r"^(going|not|rem24)\|"))
async def on_rsvp_click(callback: CallbackQuery):
    ok, p = verify_cb(callback.data)
    if not ok:
        await callback.answer("Неправильний підпис", show_alert=True); return
    event_id = p["event_id"]
    try:
        wsc = gs().worksheet(SHEET_CLIENTS); me = next((r for r in _read_all(wsc) if r.get("tg_user_id")==str(callback.from_user.id)), None)
        if not me: await callback.answer("Спочатку підключіться в боті (/start)", show_alert=True); return
        client_id = me.get("client_id")
        if p["action"] == "going":
            # обов'язково увімкнемо нагадування -24h
            _upsert_rsvp(event_id, client_id, {"rsvp":"going", "remind_24h":"TRUE",
                                               "rsvp_at": now_local().strftime("%Y-%m-%d %H:%M"),
                                               "reminded_60m":"", "reminded_24h":""})
            await callback.message.answer("Записав. Нагадаю за 24 год і за 1 год до початку.")
        elif p["action"] == "not":
            _upsert_rsvp(event_id, client_id, {"rsvp":"not_going", "rsvp_at": now_local().strftime("%Y-%m-%d %H:%M")})
            await callback.message.answer("Дякуємо! Наступні дати пришлю — оберете зручну.")
        elif p["action"] == "rem24":
            _upsert_rsvp(event_id, client_id, {"rsvp":"remind_me", "remind_24h":"TRUE",
                                               "rsvp_at": now_local().strftime("%Y-%m-%d %H:%M")})
            await callback.message.answer("Гаразд, нагадаю за 24 год до старту.")
        await callback.answer()
    except Exception:
        await callback.answer("Помилка обробки RSVP", show_alert=True)

# ======================= SCHEDULED JOBS =======================
async def job_broadcast_new():
    try:
        wse = gs().worksheet(SHEET_EVENTS)
        for r in _read_all(wse):
            if not r.get("broadcasted_at"):
                await do_broadcast(r.get("event_id"), manual=False)
    except Exception:
        pass

async def job_reminder_24h():
    try:
        wse = gs().worksheet(SHEET_EVENTS); events = _read_all(wse); now = now_local()
        for ev in events:
            start = parse_dt_local(ev.get("start_at","")); 
            if not start: continue
            mins = int((start - now).total_seconds() // 60)
            if REMINDER24_WINDOW_MIN[0] <= mins <= REMINDER24_WINDOW_MIN[1]:
                wsr = gs().worksheet(SHEET_RSVP); rsvps = _read_all(wsr)
                targets = [r for r in rsvps if r.get("event_id")==ev.get("event_id")
                           and r.get("rsvp") in ("going","remind_me")
                           and str(r.get("reminded_24h","")).lower() not in ("true","1","yes")]
                if not targets: continue
                wsc = gs().worksheet(SHEET_CLIENTS); clients = _read_all(wsc)
                tg = {c.get("client_id"): c.get("tg_user_id") for c in clients}
                title = ev.get("title") or ev.get("type")
                time_str = parse_dt_local(ev.get('start_at')).strftime('%H:%M')
                for r in targets:
                    chat = tg.get(r.get("client_id")); 
                    if not chat: continue
                    try:
                        await bot.send_message(chat_id=int(chat),
                            text=f"Нагадуємо: *завтра* відбудеться *{title}*. Початок о {time_str}.\nПосилання: {ev.get('link')}")
                        _delivery_log("reminder_24h_sent", r.get("client_id"), ev.get("event_id"), "")
                        _upsert_rsvp(ev.get("event_id"), r.get("client_id"), {"reminded_24h":"TRUE"})
                        await asyncio.sleep(0.04)
                    except Exception as e:
                        _delivery_log("fail", r.get("client_id"), ev.get("event_id"), f"rem24: {e}")
    except Exception:
        pass

async def job_reminder_60m():
    try:
        wse = gs().worksheet(SHEET_EVENTS); events = _read_all(wse); now = now_local()
        for ev in events:
            start = parse_dt_local(ev.get("start_at","")); 
            if not start: continue
            mins = int((start - now).total_seconds() // 60)
            if REMINDER60_WINDOW_MIN[0] <= mins <= REMINDER60_WINDOW_MIN[1]:
                wsr = gs().worksheet(SHEET_RSVP); rsvps = _read_all(wsr)
                targets = [r for r in rsvps if r.get("event_id")==ev.get("event_id")
                           and r.get("rsvp")=="going"
                           and str(r.get("reminded_60m","")).lower() not in ("true","1","yes")]
                if not targets: continue
                wsc = gs().worksheet(SHEET_CLIENTS); clients = _read_all(wsc)
                tg = {c.get("client_id"): c.get("tg_user_id") for c in clients}
                for r in targets:
                    chat = tg.get(r.get("client_id")); 
                    if not chat: continue
                    try:
                        await bot.send_message(chat_id=int(chat),
                            text=f"Нагадуємо: через 1 год почнеться *{ev.get('title') or ev.get('type')}*. Посилання: {ev.get('link')}")
                        _delivery_log("reminder_60m_sent", r.get("client_id"), ev.get("event_id"), "")
                        _upsert_rsvp(ev.get("event_id"), r.get("client_id"), {"reminded_60m":"TRUE"})
                        await asyncio.sleep(0.04)
                    except Exception as e:
                        _delivery_log("fail", r.get("client_id"), ev.get("event_id"), f"rem60: {e}")
    except Exception:
        pass

async def job_feedback_3h():
    try:
        wse = gs().worksheet(SHEET_EVENTS); events = _read_all(wse); now = now_local()
        for ev in events:
            start = parse_dt_local(ev.get("start_at","")); 
            if not start: continue
            try: dur = int(ev.get("duration_min") or "60")
            except Exception: dur = 60
            end = start + timedelta(minutes=dur)
            mins_after_end = int((now - end).total_seconds() // 60)
            if FEEDBACK_WINDOW_MIN[0] <= mins_after_end <= FEEDBACK_WINDOW_MIN[1]:
                wsr = gs().worksheet(SHEET_RSVP); rsvps = _read_all(wsr)
                going = [r for r in rsvps if r.get("event_id")==ev.get("event_id") and r.get("rsvp")=="going"]
                if not going: continue
                wsf = gs().worksheet(SHEET_FEEDBACK); fb = _read_all(wsf)
                already = {(r.get("event_id"), r.get("client_id")) for r in fb if r.get("submitted_at")}
                wsc = gs().worksheet(SHEET_CLIENTS); clients = _read_all(wsc)
                tg = {c.get("client_id"): c.get("tg_user_id") for c in clients}
                text = tmpl("feedback.ask","uk") or \
                    "Дякуємо за участь. Оцініть зустріч (1–5) і напишіть: так/частково/ні. Напр.: '5 так'."
                for r in going:
                    key = (ev.get("event_id"), r.get("client_id"))
                    if key in already: continue
                    chat = tg.get(r.get("client_id"))
                    if not chat: continue
                    try:
                        await bot.send_message(chat_id=int(chat), text=text)
                        _delivery_log("feedback_sent", r.get("client_id"), ev.get("event_id"), "")
                        await asyncio.sleep(0.04)
                    except Exception as e:
                        _delivery_log("fail", r.get("client_id"), ev.get("event_id"), f"fb: {e}")
    except Exception:
        pass

# ======================= Capture feedback (вільний текст) =======================
@router.message()
async def on_free_text(message: Message):
    if message.chat.type != "private": return
    text = (message.text or "").strip().lower()
    m = re.search(r"\b([1-5])\b", text)
    m2 = re.search(r"\b(так|частково|ні)\b", text)
    if not (m and m2): return
    try:
        wsc = gs().worksheet(SHEET_CLIENTS); me = next((r for r in _read_all(wsc) if r.get("tg_user_id")==str(message.from_user.id)), None)
        if not me: return
        client_id = me.get("client_id"); full_name = me.get("full_name"); phone = me.get("phone")
        wsr = gs().worksheet(SHEET_RSVP)
        rsvps = [r for r in _read_all(wsr) if r.get("client_id")==client_id and r.get("rsvp")=="going"]
        if not rsvps: return
        rsvps.sort(key=lambda r: r.get("rsvp_at") or "", reverse=True)
        event_id = rsvps[0].get("event_id")

        stars = int(m.group(1))
        clarity = {"так":"yes","частково":"partial","ні":"no"}[m2.group(1)]
        comment = message.text  # зберігаємо повне повідомлення
        follow = "TRUE" if (stars < 4 or clarity != "yes") else ""

        wsf = gs().worksheet(SHEET_FEEDBACK)
        _append(wsf, {
            "event_id": event_id, "client_id": client_id,
            "stars": str(stars), "clarity": clarity, "comment": comment,
            "submitted_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M"),
            "followup_needed": follow, "handled_at": "", "owner": ""
        })
        await message.reply("Дякуємо за відгук!")

        if follow and SUPPORT_CHAT_ID:
            ev = _event_by_id(event_id) or {}
            title = ev.get("title") or ev.get("type") or "(подія)"
            alert = (f"⚠️ Негативний фідбек: *{title}*\n"
                     f"Клієнт: {full_name} (ID: {client_id}, тел: {phone})\n"
                     f"Оцінка: {stars}/5; Ясність: {clarity}\n"
                     f"Коментар: “{comment}”")
            try: await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=alert)
            except Exception: pass
    except Exception:
        pass

# ======================= Main =======================
async def main():
    if not BOT_TOKEN or not SPREADSHEET_ID:
        raise RuntimeError("BOT_TOKEN або SPREADSHEET_ID відсутні в .env")
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(job_broadcast_new, "interval", minutes=1, id="broadcast_new")
    scheduler.add_job(job_reminder_24h, "interval", minutes=5, id="rem24")
    scheduler.add_job(job_reminder_60m, "interval", minutes=1, id="rem60")
    scheduler.add_job(job_feedback_3h, "interval", minutes=10, id="fb3h")
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
