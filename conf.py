# -*- coding: utf-8 -*-
# ========================= APP: Bot for conference invites ====================
# Фази 0..5 об'єднані в один файл для простоти деплою на Render.
# Потрібні env:
# BOT_TOKEN, ADMIN_IDS, ADMIN_DEEPLINK_TOKEN, SPREADSHEET_ID,
# GOOGLE_SA_PATH (/etc/secrets/gsheets.json) або GOOGLE_SA_JSON,
# SUPPORT_CHAT_ID, TIMEZONE=Europe/Kyiv (за замовчуванням),
# SECRET_HMAC_KEY (будь-який довгий випадковий рядок),
# INVITE_QUIET_HOURS (опц., напр. "22-08")
# ==============================================================================

import os
import json
import re
import uuid
import asyncio
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from urllib.parse import unquote

import gspread
import pytz
from google.oauth2.service_account import Credentials
from dateutil import parser as dateparser

from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import (Message, CallbackQuery,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ============================== ENV & Global =================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x]
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
ADMIN_DEEPLINK_TOKEN = os.getenv("ADMIN_DEEPLINK_TOKEN", "changeme_admin_token")
SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", "0") or "0")
SECRET_HMAC_KEY = os.getenv("SECRET_HMAC_KEY", "replace_with_strong_random_string")

tz = pytz.timezone(TIMEZONE)

# Бот для всіх роутерів
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()

# Назви листів
SHEET_CLIENTS    = "Clients"
SHEET_EVENTS     = "Events"
SHEET_EVENTTYPES = "EventTypes"
SHEET_RSVP       = "RSVP"
SHEET_ATTENDANCE = "Attendance"
SHEET_FEEDBACK   = "Feedback"
SHEET_DELIVERY   = "DeliveryLog"
SHEET_MESSAGES   = "Messages"

# ============================== Phase 0: Sheets ===============================

async def tell_admins(text: str) -> None:
    if not ADMIN_IDS:
        return
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"🛠 {text}")
        except Exception:
            pass

def _get_gspread_client():
    sa_json = (os.getenv("GOOGLE_SA_JSON") or "").strip()
    sa_path = (os.getenv("GOOGLE_SA_PATH") or "/etc/secrets/gsheets.json").strip()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if sa_json:
        data = json.loads(sa_json)
        creds = Credentials.from_service_account_info(data, scopes=scopes)
    else:
        if not os.path.exists(sa_path):
            raise RuntimeError(f"Service account file not found at {sa_path}")
        creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)

_gc = None
def gs():
    global _gc
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set")
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
    out: List[Tuple[int, Dict[str, str]]] = []
    for i in range(1, len(values)):
        d = {hdr[j]: values[i][j] if j < len(values[i]) else "" for j in range(len(hdr))}
        if predicate(d):
            out.append((i + 1, d))
    return out

def _update_row_by_key(ws, key_col: str, key_val: str, patch: Dict[str, str]) -> bool:
    vals = ws.get_all_values()
    if not vals:
        return False
    col_map = {name: i for i, name in enumerate(vals[0])}
    if key_col not in col_map:
        return False
    for idx in range(1, len(vals)):
        row = vals[idx]
        if row[col_map[key_col]] == key_val:
            for k, v in patch.items():
                if k in col_map:
                    ws.update_cell(idx + 1, col_map[k] + 1, v)
            return True
    return False

def _delivery_log(action: str, client_id: str, event_id: str, details: str = ""):
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M")
    sh = gs().worksheet(SHEET_DELIVERY)
    _append(sh, {"ts": now, "client_id": client_id, "event_id": event_id, "action": action, "details": details})

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
# >>> додай десь поруч із іншими утилітами для Sheets

def _active_eventtypes_titles() -> List[str]:
    try:
        ws = gs().worksheet(SHEET_EVENTTYPES)
        rows = _read_all(ws)
        titles = []
        for r in rows:
            active = str(r.get("active", "")).strip().lower() in ("1", "true", "yes")
            if not active:
                continue
            title = (r.get("title") or r.get("type") or r.get("type_code") or "").strip()
            if title:
                titles.append(title)
        return titles
    except Exception:
        return []
      
# ============================== Phase 1: Onboarding ===========================

onboarding_router = Router(name="onboarding")

class Onboard(StatesGroup):
    ask_name = State()
    ask_phone = State()

def _valid_name(full_name: str) -> bool:
    words = [w for w in re.split(r"\s+", (full_name or "").strip()) if w]
    return len(words) >= 2 and all(re.match(r"^[\w\-’'А-Яа-яЇїІіЄєҐґA-Za-z]+$", w) for w in words)

def _valid_phone(phone: str) -> bool:
    p = (phone or "").strip().replace(" ", "")
    return re.fullmatch(r"^\+?\d{10,15}$", p) is not None

def _norm_phone(phone: str) -> str:
    p = (phone or "").strip().replace(" ", "")
    return p if p.startswith("+") else f"+{p}"

def _norm_phone_val(val: str) -> str:
    if not val:
        return ""
    v = val.replace(" ", "")
    return v if v.startswith("+") else f"+{v}"

def now_local_str() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M")

@onboarding_router.message(CommandStart())
async def start_auto(message: Message, state: FSMContext):
    payload = ""
    if message.text:
        parts = message.text.split(" ", 1)
        payload = parts[1].strip() if len(parts) > 1 else ""
    if payload.startswith("admin_"):
        return  # адмін-роутер обробить

    me_tg = str(message.from_user.id)
    try:
        ws = gs().worksheet(SHEET_CLIENTS)
        existing_by_tg = _find_rows(ws, lambda r: (r.get("tg_user_id") or "") == me_tg)
        if existing_by_tg:
            rownum, _ = existing_by_tg[0]
            ws.update_cell(rownum, _col_index(ws, "status"), "active")
            ws.update_cell(rownum, _col_index(ws, "last_seen_at"), now_local_str())
            await message.answer("✅ Ви вже підключені. Сповіщення активовано.")
            await state.clear()
            return
    except Exception:
        pass

    await message.answer(
        "👋 Вітаємо! Напишіть, будь ласка, ваше *ПІБ* (Прізвище Ім’я).\n"
        "_Напр.: Петренко Іван_"
    )
    await state.set_state(Onboard.ask_name)

@onboarding_router.message(Onboard.ask_name)
async def ob_name(message: Message, state: FSMContext):
    full_name = (message.text or "").strip()
    if not _valid_name(full_name):
        await message.reply(
            "Здається, це не схоже на ПІБ. Вкажіть *прізвище та ім’я* (мінімум два слова).\n"
            "_Напр.: Петренко Іван_"
        )
        return
    await state.update_data(full_name=full_name)
    await message.answer(
        "Дякую! ✍️ Тепер надішліть *номер телефону* у міжнародному форматі.\n"
        "_Напр.: +380671234567_"
    )
    await state.set_state(Onboard.ask_phone)
async def _send_welcome_with_eventtypes(chat_id: int):
    titles = _active_eventtypes_titles()
    if titles:
        lst = "\n".join([f"• {t}" for t in titles])
        txt_list = f"📅 *Наші регулярні конференції:*\n{lst}\n\n"
    else:
        txt_list = ""
    friendly = ("🎉 Готово! Ви додані до системи.\n"
                "Очікуйте запрошення на конференції — вони прийдуть у цей бот найближчим часом.")
    try:
        if txt_list:
            await bot.send_message(chat_id, txt_list)
        await bot.send_message(chat_id, friendly)
    except Exception:
        # м’яко ігноруємо — реєстрація вже пройшла
        pass

@onboarding_router.message(Onboard.ask_phone)
async def ob_phone(message: Message, state: FSMContext):
    raw_phone = (message.text or "").strip()
    if not _valid_phone(raw_phone):
        await message.reply("Будь ласка, введіть коректний телефон. _Напр.: +380671234567_")
        return

    data = await state.get_data()
    full_name = data.get("full_name", "").strip()
    me_tg = str(message.from_user.id)
    phone = _norm_phone(raw_phone)

    try:
        ws = gs().worksheet(SHEET_CLIENTS)

        existing_by_tg = _find_rows(ws, lambda r: (r.get("tg_user_id") or "") == me_tg)
        if existing_by_tg:
            rownum, _ = existing_by_tg[0]
            ws.update_cell(rownum, _col_index(ws, "full_name"), full_name)
            ws.update_cell(rownum, _col_index(ws, "phone"), phone)
            ws.update_cell(rownum, _col_index(ws, "status"), "active")
            ws.update_cell(rownum, _col_index(ws, "last_seen_at"), now_local_str())
            await _send_welcome_with_eventtypes(int(message.from_user.id))
            await state.clear()
            return

        norm = _norm_phone_val
        by_phone = _find_rows(ws, lambda r: norm(r.get("phone") or "") == norm(phone))
        if by_phone:
            rownum, row = by_phone[0]
            row_tg = (row.get("tg_user_id") or "").strip()
            if not row_tg or row_tg == me_tg:
                ws.update_cell(rownum, _col_index(ws, "tg_user_id"), me_tg)
                ws.update_cell(rownum, _col_index(ws, "full_name"), full_name)
                ws.update_cell(rownum, _col_index(ws, "status"), "active")
                ws.update_cell(rownum, _col_index(ws, "last_seen_at"), now_local_str())
                await _send_welcome_with_eventtypes(int(message.from_user.id))
            else:
                client_id = f"cl_{message.from_user.id}"
                _append(ws, {
                    "client_id": client_id,
                    "tg_user_id": me_tg,
                    "phone": phone,
                    "full_name": full_name,
                    "status": "active",
                    "required_event_types": "",
                    "created_at": now_local_str(),
                    "last_seen_at": now_local_str(),
                    "program": "",
                })
                await message.answer(
                    "✅ Вас додано як нового клієнта. Якщо це ваш перший старт — усе гаразд.\n"
                    "Якщо ви вже працювали з нами раніше — ми перевіримо дані."
                )
                await _send_welcome_with_eventtypes(int(message.from_user.id))

                try:
                    if SUPPORT_CHAT_ID:
                        await message.bot.send_message(
                            SUPPORT_CHAT_ID,
                            (
                                "⚠️ Можливий дубль за телефоном.\n"
                                f"Новий TG: `{me_tg}`; ПІБ: {full_name}; Тел: {phone}\n"
                                f"Існуючий рядок прив'язано до іншого TG."
                            )
                        )
                except Exception:
                    pass
        else:
            client_id = f"cl_{message.from_user.id}"
            _append(ws, {
                "client_id": client_id,
                "tg_user_id": me_tg,
                "phone": phone,
                "full_name": full_name,
                "status": "active",
                "required_event_types": "",
                "created_at": now_local_str(),
                "last_seen_at": now_local_str(),
                "program": "",
            })
            await _send_welcome_with_eventtypes(int(message.from_user.id))
    except Exception:
        await message.answer("На жаль, не вдалося записати дані. Спробуйте пізніше.")
    finally:
        await state.clear()

# ============================== Phase 2: Admin =================================

admin_router = Router(name="admin")

def _parse_dt_local(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$", s):
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
            return tz.localize(dt)
        dt = dateparser.parse(s, dayfirst=True)
        if not dt:
            return None
        return dt.astimezone(tz) if dt.tzinfo else tz.localize(dt)
    except Exception:
        return None

def _fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")

def _event_by_id(event_id: str):
    try:
        ws = gs().worksheet(SHEET_EVENTS)
        for r in _read_all(ws):
            if r.get("event_id") == event_id:
                return r
    except Exception:
        pass
    return None

async def _notify_going_update(event_id: str, text: str, action_key: str):
    try:
        wsr = gs().worksheet(SHEET_RSVP)
        going = [r for r in _read_all(wsr) if r.get("event_id") == event_id and r.get("rsvp") == "going"]
        if not going:
            return
        wsc = gs().worksheet(SHEET_CLIENTS)
        clients = _read_all(wsc)
        tg_map = {c.get("client_id"): c.get("tg_user_id") for c in clients}
        for r in going:
            chat_id = tg_map.get(r.get("client_id"))
            if not chat_id:
                continue
            try:
                await admin_router.bot.send_message(int(chat_id), text)
                _delivery_log(action_key, r.get("client_id"), event_id, text)
            except Exception:
                _delivery_log("fail", r.get("client_id"), event_id, f"{action_key} send error")
    except Exception:
        pass

@admin_router.message(CommandStart())
async def admin_start_gate(message: Message, state: FSMContext):
    payload = ""
    if message.text:
        parts = message.text.split(" ", 1)
        payload = parts[1].strip() if len(parts) > 1 else ""
    if not payload.startswith("admin_"):
        return
    token = unquote(payload[len("admin_"):])
    if message.from_user.id in ADMIN_IDS and token == ADMIN_DEEPLINK_TOKEN:
        await _show_admin_menu(message)
    else:
        await message.answer("⛔️ Команда недоступна.")

@admin_router.message(Command("admin"))
async def admin_cmd(message: Message):
    if message.chat.type != "private":
        return
    if message.from_user.id not in ADMIN_IDS:
        return
    await _show_admin_menu(message)

async def _show_admin_menu(message: Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Додати івент", callback_data="admin:add")
    kb.button(text="📋 Ближчі івенти", callback_data="admin:list")
    kb.adjust(1,1)
    await message.answer("🔐 *Адмін-панель*. Оберіть дію:", reply_markup=kb.as_markup())

def _admin_only(handler):
    async def wrapped(callback: CallbackQuery, state: FSMContext):
        if callback.message.chat.type != "private" or callback.from_user.id not in ADMIN_IDS:
            await callback.answer("Недоступно", show_alert=True)
            return
        return await handler(callback, state)
    return wrapped

class CreateEvent(StatesGroup):
    choosing_type = State()
    entering_datetime = State()
    entering_duration = State()
    entering_link = State()
    entering_title = State()
    entering_description = State()
    confirming = State()

@admin_router.callback_query(F.data == "admin:add")
@_admin_only
async def add_event_start(cb: CallbackQuery, state: FSMContext):
    try:
        ws = gs().worksheet(SHEET_EVENTTYPES)
        types = [r for r in _read_all(ws) if str(r.get("active","")).strip().lower() in ("1","true","yes")]
        if not types:
            await cb.message.answer("ℹ️ Немає активних типів у *EventTypes*.")
            return
        await state.update_data(types=types)
        kb = InlineKeyboardBuilder()
        for r in types[:50]:
            code = r.get("type_code"); title = r.get("title") or code
            kb.button(text=f"{title}", callback_data=f"admin:add:type:{code}")
        kb.button(text="↩️ Назад", callback_data="admin:back")
        kb.adjust(1)
        await cb.message.answer("Оберіть *тип події*:", reply_markup=kb.as_markup())
        await state.set_state(CreateEvent.choosing_type)
    except Exception:
        await cb.message.answer("Помилка читання *EventTypes*. Перевірте доступи.")

@admin_router.callback_query(F.data.startswith("admin:add:type:"))
@_admin_only
async def add_event_type_selected(cb: CallbackQuery, state: FSMContext):
    type_code = cb.data.split(":")[-1]
    await state.update_data(new_event={"type": type_code})
    await cb.message.answer("Вкажіть *дату і час* у форматі `YYYY-MM-DD HH:MM` (Київ)\n_Напр.: 2025-10-05 18:00_")
    await state.set_state(CreateEvent.entering_datetime)

@admin_router.message(CreateEvent.entering_datetime)
async def add_event_dt(msg: Message, state: FSMContext):
    dt = _parse_dt_local(msg.text)
    if not dt:
        await msg.answer("Не вдалося розпізнати дату/час. Приклад: `2025-10-05 18:00`")
        return
    d = await state.get_data(); ev = d.get("new_event", {}); ev["start_at"] = _fmt_dt(dt)
    await state.update_data(new_event=ev)
    await msg.answer("⏱ Вкажіть *тривалість* у хвилинах (за замовчуванням 60).")
    await state.set_state(CreateEvent.entering_duration)

@admin_router.message(CreateEvent.entering_duration)
async def add_event_duration(msg: Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if not txt:
        dur = 60
    else:
        try:
            dur = int(txt); assert 1 <= dur <= 360
        except Exception:
            await msg.answer("Будь ласка, число хвилин 1..360. _Напр.: 60_")
            return
    d = await state.get_data(); ev = d.get("new_event", {}); ev["duration_min"] = str(dur)
    await state.update_data(new_event=ev)
    await msg.answer("🔗 Надішліть *посилання* на конференцію (https://...)")
    await state.set_state(CreateEvent.entering_link)

@admin_router.message(CreateEvent.entering_link)
async def add_event_link(msg: Message, state: FSMContext):
    link = (msg.text or "").strip()
    if not (link.startswith("http://") or link.startswith("https://")):
        await msg.answer("Повний URL (https://...)")
        return
    d = await state.get_data(); ev = d.get("new_event", {}); ev["link"] = link
    await state.update_data(new_event=ev)
    await msg.answer("📝 Заголовок (_можна пропустити — надішліть «-»_)")
    await state.set_state(CreateEvent.entering_title)

@admin_router.message(CreateEvent.entering_title)
async def add_event_title(msg: Message, state: FSMContext):
    title = (msg.text or "").strip()
    d = await state.get_data(); ev = d.get("new_event", {}); ev["title"] = "" if title == "-" else title
    await state.update_data(new_event=ev)
    await msg.answer("ℹ️ Опис (_можна пропустити — надішліть «-»_)")
    await state.set_state(CreateEvent.entering_description)

@admin_router.message(CreateEvent.entering_description)
async def add_event_desc(msg: Message, state: FSMContext):
    desc = (msg.text or "").strip()
    d = await state.get_data(); ev = d.get("new_event", {}); ev["description"] = "" if desc == "-" else desc
    await state.update_data(new_event=ev)
    ev = (await state.get_data()).get("new_event", {})
    preview = (
        f"*Попередній перегляд:*\n"
        f"• Тип: `{ev.get('type')}`\n"
        f"• Час: `{ev.get('start_at')}` (Київ)\n"
        f"• Тривалість: `{ev.get('duration_min')} хв`\n"
        f"• Лінк: {ev.get('link')}\n"
        f"• Заголовок: {ev.get('title') or '— (із EventTypes)'}\n"
        f"• Опис: {ev.get('description') or '— (із EventTypes)'}"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Зберегти", callback_data="admin:add:confirm")
    kb.button(text="↩️ Скасувати", callback_data="admin:back")
    kb.adjust(1,1)
    await msg.answer(preview, reply_markup=kb.as_markup())
    await state.set_state(CreateEvent.confirming)

@admin_router.callback_query(F.data == "admin:back")
async def admin_back(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await _show_admin_menu(cb.message)

@admin_router.callback_query(F.data == "admin:add:confirm")
async def add_event_save(cb: CallbackQuery, state: FSMContext):
    ev = (await state.get_data()).get("new_event", {})
    if not ev:
        await cb.message.answer("Немає даних про івент.")
        return
    try:
        wt = gs().worksheet(SHEET_EVENTTYPES)
        trow = next((r for r in _read_all(wt) if r.get("type_code") == ev["type"]), None)
        if trow:
            if not ev.get("title"): ev["title"] = trow.get("title","")
            if not ev.get("description"): ev["description"] = trow.get("description","")
    except Exception:
        pass

    try:
        we = gs().worksheet(SHEET_EVENTS)
        event_id = f"evt_{uuid.uuid4().hex[:8]}"
        _append(we, {
            "event_id": event_id,
            "type": ev["type"],
            "title": ev.get("title",""),
            "description": ev.get("description",""),
            "start_at": ev["start_at"],
            "duration_min": ev["duration_min"],
            "link": ev["link"],
            "created_by": cb.from_user.username or str(cb.from_user.id),
            "created_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M"),
            "broadcasted_at": "",
        })
        await cb.message.answer("✅ Подію збережено. Знайдете її у «Ближчі івенти».")
        await admin_back(cb, state)
    except Exception:
        await cb.message.answer("❌ Не вдалося зберегти подію.")

@admin_router.callback_query(F.data == "admin:list")
@_admin_only
async def list_upcoming(cb: CallbackQuery, state: FSMContext):
    try:
        we = gs().worksheet(SHEET_EVENTS)
        rows = _read_all(we)
        now_ = datetime.now(tz)
        horizon = now_ + timedelta(days=14)
        upcoming = []
        for r in rows:
            dt = _parse_dt_local(r.get("start_at",""))
            if not dt:
                continue
            if now_ <= dt <= horizon:
                upcoming.append((dt, r))
        if not upcoming:
            await cb.message.answer("ℹ️ Найближчих подій (14 днів) не знайдено.")
            return
        upcoming.sort(key=lambda x: x[0])
        kb = InlineKeyboardBuilder()
        text = "*Ближчі події (14 днів):*\n"
        for dt, r in upcoming[:50]:
            line = f"- `{r.get('event_id')}` • {r.get('title') or r.get('type')} • {dt.strftime('%Y-%m-%d %H:%M')}"
            text += line + "\n"
            kb.button(text=f"🔎 {r.get('title') or r.get('type')} {dt.strftime('%m-%d %H:%M')}",
                      callback_data=f"admin:ev:{r.get('event_id')}")
        kb.button(text="↩️ Назад", callback_data="admin:back")
        kb.adjust(1)
        await cb.message.answer(text, reply_markup=kb.as_markup())
    except Exception:
        await cb.message.answer("Помилка читання *Events*.")

@admin_router.callback_query(F.data.startswith("admin:ev:"))
@_admin_only
async def event_card(cb: CallbackQuery, state: FSMContext):
    event_id = cb.data.split(":")[-1]
    ev = _event_by_id(event_id)
    if not ev:
        await cb.message.answer("Подію не знайдено.")
        return
    dt = _parse_dt_local(ev.get("start_at",""))
    dts = dt.strftime("%Y-%m-%d %H:%M") if dt else ev.get("start_at","")
    text = (
        f"*Подія:* `{ev.get('event_id')}`\n"
        f"• Тип: `{ev.get('type')}`\n"
        f"• Назва: {ev.get('title') or '—'}\n"
        f"• Час: {dts}\n"
        f"• Тривалість: {ev.get('duration_min') or '—'} хв\n"
        f"• Лінк: {ev.get('link')}\n"
        f"• Опис: {ev.get('description') or '—'}"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Оновити", callback_data=f"admin:update:{event_id}")
    kb.button(text="❌ Скасувати", callback_data=f"admin:cancel:{event_id}")
    kb.button(text="↩️ Назад", callback_data="admin:list")
    kb.adjust(2,1)
    await cb.message.answer(text, reply_markup=kb.as_markup())

class UpdateEvent(StatesGroup):
    picking_field = State()
    entering_value = State()

_ALLOWED_FIELDS = ("start_at", "duration_min", "link", "title", "description")

@admin_router.callback_query(F.data.startswith("admin:update:"))
@_admin_only
async def update_choose_field(cb: CallbackQuery, state: FSMContext):
    event_id = cb.data.split(":")[-1]
    await state.update_data(event_id=event_id)
    kb = InlineKeyboardBuilder()
    for fld in _ALLOWED_FIELDS:
        kb.button(text=fld, callback_data=f"admin:update:field:{fld}")
    kb.button(text="↩️ Назад", callback_data=f"admin:ev:{event_id}")
    kb.adjust(3,2,1)
    await cb.message.answer("Оберіть поле для оновлення:", reply_markup=kb.as_markup())
    await state.set_state(UpdateEvent.picking_field)

@admin_router.callback_query(F.data.startswith("admin:update:field:"))
@_admin_only
async def update_enter_value_prompt(cb: CallbackQuery, state: FSMContext):
    fld = cb.data.split(":")[-1]
    d = await state.get_data()
    await state.update_data(field=fld)
    hint = {
        "start_at": "у форматі `YYYY-MM-DD HH:MM` (Київ)",
        "duration_min": "число хвилин (1..360)",
        "link": "повний URL (https://...)",
        "title": "текст заголовка",
        "description": "текст опису",
    }[fld]
    await cb.message.answer(f"Введіть нове значення для *{fld}* ({hint}):")
    await state.set_state(UpdateEvent.entering_value)

@admin_router.message(UpdateEvent.entering_value)
async def update_apply_value(msg: Message, state: FSMContext):
    d = await state.get_data()
    event_id = d.get("event_id")
    field = d.get("field")
    val = (msg.text or "").strip()
    if field == "start_at":
        dt = _parse_dt_local(val)
        if not dt:
            await msg.answer("Невалідна дата/час. Приклад: `2025-10-05 18:00`")
            return
        val = _fmt_dt(dt)
    elif field == "duration_min":
        try:
            iv = int(val); assert 1 <= iv <= 360
        except Exception:
            await msg.answer("Вкажіть число хвилин 1..360.")
            return
    elif field == "link":
        if not (val.startswith("http://") or val.startswith("https://")):
            await msg.answer("Повний URL (https://...)")
            return

    try:
        we = gs().worksheet(SHEET_EVENTS)
        ok = _update_row_by_key(we, "event_id", event_id, {field: val})
        if not ok:
            await msg.answer("Не знайшов подію для оновлення.")
            await state.clear()
            return
        ev = _event_by_id(event_id) or {}
        title = ev.get("title") or ev.get("type") or "Подія"
        await _notify_going_update(event_id, f"🛠 Оновлення: *{title}*. Змінено `{field}`.", "update_sent")
        await msg.answer("✅ Оновлено.")
        await state.clear()
    except Exception:
        await msg.answer("❌ Помилка оновлення.")
        await state.clear()

@admin_router.callback_query(F.data.startswith("admin:cancel:"))
@_admin_only
async def cancel_event(cb: CallbackQuery, state: FSMContext):
    event_id = cb.data.split(":")[-1]
    ev = _event_by_id(event_id)
    if not ev:
        await cb.message.answer("Подію не знайдено.")
        return
    try:
        we = gs().worksheet(SHEET_EVENTS)
        row = _find_rows(we, lambda r: r.get("event_id") == event_id)[0][0]
        new_title = ev.get("title") or ev.get("type") or ""
        if not new_title.startswith("[СКАСОВАНО]"):
            new_title = f"[СКАСОВАНО] {new_title}"
        we.update_cell(row, _col_index(we, "title"), new_title)
        await _notify_going_update(event_id, "❌ Зустріч скасовано. Надішлемо нову дату найближчим часом.", "cancel_notice_sent")
        await cb.message.answer("✅ Подію скасовано та повідомлено учасників.")
    except Exception:
        await cb.message.answer("❌ Не вдалося скасувати подію.")

# ============================== Phase 3: Invites/RSVP/Reminders ===============

phase3_router = Router(name="phase3")
_BOT_PHASE3 = None
def init_phase3(bot_instance):
    global _BOT_PHASE3
    _BOT_PHASE3 = bot_instance

def parse_dt_local(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$", s):
            return tz.localize(datetime.strptime(s, "%Y-%m-%d %H:%M"))
        dt = dateparser.parse(s, dayfirst=True)
        if not dt: return None
        return dt.astimezone(tz) if dt.tzinfo else tz.localize(dt)
    except Exception:
        return None

def fmt_date(dt: datetime) -> Tuple[str, str]:
    return dt.strftime("%d.%m.%Y"), dt.strftime("%H:%M")

# windows
REMINDER24_WINDOW = (23*60 - 10, 24*60 + 10)
REMINDER60_WINDOW = (55, 65)

QUIET_HOURS = None  # (22,8) якщо треба

def within_quiet_hours(dt: datetime) -> bool:
    if not QUIET_HOURS: return False
    start, end = QUIET_HOURS
    h = dt.hour
    return (start <= h < end) if start < end else (h >= start or h < end)

import hmac, hashlib
def sign_payload(payload: str) -> str:
    return hmac.new(SECRET_HMAC_KEY.encode("utf-8"), payload.encode("utf-8"),
                    hashlib.sha256).hexdigest()[:16]

def make_cb(action: str, event_id: str, client_id: str) -> str:
    ts_ = int(datetime.now(tz).timestamp())
    raw = f"{action}|{event_id}|{client_id}|{ts_}"
    return f"{raw}|{sign_payload(raw)}"

def verify_cb(data: str):
    try:
        action, event_id, client_id, ts, sig = data.split("|")
        raw = f"{action}|{event_id}|{client_id}|{ts}"
        return sign_payload(raw) == sig, {"action": action, "event_id": event_id, "client_id": client_id}
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

def default_invite(name: str, title: str, date_str: str, time_str: str, description: str) -> str:
    return (f"{name}, запрошуємо на зустріч: *{title}*\n"
            f"🗓 {date_str} о {time_str} (Київ)\n"
            f"ℹ️ {description}\n"
            "Виберіть варіант нижче:")

def _has_active_offer_for_type(client_id: str, ev_type: str,
                               events: List[Dict[str,str]],
                               rsvps: List[Dict[str,str]],
                               attendance: List[Dict[str,str]]) -> bool:
    ev_map = {e.get("event_id"): (e.get("type"), parse_dt_local(e.get("start_at",""))) for e in events}
    for a in attendance:
        if a.get("client_id") == client_id and str(a.get("attended","")).lower() in ("true","1","yes"):
            t = ev_map.get(a.get("event_id"), (None, None))[0]
            if t == ev_type:
                return True
    for r in rsvps:
        if r.get("client_id") != client_id:
            continue
        eid = r.get("event_id")
        t, dt = ev_map.get(eid, (None, None))
        if t != ev_type or not dt:
            continue
        if dt <= datetime.now(tz):
            continue
        v = (r.get("rsvp") or "").strip()
        if v in ("going", "remind_me", ""):
            return True
    return False

async def _send_invite_to_client(bot, ev: Dict[str,str], client: Dict[str,str], title: str, description: str) -> bool:
    cid = client.get("client_id")
    chat_id = client.get("tg_user_id")
    if not chat_id:
        return False
    start = parse_dt_local(ev.get("start_at",""))
    if not start:
        return False
    date_str, time_str = fmt_date(start)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Так, буду", callback_data=make_cb("going", ev["event_id"], cid)),
            InlineKeyboardButton(text="🚫 Не зможу", callback_data=make_cb("not", ev["event_id"], cid)),
        ],
        [
            InlineKeyboardButton(text="🔔 Нагадати за 24 год", callback_data=make_cb("rem24", ev["event_id"], cid))
        ]
    ])

    template = tmpl("invite.body","uk") or default_invite("{name}", "{title}", "{date}", "{time}", "{description}")
    text = (template
            .replace("{name}", client.get("full_name") or "Клієнт")
            .replace("{title}", title or ev.get("type"))
            .replace("{date}", date_str)
            .replace("{time}", time_str)
            .replace("{description}", description or ""))

    try:
        await bot.send_message(int(chat_id), text, reply_markup=kb)
        _delivery_log("invite_sent", cid, ev["event_id"], "")
        _upsert_rsvp(ev["event_id"], cid, {})  # pending
        return True
    except Exception as e:
        _delivery_log("fail", cid, ev["event_id"], f"invite: {e}")
        return False

async def do_broadcast(event_id: str):
    if not _BOT_PHASE3:
        return
    try:
        we = gs().worksheet(SHEET_EVENTS)
        events = _read_all(we)
        ev = next((r for r in events if r.get("event_id") == event_id), None)
        if not ev: return

        title = ev.get("title") or ""
        description = ev.get("description") or ""
        try:
            wt = gs().worksheet(SHEET_EVENTTYPES)
            trow = next((r for r in _read_all(wt) if r.get("type") == ev.get("type") or r.get("type_code") == ev.get("type")), None)
            if trow:
                if not title: title = trow.get("title","")
                if not description: description = trow.get("description","")
        except Exception:
            pass

        start = parse_dt_local(ev.get("start_at",""))
        if not start:
            return
        if QUIET_HOURS and within_quiet_hours(datetime.now(tz)):
            return

        wc = gs().worksheet(SHEET_CLIENTS); clients = [c for c in _read_all(wc) if c.get("status")=="active" and c.get("tg_user_id")]
        wr = gs().worksheet(SHEET_RSVP); rsvps = _read_all(wr)
        wa = gs().worksheet(SHEET_ATTENDANCE); attendance = _read_all(wa)

        sent_any = False
        for cl in clients:
            cid = cl.get("client_id")
            if _has_active_offer_for_type(cid, ev.get("type"), events, rsvps, attendance):
                continue
            ok = await _send_invite_to_client(_BOT_PHASE3, ev, cl, title, description)
            sent_any = sent_any or ok
            await asyncio.sleep(0.04)

        if sent_any or ev.get("broadcasted_at","") == "":
            _update_row_by_key(we, "event_id", event_id, {"broadcasted_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M")})
    except Exception:
        pass

async def job_broadcast_new():
    if not _BOT_PHASE3: return
    try:
        we = gs().worksheet(SHEET_EVENTS)
        for r in _read_all(we):
            if not r.get("broadcasted_at"):
                await do_broadcast(r.get("event_id"))
    except Exception:
        pass

@phase3_router.callback_query(F.data.regexp(r"^(going|not|rem24)\|"))
async def on_rsvp_click(cb: CallbackQuery):
    ok, p = verify_cb(cb.data)
    if not ok:
        await cb.answer("Невірний підпис кнопки", show_alert=True); return
    action = p["action"]; event_id = p["event_id"]

    try:
        wc = gs().worksheet(SHEET_CLIENTS)
        me = next((r for r in _read_all(wc) if r.get("tg_user_id")==str(cb.from_user.id)), None)
        if not me:
            await cb.answer("Спочатку пройдіть коротку реєстрацію.", show_alert=True); return
        client_id = me.get("client_id")
    except Exception:
        await cb.answer("Помилка доступу до бази.", show_alert=True); return

    if action == "going":
        _upsert_rsvp(event_id, client_id, {
            "rsvp": "going",
            "remind_24h": "TRUE",
            "reminded_24h": "",
            "reminded_60m": "",
            "rsvp_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M"),
        })
        _delivery_log("rsvp_going", client_id, event_id, "")
        await cb.message.answer("✅ Записав. Нагадаю *за 24 год* і *за 1 год* до початку.")
        await cb.answer(); return
    if action == "not":
        _upsert_rsvp(event_id, client_id, {
            "rsvp": "not_going",
            "rsvp_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M"),
        })
        _delivery_log("rsvp_not_going", client_id, event_id, "")
        await cb.message.answer("Дякуємо! Коли з’являться інші дати — надішлемо.")
        await cb.answer(); return
    if action == "rem24":
        _upsert_rsvp(event_id, client_id, {
            "rsvp": "remind_me",
            "remind_24h": "TRUE",
            "rsvp_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M"),
        })
        _delivery_log("rsvp_rem24", client_id, event_id, "")
        await cb.message.answer("🔔 Гаразд, нагадаю *за 24 год* до старту.")
        await cb.answer(); return

async def job_reminder_24h():
    if not _BOT_PHASE3: return
    try:
        we = gs().worksheet(SHEET_EVENTS); events = _read_all(we)
        wr = gs().worksheet(SHEET_RSVP); rsvps = _read_all(wr)
        wc = gs().worksheet(SHEET_CLIENTS); clients = _read_all(wc)
        tg_map = {c.get("client_id"): c.get("tg_user_id") for c in clients}

        for ev in events:
            start = parse_dt_local(ev.get("start_at",""))
            if not start: continue
            mins_to = int((start - datetime.now(tz)).total_seconds() // 60)
            if not (REMINDER24_WINDOW[0] <= mins_to <= REMINDER24_WINDOW[1]):
                continue
            targets = [r for r in rsvps
                       if r.get("event_id")==ev.get("event_id")
                       and (r.get("rsvp") in ("going","remind_me"))
                       and str(r.get("remind_24h","")).lower() in ("true","1","yes")
                       and str(r.get("reminded_24h","")).lower() not in ("true","1","yes")]
            if not targets: continue
            title = ev.get("title") or ev.get("type") or "Зустріч"
            t_str = start.strftime("%H:%M")
            for r in targets:
                chat_id = tg_map.get(r.get("client_id"))
                if not chat_id: continue
                try:
                    text = tmpl("reminder.24h","uk") or f"🔔 Нагадуємо: *завтра* о {t_str} відбудеться *{title}*.\nПосилання: {ev.get('link')}"
                    await _BOT_PHASE3.send_message(int(chat_id), text)
                    _delivery_log("reminder_24h_sent", r.get("client_id"), ev.get("event_id"), "")
                    _upsert_rsvp(ev.get("event_id"), r.get("client_id"), {"reminded_24h":"TRUE"})
                    await asyncio.sleep(0.04)
                except Exception as e:
                    _delivery_log("fail", r.get("client_id"), ev.get("event_id"), f"rem24: {e}")
    except Exception:
        pass

async def job_reminder_60m():
    if not _BOT_PHASE3: return
    try:
        we = gs().worksheet(SHEET_EVENTS); events = _read_all(we)
        wr = gs().worksheet(SHEET_RSVP); rsvps = _read_all(wr)
        wc = gs().worksheet(SHEET_CLIENTS); clients = _read_all(wc)
        tg_map = {c.get("client_id"): c.get("tg_user_id") for c in clients}

        for ev in events:
            start = parse_dt_local(ev.get("start_at",""))
            if not start: continue
            mins_to = int((start - datetime.now(tz)).total_seconds() // 60)
            if not (REMINDER60_WINDOW[0] <= mins_to <= REMINDER60_WINDOW[1]):
                continue
            targets = [r for r in rsvps
                       if r.get("event_id")==ev.get("event_id")
                       and r.get("rsvp")=="going"
                       and str(r.get("reminded_60m","")).lower() not in ("true","1","yes")]
            if not targets: continue
            title = ev.get("title") or ev.get("type") or "Зустріч"
            for r in targets:
                chat_id = tg_map.get(r.get("client_id"))
                if not chat_id: continue
                try:
                    text = tmpl("reminder.60m","uk") or f"⏰ Нагадуємо: через 1 год почнеться *{title}*. Посилання: {ev.get('link')}"
                    await _BOT_PHASE3.send_message(int(chat_id), text)
                    _delivery_log("reminder_60m_sent", r.get("client_id"), ev.get("event_id"), "")
                    _upsert_rsvp(ev.get("event_id"), r.get("client_id"), {"reminded_60m":"TRUE"})
                    await asyncio.sleep(0.04)
                except Exception as e:
                    _delivery_log("fail", r.get("client_id"), ev.get("event_id"), f"rem60: {e}")
    except Exception:
        pass

def setup_phase3(dp, scheduler=None, bot_instance=None):
    dp.include_router(phase3_router)
    if bot_instance is not None:
        init_phase3(bot_instance)
    if scheduler is not None:
        scheduler.add_job(job_broadcast_new, "interval", minutes=1, id="broadcast_new", replace_existing=True)
        scheduler.add_job(job_reminder_24h, "interval", minutes=5, id="reminder_24h", replace_existing=True)
        scheduler.add_job(job_reminder_60m, "interval", minutes=1, id="reminder_60m", replace_existing=True)

# ============================== Phase 4: Feedback/Escalation ==================

feedback_router = Router(name="phase4_feedback")
_FB_BOT = None
def init_phase4(bot_instance):
    global _FB_BOT
    _FB_BOT = bot_instance

def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")

def _sign(raw: str) -> str:
    return hmac.new(SECRET_HMAC_KEY.encode(), raw.encode(), hashlib.sha256).hexdigest()[:16]

def _make_cb(kind: str, event_id: str, client_id: str, extra: str = "") -> str:
    raw = f"{kind}|{event_id}|{client_id}|{extra}"
    return f"{raw}|{_sign(raw)}"

def _verify_cb(data: str):
    try:
        kind, event_id, client_id, extra, sig = data.split("|", 4)
        raw = f"{kind}|{event_id}|{client_id}|{extra}"
        return _sign(raw) == sig, {"kind":kind, "event_id":event_id, "client_id":client_id, "extra":extra}
    except Exception:
        return False, {}

def _msg_tmpl(key: str, lang="uk") -> Optional[str]:
    try:
        ws = gs().worksheet(SHEET_MESSAGES)
        for r in _read_all(ws):
            if r.get("key")==key and r.get("lang")==lang:
                return (r.get("text") or "").strip()
    except Exception:
        pass
    return None

def _feedback_prompt(title: str) -> str:
    t = _msg_tmpl("feedback.ask","uk")
    if t:
        return t.replace("{title}", title)
    return ("Дякуємо за участь у *{title}*. Оцініть, будь ласка:\n"
            "1) Корисність: ⭐️1–5\n"
            "2) Чи зрозумілі наступні кроки? ✅ Так / ⚠️ Частково / ❌ Ні\n"
            "Можна додати коментар — натисніть кнопку нижче.").replace("{title}", title)

def _get_event(event_id: str) -> Optional[Dict[str,str]]:
    try:
        ws = gs().worksheet(SHEET_EVENTS)
        for r in _read_all(ws):
            if r.get("event_id")==event_id:
                return r
    except Exception:
        pass
    return None

def _get_client_by_id(client_id: str) -> Optional[Dict[str,str]]:
    try:
        ws = gs().worksheet(SHEET_CLIENTS)
        for r in _read_all(ws):
            if r.get("client_id")==client_id:
                return r
    except Exception:
        pass
    return None

def _upsert_feedback(event_id: str, client_id: str, patch: Dict[str,str]):
    sh = gs().worksheet(SHEET_FEEDBACK)
    rows = _find_rows(sh, lambda r: r.get("event_id")==event_id and r.get("client_id")==client_id)
    if rows:
        rownum, _ = rows[0]
        for k, v in patch.items():
            sh.update_cell(rownum, _col_index(sh, k), v)
    else:
        row = {"event_id": event_id, "client_id": client_id,
               "stars":"", "clarity":"", "comment":"", "submitted_at":"",
               "followup_needed":"", "handled_at":"", "owner":""}
        row.update(patch)
        _append(sh, row)

def _delivery_has_feedback_ask(event_id: str, client_id: str) -> bool:
    try:
        ws = gs().worksheet(SHEET_DELIVERY)
        rows = _read_all(ws)
        return any(r.get("event_id")==event_id and r.get("client_id")==client_id and r.get("action")=="feedback_ask_sent" for r in rows)
    except Exception:
        return False

def _kb_feedback(event_id: str, client_id: str) -> InlineKeyboardMarkup:
    stars_row = [
        InlineKeyboardButton(text="⭐ 1", callback_data=_make_cb("fb_star", event_id, client_id, "1")),
        InlineKeyboardButton(text="⭐ 2", callback_data=_make_cb("fb_star", event_id, client_id, "2")),
        InlineKeyboardButton(text="⭐ 3", callback_data=_make_cb("fb_star", event_id, client_id, "3")),
        InlineKeyboardButton(text="⭐ 4", callback_data=_make_cb("fb_star", event_id, client_id, "4")),
        InlineKeyboardButton(text="⭐ 5", callback_data=_make_cb("fb_star", event_id, client_id, "5")),
    ]
    clarity_row = [
        InlineKeyboardButton(text="✅ Так", callback_data=_make_cb("fb_clr", event_id, client_id, "yes")),
        InlineKeyboardButton(text="⚠️ Частково", callback_data=_make_cb("fb_clr", event_id, client_id, "partial")),
        InlineKeyboardButton(text="❌ Ні", callback_data=_make_cb("fb_clr", event_id, client_id, "no")),
    ]
    comment_row = [
        InlineKeyboardButton(text="✍️ Написати відгук", callback_data=_make_cb("fb_cmt", event_id, client_id, "")),
    ]
    return InlineKeyboardMarkup(inline_keyboard=[stars_row, clarity_row, comment_row])

async def _escalate_if_needed(event_id: str, client_id: str):
    if not SUPPORT_CHAT_ID:
        return
    ev = _get_event(event_id) or {}
    cl = _get_client_by_id(client_id) or {}
    try:
        ws = gs().worksheet(SHEET_FEEDBACK)
        rows = _find_rows(ws, lambda r: r.get("event_id")==event_id and r.get("client_id")==client_id)
        if not rows:
            return
        _, fb = rows[0]
        stars = int(fb.get("stars") or "0")
        clarity = (fb.get("clarity") or "").lower()
        comment = fb.get("comment") or ""
        negative = (stars and stars < 4) or (clarity not in ("", "yes"))
        if not negative:
            return
        _upsert_feedback(event_id, client_id, {"followup_needed":"TRUE"})
        title = ev.get("title") or ev.get("type") or "Зустріч"
        dt = parse_dt_local(ev.get("start_at",""))
        dts = dt.strftime("%Y-%m-%d %H:%M") if dt else (ev.get("start_at") or "")
        text = (
            f"🚨 *Негативний фідбек*\n"
            f"• Клієнт: {cl.get('full_name') or '—'} ({cl.get('phone') or '—'})\n"
            f"• Подія: {title} `{event_id}` ({dts})\n"
            f"• Оцінка: {stars or '—'} ⭐; Зрозумілість: {clarity or '—'}\n"
            f"• Коментар: {comment or '—'}"
        )
        try:
            await _FB_BOT.send_message(int(SUPPORT_CHAT_ID), text, parse_mode=ParseMode.MARKDOWN)
            _delivery_log("feedback_escalated", client_id, event_id, "")
        except Exception:
            _delivery_log("fail", client_id, event_id, "feedback_escalation_send_error")
    except Exception:
        pass

class FBComment(StatesGroup):
    waiting = State()

@feedback_router.callback_query(F.data.regexp(r"^fb_cmt\|"))
async def fb_comment_start(cb: CallbackQuery, state: FSMContext):
    ok, p = _verify_cb(cb.data)
    if not ok:
        await cb.answer("Невірний підпис", show_alert=True); return
    try:
        wc = gs().worksheet(SHEET_CLIENTS)
        me = next((r for r in _read_all(wc) if r.get("tg_user_id")==str(cb.from_user.id)), None)
        if not me or me.get("client_id") != p["client_id"]:
            await cb.answer("Недоступно", show_alert=True); return
    except Exception:
        await cb.answer("Помилка доступу", show_alert=True); return
    await state.update_data(event_id=p["event_id"], client_id=p["client_id"])
    await cb.message.answer("✍️ Напишіть, будь ласка, ваш коментар одним повідомленням.")
    await state.set_state(FBComment.waiting)
    await cb.answer()

@feedback_router.message(FBComment.waiting)
async def fb_comment_save(msg: Message, state: FSMContext):
    d = await state.get_data()
    event_id = d.get("event_id"); client_id = d.get("client_id")
    comment = (msg.text or "").strip()
    try:
        _upsert_feedback(event_id, client_id, {"comment": comment, "submitted_at": fmt_dt(datetime.now(tz))})
        await msg.answer("✅ Дякуємо! Ваш коментар збережено.")
        await _escalate_if_needed(event_id, client_id)
    except Exception:
        await msg.answer("Не вдалося зберегти відгук. Спробуйте пізніше.")
    finally:
        await state.clear()

@feedback_router.callback_query(F.data.regexp(r"^fb_star\|"))
async def fb_set_stars(cb: CallbackQuery):
    ok, p = _verify_cb(cb.data)
    if not ok:
        await cb.answer("Невірний підпис", show_alert=True); return
    try:
        wc = gs().worksheet(SHEET_CLIENTS)
        me = next((r for r in _read_all(wc) if r.get("tg_user_id")==str(cb.from_user.id)), None)
        if not me or me.get("client_id") != p["client_id"]:
            await cb.answer("Недоступно", show_alert=True); return
    except Exception:
        await cb.answer("Помилка доступу", show_alert=True); return

    stars = p["extra"]
    try:
        _upsert_feedback(p["event_id"], p["client_id"], {"stars": stars, "submitted_at": fmt_dt(datetime.now(tz))})
        await cb.message.answer(f"⭐ Дякуємо! Оцінка: *{stars}*.")
        await _escalate_if_needed(p["event_id"], p["client_id"])
    except Exception:
        await cb.message.answer("Не вдалося зберегти оцінку.")
    await cb.answer()

@feedback_router.callback_query(F.data.regexp(r"^fb_clr\|"))
async def fb_set_clarity(cb: CallbackQuery):
    ok, p = _verify_cb(cb.data)
    if not ok:
        await cb.answer("Невірний підпис", show_alert=True); return
    try:
        wc = gs().worksheet(SHEET_CLIENTS)
        me = next((r for r in _read_all(wc) if r.get("tg_user_id")==str(cb.from_user.id)), None)
        if not me or me.get("client_id") != p["client_id"]:
            await cb.answer("Недоступно", show_alert=True); return
    except Exception:
        await cb.answer("Помилка доступу", show_alert=True); return

    clarity = p["extra"]
    try:
        _upsert_feedback(p["event_id"], p["client_id"], {"clarity": clarity, "submitted_at": fmt_dt(datetime.now(tz))})
        pretty = {"yes":"✅ Так", "partial":"⚠️ Частково", "no":"❌ Ні"}.get(clarity, clarity)
        await cb.message.answer(f"🧭 Зрозумілість: {pretty}. Дякуємо!")
        await _escalate_if_needed(p["event_id"], p["client_id"])
    except Exception:
        await cb.message.answer("Не вдалося зберегти відповідь.")
    await cb.answer()

FEEDBACK_WINDOW_MIN = (175, 205)
def parse_dt_local_fb(s: str) -> Optional[datetime]:
    return parse_dt_local(s)

async def job_feedback_ask():
    if not _FB_BOT:
        return
    try:
        we = gs().worksheet(SHEET_EVENTS); events = _read_all(we)
        wr = gs().worksheet(SHEET_RSVP);   rsvps  = _read_all(wr)
        wc = gs().worksheet(SHEET_CLIENTS);clients = _read_all(wc)
        tg_map = {c.get("client_id"): c.get("tg_user_id") for c in clients}

        for ev in events:
            start = parse_dt_local_fb(ev.get("start_at","")); 
            if not start: continue
            try:
                dur = int(ev.get("duration_min") or "60")
            except Exception:
                dur = 60
            end_dt = start + timedelta(minutes=dur)
            mins_after = int((datetime.now(tz) - end_dt).total_seconds() // 60)

            if not (FEEDBACK_WINDOW_MIN[0] <= mins_after <= FEEDBACK_WINDOW_MIN[1]):
                continue

            eid = ev.get("event_id")
            title = ev.get("title") or ev.get("type") or "Зустріч"
            candidates = [r for r in rsvps if r.get("event_id")==eid and r.get("rsvp")=="going"]

            for r in candidates:
                cid = r.get("client_id")
                chat_id = tg_map.get(cid)
                if not chat_id:
                    continue
                if _delivery_has_feedback_ask(eid, cid) or _find_rows(gs().worksheet(SHEET_FEEDBACK),
                    lambda rr: rr.get("event_id")==eid and rr.get("client_id")==cid):
                    continue

                text = _feedback_prompt(title)
                kb = _kb_feedback(eid, cid)
                try:
                    await _FB_BOT.send_message(int(chat_id), text, reply_markup=kb)
                    _delivery_log("feedback_ask_sent", cid, eid, "")
                    await asyncio.sleep(0.04)
                except Exception as e:
                    _delivery_log("fail", cid, eid, f"feedback_ask: {e}")
    except Exception:
        pass

def setup_phase4(dp, scheduler=None, bot_instance=None):
    dp.include_router(feedback_router)
    if bot_instance is not None:
        init_phase4(bot_instance)
    if scheduler is not None:
        scheduler.add_job(job_feedback_ask, "interval", minutes=5, id="feedback_ask", replace_existing=True)

# ============================== Phase 5: Polish (/help, quiet hours opt-in) ===

phase5_router = Router(name="phase5")

# прості шаблони повідомлень із Messages з fallback
def _messages_get(key: str, lang: str="uk") -> Optional[str]:
    try:
        ws = gs().worksheet(SHEET_MESSAGES)
        for r in _read_all(ws):
            if r.get("key")==key and (r.get("lang") or "uk")==lang:
                return r.get("text") or ""
    except Exception:
        pass
    return None

_FALLBACKS = {
    "help.body": (
        "👋 Це бот для запрошень на наші онлайн-зустрічі.\n\n"
        "Ви отримуватимете інвайти та нагадування. Кнопки під повідомленням:\n"
        "• ✅ Так, буду — підтвердити участь (ми нагадаємо за 24 год і за 1 год)\n"
        "• 🚫 Не зможу — пропустити цю дату (ми запропонуємо іншу)\n"
        "• 🔔 Нагадати за 24 год — якщо ще не вирішили\n\n"
        "Питання? Напишіть нам у відповідь, ми допоможемо 💬"
    ),
}

def msg_text(key: str, lang="uk", **fmt) -> str:
    txt = _messages_get(key, lang) or _FALLBACKS.get(key, "")
    for k,v in fmt.items():
        txt = txt.replace("{"+k+"}", str(v))
    return txt

@phase5_router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(msg_text("help.body"))

@phase5_router.message(Command("profile"))
async def cmd_profile(message: Message):
    try:
        ws = gs().worksheet(SHEET_CLIENTS)
        me = next((r for r in _read_all(ws) if r.get("tg_user_id")==str(message.from_user.id)), None)
        if not me:
            await message.answer("Ще не бачу ваших даних. Надішліть /start, щоб зареєструватися.")
            return
        txt = (
            "📇 *Ваш профіль:*\n"
            f"• ПІБ: {me.get('full_name') or '—'}\n"
            f"• Телефон: {me.get('phone') or '—'}\n"
            f"• Статус: {me.get('status') or '—'}"
        )
        await message.answer(txt)
    except Exception:
        await message.answer("Не вдалося отримати дані профілю.")

# ============================== Wiring & Run ==================================

def main():
    # Підключаємо роутери
    dp.include_router(onboarding_router)   # /start + онбординг
    dp.include_router(admin_router)        # адмін-панель
    setup_phase3(dp, None, bot)            # Фаза 3 (router + потім scheduler нижче)
    setup_phase4(dp, None, bot)            # Фаза 4 (router + потім scheduler нижче)
    dp.include_router(phase5_router)       # /help, /profile

    # Планувальник
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(job_broadcast_new, "interval", minutes=1, id="broadcast_new", replace_existing=True)
    scheduler.add_job(job_reminder_24h, "interval", minutes=5, id="reminder_24h", replace_existing=True)
    scheduler.add_job(job_reminder_60m, "interval", minutes=1, id="reminder_60m", replace_existing=True)
    scheduler.add_job(job_feedback_ask, "interval", minutes=5, id="feedback_ask", replace_existing=True)
    scheduler.start()

    # Старт поллінгу
    asyncio.run(dp.start_polling(bot))

if __name__ == "__main__":
    main()
