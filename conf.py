# -*- coding: utf-8 -*-
import os
import re
import uuid
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest

import asyncpg
from asyncpg import Pool

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
    ReplyKeyboardMarkup, KeyboardButton,
)

# =============================== CONFIG =======================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", ""))
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
TZ = ZoneInfo(TIMEZONE)

# ========================== DATABASE CONNECTION ================================

db_pool: Optional[Pool] = None

async def init_db():
    """Инициализация пула подключений к базе данных"""
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        command_timeout=60
    )
    return db_pool

async def close_db():
    """Закрытие пула подключений"""
    global db_pool
    if db_pool:
        await db_pool.close()

# =============================== HELPERS =======================================

async def safe_edit_message(message: Message, text: str, reply_markup=None, parse_mode=None):
    """Безопасное редактирование сообщения с обработкой ошибки 'message is not modified'."""
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise

def now_kyiv() -> datetime:
    """Возвращает текущее время в timezone Киева С timezone info (aware)
    PostgreSQL TIMESTAMPTZ корректно обрабатывает aware datetime"""
    from datetime import timezone as tz_module
    utc_now = datetime.now(tz_module.utc)
    # Конвертируем в киевское время и возвращаем aware datetime
    return utc_now.astimezone(TZ)

def iso_dt(dt: Optional[datetime] = None) -> str:
    """Конвертирует datetime в строку в киевском времени"""
    dt = dt or now_kyiv()
    # Если datetime имеет timezone info, конвертируем в киевское время
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ)
    return dt.strftime("%Y-%m-%d %H:%M")

def parse_dt(s: str) -> Optional[datetime]:
    """Парсит строку в aware datetime (с timezone info для Киева)"""
    try:
        naive_dt = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M")
        # Корректно локализуем в киевскую зону (учитывает DST)
        # Используем конструктор datetime с tzinfo напрямую
        return datetime(
            naive_dt.year, naive_dt.month, naive_dt.day,
            naive_dt.hour, naive_dt.minute, naive_dt.second,
            naive_dt.microsecond, tzinfo=TZ
        )
    except Exception:
        return None

def fmt_date(dt: datetime) -> str:
    """Форматирует дату в киевском времени"""
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ)
    return dt.strftime("%d.%m.%Y")

def fmt_time(dt: datetime) -> str:
    """Форматирует время в киевском времени"""
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ)
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
        return int(str(v).strip()) if v is not None else default
    except Exception:
        return default

# =============================== DATABASE LAYER ==================================

async def messages_get(key: str, lang: str = "uk") -> str:
    """Получение сообщения из БД по ключу"""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT text FROM messages WHERE key = $1 AND lang = $2",
                key, lang
            )
            if row:
                return str(row['text']).replace("\\n", "\n")
    except Exception:
        pass

    FALLBACKS = {
        "invite.title": "Запрошення на конференцію: {title}",
        "invite.body": "Шановний(-а) {name}!\n\nЗапрошуємо Вас на конференцію: {title}\n🗓 Дата: {date}\n⏰ Час: {time} (за київським часом)\nℹ️ {description}\n\nБудь ласка, підтвердіть Вашу участь за допомогою кнопок нижче.",
        "reminder.60m": "⏰ Нагадування: через 1 годину почнеться конференція {title}.\n🔗 Посилання для підключення: {link}",
        "feedback.ask": "Дякуємо за участь у конференції «{title}»!\n\nБудь ласка, оцініть захід за шкалою від 1 до 5 зірок.\nВи також можете залишити коментар.",
        "reminder.24h": "🔔 Нагадування: завтра о {time} відбудеться конференція {title}.\n🔗 Посилання для підключення: {link}",
        "update.notice": "🛠 Інформація про зміни\n\nУ конференції «{title}» відбулися зміни:\n{what}\n\nДякуємо за розуміння!",
        "cancel.notice": "❌ Інформація про скасування\n\nКонференцію «{title}» скасовано.\nМи повідомимо Вас про нову дату найближчим часом.",
        "help.body": "Вітаємо!\n\nЦей бот призначений для надсилання запрошень на наші онлайн-конференції.\n\nВи отримуватимете запрошення та нагадування про заходи.\n\nКнопки під повідомленням:\n• ✅ Так, буду — підтвердити участь (Ви отримаєте нагадування за 24 години та за 1 годину до початку)\n• 🚫 Не зможу — повідомити про відсутність (Ви зможете обрати альтернативну дату)\n• 🔔 Нагадати за 24 год — якщо Ви ще не визначилися",
    }
    return FALLBACKS.get(key, "")

async def log_action(action: str, client_id: Optional[int] = None,
               event_id: Optional[int] = None, details: str = "") -> None:
    """Запись действия в лог"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO delivery_log (ts, client_id, event_id, action, details)
                   VALUES ($1, $2, $3, $4, $5)""",
                now_kyiv(), client_id, event_id, action, details
            )
    except Exception as e:
        print(f"Error logging action: {e}")

async def has_log(action: str, client_id: int, event_id: int) -> bool:
    """Проверка наличия записи в логе"""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT 1 FROM delivery_log
                   WHERE action = $1 AND client_id = $2 AND event_id = $3
                   LIMIT 1""",
                action, client_id, event_id
            )
            return row is not None
    except Exception:
        return False

async def get_eventtypes_active() -> List[Dict[str, Any]]:
    """Получение активных типов событий"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM event_types WHERE active = TRUE"
        )
        return [dict(row) for row in rows]

async def get_eventtype_by_code(type_code: int) -> Optional[Dict[str, Any]]:
    """Получение типа события по коду"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM event_types WHERE type_code = $1 AND active = TRUE",
            type_code
        )
        return dict(row) if row else None

def client_id_for_tg(tg_user_id: int) -> str:
    """Генерация client_id (для обратной совместимости, теперь используем INT)"""
    return f"cl_{tg_user_id}"

async def get_client_by_tg(tg_user_id: int) -> Optional[Dict[str, Any]]:
    """Получение клиента по Telegram ID"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM clients WHERE tg_user_id = $1",
            tg_user_id
        )
        return dict(row) if row else None

async def upsert_client(tg_user_id: int, full_name: str, phone: str, status: str = "active") -> Dict[str, Any]:
    """Создание или обновление клиента"""
    now = now_kyiv()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO clients (tg_user_id, phone, full_name, status, created_at, last_seen_at)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (tg_user_id)
               DO UPDATE SET
                   phone = EXCLUDED.phone,
                   full_name = EXCLUDED.full_name,
                   status = EXCLUDED.status,
                   last_seen_at = EXCLUDED.last_seen_at
               RETURNING *""",
            tg_user_id, phone, full_name, status, now, now
        )
        client = dict(row)
        await log_action("client_registered", client_id=client['client_id'], details=f"tg={tg_user_id}")
        return client

async def touch_client_seen(tg_user_id: int) -> None:
    """Обновление времени последнего визита"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE clients SET last_seen_at = $1 WHERE tg_user_id = $2",
            now_kyiv(), tg_user_id
        )

async def list_active_clients() -> List[Dict[str, Any]]:
    """Получение списка активных клиентов"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM clients WHERE status = 'active'"
        )
        return [dict(row) for row in rows]

async def create_event(type_code: int, title: str, description: str, start_at: str,
                 duration_min: int, link: str, created_by: int) -> Dict[str, Any]:
    """Создание события"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO events (type, title, description, start_at, duration_min, link, created_by, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               RETURNING *""",
            type_code, title, description, parse_dt(start_at), duration_min, link, created_by, now_kyiv()
        )
        event = dict(row)
        event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
        await log_action("event_created", event_id=event['event_id'], details=f"type={type_code}")
        return event

async def get_all_events() -> List[Dict[str, Any]]:
    """Получение всех событий"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM events ORDER BY start_at")
        result = []
        for row in rows:
            event = dict(row)
            event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
            result.append(event)
        return result

async def get_event_by_id(event_id: int) -> Optional[Dict[str, Any]]:
    """Получение события по ID"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM events WHERE event_id = $1",
            event_id
        )
        if row:
            event = dict(row)
            event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
            return event
        return None

async def update_event_field(event_id: int, field: str, value: Any) -> None:
    """Обновление поля события"""
    # Защита от SQL injection - используем белый список полей
    allowed_fields = {'title', 'description', 'start_at', 'duration_min', 'link'}
    if field not in allowed_fields:
        return

    async with db_pool.acquire() as conn:
        if field == 'start_at':
            value = parse_dt(value) if isinstance(value, str) else value

        query = f"UPDATE events SET {field} = $1 WHERE event_id = $2"
        await conn.execute(query, value, event_id)
        await log_action("event_updated", event_id=event_id, details=f"{field}={value}")

async def delete_event(event_id: int) -> None:
    """Удаление события"""
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM events WHERE event_id = $1", event_id)
        await log_action("event_canceled", event_id=event_id, details="deleted")

def event_start_dt(event: Dict[str, Any]) -> Optional[datetime]:
    """Получение datetime начала события (aware datetime в киевском времени)"""
    start_at = event.get("start_at")
    if isinstance(start_at, datetime):
        # Если есть timezone info — конвертируем в киевское время
        if start_at.tzinfo:
            return start_at.astimezone(TZ)
        else:
            # Корректная локализация naive datetime (предполагаем, что это уже киевское время)
            return datetime(
                start_at.year, start_at.month, start_at.day,
                start_at.hour, start_at.minute, start_at.second,
                start_at.microsecond, tzinfo=TZ
            )
    if isinstance(start_at, str):
        return parse_dt(start_at)
    return None

async def list_future_events_sorted() -> List[Dict[str, Any]]:
    """Получение будущих событий, отсортированных по дате"""
    now = now_kyiv()
    one_day_ago = now - timedelta(days=1)
    async with db_pool.acquire() as conn:
        # Убираем ::timestamp cast - asyncpg корректно обрабатывает aware datetime для TIMESTAMPTZ
        rows = await conn.fetch(
            "SELECT * FROM events WHERE start_at >= $1 ORDER BY start_at",
            one_day_ago
        )
        result = []
        for row in rows:
            event = dict(row)
            event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
            result.append(event)
        return result

async def list_alternative_events_same_type(type_code: int, exclude_event_id: int) -> List[Dict[str, Any]]:
    """Получение альтернативных событий того же типа"""
    now = now_kyiv()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM events
               WHERE type = $1 AND event_id != $2 AND start_at >= $3
               ORDER BY start_at""",
            type_code, exclude_event_id, now
        )
        result = []
        for row in rows:
            event = dict(row)
            event['start_at'] = iso_dt(event['start_at']) if event.get('start_at') else ""
            result.append(event)
        return result

async def mark_attendance(event_id: int, client_id: int, attended: bool = True) -> None:
    """Отметка посещения"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO attendance (event_id, client_id, attended, marked_at)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (event_id, client_id)
               DO UPDATE SET attended = EXCLUDED.attended, marked_at = EXCLUDED.marked_at""",
            event_id, client_id, attended, now_kyiv()
        )
        await log_action("attendance_marked", client_id=client_id, event_id=event_id, details=f"attended={attended}")

async def attendance_clear_for_event(event_id: int, mode: str = "zero") -> int:
    """Очистка записей о посещении для события"""
    async with db_pool.acquire() as conn:
        if mode == "delete":
            result = await conn.execute("DELETE FROM attendance WHERE event_id = $1", event_id)
        else:
            result = await conn.execute(
                "UPDATE attendance SET attended = FALSE, marked_at = $1 WHERE event_id = $2",
                now_kyiv(), event_id
            )

        touched = int(result.split()[-1]) if result else 0
        await log_action("attendance_cleared_on_cancel", event_id=event_id, details=f"mode={mode}; rows={touched}")
        return touched

async def client_has_attended_type(client_id: int, type_code: int) -> bool:
    """Проверка, посещал ли клиент событие данного типа"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT 1 FROM attendance a
               JOIN events e ON a.event_id = e.event_id
               WHERE a.client_id = $1 AND e.type = $2 AND a.attended = TRUE
               LIMIT 1""",
            client_id, type_code
        )
        return row is not None

async def rsvp_upsert(event_id: int, client_id: int, rsvp: Optional[str] = None,
                remind_24h: Optional[bool] = None,
                reminded_24h: Optional[bool] = None,
                reminded_60m: Optional[bool] = None) -> None:
    """Создание или обновление RSVP"""
    async with db_pool.acquire() as conn:
        # Сначала получаем текущие значения, если запись существует
        current = await conn.fetchrow(
            "SELECT * FROM rsvp WHERE event_id = $1 AND client_id = $2",
            event_id, client_id
        )

        # Используем текущие значения, если новые не предоставлены
        if current:
            rsvp_val = rsvp if rsvp is not None else current['rsvp']
            remind_24h_val = remind_24h if remind_24h is not None else current['remind_24h']
            reminded_24h_val = reminded_24h if reminded_24h is not None else current['reminded_24h']
            reminded_60m_val = reminded_60m if reminded_60m is not None else current['reminded_60m']
        else:
            rsvp_val = rsvp or ""
            remind_24h_val = remind_24h or False
            reminded_24h_val = reminded_24h or False
            reminded_60m_val = reminded_60m or False

        await conn.execute(
            """INSERT INTO rsvp (event_id, client_id, rsvp, remind_24h, reminded_24h, reminded_60m, rsvp_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (event_id, client_id)
               DO UPDATE SET
                   rsvp = EXCLUDED.rsvp,
                   remind_24h = EXCLUDED.remind_24h,
                   reminded_24h = EXCLUDED.reminded_24h,
                   reminded_60m = EXCLUDED.reminded_60m,
                   rsvp_at = EXCLUDED.rsvp_at""",
            event_id, client_id, rsvp_val, remind_24h_val, reminded_24h_val, reminded_60m_val, now_kyiv()
        )

async def rsvp_get_for_event(event_id: int) -> List[Dict[str, Any]]:
    """Получение RSVP для события"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM rsvp WHERE event_id = $1",
            event_id
        )
        return [dict(row) for row in rows]

async def rsvp_get_for_client(client_id: int) -> List[Dict[str, Any]]:
    """Получение RSVP для клиента"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM rsvp WHERE client_id = $1",
            client_id
        )
        return [dict(row) for row in rows]

async def client_has_active_invite_for_type(client_id: int, type_code: int) -> bool:
    """Проверка наличия активного приглашения для типа события"""
    now = now_kyiv()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT 1 FROM rsvp r
               JOIN events e ON r.event_id = e.event_id
               WHERE r.client_id = $1
                 AND e.type = $2
                 AND e.start_at >= $3
                 AND (r.rsvp = '' OR r.rsvp = 'going')
               LIMIT 1""",
            client_id, type_code, now
        )
        return row is not None

async def is_earliest_upcoming_event_of_type(event: Dict[str, Any]) -> bool:
    """Проверка, является ли событие самым ранним предстоящим событием данного типа"""
    now = now_kyiv()
    event_type = event.get('type')
    dt_this = event_start_dt(event)

    if not dt_this:
        return False

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT event_id FROM events
               WHERE type = $1 AND start_at >= $2
               ORDER BY start_at
               LIMIT 1""",
            event_type, now
        )
        return row and row['event_id'] == event.get('event_id')

async def feedback_get(event_id: int, client_id: int) -> Optional[Dict[str, Any]]:
    """Получение отзыва"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM feedback WHERE event_id = $1 AND client_id = $2",
            event_id, client_id
        )
        return dict(row) if row else None

async def feedback_upsert(event_id: int, client_id: int, stars: Optional[int] = None, comment: Optional[str] = None) -> Dict[str, Any]:
    """Создание или обновление отзыва"""
    async with db_pool.acquire() as conn:
        current = await conn.fetchrow(
            "SELECT * FROM feedback WHERE event_id = $1 AND client_id = $2",
            event_id, client_id
        )

        if current:
            stars_val = stars if stars is not None else current['stars']
            comment_val = comment if comment is not None else current['comment']
            owner_val = current['owner']
        else:
            stars_val = stars or 0
            comment_val = comment or ""
            owner_val = ""

        row = await conn.fetchrow(
            """INSERT INTO feedback (event_id, client_id, stars, comment, owner, created_at)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (event_id, client_id)
               DO UPDATE SET
                   stars = EXCLUDED.stars,
                   comment = EXCLUDED.comment
               RETURNING *""",
            event_id, client_id, stars_val, comment_val, owner_val, now_kyiv()
        )
        return dict(row)

async def feedback_assign_owner(event_id: int, client_id: int, owner: str) -> None:
    """Назначение ответственного за отзыв"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE feedback SET owner = $1 WHERE event_id = $2 AND client_id = $3",
            owner, event_id, client_id
        )

async def try_get_tg_from_client_id(client_id: int) -> Optional[int]:
    """Получение Telegram ID по client_id"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT tg_user_id FROM clients WHERE client_id = $1",
            client_id
        )
        return row['tg_user_id'] if row else None

async def get_event_statistics(event_id: int) -> Dict[str, Any]:
    """Получение статистики по событию"""
    async with db_pool.acquire() as conn:
        # Количество отправленных приглашений
        invitations_sent = await conn.fetchval(
            "SELECT COUNT(*) FROM delivery_log WHERE action = 'invite_sent' AND event_id = $1",
            event_id
        )

        # Подтвержденные участники
        confirmed = await conn.fetch(
            """SELECT c.client_id, c.full_name, c.phone
               FROM rsvp r
               JOIN clients c ON r.client_id = c.client_id
               WHERE r.event_id = $1 AND r.rsvp = 'going'""",
            event_id
        )

        confirmed_clients = [
            {
                "client_id": row['client_id'],
                "full_name": row['full_name'] or "—",
                "phone": row['phone'] or "—"
            }
            for row in confirmed
        ]

        return {
            "invitations_sent": invitations_sent or 0,
            "confirmed_count": len(confirmed_clients),
            "confirmed_clients": confirmed_clients
        }

async def build_types_overview_text(cli: Dict[str, Any]) -> str:
    """Построение обзорного текста по типам событий"""
    text = (
        "✅ Ви успішно зареєстровані для отримання запрошень на конференції.\n"
        "Ви отримуватимете запрошення на найближчі заходи.\n\n"
        "Доступні типи конференцій:\n"
    )
    rows = await get_eventtypes_active()
    if not rows:
        return text + "На даний момент немає активних типів конференцій."

    lines = []
    for rt in rows:
        tcode = rt.get("type_code")
        title = str(rt.get("title"))
        attended = await client_has_attended_type(cli['client_id'], tcode)
        flag = "✅ Відвідано" if attended else "⭕️ Ще не відвідували"
        lines.append(f"• {title} — {flag}")

    return text + "\n".join(lines)

# ============================== KEYBOARDS ======================================

def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати конференцію", callback_data="admin:add")],
        [InlineKeyboardButton(text="📋 Список конференцій", callback_data="admin:list:0")],
    ])

def kb_rsvp(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Так, буду", callback_data=f"rsvp:{event_id}:going"),
            InlineKeyboardButton(text="🚫 Не зможу", callback_data=f"rsvp:{event_id}:declined"),
        ]
    ])

def kb_event_actions(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="ℹ️ Інфо", callback_data=f"admin:info:{event_id}")],
        [InlineKeyboardButton(text="✏️ Змінити", callback_data=f"admin:edit:{event_id}")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data=f"admin:cancel:{event_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:list:0")],
    ])

def kb_edit_event_menu(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Назва", callback_data=f"admin:edit:{event_id}:field:title")],
        [InlineKeyboardButton(text="✏️ Опис", callback_data=f"admin:edit:{event_id}:field:description")],
        [InlineKeyboardButton(text="🗓 Дата/час", callback_data=f"admin:edit:{event_id}:field:start_at")],
        [InlineKeyboardButton(text="⏱ Тривалість (хв)", callback_data=f"admin:edit:{event_id}:field:duration_min")],
        [InlineKeyboardButton(text="🔗 Посилання", callback_data=f"admin:edit:{event_id}:field:link")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:list:0")],
    ])

def kb_cancel_confirm(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Так, скасувати", callback_data=f"admin:cancel:{event_id}:yes")],
        [InlineKeyboardButton(text="⬅️ Ні, назад", callback_data=f"admin:edit:{event_id}")],
    ])

def kb_claim_feedback(event_id: int, client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛠 Беру в роботу", callback_data=f"claim:{event_id}:{client_id}")],
    ])

def kb_event_info(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Оновити", callback_data=f"admin:info:{event_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin:event:{event_id}")],
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
    await touch_client_seen(m.from_user.id)
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
    cli = await get_client_by_tg(m.from_user.id)
    if not cli or not cli.get("full_name") or not cli.get("phone"):
        await state.set_state(RegisterSG.wait_name)
        await m.answer("Доброго дня! Будь ласка, вкажіть Ваше прізвище, ім'я та по батькові.")
        return

    await send_welcome_and_types_list(m, cli)

async def send_welcome_and_types_list(m: Message, cli: Dict[str, Any]):
    text = await build_types_overview_text(cli)
    await m.answer(text, reply_markup=kb_client_main())

@dp.message(Command("help"))
async def cmd_help(m: Message):
    text = await messages_get("help.body")
    await m.answer(text)

# ---------- Реєстрація клієнта ----------

@dp.message(RegisterSG.wait_name)
async def reg_wait_name(m: Message, state: FSMContext):
    full_name = (m.text or "").strip()
    if len(full_name) < 3:
        await m.answer("Будь ласка, введіть повне прізвище, ім'я та по батькові.")
        return
    await state.update_data(full_name=full_name)
    await state.set_state(RegisterSG.wait_phone)
    await m.answer("Будь ласка, вкажіть номер телефону у форматі 380XXXXXXXXX:")

@dp.message(RegisterSG.wait_phone)
async def reg_wait_phone(m: Message, state: FSMContext):
    phone = normalize_phone(m.text or "")
    if not phone:
        await m.answer("Невірний формат номера. Приклад: 380671234567. Будь ласка, спробуйте ще раз:")
        return
    data = await state.get_data()
    cli = await upsert_client(m.from_user.id, data["full_name"], phone)
    await state.clear()
    await send_welcome_and_types_list(m, cli)

@dp.message(F.text == "📋 Мої конференції")
async def show_my_conferences(m: Message):
    cli = await get_client_by_tg(m.from_user.id)
    if not cli:
        await m.answer("Будь ласка, зареєструйтеся за допомогою команди /start.", reply_markup=kb_client_main())
        return
    text = await build_types_overview_text(cli)
    await m.answer(text, reply_markup=kb_client_main())

# ---------- Адмін меню / додати / список / редагування ----------

@dp.callback_query(F.data == "admin:add")
async def admin_add(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    types = await get_eventtypes_active()
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
    et = await get_eventtype_by_code(type_code)
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
    created = await create_event(
        type_code=int(data["type_code"]),
        title=data["title"],
        description=data["description"],
        start_at=data["start_at"],
        duration_min=int(data["duration_min"]),
        link=link,
        created_by=m.from_user.id,
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
    events = await list_future_events_sorted()
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
    event_id = int(parts[-1])
    e = await get_event_by_id(event_id)
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

@dp.callback_query(F.data.startswith("admin:info:"))
async def admin_info(q: CallbackQuery):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return
    event_id = int(parts[-1])
    e = await get_event_by_id(event_id)
    if not e:
        await q.message.edit_text("Подію не знайдено.", reply_markup=kb_admin_main())
        await q.answer()
        return

    stats = await get_event_statistics(event_id)

    text = (
        f"ℹ️ Статистика події\n\n"
        f"📌 Подія: {e['title']}\n"
        f"🗓 Початок: {e['start_at']}\n\n"
        f"📊 Статистика:\n"
        f"• Відправлено запрошень: {stats['invitations_sent']}\n"
        f"• Підтвердили участь: {stats['confirmed_count']}\n"
    )

    if stats['confirmed_clients']:
        text += f"\n✅ Підтвердили участь:\n"
        for i, cli in enumerate(stats['confirmed_clients'], 1):
            text += f"{i}. {cli['full_name']} ({cli['phone']})\n"
    else:
        text += f"\n⚠️ Ще ніхто не підтвердив участь\n"

    await q.message.edit_text(text, reply_markup=kb_event_info(event_id))
    await q.answer()

@dp.callback_query(F.data.startswith("admin:edit:"))
async def admin_edit(q: CallbackQuery, state: FSMContext):
    if q.from_user.id not in ADMINS:
        await q.answer()
        return
    parts = q.data.split(":")
    if len(parts) == 3:
        event_id = int(parts[-1])
        await q.message.edit_text("Оберіть поле для редагування:", reply_markup=kb_edit_event_menu(event_id))
        await q.answer()
        return
    if len(parts) == 5 and parts[3] == "field":
        event_id = int(parts[2])
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
async def admin_edit_field_value(m: Message, state: FSMContext):
    data = await state.get_data()
    event_id = data.get("event_id")
    field = data.get("field")

    if field in {"title", "description", "link"}:
        val = (m.text or "").strip()
        await update_event_field(event_id, field, val)
        await m.answer("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
        await state.clear()

        if field == "title":
            await notify_event_update(event_id, f"Оновлено назву: {val}")
        elif field == "description":
            await notify_event_update(event_id, "Оновлено опис.")
        elif field == "link":
            await notify_event_update(event_id, f"Оновлено посилання: {val}")
        return

    if field == "start_at":
        dt = parse_dt(m.text or "")
        if not dt:
            await m.answer("Невірний формат. Приклад: 2025-10-05 15:00. Спробуйте ще раз:")
            return
        await update_event_field(event_id, "start_at", iso_dt(dt))
        await m.answer("✅ Зміни збережено.", reply_markup=kb_edit_event_menu(event_id))
        await state.clear()
        await notify_event_update(event_id, f"Змінено дату/час: {fmt_date(dt)} о {fmt_time(dt)} (Київ)")
        return

    if field == "duration_min":
        try:
            dur = int((m.text or "").strip())
            if dur <= 0:
                raise ValueError()
        except Exception:
            await m.answer("Введіть додатне ціле число. Спробуйте ще раз:")
            return
        await update_event_field(event_id, "duration_min", dur)
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
        event_id = int(parts[-1])
        await q.message.edit_text("Підтвердити скасування події?", reply_markup=kb_cancel_confirm(event_id))
        await q.answer()
        return
    if len(parts) == 4 and parts[-1] == "yes":
        event_id = int(parts[2])
        await notify_event_cancel(event_id)
        await attendance_clear_for_event(event_id, mode="zero")
        await delete_event(event_id)
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
    _, event_id_str, action = parts
    event_id = int(event_id_str)

    cli = await get_client_by_tg(q.from_user.id)
    if not cli:
        await safe_edit_message(q.message, "Будь ласка, зареєструйтеся за допомогою команди /start.")
        await q.answer()
        return

    client_id = cli["client_id"]
    event = await get_event_by_id(event_id)
    if not event:
        await safe_edit_message(q.message, "Конференцію не знайдено.")
        await q.answer()
        return

    # Проверяем, не началась ли уже конференция
    dt = event_start_dt(event)
    if dt and dt <= now_kyiv():
        await safe_edit_message(
            q.message,
            q.message.text + "\n\n⚠️ Конференція вже почалася. Підтвердження участі недоступне."
        )
        await q.answer("Конференція вже почалася")
        return

    if action == "going":
        await rsvp_upsert(event_id, client_id, rsvp="going")
        await mark_attendance(event_id, client_id, True)
        await log_action("rsvp_yes", client_id=client_id, event_id=event_id, details="")

        # Сохраняем исходное сообщение и добавляем подтверждение
        original_text = q.message.text or ""
        new_text = original_text + "\n\n✅ Дякуємо! Вашу участь підтверджено."
        await safe_edit_message(q.message, new_text)
        await q.answer()
        return

    if action == "declined":
        await rsvp_upsert(event_id, client_id, rsvp="declined")
        await log_action("rsvp_no", client_id=client_id, event_id=event_id, details="")

        alt = await list_alternative_events_same_type(a2i(event.get("type")), event_id)
        if not alt:
            # Сохраняем исходное сообщение и добавляем ответ
            original_text = q.message.text or ""
            new_text = original_text + "\n\n❌ Дякуємо за відповідь. Ми надішлемо Вам запрошення на іншу дату."
            await safe_edit_message(q.message, new_text)
        else:
            rows = []
            for a in alt[:8]:
                dt = event_start_dt(a)
                when = f"{fmt_date(dt)} о {fmt_time(dt)}" if dt else a.get('start_at', '')
                rows.append([InlineKeyboardButton(text=when, callback_data=f"alt:pick:{a['event_id']}")])
            rows.append([InlineKeyboardButton(text="❌ Закрити", callback_data="noop")])

            title_for_info = event.get("title", "конференція")
            # Сохраняем исходное сообщение
            original_text = q.message.text or ""
            new_text = original_text + f"\n\nАльтернативні дати проведення конференції «{title_for_info}»:"
            await safe_edit_message(
                q.message,
                new_text,
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
    _, event_id_str, client_id_str = parts
    event_id = int(event_id_str)
    client_id = int(client_id_str)

    owner = f"@{q.from_user.username}" if q.from_user and q.from_user.username else f"id:{q.from_user.id}"
    await feedback_assign_owner(event_id, client_id, owner)
    await log_action("complaint_taken", client_id=client_id, event_id=event_id, details=f"owner={owner}")

    # Сохраняем исходное сообщение и добавляем информацию о взятии в работу
    original_text = q.message.text or ""
    new_text = original_text + f"\n\n✅ Взято в роботу ({owner})"
    await q.message.edit_text(new_text)
    await q.answer()

@dp.callback_query(F.data.startswith("alt:pick:"))
async def alt_pick(q: CallbackQuery):
    parts = q.data.split(":")
    if len(parts) != 3:
        await q.answer()
        return

    alt_event_id = int(parts[2])
    cli = await get_client_by_tg(q.from_user.id)
    if not cli:
        await q.message.edit_text("Будь ласка, зареєструйтеся за допомогою команди /start.")
        await q.answer()
        return

    client_id = cli["client_id"]
    alt_event = await get_event_by_id(alt_event_id)
    if not alt_event:
        await q.message.edit_text("На жаль, обрану дату не знайдено.")
        await q.answer()
        return

    await rsvp_upsert(alt_event_id, client_id, rsvp="going")
    await mark_attendance(alt_event_id, client_id, True)
    await log_action("rsvp_alt_yes", client_id=client_id, event_id=alt_event_id, details="picked_alternative")

    dt = event_start_dt(alt_event)
    when = f"{fmt_date(dt)} о {fmt_time(dt)}" if dt else alt_event.get("start_at", "")
    await q.message.edit_text(
        f"✅ Дякуємо! Вашу участь підтверджено.\n\n"
        f"Конференція: {alt_event.get('title','')}\n"
        f"🗓 Дата та час: {when}\n"
        f"🔗 Посилання: {alt_event.get('link','')}"
    )
    await q.answer()

@dp.callback_query(F.data == "noop")
async def noop(q: CallbackQuery):
    await q.answer()

# ---------- FEEDBACK (зірки + коментар) ----------

async def route_low_feedback(event_id: int, client_id: int, stars: int, comment: str):
    cli_tg = await try_get_tg_from_client_id(client_id)
    cli_row = await get_client_by_tg(cli_tg) if cli_tg else None
    full_name = cli_row["full_name"] if cli_row else str(client_id)
    phone = cli_row["phone"] if cli_row else "—"
    event = await get_event_by_id(event_id) or {}

    text = (
        f"⚠️ Низька оцінка події\n"
        f"• Подія: {event.get('title','')}\n"
        f"• Клієнт: {full_name} (tg_id={cli_tg})\n"
        f"• Телефон: {phone}\n"
        f"• Оцінка: {stars}\n"
        f"• Коментар: {comment or '—'}"
    )
    kb = kb_claim_feedback(event_id, client_id)

    try:
        await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text, reply_markup=kb, parse_mode=None)
        await log_action("feedback_low_notified", client_id=client_id, event_id=event_id, details=f"support_chat:{SUPPORT_CHAT_ID}")
        return
    except TelegramRetryAfter as ex:
        await asyncio.sleep(ex.retry_after + 1)
        try:
            await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text, reply_markup=kb, parse_mode=None)
            await log_action("feedback_low_notified", client_id=client_id, event_id=event_id, details=f"support_chat:{SUPPORT_CHAT_ID}/after_retry")
            return
        except Exception as ex2:
            await log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"retry_fail:{type(ex2).__name__}")
    except (TelegramForbiddenError, TelegramBadRequest) as ex:
        await log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"{type(ex).__name__}:{ex}")
    except Exception as ex:
        await log_action("feedback_low_notify_fail", client_id=client_id, event_id=event_id, details=f"unknown:{type(ex).__name__}")

    if ADMINS:
        for admin_id in list(ADMINS):
            try:
                await bot.send_message(chat_id=admin_id, text="(фолбэк) " + text, reply_markup=kb, parse_mode=None)
                await log_action("feedback_low_notified_admin_dm", client_id=client_id, event_id=event_id, details=f"to_admin:{admin_id}")
            except Exception as ex:
                await log_action("feedback_low_admin_dm_fail", client_id=client_id, event_id=event_id, details=f"{admin_id}:{type(ex).__name__}")

async def route_low_feedback_comment_update(event_id: int, client_id: int, comment: str):
    cli_tg = await try_get_tg_from_client_id(client_id)
    event = await get_event_by_id(event_id) or {}
    text = (
        f"📝 Доповнення до скарги\n"
        f"• Подія: {event.get('title','')}\n"
        f"• Клієнт: {client_id} (tg_id={cli_tg})\n"
        f"• Коментар: {comment or '—'}"
    )
    try:
        await bot.send_message(chat_id=SUPPORT_CHAT_ID, text=text, parse_mode=None)
        await log_action("low_fb_comment_update_sent", client_id=client_id, event_id=event_id, details="")
    except Exception as e:
        await log_action("support_send_error", client_id=client_id, event_id=event_id, details=f"{e!r}")

@dp.callback_query(F.data.startswith("fb:"))
async def fb_callbacks(q: CallbackQuery, state: FSMContext):
    data = q.data or ""

    # Выбор звёзд: fb:<event_id>:<client_id>:<stars>
    if data.startswith("fb:") and data.count(":") == 3 and not data.startswith("fb:comment:") and not data.startswith("fb:skip:"):
        _, event_id_str, client_id_str, stars_str = data.split(":")
        event_id = int(event_id_str)
        client_id = int(client_id_str)
        stars = int(stars_str)

        await feedback_upsert(event_id, client_id, stars=stars)

        if stars < 4:
            try:
                await route_low_feedback(event_id, client_id, stars, "")
                await log_action("low_fb_alert_sent", client_id=client_id, event_id=event_id, details=f"stars={stars}")
            except Exception as e:
                await log_action("support_send_error", client_id=client_id, event_id=event_id, details=f"{e!r}")

        prompt = f"Дякуємо! Вашу оцінку {stars}⭐️ збережено.\nБажаєте додати коментар?"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Написати коментар", callback_data=f"fb:comment:{event_id}:{client_id}")],
            [InlineKeyboardButton(text="⏭ Пропустити", callback_data=f"fb:skip:{event_id}:{client_id}")]
        ])
        await q.message.edit_text(prompt, reply_markup=kb)
        await q.answer()
        return

    if data.startswith("fb:skip:"):
        await q.message.edit_text("Дякуємо за Ваш відгук! ✅")
        await q.answer()
        return

    if data.startswith("fb:comment:"):
        _, _, event_id_str, client_id_str = data.split(":")
        event_id = int(event_id_str)
        client_id = int(client_id_str)

        tg_id = await try_get_tg_from_client_id(client_id)
        if not tg_id or not q.from_user or q.from_user.id != int(tg_id):
            await q.message.edit_text("Будь ласка, введіть коментар у приватному діалозі з ботом.")
            await q.answer()
            return
        await state.set_state(FeedbackSG.wait_comment)
        await state.update_data(event_id=event_id, client_id=client_id)
        await q.message.edit_text("Будь ласка, надішліть Ваш коментар одним повідомленням.\nДля пропуску надішліть символ «-».")
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

    saved = await feedback_upsert(event_id, client_id, comment=comment)
    stars = a2i(saved.get("stars"), 0)

    await m.answer("Дякуємо! Ваш відгук збережено. ✅")
    await state.clear()

    if stars and stars < 4 and comment:
        await route_low_feedback_comment_update(event_id, client_id, comment)

# =============================== NOTIFY HELPERS ================================

async def notify_event_update(event_id: int, what: str):
    event = await get_event_by_id(event_id)
    if not event:
        return
    templ = await messages_get("update.notice")
    body = templ.format(title=event["title"], what=what)
    for r in await rsvp_get_for_event(event_id):
        if str(r.get("rsvp")) == "going":
            tg_id = await try_get_tg_from_client_id(r.get("client_id"))
            if tg_id:
                try:
                    await bot.send_message(chat_id=int(tg_id), text=body)
                except Exception:
                    pass

async def notify_event_cancel(event_id: int):
    event = await get_event_by_id(event_id)
    if not event:
        return
    templ = await messages_get("cancel.notice")
    body = templ.format(title=event["title"])
    for r in await rsvp_get_for_event(event_id):
        if str(r.get("rsvp")) == "going":
            tg_id = await try_get_tg_from_client_id(r.get("client_id"))
            if tg_id:
                try:
                    await bot.send_message(chat_id=int(tg_id), text=body)
                except Exception:
                    pass

async def send_initial_invites_for_event(event: Dict[str, Any]):
    """Рассылка начальных приглашений на событие"""
    event_id = event.get("event_id")
    dt = event_start_dt(event)
    if not dt:
        await log_action("invite_skip", event_id=event_id, details="No valid datetime")
        return

    if not await is_earliest_upcoming_event_of_type(event):
        await log_action("invite_skip", event_id=event_id, details="Not earliest event of type")
        return

    type_code = event.get("type")
    active_clients = await list_active_clients()

    await log_action("invite_process_start", event_id=event_id, details=f"Processing {len(active_clients)} active clients, type={type_code}")

    sent_count = 0
    skip_reasons = {}

    for cli in active_clients:
        cid = cli.get("client_id")
        tg_id = cli.get("tg_user_id")

        if not cid or not tg_id:
            skip_reasons["no_cid_or_tg"] = skip_reasons.get("no_cid_or_tg", 0) + 1
            await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"no_cid_or_tg")
            continue

        if await client_has_attended_type(cid, type_code):
            skip_reasons["already_attended"] = skip_reasons.get("already_attended", 0) + 1
            await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"already_attended type={type_code}")
            continue

        if await client_has_active_invite_for_type(cid, type_code):
            skip_reasons["has_active_invite"] = skip_reasons.get("has_active_invite", 0) + 1
            await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"has_active_invite type={type_code}")
            continue

        if await has_log("invite_sent", cid, event_id):
            skip_reasons["already_sent"] = skip_reasons.get("already_sent", 0) + 1
            await log_action("invite_skip", client_id=cid, event_id=event_id, details=f"already_sent")
            continue

        body = (await messages_get("invite.body")).format(
            name=cli.get("full_name","Клієнт"),
            title=event["title"],
            date=fmt_date(dt),
            time=fmt_time(dt),
            description=event["description"]
        )

        try:
            title_msg = await messages_get("invite.title")
            await bot.send_message(chat_id=int(tg_id), text=title_msg.format(title=event["title"]))
            await bot.send_message(chat_id=int(tg_id), text=body, reply_markup=kb_rsvp(event_id))
            await rsvp_upsert(event_id, cid, rsvp="")
            await log_action("invite_sent", client_id=cid, event_id=event_id, details="immediate")
            sent_count += 1
        except TelegramRetryAfter as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=f"RetryAfter {e.retry_after}s")
            skip_reasons["telegram_retry_after"] = skip_reasons.get("telegram_retry_after", 0) + 1
        except TelegramForbiddenError:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=f"ForbiddenError: user blocked bot")
            skip_reasons["user_blocked_bot"] = skip_reasons.get("user_blocked_bot", 0) + 1
        except TelegramBadRequest as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=f"BadRequest: {str(e)}")
            skip_reasons["telegram_bad_request"] = skip_reasons.get("telegram_bad_request", 0) + 1
        except Exception as e:
            await log_action("invite_immediate_error", client_id=cid, event_id=event_id, details=f"{type(e).__name__}: {str(e)}")
            skip_reasons["other_error"] = skip_reasons.get("other_error", 0) + 1

    await log_action("invite_process_complete", event_id=event_id, details=f"Sent={sent_count}, Skipped={skip_reasons}")

# =============================== SCHEDULER TICK ================================

async def scheduler_tick():
    try:
        now = now_kyiv()

        # ДЛЯ ТЕСТИРОВАНИЯ: уменьшенные интервалы
        # REM_24H = 2*60      # 2 минуты вместо 24 часов (напоминание за "24ч")
        # REM_60M = 1*60      # 1 минута вместо 1 часа (напоминание за "1ч")
        # FEEDBACK_DELAY = 1*60   # 1 минута после окончания
        # JITTER = 30             # 30 секунд для точности срабатывания

        # ДЛЯ ПРОДАКШЕНА раскомментируй:
        REM_24H = 24*3600
        REM_60M = 60*60
        FEEDBACK_DELAY = 5*60
        JITTER = 60

        for e in await list_future_events_sorted():
            dt = event_start_dt(e)
            if not dt:
                continue

            diff = (dt - now).total_seconds()

            # Напоминание за 24 часа
            if abs(diff - REM_24H) <= JITTER:
                for r in await rsvp_get_for_event(e["event_id"]):
                    cid = r.get("client_id")
                    tg_id = await try_get_tg_from_client_id(cid)
                    if not tg_id:
                        continue
                    if r.get("reminded_24h"):
                        continue
                    if str(r.get("rsvp")) == "going":
                        body = (await messages_get("reminder.24h")).format(
                            title=e["title"], time=fmt_time(dt), link=e["link"]
                        )
                        try:
                            await bot.send_message(chat_id=int(tg_id), text=body)
                            await rsvp_upsert(e["event_id"], cid, reminded_24h=True)
                            await log_action("remind_24h_sent", client_id=cid, event_id=e["event_id"], details="prod_24h")
                        except Exception:
                            pass

            # Напоминание за 60 минут
            if abs(diff - REM_60M) <= JITTER:
                for r in await rsvp_get_for_event(e["event_id"]):
                    cid = r.get("client_id")
                    tg_id = await try_get_tg_from_client_id(cid)
                    if not tg_id:
                        continue
                    if r.get("reminded_60m"):
                        continue
                    if str(r.get("rsvp")) == "going":
                        body = (await messages_get("reminder.60m")).format(title=e["title"], link=e["link"])
                        try:
                            await bot.send_message(chat_id=int(tg_id), text=body)
                            await rsvp_upsert(e["event_id"], cid, reminded_60m=True)
                            await log_action("remind_60m_sent", client_id=cid, event_id=e["event_id"], details="prod_60m")
                        except Exception:
                            pass

            # Фидбэк после окончания
            end_dt = dt + timedelta(minutes=a2i(e.get("duration_min")))
            post_end = (now - end_dt).total_seconds()
            if abs(post_end - FEEDBACK_DELAY) <= JITTER:
                if await has_log("feedback_requested", 0, e["event_id"]):
                    continue

                async with db_pool.acquire() as conn:
                    rows_att = await conn.fetch(
                        "SELECT * FROM attendance WHERE event_id = $1 AND attended = TRUE",
                        e["event_id"]
                    )

                    for r in rows_att:
                        cid = r.get("client_id")
                        tg_id = await try_get_tg_from_client_id(cid)
                        if not tg_id:
                            continue
                        text = (await messages_get("feedback.ask")).format(title=e["title"])
                        kb = InlineKeyboardMarkup(inline_keyboard=[[
                            InlineKeyboardButton(text="⭐️1", callback_data=f"fb:{e['event_id']}:{cid}:1"),
                            InlineKeyboardButton(text="⭐️2", callback_data=f"fb:{e['event_id']}:{cid}:2"),
                            InlineKeyboardButton(text="⭐️3", callback_data=f"fb:{e['event_id']}:{cid}:3"),
                            InlineKeyboardButton(text="⭐️4", callback_data=f"fb:{e['event_id']}:{cid}:4"),
                            InlineKeyboardButton(text="⭐️5", callback_data=f"fb:{e['event_id']}:{cid}:5"),
                        ]])
                        try:
                            await bot.send_message(chat_id=int(tg_id), text=text, reply_markup=kb)
                        except Exception:
                            pass

                await log_action("feedback_requested", event_id=e["event_id"], details=f"delay={FEEDBACK_DELAY}")

    except Exception as e:
        import traceback
        print(f"Error in scheduler_tick: {e}\n{traceback.format_exc()}")

# ================================ STARTUP ======================================

async def on_startup():
    await init_db()
    scheduler.add_job(scheduler_tick, "interval", seconds=60, id="tick", replace_existing=True)
    scheduler.start()

async def on_shutdown():
    await close_db()

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await on_startup()
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
