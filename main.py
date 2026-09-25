import os
import json
import secrets
import time
import asyncio
import random
import re
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, List

import redis.asyncio as redis
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router, html
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, BotCommand, ErrorEvent, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart, Command, StateFilter, CommandObject
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from aiogram.utils.deep_linking import create_start_link, create_startgroup_link  # [web:24]
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application  # [web:1]

from aiogram.utils.deep_linking import decode_payload
from aiogram.enums import ChatType

def is_group_chat(message: Message) -> bool:
    return message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}
    

# =========================================================
# Logging
# =========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("cafebotify-saas")


# =========================================================
# Time
# =========================================================
MSK_TZ = timezone(timedelta(hours=3))

def get_moscow_time() -> datetime:
    return datetime.now(MSK_TZ)


# =========================================================
# Env
# =========================================================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
REDIS_URL = (os.getenv("REDIS_URL") or "").strip()

WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "cafebot123").strip()
PUBLIC_HOST = (os.getenv("PUBLIC_HOST") or os.getenv("RENDER_EXTERNAL_HOSTNAME") or "").strip()
DEMO_PAY_BASE = (os.getenv("DEMO_PAY_BASE") or "").strip()
PORT = int(os.getenv("PORT", "10000"))

WEBHOOK_PATH = f"/{WEBHOOK_SECRET}/webhook"
WEBHOOK_URL = f"https://{PUBLIC_HOST}{WEBHOOK_PATH}"

DEMO_PAY_BASE = (os.getenv("DEMO_PAY_BASE") or "").strip()
DEMO_MODE = bool(int(os.getenv("DEMO_MODE", "0")))


# =========================================================
# Config
# =========================================================
def load_config() -> Dict[str, Any]:
    env_path = (os.getenv("CONFIG_PATH") or "").strip()
    base_dir = Path(__file__).resolve().parent

    candidates: List[Path] = []
    if env_path:
        candidates.append(Path(env_path) if Path(env_path).is_absolute() else base_dir / env_path)
    candidates += [base_dir / "config_330_template.json", base_dir / "config.json"]

    tried = []
    last_err: Optional[Exception] = None

    for p in candidates:
        tried.append(str(p))
        if not p.exists() or not p.is_file():
            continue

        raw = p.read_text(encoding="utf-8", errors="replace")
        if not raw.strip():
            last_err = ValueError(f"Config empty: {p}")
            continue

        try:
            data = json.loads(raw)
        except Exception as e:
            last_err = e
            continue

        if not isinstance(data, dict) or not isinstance(data.get("cafes"), dict):
            last_err = ValueError("config must contain object field 'cafes'")
            continue

        logger.info("CONFIG loaded: %s (cafes=%d)", p, len(data["cafes"]))
        return data

    msg = "Config load failed. Tried: " + ", ".join(tried)
    raise RuntimeError(msg) from last_err


CONFIG = load_config()
CAFES: Dict[str, Dict[str, Any]] = CONFIG["cafes"]
DEFAULT_CAFE_ID: str = str(CONFIG.get("default_cafe_id") or next(iter(CAFES.keys())))
SUPERADMIN_ID: int = int(CONFIG.get("superadmin_id") or 0)


# =========================================================
# Redis keys
# =========================================================
def k_user_cafe(user_id: int) -> str:
    return f"user:{user_id}:cafe_id"

def k_view_mode(user_id: int) -> str:
    # "admin" | "client"
    return f"user:{user_id}:view_mode"

def k_staff_group(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:staff_group_id"

def k_menu(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:menu"

def k_stats_total_orders(cafe_id: str) -> str:
    return f"stats:{cafe_id}:total_orders"

def k_stats_total_revenue(cafe_id: str) -> str:
    return f"stats:{cafe_id}:total_revenue"

def k_stats_drink_cnt(cafe_id: str, drink: str) -> str:
    return f"stats:{cafe_id}:drink:{drink}:cnt"

def k_stats_drink_rev(cafe_id: str, drink: str) -> str:
    return f"stats:{cafe_id}:drink:{drink}:rev"

def k_rate_limit(user_id: int) -> str:
    return f"rate_limit:{user_id}"

def k_last_seen(cafe_id: str, user_id: int) -> str:
    return f"last_seen:{cafe_id}:{user_id}"

def k_last_order(cafe_id: str, user_id: int) -> str:
    return f"last_order:{cafe_id}:{user_id}"

def k_customers_set(cafe_id: str) -> str:
    return f"customers:{cafe_id}:set"

def k_customer_profile(cafe_id: str, user_id: int) -> str:
    return f"customer:{cafe_id}:{user_id}:profile"

def k_customer_drinks(cafe_id: str, user_id: int) -> str:
    return f"customer:{cafe_id}:{user_id}:drinks"

def k_cafe_profile(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:profile"

def redis_text(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")

    return str(value)


async def apply_cafe_profile(
    r: redis.Redis,
    cafe_id: str,
    cafe: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Возвращает копию базового cafe с данными профиля из Redis.
    Если профиль не заполнен, возвращается исходный cafe без изменений.
    """
    result = dict(cafe)

    try:
        profile = await r.hgetall(k_cafe_profile(cafe_id))
    except Exception:
        logger.exception(
            "Не удалось загрузить профиль кафе: cafe_id=%s",
            cafe_id,
        )
        return result

    if not profile:
        return result

    normalized = {
        redis_text(key): redis_text(value).strip()
        for key, value in profile.items()
    }

    if normalized.get("profile_configured") != "1":
        return result

    title = normalized.get("title", "")
    address = normalized.get("address", "")
    work_start = normalized.get("work_start", "")
    work_end = normalized.get("work_end", "")

    if title:
        result["title"] = title

    if address:
        result["address"] = address

    if work_start:
        result["work_start"] = work_start

    if work_end:
        result["work_end"] = work_end

    return result

def k_support_ticket(ticket_id: str) -> str:
    return f"support:ticket:{ticket_id}"

def k_support_open() -> str:
    return "support:open"

def k_support_cafe(cafe_id: str) -> str:
    return f"support:cafe:{cafe_id}"

def k_support_user(user_id: int) -> str:
    return f"support:user:{user_id}"

def k_support_counter() -> str:
    return "support:counter"

def k_support_active(cafe_id: str, user_id: int) -> str:
    return f"support:active:{cafe_id}:{user_id}"

def k_cafe_sub_notify(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:sub_notify"

# После других def k_... 
def k_admin_subscription(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:admin_subscription"

def k_cafe_profile(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:profile"

def redis_text(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")

    return str(value)


async def apply_cafe_profile(
    r: redis.Redis,
    cafe_id: str,
    cafe: Dict[str, Any],
) -> Dict[str, Any]:
    result = dict(cafe)

    try:
        profile = await r.hgetall(k_cafe_profile(cafe_id))
    except Exception:
        logger.exception(
            "Не удалось загрузить профиль кафе: cafe_id=%s",
            cafe_id,
        )
        return result

    if not profile:
        return result

    normalized = {
        redis_text(key): redis_text(value).strip()
        for key, value in profile.items()
    }

    if normalized.get("profile_configured") != "1":
        return result

    title = normalized.get("title", "")
    address = normalized.get("address", "")
    work_start_raw = normalized.get("work_start", "")
    work_end_raw = normalized.get("work_end", "")

    if title:
        result["title"] = title

    if address:
        result["address"] = address

    try:
        work_start = int(work_start_raw)
        work_end = int(work_end_raw)

        if not (0 <= work_start <= 23 and 0 <= work_end <= 23):
            raise ValueError("Время должно быть в диапазоне 0–23")

        features = dict(result.get("features") or {})
        features["work_start"] = work_start
        features["work_end"] = work_end
        result["features"] = features

    except (TypeError, ValueError):
        logger.warning(
            "Некорректный график в профиле кафе: cafe_id=%s, "
            "work_start=%r, work_end=%r",
            cafe_id,
            work_start_raw,
            work_end_raw,
        )

    return result
# ============================================================
# START -> DEMO: очередь onboarding после /bind_paid_draft
# ============================================================

DEMO_ONBOARDING_STREAM = "cafebotify:demo_onboarding"
DEMO_ONBOARDING_STREAM_MAXLEN = 1000


def k_demo_onboarding_stream() -> str:
    return DEMO_ONBOARDING_STREAM


async def enqueue_demo_start_onboarding(
    r: redis.Redis,
    *,
    client_id: int,
    cafe_id: str,
    draft_id: str,
) -> str:
    """
    После успешного bind_paid_draft просит DEMO отправить клиенту
    кнопку перехода в START в админ-режиме нужного кафе.

    В событии только технические идентификаторы — без ФИО,
    username, текста заказа, текста оплаты или платёжных данных.
    """
    stream_id = await r.xadd(
        k_demo_onboarding_stream(),
        {
            "event": "send_start_onboarding",
            "client_id": str(client_id),
            "cafe_id": str(cafe_id),
            "draft_id": str(draft_id),
            "created_at": str(int(time.time())),
        },
        maxlen=DEMO_ONBOARDING_STREAM_MAXLEN,
        approximate=True,
    )

    return (
        stream_id.decode("utf-8", "ignore")
        if isinstance(stream_id, bytes)
        else str(stream_id)
    )
    
def k_cafe_links_sent(cafe_id: str, user_id: int) -> str:
    return f"cafe:{cafe_id}:links_sent:{user_id}"
    
# Draft первой оплаты, создаётся DEMO-ботом в общем Redis.
PAY_DRAFT_PREFIX = "paydraft:"

def _pay_draft_key(draft_id: str) -> str:
    return f"{PAY_DRAFT_PREFIX}{draft_id}"

def k_cafe_promo(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:promo"

async def get_cafe_promo(r: redis.Redis, cafe_id: str) -> dict | None:
    raw = await r.get(k_cafe_promo(cafe_id))
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


async def set_cafe_promo(r: redis.Redis, cafe_id: str, promo: dict) -> None:
    await r.set(k_cafe_promo(cafe_id), json.dumps(promo, ensure_ascii=False))


async def clear_cafe_promo(r: redis.Redis, cafe_id: str) -> None:
    await r.delete(k_cafe_promo(cafe_id))


def promo_defaults() -> dict:
    return {
        "enabled": False,
        "text": "",
        "url": "",
        "photo_file_id": "",
        "button_text": "Подробнее",
    }


def normalize_promo(data: dict | None) -> dict:
    base = promo_defaults()
    if isinstance(data, dict):
        base["enabled"] = bool(data.get("enabled"))
        base["text"] = str(data.get("text") or "").strip()
        base["url"] = str(data.get("url") or "").strip()
        base["photo_file_id"] = str(data.get("photo_file_id") or "").strip()
        base["button_text"] = str(data.get("button_text") or "Подробнее").strip() or "Подробнее"
    return base

def k_broadcast_counter(cafe_id: str) -> str:
    return f"broadcast:{cafe_id}:counter"

def k_broadcast_meta(cafe_id: str, broadcast_id: str) -> str:
    return f"broadcast:{cafe_id}:{broadcast_id}:meta"

def k_broadcast_stats(cafe_id: str, broadcast_id: str) -> str:
    return f"broadcast:{cafe_id}:{broadcast_id}:stats"

def k_broadcast_clicked_users(cafe_id: str, broadcast_id: str) -> str:
    return f"broadcast:{cafe_id}:{broadcast_id}:clicked_users"

def k_broadcast_ordered_users(cafe_id: str, broadcast_id: str) -> str:
    return f"broadcast:{cafe_id}:{broadcast_id}:ordered_users"

def k_broadcast_sent_users(cafe_id: str, broadcast_id: str) -> str:
    return f"broadcast:{cafe_id}:{broadcast_id}:sent_users"

def k_broadcast_failed_users(cafe_id: str, broadcast_id: str) -> str:
    return f"broadcast:{cafe_id}:{broadcast_id}:failed_users"

def k_broadcast_active(cafe_id: str) -> str:
    return f"broadcast:{cafe_id}:active"

def k_broadcast_last(cafe_id: str) -> str:
    return f"broadcast:{cafe_id}:last"

def k_broadcast_draft(cafe_id: str) -> str:
    return f"broadcast:{cafe_id}:draft"


BROADCAST_WEEK_SECONDS = 7 * 24 * 60 * 60


def k_broadcast_last_run_at(cafe_id: str) -> str:
    return f"broadcast:{cafe_id}:last_run_at"


async def get_broadcast_weekly_left(r: redis.Redis, cafe_id: str) -> int:
    raw = await r.get(k_broadcast_last_run_at(cafe_id))
    if not raw:
        return 0

    try:
        last_ts = int(raw)
    except Exception:
        return 0

    left = (last_ts + BROADCAST_WEEK_SECONDS) - int(time.time())
    return max(0, left)


def format_broadcast_wait(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60

    parts = []
    if days:
        parts.append(f"{days} д.")
    if hours:
        parts.append(f"{hours} ч.")
    if minutes and days == 0:
        parts.append(f"{minutes} мин.")

    return " ".join(parts) if parts else "меньше минуты"
    

# Функция миграции старых подписок (запускается ОДИН раз)
async def migrate_old_subscriptions(r: redis.Redis):
    """Переносит user:{uid}.cafebotify_valid_until -> cafe:{cafe_id}:admin_subscription"""
    try:
        migrated = 0

        user_cafe_keys = await r.keys("user:*:cafe_id")
        for key in user_cafe_keys:
            try:
                uid = int(str(key).split(":")[1])
            except Exception:
                continue

            cafe_id = await r.get(key)
            if not cafe_id or cafe_id not in CAFES:
                continue

            raw_until = await r.hget(f"user:{uid}", "cafebotify_valid_until")
            if not raw_until:
                continue

            sub_key = k_admin_subscription(cafe_id)
            await r.hset(sub_key, mapping={
                "cafebotify_valid_until": raw_until,
                "cafebotify_paid": "1",
                "admin_id": str(uid),
            })
            migrated += 1

        logger.info(f"Мигрировано {migrated} подписок")
    except Exception as e:
        logger.error(f"Миграция подписок упала: {e}")

async def collect_cafe_wipe_keys(r: redis.Redis, cafe_id: str) -> List[str]:
    keys: List[str] = [
        k_menu(cafe_id),
        k_staff_group(cafe_id),
        k_cafe_profile(cafe_id),
        k_admin_subscription(cafe_id),
        k_stats_total_orders(cafe_id),
        k_stats_total_revenue(cafe_id),
        k_customers_set(cafe_id),
        k_cafe_promo(cafe_id),
        k_cafe_sub_notify(cafe_id),
        k_broadcast_counter(cafe_id),
        k_broadcast_last(cafe_id),
        k_broadcast_draft(cafe_id),
        k_broadcast_active(cafe_id),
        k_broadcast_last_run_at(cafe_id),
        k_support_cafe(cafe_id),
    ]

    patterns = [
        f"stats:{cafe_id}:drink:*",
        f"customer:{cafe_id}:*:profile",
        f"customer:{cafe_id}:*:drinks",
        f"last_seen:{cafe_id}:*",
        f"last_order:{cafe_id}:*",
        f"broadcast:{cafe_id}:*:meta",
        f"broadcast:{cafe_id}:*:stats",
        f"broadcast:{cafe_id}:*:clicked_users",
        f"broadcast:{cafe_id}:*:ordered_users",
        f"broadcast:{cafe_id}:*:sent_users",
        f"broadcast:{cafe_id}:*:failed_users",
        f"support:active:{cafe_id}:*",
    ]

    for pattern in patterns:
        found = await r.keys(pattern)
        if found:
            keys.extend(found)

    seen = set()
    out: List[str] = []
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)

    return out

async def collect_linked_users_for_cafe(r: redis.Redis, cafe_id: str) -> List[Tuple[int, str]]:
    linked: List[Tuple[int, str]] = []

    user_cafe_keys = await r.keys("user:*:cafe_id")
    for key in user_cafe_keys:
        try:
            uid = int(str(key).split(":")[1])
            val = await r.get(key)
            if str(val) == cafe_id:
                linked.append((uid, key))
        except Exception:
            continue

    return linked

# =========================================================
# Redis client
# =========================================================
async def get_redis_client() -> redis.Redis:
    client = redis.from_url(REDIS_URL, decode_responses=True)
    await client.ping()
    return client


# =========================================================
# Cafe helpers
# =========================================================
def is_superadmin(user_id: int) -> bool:
    return bool(SUPERADMIN_ID) and user_id == SUPERADMIN_ID

def cafe_or_default(cafe_id: Optional[str]) -> Dict[str, Any]:
    if cafe_id and cafe_id in CAFES:
        return CAFES[cafe_id]
    return CAFES[DEFAULT_CAFE_ID]

def cafe_title(cafe: Dict[str, Any]) -> str:
    return str(cafe.get("title") or cafe.get("name") or "Кафе")

def cafe_phone(cafe: Dict[str, Any]) -> str:
    return str(cafe.get("phone") or "")

def cafe_address(cafe: Dict[str, Any]) -> str:
    return str(cafe.get("address") or "")

def cafe_admin_id_from_json(cafe: Dict[str, Any]) -> int:
    try:
        return int(cafe.get("admin_id") or cafe.get("admin_chat_id") or 0)
    except Exception:
        return 0

async def get_effective_admin_id(r: redis.Redis, cafe_id: str) -> int:
    try:
        raw = await r.hget(k_cafe_profile(cafe_id), "admin_id")
        if raw is None:
            return 0

        raw_s = str(raw).strip()
        if raw_s in ("", "0", "None", "none", "null"):
            return 0

        admin_id = int(raw_s)
        return admin_id if admin_id > 0 else 0
    except Exception:
        return 0

async def is_cafe_admin(r: redis.Redis, user_id: int, cafe_id: str) -> bool:
    if is_superadmin(user_id):
        return True
    admin_id = await get_effective_admin_id(r, cafe_id)
    return admin_id != 0 and admin_id == user_id

async def is_subscription_active(r: redis.Redis, user_id: int, cafe_id: str) -> bool:
    if is_superadmin(user_id):
        return True

    raw_until = await r.hget(k_admin_subscription(cafe_id), "cafebotify_valid_until")
    until_ts = int(raw_until) if raw_until else 0
    return until_ts > int(time.time())


async def ensure_subscription_active(message: Message, r: redis.Redis, cafe_id: str) -> bool:
    uid = message.from_user.id

    if await is_subscription_active(r, uid, cafe_id):
        return True

    renew_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_RENEW_30)],
            [KeyboardButton(text=BTN_RENEW_360)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

    await message.answer(
        f"🔒 <b>{html.quote(cafe_title(cafe_or_default(cafe_id)))}</b>\n\n"
        "❌ Подписка просрочена.\n"
        "Оплатите для доступа к админ-функциям:",
        reply_markup=renew_kb,
    )
    return False

def cafe_hours(cafe: Dict[str, Any]) -> Tuple[int, int]:
    feat = cafe.get("features") or {}
    ws = int(feat.get("work_start", cafe.get("work_start", 9)))
    we = int(feat.get("work_end", cafe.get("work_end", 21)))
    return ws, we

def cafe_rate_limit_seconds(cafe: Dict[str, Any]) -> int:
    feat = cafe.get("features") or {}
    try:
        return int(feat.get("rate_limit_seconds", 60))
    except Exception:
        return 60

def cafe_open(cafe: Dict[str, Any]) -> bool:
    ws, we = cafe_hours(cafe)
    return ws <= get_moscow_time().hour < we

def work_status(cafe: Dict[str, Any]) -> str:
    ws, we = cafe_hours(cafe)
    if cafe_open(cafe):
        return f"🟢 <b>Открыто</b> (до {we}:00 МСК)"
    return f"🔴 <b>Закрыто</b>\n🕐 Открываемся: {ws}:00 (МСК)"

def address_line(cafe: Dict[str, Any]) -> str:
    addr = cafe_address(cafe)
    return f"\n📍 <b>Адрес:</b> {html.quote(addr)}" if addr else ""

def closed_message(cafe: Dict[str, Any], menu: Dict[str, int]) -> str:
    menu_text = " • ".join([f"<b>{html.quote(d)}</b> {p}₽" for d, p in menu.items()]) if menu else "—"
    return (
        f"🔒 <b>{html.quote(cafe_title(cafe))} сейчас закрыто!</b>\n\n"
        f"⏰ {work_status(cafe)}{address_line(cafe)}\n\n"
        f"☕ <b>Меню:</b>\n{menu_text}\n\n"
        f"📞 <b>Телефон:</b> <code>{html.quote(cafe_phone(cafe))}</code>"
    )

def user_name(message: Message) -> str:
    return (message.from_user.first_name if message.from_user else None) or "друг"

def support_topic_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Подписка и оплата", callback_data=f"{SUP_CB_TOPIC}{SUPPORT_TOPIC_SUB}")],
            [InlineKeyboardButton(text="Меню и товары", callback_data=f"{SUP_CB_TOPIC}{SUPPORT_TOPIC_MENU}")],
            [InlineKeyboardButton(text="Заказы и клиенты", callback_data=f"{SUP_CB_TOPIC}{SUPPORT_TOPIC_ORDERS}")],
            [InlineKeyboardButton(text="Персонал / staff-чат", callback_data=f"{SUP_CB_TOPIC}{SUPPORT_TOPIC_STAFF}")],
            [InlineKeyboardButton(text="Техническая ошибка", callback_data=f"{SUP_CB_TOPIC}{SUPPORT_TOPIC_BUG}")],
            [InlineKeyboardButton(text="Другое", callback_data=f"{SUP_CB_TOPIC}{SUPPORT_TOPIC_OTHER}")],
        ]
    )


def support_admin_ticket_kb(ticket_id: str, closed: bool = False) -> InlineKeyboardMarkup:
    if closed:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Закрыто", callback_data=f"{SUP_CB_CLOSE}{ticket_id}")]
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🛠 В работу", callback_data=f"{SUP_CB_INWORK}{ticket_id}"),
                InlineKeyboardButton(text="✉️ Ответить", callback_data=f"{SUP_CB_REPLY}{ticket_id}"),
            ],
            [
                InlineKeyboardButton(text="✅ Закрыть", callback_data=f"{SUP_CB_CLOSE}{ticket_id}")
            ],
        ]
    )


async def next_support_ticket_id(r: redis.Redis) -> str:
    n = await r.incr(k_support_counter())
    return f"T{int(n):06d}"


def support_status_label(status: str) -> str:
    return {
        SUPPORT_STATUS_NEW: "🆕 Новый",
        SUPPORT_STATUS_IN_WORK: "🛠 В работе",
        SUPPORT_STATUS_ANSWERED: "✉️ Отвечен",
        SUPPORT_STATUS_CLOSED: "✅ Закрыт",
    }.get(status, status)


def render_support_ticket_text(ticket: Dict[str, Any]) -> str:
    cafe_title_text = str(ticket.get("cafe_title") or ticket.get("cafe_id") or "-")
    topic_code = str(ticket.get("topic") or "")
    topic_title = SUPPORT_TOPICS.get(topic_code, topic_code or "-")
    user_name = str(ticket.get("user_name") or "—")
    username = str(ticket.get("username") or "")
    username_line = f"@{html.quote(username)}" if username else "—"
    created_at = str(ticket.get("created_at") or "—")
    status = support_status_label(str(ticket.get("status") or SUPPORT_STATUS_NEW))
    text = str(ticket.get("text") or "")

    return (
        f"🎫 <b>Тикет {html.quote(str(ticket.get('ticket_id') or '-'))}</b>\n"
        f"Статус: <b>{html.quote(status)}</b>\n"
        f"Кафе: <b>{html.quote(cafe_title_text)}</b>\n"
        f"Cafe ID: <code>{html.quote(str(ticket.get('cafe_id') or '-'))}</code>\n"
        f"Тема: <b>{html.quote(topic_title)}</b>\n"
        f"User ID: <code>{html.quote(str(ticket.get('user_id') or '-'))}</code>\n"
        f"Имя: <b>{html.quote(user_name)}</b>\n"
        f"Username: {username_line}\n"
        f"Создан: <b>{html.quote(created_at)}</b>\n\n"
        f"📝 <b>Сообщение:</b>\n{html.quote(text)}"
    )


async def create_support_ticket(
    r: redis.Redis,
    *,
    cafe_id: str,
    cafe_title_text: str,
    user_id: int,
    user_name: str,
    username: str,
    topic: str,
    text: str,
) -> Dict[str, Any]:
    ticket_id = await next_support_ticket_id(r)
    now_text = get_moscow_time().strftime("%d.%m.%Y %H:%M")

    ticket = {
        "ticket_id": ticket_id,
        "status": SUPPORT_STATUS_NEW,
        "cafe_id": cafe_id,
        "cafe_title": cafe_title_text,
        "user_id": str(user_id),
        "user_name": user_name or "",
        "username": username or "",
        "topic": topic,
        "text": text,
        "created_at": now_text,
        "updated_at": now_text,
        "superadmin_chat_id": "",
        "superadmin_message_id": "",
    }

    pipe = r.pipeline()
    pipe.hset(k_support_ticket(ticket_id), mapping={k: str(v) for k, v in ticket.items()})
    pipe.sadd(k_support_open(), ticket_id)
    pipe.sadd(k_support_cafe(cafe_id), ticket_id)
    pipe.sadd(k_support_user(user_id), ticket_id)
    pipe.set(k_support_active(cafe_id, user_id), ticket_id)
    await pipe.execute()

    return ticket


async def get_support_ticket(r: redis.Redis, ticket_id: str) -> Dict[str, Any]:
    data = await r.hgetall(k_support_ticket(ticket_id))
    return dict(data or {})


async def update_support_ticket(r: redis.Redis, ticket_id: str, **fields) -> Dict[str, Any]:
    if not fields:
        return await get_support_ticket(r, ticket_id)

    fields["updated_at"] = get_moscow_time().strftime("%d.%m.%Y %H:%M")
    await r.hset(k_support_ticket(ticket_id), mapping={k: str(v) for k, v in fields.items()})
    return await get_support_ticket(r, ticket_id)


async def bind_support_admin_message(
    r: redis.Redis,
    ticket_id: str,
    *,
    chat_id: int,
    message_id: int,
) -> Dict[str, Any]:
    return await update_support_ticket(
        r,
        ticket_id,
        superadmin_chat_id=str(chat_id),
        superadmin_message_id=str(message_id),
    )


# =========================================================
# Menu per cafe (Redis)
# =========================================================
async def get_menu(r: redis.Redis, cafe_id: str) -> Dict[str, int]:
    data = await r.hgetall(k_menu(cafe_id))
    if data:
        out: Dict[str, int] = {}
        for k, v in data.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                continue
        if out:
            return out

        # ✅ ВСТАВИТЬ ВОТ ЭТУ СТРОКУ (если Redis-меню есть, но оно "битое"/пустое)
        await r.delete(k_menu(cafe_id))

    cafe = cafe_or_default(cafe_id)
    base = cafe.get("menu") or {}
    out: Dict[str, int] = {}
    seed: Dict[str, str] = {}
    if isinstance(base, dict):
        for k, v in base.items():
            try:
                out[str(k)] = int(v)
                seed[str(k)] = str(int(v))
            except Exception:
                continue
    if seed:
        await r.hset(k_menu(cafe_id), mapping=seed)
    return out

async def menu_set_item(r: redis.Redis, cafe_id: str, drink: str, price: int):
    await r.hset(k_menu(cafe_id), mapping={drink: str(int(price))})

async def menu_delete_item(r: redis.Redis, cafe_id: str, drink: str):
    await r.hdel(k_menu(cafe_id), drink)


# =========================================================
# /start payload
# =========================================================
def parse_start_payload(payload: str) -> tuple[Optional[str], str]:
    """
    Разбирает deep-link payload для START.

    Поддерживает:
    - admin_cafe_002 — текущий безопасный формат;
    - super_cafe_002 — текущий безопасный формат;
    - admin:cafe_002 — старый формат, для ранее выданных ссылок;
    - super:cafe_002 — старый формат, для ранее выданных ссылок;
    - cafe_002 — обычная клиентская ссылка.
    """
    p = (payload or "").strip()

    if not p:
        return None, "client"

    # Новый формат: допустим для Telegram start payload без кодирования.
    if p.startswith("admin_"):
        cafe_id = p[len("admin_"):].strip()
        return (cafe_id or None), "admin"

    if p.startswith("super_"):
        cafe_id = p[len("super_"):].strip()
        return (cafe_id or None), "super"

    # Старый формат: оставляем поддержку уже выданных ссылок.
    # Для создания новых ссылок используем admin_<cafe_id>.
    if p.startswith("admin:"):
        cafe_id = p.split("admin:", 1)[1].strip()
        return (cafe_id or None), "admin"

    if p.startswith("super:"):
        cafe_id = p.split("super:", 1)[1].strip()
        return (cafe_id or None), "super"

    # Обычная клиентская ссылка, например cafe_002.
    return p, "client"

async def resolve_cafe_id(r: redis.Redis, message: Message, cafe_id_from_payload: Optional[str]) -> str:
    uid = message.from_user.id
    if cafe_id_from_payload and cafe_id_from_payload in CAFES:
        await r.set(k_user_cafe(uid), cafe_id_from_payload)
        return cafe_id_from_payload

    saved = await r.get(k_user_cafe(uid))
    if saved and str(saved) in CAFES:
        return str(saved)

    await r.set(k_user_cafe(uid), DEFAULT_CAFE_ID)
    return DEFAULT_CAFE_ID


# =========================================================
# Buttons
# =========================================================
BTN_CALL = "📞 Позвонить"
BTN_HOURS = "⏰ Часы работы"
BTN_BOOKING = "📅 Бронирование"
BTN_CART = "🛒 Корзина"
BTN_CHECKOUT = "✅ Оформить"
BTN_CLEAR_CART = "🧹 Очистить"
BTN_CANCEL_ORDER = "❌ Отменить заказ"
BTN_EDIT_CART = "✏️ Изменить"
BTN_CANCEL = "🔙 Отмена"
BTN_CONFIRM = "Подтвердить"
BTN_READY_NOW = "🚶 Сейчас"
BTN_READY_20 = "⏱ Через 20 мин"
# ПЕРСОНАЛЬНЫЕ ДАННЫЕ: согласие перед оформлением заказа
PRIVACY_POLICY_URL = "https://cafebotify.tilda.ws/politica"
PRIVACY_POLICY_VERSION = "2026-09-25"
# ============================================================
# Автоматическое освобождение кафе после окончания подписки
# ============================================================

# Через сколько времени после истечения подписки освободить слот.
# 3 суток = 72 часа = 259 200 секунд.
CAFE_AUTO_WIPE_AFTER_EXPIRY_SEC = 3 * 24 * 60 * 60

# Как часто worker будет проверять просроченные подписки.
# Один раз в час.
CAFE_AUTO_WIPE_CHECK_EVERY_SEC = 60 * 60
PD_CONSENT_CB_PREFIX = "pd_consent:"
PD_CONSENT_ORDER = "pd_consent:order"

BTN_REPEAT_LAST = "🔁 Повторить последний заказ"
BTN_REPEAT_NO = "❌ Нет, спасибо"

CART_ACT_PLUS = "➕ +1"
CART_ACT_MINUS = "➖ -1"
CART_ACT_DEL = "🗑 Удалить"
CART_ACT_DONE = "✅ Готово"

BTN_STATS = "📊 Статистика"
BTN_MENU_EDIT = "🛠 Меню"
BTN_STAFF_GROUP = "👥 Группа персонала"
BTN_LINKS = "🔗 Ссылки"
BTN_CAFE_PROFILE = "🏪 Профиль кафе"
BTN_CANCEL_CAFE_PROFILE = "❌ Отменить заполнение"
BTN_ADMIN_INFO = "ℹ️ Справка админа"
BTN_BACK = "⬅️ Назад"

MENU_EDIT_ADD = "➕ Добавить позицию"
MENU_EDIT_EDIT = "✏️ Изменить цену"
MENU_EDIT_DEL = "🗑 Удалить позицию"

BTN_VIEW_CLIENT = "⬅️ В клиентский режим"
BTN_VIEW_ADMIN = "🛠 В админ-режим"

BTN_RENEW_SUB = "💳 Продлить подписку"
BTN_RENEW_30 = "💳Продлить на 30 дней"
BTN_RENEW_360 = "💳Продлить на 360 дней"

BTN_SUB_INFO = "🗓️Подписка"     # для админа кафе
BTN_HELP_ADMIN = "/help_admin"    # для супер-админа (именно команда)

BTN_ADMIN_SUPPORT = "🛟 Поддержка"

SUPPORT_TOPIC_SUB = "sub"
SUPPORT_TOPIC_MENU = "menu"
SUPPORT_TOPIC_ORDERS = "orders"
SUPPORT_TOPIC_STAFF = "staff"
SUPPORT_TOPIC_BUG = "bug"
SUPPORT_TOPIC_OTHER = "other"

SUPPORT_TOPICS = {
    SUPPORT_TOPIC_SUB: "Подписка и оплата",
    SUPPORT_TOPIC_MENU: "Меню и товары",
    SUPPORT_TOPIC_ORDERS: "Заказы и клиенты",
    SUPPORT_TOPIC_STAFF: "Персонал / staff-чат",
    SUPPORT_TOPIC_BUG: "Техническая ошибка",
    SUPPORT_TOPIC_OTHER: "Другое",
}

SUPPORT_STATUS_NEW = "new"
SUPPORT_STATUS_IN_WORK = "in_work"
SUPPORT_STATUS_ANSWERED = "answered"
SUPPORT_STATUS_CLOSED = "closed"

SUP_CB_TOPIC = "sup_topic:"
SUP_CB_REPLY = "sup_reply:"
SUP_CB_CLOSE = "sup_close:"
SUP_CB_INWORK = "sup_inwork:"

BTN_PROMO = "📢 Реклама"

PROMO_EDIT_TEXT = "✏️ Текст"
PROMO_EDIT_URL = "🔗 Ссылка"
PROMO_EDIT_PHOTO = "🖼 Картинка"
PROMO_TOGGLE = "🟢 Вкл/выкл"
PROMO_CLEAR = "🧹 Очистить"
PROMO_PREVIEW = "👀 Предпросмотр"
PROMO_BACK = "⬅️ Назад"
PROMO_SKIP = "⏭ Пропустить"
PROMO_DELETE_PHOTO = "🗑 Удалить картинку"
PROMO_DELETE_URL = "🗑 Удалить ссылку"
PROMO_DELETE_TEXT = "🗑 Удалить текст"

BTN_BROADCAST = "📣 Рассылка"

BROADCAST_EDIT_TEXT = "✏️ Текст"
BROADCAST_EDIT_URL = "🔗 Ссылка"
BROADCAST_SEND = "🚀 Запустить"
BROADCAST_STATS = "📊 Статистика"
BROADCAST_BACK = "⬅️ Назад"
BROADCAST_CANCEL = "❌ Отмена"



# =========================================================
# Keyboards
# =========================================================
def k_pd_consent(cafe_id: str, user_id: int) -> str:
    """
    Согласие пользователя на обработку данных для оформления заказа
    в конкретном кафе.
    """
    return f"cafebotify:pd_consent:{cafe_id}:{user_id}"

async def has_order_pd_consent(
    r: redis.Redis,
    cafe_id: str,
    user_id: int,
) -> bool:
    return bool(await r.exists(k_pd_consent(cafe_id, user_id)))

async def save_order_pd_consent(
    r: redis.Redis,
    cafe_id: str,
    user_id: int,
) -> None:
    """
    Фиксируем минимальный журнал согласия:
    кафе, пользователь, время и версия опубликованной политики.

    13 месяцев — стартовый срок. Его затем согласуем с окончательной
    политикой и правилами хранения в сервисе.
    """
    consent_key = k_pd_consent(cafe_id, user_id)

    await r.hset(
        consent_key,
        mapping={
            "cafe_id": cafe_id,
            "user_id": user_id,
            "policy_url": PRIVACY_POLICY_URL,
            "policy_version": PRIVACY_POLICY_VERSION,
            "consented_at": int(time.time()),
            "source": "telegram_order_checkout",
        },
    )

    await r.expire(
        consent_key,
        13 * 30 * 24 * 60 * 60,
    )

def kb_pd_consent() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Согласен(на)",
                    callback_data=PD_CONSENT_ORDER,
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📄 Политика",
                    url=PRIVACY_POLICY_URL,
                ),
            ],
        ]
    )

async def show_order_confirmation_after_consent(
    message: Message,
    state: FSMContext,
    *,
    cafe_id: str,
) -> None:
    """
    Единая функция показа подтверждения заказа.
    Её вызываем и после обычной проверки consent,
    и после нажатия «Согласен(на)».
    """
    menu = await get_menu(message.bot._redis, cafe_id)
    cart = get_cart(await state.get_data())

    if not cart:
        await message.answer(
            "Корзина пуста. Добавьте позиции и попробуйте снова."
        )
        return

    await state.set_state(OrderStates.waiting_for_confirmation)

    await message.answer(
        "✅ <b>Подтвердите заказ</b>\n\n"
        + cart_text(cart, menu),
        reply_markup=kb_confirm(),
    )

def kb_client_main(menu: Dict[str, int], show_admin_button: bool = False) -> ReplyKeyboardMarkup:
    kb: List[List[KeyboardButton]] = []
    for drink in menu.keys():
        kb.append([KeyboardButton(text=drink)])
    kb.append([KeyboardButton(text=BTN_CART), KeyboardButton(text=BTN_CHECKOUT), KeyboardButton(text=BTN_BOOKING)])
    kb.append([KeyboardButton(text=BTN_CALL), KeyboardButton(text=BTN_HOURS)])
    if show_admin_button:
        kb.append([KeyboardButton(text=BTN_VIEW_ADMIN)])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, is_persistent=True)

def kb_cart(menu: Dict[str, int], has_items: bool) -> ReplyKeyboardMarkup:
    kb: List[List[KeyboardButton]] = []
    kb.append([KeyboardButton(text=BTN_CART), KeyboardButton(text=BTN_CHECKOUT)])
    if has_items:
        kb.append([KeyboardButton(text=BTN_EDIT_CART), KeyboardButton(text=BTN_CLEAR_CART), KeyboardButton(text=BTN_CANCEL_ORDER)])
    else:
        kb.append([KeyboardButton(text=BTN_CANCEL_ORDER)])
    for drink in menu.keys():
        kb.append([KeyboardButton(text=drink)])
    kb.append([KeyboardButton(text=BTN_BOOKING)])
    kb.append([KeyboardButton(text=BTN_CALL), KeyboardButton(text=BTN_HOURS)])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, is_persistent=True)

def kb_qty() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1️⃣"), KeyboardButton(text="2️⃣"), KeyboardButton(text="3️⃣")],
            [KeyboardButton(text="4️⃣"), KeyboardButton(text="5️⃣"), KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_confirm() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CONFIRM), KeyboardButton(text=BTN_CART)],
            [KeyboardButton(text=BTN_CANCEL_ORDER)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_ready_time() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_READY_NOW), KeyboardButton(text=BTN_READY_20)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_repeat_offer() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_REPEAT_LAST), KeyboardButton(text=BTN_REPEAT_NO)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_cart_pick_item(cart: Dict[str, int]) -> ReplyKeyboardMarkup:
    rows: List[List[KeyboardButton]] = [[KeyboardButton(text=k)] for k in cart.keys()]
    rows.append([KeyboardButton(text=BTN_CANCEL), KeyboardButton(text=BTN_CART)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)

def kb_cart_edit_actions() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=CART_ACT_PLUS), KeyboardButton(text=CART_ACT_MINUS)],
            [KeyboardButton(text=CART_ACT_DEL), KeyboardButton(text=CART_ACT_DONE)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_booking_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN_CANCEL)]], resize_keyboard=True, one_time_keyboard=True)

def kb_booking_people() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3"), KeyboardButton(text="4")],
            [KeyboardButton(text="5"), KeyboardButton(text="6"), KeyboardButton(text="7"), KeyboardButton(text="8")],
            [KeyboardButton(text="9"), KeyboardButton(text="10"), KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_admin_main(is_super: bool) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text=BTN_STATS), KeyboardButton(text=BTN_MENU_EDIT)],
        [KeyboardButton(text=BTN_STAFF_GROUP), KeyboardButton(text=BTN_LINKS)],
        [KeyboardButton(text=BTN_CAFE_PROFILE)],
        [KeyboardButton(text=BTN_RENEW_SUB), KeyboardButton(text=BTN_SUB_INFO)],
        [KeyboardButton(text=BTN_PROMO), KeyboardButton(text=BTN_BROADCAST)],
        [KeyboardButton(text=BTN_ADMIN_INFO), KeyboardButton(text=BTN_ADMIN_SUPPORT)],
        [KeyboardButton(text=BTN_VIEW_CLIENT)],
    ]
    if is_super:
        kb.append([KeyboardButton(text=BTN_HELP_ADMIN)])
    return ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        is_persistent=True,
    )

def kb_renew_sub() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_RENEW_30), KeyboardButton(text=BTN_RENEW_360)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_menu_edit() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_EDIT_ADD), KeyboardButton(text=MENU_EDIT_EDIT)],
            [KeyboardButton(text=MENU_EDIT_DEL), KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_menu_edit_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_BACK)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_pick_menu_item(menu: Dict[str, int]) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=k)] for k in menu.keys()]
    rows.append([KeyboardButton(text=BTN_BACK)])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_staff_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_LINKS)],       # Ссылки
            [KeyboardButton(text=BTN_SUB_INFO)],    # Подписка   
        ],
        resize_keyboard=True,
        is_persistent=True,
    )
    

def kb_promo_manage() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=PROMO_EDIT_TEXT), KeyboardButton(text=PROMO_EDIT_URL)],
            [KeyboardButton(text=PROMO_EDIT_PHOTO), KeyboardButton(text=PROMO_TOGGLE)],
            [KeyboardButton(text=PROMO_DELETE_TEXT), KeyboardButton(text=PROMO_DELETE_URL)],
            [KeyboardButton(text=PROMO_DELETE_PHOTO), KeyboardButton(text=PROMO_CLEAR)],
            [KeyboardButton(text=PROMO_PREVIEW)],
            [KeyboardButton(text=PROMO_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_promo_input() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=PROMO_SKIP)],
            [KeyboardButton(text=PROMO_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_promo_photo_input() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=PROMO_SKIP), KeyboardButton(text=PROMO_DELETE_PHOTO)],
            [KeyboardButton(text=PROMO_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_promo_url(url: str | None, button_text: str | None = None) -> InlineKeyboardMarkup | None:
    url = str(url or "").strip()
    if not url:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=(button_text or "Подробнее"), url=url)]
        ]
    )


def kb_broadcast_manage() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BROADCAST_EDIT_TEXT), KeyboardButton(text=BROADCAST_EDIT_URL)],
            [KeyboardButton(text=BROADCAST_SEND), KeyboardButton(text=BROADCAST_STATS)],
            [KeyboardButton(text=BROADCAST_BACK), KeyboardButton(text=BROADCAST_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_broadcast_input() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BROADCAST_CANCEL), KeyboardButton(text=BROADCAST_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def promo_summary_text(promo: dict) -> str:
    p = normalize_promo(promo)
    return (
        "📢 <b>Реклама после заказа</b>\n\n"
        f"Статус: {'🟢 включена' if p['enabled'] else '🔴 выключена'}\n"
        f"Текст: {'✅ есть' if p['text'] else '— нет'}\n"
        f"Ссылка: {'✅ есть' if p['url'] else '— нет'}\n"
        f"Картинка: {'✅ есть' if p['photo_file_id'] else '— нет'}\n"
        f"Кнопка: <b>{html.quote(p['button_text'])}</b>"
    )
    

# =========================================================
# FSM
# =========================================================
class OrderStates(StatesGroup):
    waiting_for_quantity = State()
    cart_view = State()
    cart_edit_pick_item = State()
    cart_edit_pick_action = State()
    waiting_for_confirmation = State()
    waiting_for_ready_time = State()

class BookingStates(StatesGroup):
    waiting_for_datetime = State()
    waiting_for_people = State()
    waiting_for_comment = State()

class MenuEditStates(StatesGroup):
    waiting_for_action = State()
    waiting_for_add_name = State()
    waiting_for_add_price = State()
    pick_edit_item = State()
    waiting_for_edit_price = State()
    pick_remove_item = State()

class CafeProfileStates(StatesGroup):
    waiting_for_title = State()
    waiting_for_address = State()
    waiting_for_work_start = State()
    waiting_for_work_end = State()

class SupportStates(StatesGroup):
    waiting_for_topic_message = State()
    waiting_for_superadmin_reply = State()

class PromoStates(StatesGroup):
    waiting_for_action = State()
    waiting_for_text = State()
    waiting_for_url = State()
    waiting_for_photo = State()

class BroadcastStates(StatesGroup):
    waiting_for_action = State()
    waiting_for_text = State()
    waiting_for_url = State()


# =========================================================
# Cart helpers
# =========================================================
def get_cart(data: Dict[str, Any]) -> Dict[str, int]:
    cart = data.get("cart")
    out: Dict[str, int] = {}
    if isinstance(cart, dict):
        for k, v in cart.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                continue
    return out

def cart_total(cart: Dict[str, int], menu: Dict[str, int]) -> int:
    return sum(int(menu.get(d, 0)) * int(q) for d, q in cart.items())

def cart_lines(cart: Dict[str, int], menu: Dict[str, int]) -> List[str]:
    lines = []
    for d, q in cart.items():
        p = int(menu.get(d, 0))
        lines.append(f"• {html.quote(d)} × {q} = <b>{p * int(q)}₽</b>")
    return lines

def cart_text(cart: Dict[str, int], menu: Dict[str, int]) -> str:
    if not cart:
        return "🛒 <b>Корзина пустая</b>\n\nЧтобы добавить: нажмите напиток → выберите количество."
    return "🛒 <b>Ваш заказ:</b>\n" + "\n".join(cart_lines(cart, menu)) + f"\n\n💰 Итого: <b>{cart_total(cart, menu)}₽</b>"


# =========================================================
# Repeat last order
# =========================================================
async def set_last_seen(r: redis.Redis, cafe_id: str, user_id: int):
    await r.set(k_last_seen(cafe_id, user_id), str(time.time()))

async def should_offer_repeat(
    r: redis.Redis,
    cafe_id: str,
    user_id: int,
) -> bool:
    snapshot = await get_last_order_snapshot(r, cafe_id, user_id)

    if not snapshot:
        return False

    cart = snapshot.get("cart")

    if not isinstance(cart, dict) or not cart:
        return False

    try:
        order_ts = int(snapshot.get("ts") or 0)
    except (TypeError, ValueError):
        return False

    if order_ts <= 0:
        return False

    last_order_date = datetime.fromtimestamp(
        order_ts,
        tz=MSK_TZ,
    ).date()

    # Не навязываем повтор в день оформления.
    # Начиная со следующего календарного дня — предлагаем.
    return last_order_date < get_moscow_time().date()

async def get_last_order_snapshot(r: redis.Redis, cafe_id: str, user_id: int) -> Optional[dict]:
    raw = await r.get(k_last_order(cafe_id, user_id))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

async def set_last_order_snapshot(r: redis.Redis, cafe_id: str, user_id: int, snapshot: dict):
    await r.set(k_last_order(cafe_id, user_id), json.dumps(snapshot, ensure_ascii=False))


# =========================================================
# Smart return
# =========================================================
RETURN_REMINDER_DAYS = 7
RETURN_OFFER_DAYS = 14
RETURN_CHECK_EVERY_SECONDS = 6 * 60 * 60
RETURN_SEND_FROM_HOUR = 10
RETURN_SEND_TO_HOUR = 20
RETURN_OFFER_DISCOUNT_PERCENT = 20

def in_send_window_msk() -> bool:
    h = get_moscow_time().hour
    return RETURN_SEND_FROM_HOUR <= h < RETURN_SEND_TO_HOUR

async def kb_return_to_menu(bot: Bot, cafe_id: str) -> InlineKeyboardMarkup:
    start_link = await create_start_link(bot, payload=cafe_id, encode=True)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть меню", url=start_link)]
        ]
    )

def promo_code(user_id: int) -> str:
    return f"CB{user_id % 10000:04d}{int(time.time()) % 10000:04d}"

async def customer_mark_order(
    r: redis.Redis,
    cafe_id: str,
    *,
    user_id: int,
    first_name: str,
    username: str,
    cart: Dict[str, int],
    total_sum: int,
):
    now_ts = int(time.time())
    customer_key = k_customer_profile(cafe_id, user_id)
    drinks_key = k_customer_drinks(cafe_id, user_id)
    last_drink = next(iter(cart.keys()), "")

    pipe = r.pipeline()
    pipe.sadd(k_customers_set(cafe_id), user_id)
    pipe.hsetnx(customer_key, "first_order_ts", now_ts)
    pipe.hsetnx(customer_key, "offers_opt_out", 0)
    pipe.hsetnx(customer_key, "last_trigger_ts", 0)
    pipe.hset(
        customer_key,
        mapping={
            "first_name": first_name or "",
            "username": username or "",
            "last_order_ts": now_ts,
            "last_order_sum": int(total_sum),
            "last_drink": last_drink,
            "return_stage_7_sent_at": 0,
            "return_stage_14_sent_at": 0,
        },
    )
    pipe.hincrby(customer_key, "total_orders", 1)
    pipe.hincrby(customer_key, "total_spent", int(total_sum))
    for drink, qty in cart.items():
        pipe.hincrby(drinks_key, drink, int(qty))
    await pipe.execute()

async def get_favorite_drink(r: redis.Redis, cafe_id: str, user_id: int) -> str:
    data = await r.hgetall(k_customer_drinks(cafe_id, user_id))
    best_name, best_cnt = "", -1
    for k, v in data.items():
        try:
            cnt = int(v)
            if cnt > best_cnt:
                best_cnt = cnt
                best_name = str(k)
        except Exception:
            continue
    return best_name


# ============================================================
# ЗАКАЗЫ: карточка, статус «выполнен», одно напоминание
# ============================================================

ORDER_REMINDER_DELAY_SEC = 15 * 60

def k_active_order(cafe_id: str, order_id: str) -> str:
    return f"cafebotify:order:{cafe_id}:{order_id}"

def k_active_order_messages(cafe_id: str, order_id: str) -> str:
    return f"cafebotify:order_messages:{cafe_id}:{order_id}"

def k_order_reminders() -> str:
    return "cafebotify:order_reminders"

def make_active_order_id() -> str:
    """
    Короткий ID для карточки и callback_data.
    6 символов: например 4FA1B9.
    """
    return secrets.token_hex(3).upper()

def _as_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")
    return str(value)

def kb_order_done(cafe_id: str, order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Выполнен",
                    callback_data=f"order_done:{cafe_id}:{order_id}",
                )
            ]
        ]
    )

async def create_active_order(
    r: redis.Redis,
    *,
    cafe_id: str,
    order_id: str,
    created_at: int,
    ready_at: int,
    order_text: str,
) -> None:
    """
    Сохраняет статус заказа и ставит единственное напоминание:
    ready_at + 15 минут.
    """
    order_key = k_active_order(cafe_id, order_id)
    reminder_at = ready_at + ORDER_REMINDER_DELAY_SEC
    reminder_token = f"{cafe_id}|{order_id}"

    await r.hset(
        order_key,
        mapping={
            "cafe_id": cafe_id,
            "order_id": order_id,
            "status": "new",
            "created_at": created_at,
            "ready_at": ready_at,
            "reminder_at": reminder_at,
            "reminder_sent": 0,
            "order_text": order_text,
            "done_at": 0,
            "done_by_id": 0,
            "done_by_name": "",
        },
    )

    # Храним заказ 7 дней — это удобно для разбора спорных ситуаций,
    # после чего Redis сам очистит его.
    await r.expire(order_key, 7 * 24 * 60 * 60)

    # ZSET: score — точное время единственного напоминания.
    await r.zadd(
        k_order_reminders(),
        {reminder_token: reminder_at},
    )

async def send_order_card(
    bot: Bot,
    r: redis.Redis,
    *,
    cafe_id: str,
    order_id: str,
    text: str,
) -> None:
    """
    Посылает одну и ту же карточку заказа админу и в staff-группу.
    Сохраняет chat_id/message_id всех отправленных копий,
    чтобы позже обновить их одновременно.
    """
    message_key = k_active_order_messages(cafe_id, order_id)
    markup = kb_order_done(cafe_id, order_id)

    admin_id = await get_effective_admin_id(r, cafe_id)
    if admin_id:
        try:
            sent = await bot.send_message(
                chat_id=int(admin_id),
                text=text,
                disable_web_page_preview=True,
                reply_markup=markup,
            )
            await r.rpush(
                message_key,
                json.dumps(
                    {
                        "chat_id": sent.chat.id,
                        "message_id": sent.message_id,
                    },
                    ensure_ascii=False,
                ),
            )
        except Exception:
            logger.exception(
                "Не удалось отправить карточку заказа админу: "
                "cafe_id=%s order_id=%s",
                cafe_id,
                order_id,
            )

    try:
        group_id = await r.get(k_staff_group(cafe_id))
        if group_id:
            sent = await bot.send_message(
                chat_id=int(group_id),
                text=text,
                disable_web_page_preview=True,
                reply_markup=markup,
            )
            await r.rpush(
                message_key,
                json.dumps(
                    {
                        "chat_id": sent.chat.id,
                        "message_id": sent.message_id,
                    },
                    ensure_ascii=False,
                ),
            )
    except Exception:
        logger.exception(
            "Не удалось отправить карточку заказа в staff-группу: "
            "cafe_id=%s order_id=%s",
            cafe_id,
            order_id,
        )

    await r.expire(message_key, 7 * 24 * 60 * 60)

async def update_order_cards_as_done(
    bot: Bot,
    r: redis.Redis,
    *,
    cafe_id: str,
    order_id: str,
    done_by_name: str,
    done_at: int,
) -> None:
    """
    Убирает кнопку у всех копий карточки и показывает,
    кто и когда отметил выдачу.
    """
    order_key = k_active_order(cafe_id, order_id)
    base_text = _as_text(await r.hget(order_key, "order_text"))

    done_time = datetime.fromtimestamp(
        done_at,
        tz=get_moscow_time().tzinfo,
    ).strftime("%H:%M")

    final_text = (
        f"{base_text}\n\n"
        "✅ <b>ВЫПОЛНЕН</b>\n"
        f"👤 Отметил: {html.quote(done_by_name)}\n"
        f"🕒 Время: {done_time} МСК"
    )

    raw_items = await r.lrange(
        k_active_order_messages(cafe_id, order_id),
        0,
        -1,
    )

    for raw_item in raw_items:
        try:
            item = json.loads(_as_text(raw_item))
            await bot.edit_message_text(
                chat_id=int(item["chat_id"]),
                message_id=int(item["message_id"]),
                text=final_text,
                reply_markup=None,
                disable_web_page_preview=True,
            )
        except Exception:
            logger.exception(
                "Не удалось обновить карточку выполненного заказа: "
                "cafe_id=%s order_id=%s",
                cafe_id,
                order_id,
            )

async def send_unfinished_order_reminder(
    bot: Bot,
    r: redis.Redis,
    *,
    cafe_id: str,
    order_id: str,
) -> None:
    """
    Отправляет одно напоминание через 15 минут после готовности
    и только если заказ всё ещё имеет статус new.
    """
    order_key = k_active_order(cafe_id, order_id)
    order = await r.hgetall(order_key)

    if not order:
        return

    status = _as_text(order.get(b"status") or order.get("status"))
    reminder_sent = _as_text(
        order.get(b"reminder_sent") or order.get("reminder_sent")
    )

    if status == "done" or reminder_sent == "1":
        return

    base_text = _as_text(
        order.get(b"order_text") or order.get("order_text")
    )

    reminder_text = (
        f"⚠️ <b>Заказ #{html.quote(order_id)} не отмечен как выполненный</b>\n\n"
        "Прошло 15 минут после указанного времени готовности. "
        "Проверьте выдачу и нажмите кнопку после выполнения.\n\n"
        f"{base_text}"
    )

    await send_order_card(
        bot,
        r,
        cafe_id=cafe_id,
        order_id=order_id,
        text=reminder_text,
    )

    await r.hset(order_key, mapping={"reminder_sent": 1})

async def unfinished_orders_reminder_worker(bot: Bot) -> None:
    """
    Один раз в минуту проверяет Redis ZSET с заказами,
    для которых наступило время единственного напоминания.
    """
    logger.info("Unfinished orders reminder worker started")

    while True:
        try:
            r: redis.Redis = bot._redis
            now_ts = int(time.time())

            due_tokens = await r.zrangebyscore(
                k_order_reminders(),
                min="-inf",
                max=now_ts,
                start=0,
                num=100,
            )

            for raw_token in due_tokens:
                token = _as_text(raw_token)

                # Удаляем из очереди ДО отправки: это гарантирует,
                # что повторного автонапоминания не будет даже при ошибке.
                await r.zrem(k_order_reminders(), raw_token)

                if "|" not in token:
                    continue

                cafe_id, order_id = token.split("|", 1)

                try:
                    await send_unfinished_order_reminder(
                        bot,
                        r,
                        cafe_id=cafe_id,
                        order_id=order_id,
                    )
                except Exception:
                    logger.exception(
                        "Ошибка напоминания о неотмеченном заказе: "
                        "cafe_id=%s order_id=%s",
                        cafe_id,
                        order_id,
                    )

        except Exception:
            logger.exception("Ошибка фонового воркера напоминаний по заказам")

        await asyncio.sleep(60)
        

# =========================================================
# Admin notify
# =======================================================
async def notify_admin(bot: Bot, r: redis.Redis, cafe_id: str, text: str):
    admin_id = await get_effective_admin_id(r, cafe_id)
    if admin_id:
        try:
            await bot.send_message(admin_id, text, disable_web_page_preview=True)
        except Exception:
            pass

    try:
        group_id = await r.get(k_staff_group(cafe_id))
        if group_id:
            await bot.send_message(
                int(group_id),
                text,
                disable_web_page_preview=True,  # без клавиатуры
            )
    except Exception:
        pass

async def send_promo_after_order(message: Message, r: redis.Redis):
    if is_group_chat(message):
        return

    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    promo = await get_cafe_promo(r, cafe_id)
    promo = normalize_promo(promo)

    if not promo.get("enabled"):
        return

    text = str(promo.get("text") or "").strip()
    photo_file_id = str(promo.get("photo_file_id") or "").strip()
    url = str(promo.get("url") or "").strip()

    if not text and not photo_file_id:
        return

    reply_markup = None
    if url:
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Подробнее", url=url)]
            ]
        )

    if photo_file_id:
        await message.answer_photo(
            photo=photo_file_id,
            caption=text or "📢 Спецпредложение",
            reply_markup=reply_markup,
        )
        return

    await message.answer(
        text,
        reply_markup=reply_markup,
        disable_web_page_preview=False,
    )

async def send_admin_demo_to_user(bot: Bot, user_id: int, admin_like_text: str):
    if not DEMO_MODE:
        return
    demo_text = "ℹ️ <b>DEMO</b>: так это увидит админ:\n\n" + admin_like_text
    try:
        await bot.send_message(user_id, demo_text, disable_web_page_preview=True)
    except Exception:
        pass


def is_valid_http_url(value: str) -> bool:
    value = (value or "").strip()
    return bool(re.match(r"^https?://\S+$", value, flags=re.IGNORECASE))


async def show_promo_menu(message: Message, r: redis.Redis, cafe_id: str) -> None:
    promo = normalize_promo(await get_cafe_promo(r, cafe_id))
    await message.answer(
        promo_summary_text(promo),
        reply_markup=kb_promo_manage(),
    )


async def send_promo_preview(message: Message, promo: dict) -> None:
    p = normalize_promo(promo)
    reply_markup = kb_promo_url(p["url"], p["button_text"])

    if p["photo_file_id"]:
        await message.answer_photo(
            photo=p["photo_file_id"],
            caption=p["text"] or "Пример рекламного сообщения",
            reply_markup=reply_markup,
        )
        return

    if p["text"]:
        await message.answer(
            p["text"],
            reply_markup=reply_markup,
            disable_web_page_preview=False,
        )
        return

    if p["url"]:
        await message.answer(
            "Пример рекламного сообщения 👇",
            reply_markup=reply_markup,
            disable_web_page_preview=False,
        )
        return

    await message.answer("Сейчас реклама пустая: нет ни текста, ни ссылки, ни картинки.")


def broadcast_defaults() -> Dict[str, Any]:
    return {
        "text": "",
        "url": "",
    }


def normalize_broadcast(data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = broadcast_defaults()
    if not isinstance(data, dict):
        return base
    base["text"] = str(data.get("text") or "").strip()
    base["url"] = str(data.get("url") or "").strip()
    return base


async def next_broadcast_id(r: redis.Redis, cafe_id: str) -> str:
    n = await r.incr(k_broadcast_counter(cafe_id))
    return f"B{int(n):06d}"


async def create_broadcast(
    r: redis.Redis,
    cafe_id: str,
    created_by: int,
    text: str,
    url: str = "",
) -> str:
    broadcast_id = await next_broadcast_id(r, cafe_id)
    now_ts = int(time.time())

    await r.hset(
        k_broadcast_meta(cafe_id, broadcast_id),
        mapping={
            "broadcast_id": broadcast_id,
            "cafe_id": cafe_id,
            "created_by": str(created_by),
            "text": text.strip(),
            "url": url.strip(),
            "status": "draft",
            "created_ts": str(now_ts),
            "started_ts": "0",
            "finished_ts": "0",
        },
    )

    await r.hset(
        k_broadcast_stats(cafe_id, broadcast_id),
        mapping={
            "planned": "0",
            "sent": "0",
            "failed": "0",
            "clicked": "0",
            "ordered": "0",
            "ordered_revenue": "0",
        },
    )

    await r.set(k_broadcast_last(cafe_id), broadcast_id)
    return broadcast_id


async def get_broadcast_meta(r: redis.Redis, cafe_id: str, broadcast_id: str) -> Dict[str, Any]:
    return dict(await r.hgetall(k_broadcast_meta(cafe_id, broadcast_id)) or {})


async def get_broadcast_stats(r: redis.Redis, cafe_id: str, broadcast_id: str) -> Dict[str, Any]:
    return dict(await r.hgetall(k_broadcast_stats(cafe_id, broadcast_id)) or {})


def broadcast_stats_text(
    cafe_id: str,
    broadcast_id: str,
    meta: Dict[str, Any],
    stats: Dict[str, Any],
) -> str:
    planned = int(stats.get("planned", 0) or 0)
    sent = int(stats.get("sent", 0) or 0)
    failed = int(stats.get("failed", 0) or 0)
    clicked = int(stats.get("clicked", 0) or 0)
    ordered = int(stats.get("ordered", 0) or 0)
    ordered_revenue = int(stats.get("ordered_revenue", 0) or 0)
    status = str(meta.get("status") or "draft")

    ctr = round((clicked / sent) * 100, 1) if sent > 0 else 0.0
    conv = round((ordered / clicked) * 100, 1) if clicked > 0 else 0.0

    return (
        f"📊 <b>Рассылка {html.quote(broadcast_id)}</b>\n"
        f"Кафе: <code>{html.quote(cafe_id)}</code>\n"
        f"Статус: <b>{html.quote(status)}</b>\n\n"
        f"👥 Запланировано: <b>{planned}</b>\n"
        f"✅ Доставлено: <b>{sent}</b>\n"
        f"⚠️ Ошибок: <b>{failed}</b>\n"
        f"👆 Перешли: <b>{clicked}</b>\n"
        f"🛒 Заказали: <b>{ordered}</b>\n"
        f"💰 Выручка: <b>{ordered_revenue}₽</b>\n\n"
        f"CTR: <b>{ctr}%</b>\n"
        f"CR в заказ: <b>{conv}%</b>"
    )


async def run_broadcast_send(bot: Bot, cafe_id: str, broadcast_id: str):
    r: redis.Redis = bot._redis

    meta = await get_broadcast_meta(r, cafe_id, broadcast_id)
    text = str(meta.get("text") or "").strip()
    url = str(meta.get("url") or "").strip()
    if not text:
        return

    now_ts = int(time.time())

    await r.set(k_broadcast_active(cafe_id), broadcast_id)
    await r.set(k_broadcast_last_run_at(cafe_id), str(now_ts))
    await r.hset(
        k_broadcast_meta(cafe_id, broadcast_id),
        mapping={
            "status": "running",
            "started_ts": str(now_ts),
        },
    )

    try:
        ids = await r.smembers(k_customers_set(cafe_id))
        user_ids = sorted(int(x) for x in ids) if ids else []
    except Exception:
        user_ids = []

    await r.hset(k_broadcast_stats(cafe_id, broadcast_id), "planned", str(len(user_ids)))

    reply_markup = None
    if url:
        bot_link = await create_start_link(bot, payload=f"bc:{cafe_id}:{broadcast_id}", encode=True)
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Открыть", url=bot_link)]]
        )

    try:
        for user_id in user_ids:
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=text,
                    reply_markup=reply_markup,
                    disable_web_page_preview=False,
                )
                await r.hincrby(k_broadcast_stats(cafe_id, broadcast_id), "sent", 1)
                await r.sadd(k_broadcast_sent_users(cafe_id, broadcast_id), user_id)
            except Exception:
                await r.hincrby(k_broadcast_stats(cafe_id, broadcast_id), "failed", 1)
                await r.sadd(k_broadcast_failed_users(cafe_id, broadcast_id), user_id)

            await asyncio.sleep(0.06)

        await r.hset(
            k_broadcast_meta(cafe_id, broadcast_id),
            mapping={
                "status": "done",
                "finished_ts": str(int(time.time())),
            },
        )
    finally:
        await r.delete(k_broadcast_active(cafe_id))



def broadcast_summary_text(draft: Dict[str, Any], last_id: str = "") -> str:
    d = normalize_broadcast(draft)
    return (
        "📣 <b>Рассылка по базе кафе</b>\n\n"
        f"Текст: {'✅ задан' if d['text'] else '— не задан'}\n"
        f"Ссылка: {'✅ задана' if d['url'] else '— не задана'}\n"
        f"Последняя рассылка: <code>{html.quote(last_id)}</code>\n\n"
        "Сначала заполните текст, затем при желании добавьте ссылку и запускайте рассылку."
    )


async def get_broadcast_draft(r: redis.Redis, cafe_id: str) -> Dict[str, Any]:
    return normalize_broadcast(dict(await r.hgetall(k_broadcast_draft(cafe_id)) or {}))


async def set_broadcast_draft(r: redis.Redis, cafe_id: str, data: Dict[str, Any]) -> None:
    d = normalize_broadcast(data)
    await r.hset(
        k_broadcast_draft(cafe_id),
        mapping={
            "text": d["text"],
            "url": d["url"],
        },
    )


async def clear_broadcast_draft(r: redis.Redis, cafe_id: str) -> None:
    await r.delete(k_broadcast_draft(cafe_id))


async def show_broadcast_menu(message: Message, r: redis.Redis, cafe_id: str) -> None:
    draft = await get_broadcast_draft(r, cafe_id)
    last_id = str(await r.get(k_broadcast_last(cafe_id)) or "—")
    await message.answer(
        broadcast_summary_text(draft, last_id),
        reply_markup=kb_broadcast_manage(),
        disable_web_page_preview=True,
    )


async def set_broadcast_click_attribution(
    r: redis.Redis,
    user_id: int,
    cafe_id: str,
    broadcast_id: str,
) -> None:
    await r.set(
        f"user:{user_id}:broadcast_attribution",
        json.dumps({
            "cafe_id": str(cafe_id),
            "broadcast_id": str(broadcast_id),
            "ts": int(time.time()),
        }, ensure_ascii=False),
        ex=7 * 24 * 60 * 60,
    )


async def track_broadcast_click(
    r: redis.Redis,
    user_id: int,
    cafe_id: str,
    broadcast_id: str,
) -> None:
    if not cafe_id or not broadcast_id:
        return

    added = await r.sadd(k_broadcast_clicked_users(cafe_id, broadcast_id), user_id)
    if added:
        await r.hincrby(k_broadcast_stats(cafe_id, broadcast_id), "clicked", 1)

    await set_broadcast_click_attribution(r, user_id, cafe_id, broadcast_id)


async def track_broadcast_order(
    r: redis.Redis,
    user_id: int,
    order_cafe_id: str,
    total: int,
) -> None:
    raw = await r.get(f"user:{user_id}:broadcast_attribution")
    if not raw:
        return

    try:
        data = json.loads(raw)
    except Exception:
        return

    attr_cafe_id = str(data.get("cafe_id") or "").strip()
    broadcast_id = str(data.get("broadcast_id") or "").strip()

    if not attr_cafe_id or not broadcast_id:
        return

    if attr_cafe_id != str(order_cafe_id):
        return

    pipe = r.pipeline()
    added = await r.sadd(k_broadcast_ordered_users(attr_cafe_id, broadcast_id), user_id)
    if added:
        pipe.hincrby(k_broadcast_stats(attr_cafe_id, broadcast_id), "ordered", 1)
    pipe.hincrby(k_broadcast_stats(attr_cafe_id, broadcast_id), "orderedrevenue", int(total))
    pipe.expire(f"user:{user_id}:broadcast_attribution", 7 * 24 * 60 * 60)
    await pipe.execute()


# =========================================================
# Router
# =========================================================
router = Router()

@router.error()
async def error_handler(event: ErrorEvent):
    logger.critical("Update handling error: %r", event.exception, exc_info=True)


# =========================================================
# Commands
# =========================================================
async def set_commands(bot: Bot):
    cmds = [
        BotCommand(command="start", description="Запуск"),
        BotCommand(command="myid", description="Показать мой Telegram ID"),
        BotCommand(command="whoami", description="Кто я (роль/кафе)"),
        BotCommand(command="help_admin", description="Справка супер-админа"),
        BotCommand(command="bind", description="Привязать staff-группу (в группе)"),
        BotCommand(command="set_admin", description="👑 Назначить админа"),
        BotCommand(command="unset_admin", description="Сбросить override admin_id (superadmin)"),
        BotCommand(command="wipe_cafe", description="⚠️ Полная очистка кафе (superadmin)"),
        BotCommand(command="wipe_cafe_confirm", description="⚠️ Подтверждение очистки кафе (superadmin)"),
        BotCommand(command="set_cafe_subscription", description="Дата или +дни для подписки кафе (superadmin)"),
    ]
    await bot.set_my_commands(cmds)

@router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@router.message(Command("whoami"))
async def cmd_whoami(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    role = "SUPERADMIN" if is_superadmin(message.from_user.id) else "user/admin"
    eff_admin = await get_effective_admin_id(r, cafe_id)
    await message.answer(
        "👤 <b>Профиль</b>\n\n"
        f"ID: <code>{message.from_user.id}</code>\n"
        f"Роль: <b>{role}</b>\n"
        f"Текущее кафе: <code>{html.quote(cafe_id)}</code>\n"
        f"admin_id (effective) для этого кафе: <code>{eff_admin}</code>"
    )

@router.message(Command("help_admin"))
async def cmd_help_admin(message: Message, command: CommandObject):
    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    
    # 🚨 ТОЛЬКО SUPERADMIN!
    if not is_superadmin(uid):
        await message.answer("🔒 <b>/help_admin</b> — только для супер-админа!", parse_mode="HTML")
        return
    
    args = (command.args or "").strip()
    cafe_id = args if args and args in CAFES else None

    lines: List[str] = []
    lines.append("🧾 <b>SUPERADMIN: Справка</b>")
    lines.append(f"🆔 Ваш ID: <code>{uid}</code>")
    lines.append("⭐ Роль: <b>SUPERADMIN</b>")
    lines.append("")
    lines.append("✅ <b>Базовые команды</b>")
    lines.append("• <code>/myid</code> — ваш Telegram ID")
    lines.append("• <code>/whoami</code> — роль/кафе")
    lines.append("• <code>/start admin:cafe_001</code> — админ-панель")
    lines.append("• <code>/bind cafe_001</code> — staff-группа (в группе)")
    lines.append("")
    
    # ⭐ Супер-админ команды
    lines.append("👑 <b>SUPERADMIN команды</b>")
    lines.append("• <code>/set_admin cafe_001 123456789</code> — назначить админа")
    lines.append("• <code>/bind_paid_draft cafe_001 88da376df710</code> — привязать первую оплаченную подписку к кафе")
    lines.append("• <code>/unset_admin cafe_001</code> — убрать админа")
    lines.append("• <code>/set_cafe_subscription cafe_001 2026-12-31</code> — выставить конец подписки")
    lines.append("• <code>/set_cafe_subscription cafe_001 +30</code> — продлить на 30 дней от текущего срока/сегодня")
    lines.append("• <code>/wipe_cafe cafe_001</code> — показать, что будет очищено")
    lines.append("• <code>/wipe_cafe_confirm cafe_001 WIPE</code> — ПОЛНОСТЬЮ очистить кафе")
    lines.append("ℹ️ <code>/help_admin cafe_001</code> — справка по кафе")

    # Конкретное кафе
    if cafe_id:
        lines.append("🎯 <b>КАФЕ: " + html.quote(CAFES.get(cafe_id, {}).get("name", cafe_id)).upper() + "</b>")
        lines.append(f"🆔 <code>{cafe_id}</code>")
        
        try:
            cafe = cafe_or_default(cafe_id)
            eff_admin = await get_effective_admin_id(r, cafe_id)
            client_link = await create_start_link(message.bot, payload=cafe_id, encode=True)
            admin_link = await create_start_link(message.bot, payload=f"admin:{cafe_id}", encode=True)
            staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)
            
            lines.append("")
            lines.append("👥 <b>Staff-группа (уведомления)</b>")
            lines.append("1️⃣ <code>" + staff_link + "</code> ← открыть в группах")
            lines.append("2️⃣ Добавить бота (права: отправка сообщений)")
            lines.append(f"3️⃣ <code>/bind {html.quote(cafe_id)}</code> ← в группе")
            
            lines.append("")
            lines.append("🔗 <b>Ссылки для копирования</b>")
            lines.append(f"👤 Клиентам: <code>{client_link}</code>")
            lines.append(f"👑 Админу: <code>{admin_link}</code>")
            lines.append(f"📢 Staff: <code>{staff_link}</code>")
            lines.append(f"💳 Tilda: <code>https://cafebotify.tilda.ws/pay-month?cafe_id={cafe_id}</code>")
            
            lines.append("")
            lines.append(f"🛠 Админ: <code>{eff_admin or 'не назначен'}</code>")
        except Exception as e:
            lines.append(f"❌ Ошибка кафе: {str(e)}")

    await message.answer("\n".join(lines), 
                        parse_mode="HTML", 
                        disable_web_page_preview=True)


@router.message(Command("set_admin"))
async def cmd_set_admin(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    args = (command.args or "").strip().split()
    if len(args) != 2:
        await message.answer("Формат: <code>/set_admin cafe_001 123456789</code>")
        return

    cafe_id, admin_id_s = args[0], args[1]
    if cafe_id not in CAFES:
        await message.answer("Неизвестный cafe_id.")
        return
    try:
        admin_id = int(admin_id_s)
        if admin_id <= 0:
            raise ValueError
    except Exception:
        await message.answer("admin_id должен быть положительным числом.")
        return

    r: redis.Redis = message.bot._redis
    await r.hset(k_cafe_profile(cafe_id), mapping={"admin_id": str(admin_id)})  # [web:25]
    await message.answer(f"✅ Назначил admin_id=<code>{admin_id}</code> для <code>{html.quote(cafe_id)}</code>.")

@router.message(Command("unset_admin"))
async def cmd_unset_admin(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    cafe_id = (command.args or "").strip()
    if not cafe_id or cafe_id not in CAFES:
        await message.answer("Формат: <code>/unset_admin cafe_001</code>")
        return

    r: redis.Redis = message.bot._redis

    await r.hset(k_cafe_profile(cafe_id), mapping={"admin_id": ""})
    await r.delete(k_staff_group(cafe_id))
    await r.delete(k_cafe_sub_notify(cafe_id))

    await message.answer(
        f"✅ Для <code>{html.quote(cafe_id)}</code> admin_id очищен.\n"
        "Кафе переведено в состояние без привязанного админа."
    )


@router.message(Command("bind_paid_draft"))
async def cmd_bind_paid_draft(
    message: Message,
    command: CommandObject,
):
    if not is_superadmin(message.from_user.id):
        await message.answer("🔒 Доступ запрещён.")
        return

    args = (command.args or "").strip().split()

    if len(args) != 2:
        await message.answer(
            "Формат:\n"
            "<code>/bind_paid_draft cafe_023 88da376df710</code>\n\n"
            "Где второй аргумент — Draft ID из уведомления DEMO "
            "о подтверждённой оплате."
        )
        return

    cafe_id = args[0].strip()
    draft_id = args[1].strip()

    if cafe_id not in CAFES:
        await message.answer("❌ Неизвестный cafe_id.")
        return

    if not draft_id:
        await message.answer("❌ Не указан Draft ID.")
        return

    r: Optional[redis.Redis] = None

    try:
        r = message.bot._redis

        raw = await r.get(_pay_draft_key(draft_id))

        if not raw:
            await message.answer(
                "❌ Draft не найден или истёк.\n\n"
                "DEMO хранит payment draft в Redis 7 дней после оплаты."
            )
            return

        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            await message.answer("❌ Draft повреждён: не удалось прочитать JSON.")
            return

        if not isinstance(payload, dict):
            await message.answer("❌ Draft имеет неверный формат.")
            return

        draft_status = str(payload.get("status") or "").strip()

        if draft_status in {"bound", "links_sent"}:
            bound_cafe_id = str(payload.get("cafe_id") or "").strip()

            await message.answer(
                "⚠️ Этот Draft уже был обработан.\n\n"
                f"Draft ID: <code>{html.quote(draft_id)}</code>\n"
                f"Статус: <code>{html.quote(draft_status)}</code>\n"
                f"Кафе: <code>{html.quote(bound_cafe_id or '-')}</code>"
            )
            return

        try:
            client_id = int(payload.get("tgid"))
            valid_until = int(payload.get("valid_until") or 0)
        except (TypeError, ValueError):
            await message.answer(
                "❌ В Draft нет корректного Telegram ID или срока подписки."
            )
            return

        now_ts = int(time.time())

        if valid_until <= now_ts:
            await message.answer(
                "❌ Срок этой оплаченной подписки уже истёк."
            )
            return

        # Не даём новой оплате случайно перезаписать уже занятое кафе.
        existing_admin_raw = await r.hget(
            k_cafe_profile(cafe_id),
            "admin_id",
        )

        existing_sub = await r.hgetall(
            k_admin_subscription(cafe_id)
        )

        try:
            existing_until = int(
                existing_sub.get("cafebotify_valid_until", "0") or 0
            )
        except (TypeError, ValueError):
            existing_until = 0

        existing_admin_id = str(existing_admin_raw or "").strip()

        if (
            existing_admin_id not in {"", "0", str(client_id)}
            or existing_until > now_ts
        ):
            until_text = (
                datetime.fromtimestamp(
                    existing_until,
                    tz=MSK_TZ,
                ).strftime("%d.%m.%Y %H:%M")
                if existing_until > now_ts
                else "нет"
            )

            await message.answer(
                "⚠️ Привязка отменена: кафе уже занято "
                "или у него есть активная подписка.\n\n"
                f"Кафе: <code>{html.quote(cafe_id)}</code>\n"
                f"Текущий admin_id: "
                f"<code>{html.quote(existing_admin_id or '-')}</code>\n"
                f"Подписка до: <b>{until_text}</b>\n\n"
                "Выберите другое свободное кафе."
            )
            return

        payment_id = str(payload.get("payment_id") or "")
        product = str(
            payload.get("product") or "cafebotify_start_month"
        )
        amount_value = str(payload.get("amount_value") or "")
        amount_currency = str(payload.get("amount_currency") or "")

        # Сохраняем факт привязки непосредственно в draft, чтобы одна
        # успешная оплата не могла быть применена к двум разным кафе.
        payload.update(
            {
                "cafe_id": cafe_id,
                "status": "bound",
                "bound_at": now_ts,
                "bound_by": message.from_user.id,
            }
        )

        pipe = r.pipeline(transaction=True)

        # Тот же ключ, который использует START-команда /set_admin.
        pipe.hset(
            k_cafe_profile(cafe_id),
            mapping={
                "admin_id": str(client_id),
            },
        )

        # Тот же ключ и поля, которые читает is_subscription_active().
        pipe.hset(
            k_admin_subscription(cafe_id),
            mapping={
                "cafebotify_paid": "1",
                "cafebotify_valid_until": str(valid_until),
                "admin_id": str(client_id),
                "last_payment_id": payment_id,
                "last_product": product,
                "last_amount_value": amount_value,
                "last_amount_currency": amount_currency,
                "last_paid_at": str(now_ts),
                "source_draft_id": draft_id,
            },
        )

        # В DEMO draft живёт 7 дней — после привязки сохраняем его
        # ещё на 7 дней как журнал операции.
        pipe.setex(
            _pay_draft_key(draft_id),
            7 * 86400,
            json.dumps(payload, ensure_ascii=False),
        )

        await pipe.execute()

        try:
            onboarding_event_id = await enqueue_demo_start_onboarding(
                r,
                client_id=client_id,
                cafe_id=cafe_id,
                draft_id=draft_id,
            )

            logger.info(
                "DEMO onboarding queued: event_id=%s draft_id=%s "
                "cafe_id=%s client_id=%s",
                onboarding_event_id,
                draft_id,
                cafe_id,
                client_id,
            )
        except Exception:
            logger.exception(
                "Не удалось поставить DEMO onboarding в очередь: "
                "draft_id=%s cafe_id=%s client_id=%s",
                draft_id,
                cafe_id,
                client_id,
            )

        until_text = datetime.fromtimestamp(
            valid_until,
            tz=MSK_TZ,
        ).strftime("%d.%m.%Y %H:%M")

        logger.info(
            "SUPERADMIN bind_paid_draft: by=%s draft_id=%s "
            "cafe_id=%s client_id=%s valid_until=%s",
            message.from_user.id,
            draft_id,
            cafe_id,
            client_id,
            valid_until,
        )

        await message.answer(
            "✅ <b>Оплата привязана к кафе</b>\n\n"
            f"Кафе: <code>{html.quote(cafe_id)}</code>\n"
            f"Администратор: <code>{client_id}</code>\n"
            f"Подписка до: <b>{until_text}</b>\n"
            f"Draft ID: <code>{html.quote(draft_id)}</code>\n\n"
            "Клиенту поставлена задача на отправку кнопки перехода в START."
        )

        # Показываем ссылки суперадмину в текущем чате:
        await send_cafe_links(
            bot=message.bot,
            chat_id=message.chat.id,
            cafe_id=cafe_id,
            heading="🔗 <b>Ссылки для нового кафе</b>",
        )

    except Exception:
        logger.exception(
            "bind_paid_draft failed: draft_id=%s",
            draft_id if "draft_id" in locals() else "?",
        )
        await message.answer(
            "❌ Не удалось привязать оплату. "
            "Проверьте runtime-логи START."
        )


@router.message(Command("set_cafe_subscription"))
async def cmd_set_cafe_subscription(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        await message.answer("🔒 Доступ запрещён.")
        return

    args = (command.args or "").strip().split()
    if len(args) != 2:
        await message.answer(
            "Формат:\\n"
            "<code>/set_cafe_subscription cafe_001 2026-12-31</code>\\n"
            "<code>/set_cafe_subscription cafe_001 +30</code>\\n"
            "<code>/set_cafe_subscription cafe_001 +360</code>"
        )
        return

    cafe_id, value = args
    if cafe_id not in CAFES:
        await message.answer("Неизвестный cafe_id.")
        return

    r: redis.Redis = message.bot._redis
    sub_key = k_admin_subscription(cafe_id)
    now_ts = int(time.time())

    try:
        if value.startswith("+"):
            add_days = int(value[1:])
            if add_days <= 0:
                raise ValueError

            raw_until = await r.hget(sub_key, "cafebotify_valid_until")
            current_until = int(raw_until) if raw_until else 0

            base_ts = current_until if current_until > now_ts else now_ts
            base_dt = datetime.fromtimestamp(base_ts, tz=MSK_TZ)
            target_dt = base_dt + timedelta(days=add_days)
            target_dt = target_dt.replace(hour=23, minute=59, second=59, microsecond=0)
        else:
            year, month, day = map(int, value.split("-"))
            target_dt = datetime(year, month, day, 23, 59, 59, tzinfo=MSK_TZ)

        until_ts = int(target_dt.timestamp())
    except Exception:
        await message.answer(
            "Неверный формат.\\n"
            "Используй либо дату YYYY-MM-DD, либо +N дней.\\n"
            "Примеры:\\n"
            "<code>/set_cafe_subscription cafe_001 2026-12-31</code>\\n"
            "<code>/set_cafe_subscription cafe_001 +30</code>"
        )
        return

    eff_admin = await get_effective_admin_id(r, cafe_id)

    await r.hset(sub_key, mapping={
        "cafebotify_valid_until": str(until_ts),
        "cafebotify_paid": "1",
        "admin_id": str(eff_admin or 0),
    })

    await r.delete(k_cafe_sub_notify(cafe_id))

    await message.answer(
        "✅ Подписка обновлена\\n\\n"
        f"Кафе: <code>{html.quote(cafe_id)}</code>\\n"
        f"Новая дата окончания: <b>{target_dt.strftime('%d.%m.%Y')}</b>"
    )


# =========================================================
# /bind staff group
# =========================================================
async def send_cafe_links(
    bot: Bot,
    chat_id: int,
    cafe_id: str,
    *,
    heading: str = "🔗 <b>Ссылки кафе</b>",
) -> None:
    """
    Отправляет компактный набор deep-link ссылок конкретного кафе.
    chat_id — Telegram ID личного чата или группы, куда надо отправить блок.
    """

    cafe = cafe_or_default(cafe_id)

    client_link = await create_start_link(
        bot,
        payload=cafe_id,
        encode=True,
    )

    admin_link = await create_start_link(
        bot,
        payload=f"admin_{cafe_id}",
        encode=True,
    )

    staff_link = await create_startgroup_link(
        bot,
        payload=cafe_id,
        encode=True,
    )

    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"{heading}\n\n"
            f"Кафе: <b>{html.quote(cafe_title(cafe))}</b>\n"
            f"ID: <code>{html.quote(cafe_id)}</code>\n\n"
            f"• <a href=\"{html.quote(client_link)}\">👥 Клиентам — открыть меню</a>\n"
            f"• <a href=\"{html.quote(admin_link)}\">🛠 Админу — открыть админ-панель</a>\n"
            f"• <a href=\"{html.quote(staff_link)}\">👨‍🍳 Добавить бота в staff-группу</a>\n\n"
            "Клиентскую ссылку используйте для QR-кода, сайта и соцсетей. "
            "Админскую ссылку не публикуйте — она предназначена владельцу кафе."
        ),
    )

async def send_cafe_links_once(
    bot: Bot,
    r: redis.Redis,
    user_id: int,
    cafe_id: str,
) -> None:
    """
    Отправляет набор стартовых ссылок владельцу только один раз
    для конкретной пары cafe_id + Telegram user_id.
    """

    sent_key = k_cafe_links_sent(cafe_id, user_id)

    # Уже показывали ссылки этому админу этого кафе.
    if await r.get(sent_key):
        return

    await send_cafe_links(
        bot=bot,
        chat_id=user_id,
        cafe_id=cafe_id,
        heading=(
            "🎉 <b>Кафе подключено</b>\n\n"
            "🔗 <b>Ваши рабочие ссылки</b>"
        ),
    )

    # Флаг на 400 дней: ссылка будет показана только один раз.
    await r.set(
        sent_key,
        "1",
        ex=400 * 24 * 60 * 60,
    )


@router.message(Command("bind"))
async def cmd_bind(message: Message, command: CommandObject):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда /bind работает только в группе.")
        return

    cafe_id = (command.args or "").strip()

    if not cafe_id or cafe_id not in CAFES:
        await message.answer(
            "Формат: <code>/bind cafe_001</code>"
        )
        return

    r: redis.Redis = message.bot._redis
    user_id = message.from_user.id

    # Привязать staff-группу может только администратор этого кафе.
    if not await is_cafe_admin(r, user_id, cafe_id):
        await message.answer(
            "🔒 Только администратор этого кафе может привязать staff-группу."
        )
        return

    # Суперадмин имеет доступ всегда.
    # Обычному админу нужна активная подписка.
    if not is_superadmin(user_id):
        if not await is_subscription_active(r, user_id, cafe_id):
            await message.answer(
                "🔒 <b>Подписка кафе не активна.</b>\n\n"
                "Сначала продлите подписку в админ-панели, "
                "после этого повторите команду /bind."
            )
            return

    # Сохраняем Telegram chat_id staff-группы именно для выбранного кафе.
    await r.set(k_staff_group(cafe_id), str(message.chat.id))

    # Ссылки формируются из cafe_id команды /bind — не из DEFAULT_CAFE_ID.
    # encode=False делает payload коротким и читаемым.
    client_link = await create_start_link(
        message.bot,
        payload=cafe_id,
        encode=False,
    )

    admin_link = await create_start_link(
        message.bot,
        payload=f"admin:{cafe_id}",
        encode=False,
    )

    staff_link = await create_startgroup_link(
        message.bot,
        payload=cafe_id,
        encode=False,
    )

    cafe = cafe_or_default(cafe_id)
    cafe_name = html.quote(cafe_title(cafe))

    await message.answer(
        "✅ <b>Staff-группа успешно привязана</b>\n\n"
        f"Кафе: <b>{cafe_name}</b>\n"
        f"ID: <code>{html.quote(cafe_id)}</code>\n"
        f"Группа: <code>{message.chat.id}</code>\n\n"
        "🔗 <b>Ссылки кафе</b>\n"
        f"• <a href=\"{html.quote(client_link)}\">👥 Клиентам — открыть меню</a>\n"
        f"• <a href=\"{html.quote(admin_link)}\">🛠 Администратору — открыть админ-панель</a>\n"
        f"• <a href=\"{html.quote(staff_link)}\">👨‍🍳 Добавить бота в staff-группу</a>\n\n"
        "Клиентскую ссылку можно использовать для QR-кода и соцсетей. "
        "Админскую ссылку не публикуйте — отправляйте её только владельцу кафе."
    )
    

@router.message(Command("wipe_cafe"))
async def cmd_wipe_cafe(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        await message.answer("🔒 Доступ запрещён.")
        return

    cafe_id = (command.args or "").strip()
    if not cafe_id or cafe_id not in CAFES:
        await message.answer("Формат: <code>/wipe_cafe cafe001</code>")
        return

    r: redis.Redis = message.bot._redis

    keys = await collect_cafe_wipe_keys(r, cafe_id)
    linked_users = await collect_linked_users_for_cafe(r, cafe_id)

    await message.answer(
        "⚠️ <b>Подтверждение очистки кафе</b>\n\n"
        f"Кафе: <code>{html.quote(cafe_id)}</code>\n"
        f"Будет удалено Redis-ключей: <b>{len(keys)}</b>\n"
        f"Найдено пользовательских привязок: <b>{len(linked_users)}</b>\n\n"
        "Что будет очищено:\n"
        "• Меню кафе\n"
        "• Staff group bind\n"
        "• Override admin_id профиля кафе\n"
        "• Подписка кафе\n"
        "• Статистика заказов и выручки\n"
        "• Профили клиентов и их напитки\n"
        "• Last seen / last order\n"
        "• Promo-данные кафе\n"
        "• Draft/active/history-ключи рассылок\n"
        "• Support active / support set кафе\n"
        "• user -> cafe_id привязки для связанных пользователей\n"
        "• user view_mode для связанных пользователей\n"
        "• Legacy user.cafebotify_valid_until для связанных пользователей\n"
        "• user broadcast attribution для связанных пользователей\n\n"
        "Что НЕ будет очищено:\n"
        "• config.json\n"
        "• Глобальные user hash целиком\n"
        "• Общие support:user:* наборы\n"
        "• support:ticket:* записи\n\n"
        f"Для подтверждения отправьте:\n<code>/wipe_cafe_confirm {html.quote(cafe_id)} WIPE</code>"
    )

async def wipe_cafe_runtime_data(
    r: redis.Redis,
    *,
    cafe_id: str,
    actor_id: Optional[int] = None,
    reason: str = "manual",
) -> dict[str, int]:
    """
    Очищает все оперативные Redis-данные одного кафе.

    Сам cafe_id остаётся в config_330_template.json, поэтому после
    очистки слот можно выдать следующему клиенту.

    reason:
    - manual: ручная очистка суперадмином;
    - subscription_expired_3d: будущая автоочистка через 3 суток.
    """

    keys = await collect_cafe_wipe_keys(r, cafe_id)
    linked_users = await collect_linked_users_for_cafe(r, cafe_id)

    pipe = r.pipeline()

    if keys:
        pipe.delete(*keys)

    # После удаления оставляем только безопасные технические маркеры
    # свободного слота. Реальные данные предыдущего кафе здесь не хранятся.
    pipe.hset(
        k_cafe_profile(cafe_id),
        mapping={
            "admin_id": "",
            "slot_status": "free",
            "wiped_at": str(int(time.time())),
            "wipe_reason": reason,
        },
    )

    pipe.hset(
        k_admin_subscription(cafe_id),
        mapping={
            "cafebotify_paid": "0",
            "cafebotify_valid_until": "0",
            "admin_id": "0",
            "slot_status": "free",
        },
    )

    pipe.delete(k_staff_group(cafe_id))
    pipe.delete(k_cafe_sub_notify(cafe_id))

    fixed_users = 0
    cleared_legacy_sub = 0
    cleared_broadcast_attr = 0

    for uid, user_cafe_key in linked_users:
        fixed_users += 1

        if cafe_id == DEFAULT_CAFE_ID:
            pipe.delete(user_cafe_key)
        else:
            pipe.set(user_cafe_key, DEFAULT_CAFE_ID)

        pipe.delete(k_view_mode(uid))
        pipe.delete(k_support_active(cafe_id, uid))

        try:
            had_legacy = await r.hget(
                f"user:{uid}",
                "cafebotify_valid_until",
            )

            if had_legacy is not None:
                pipe.hdel(
                    f"user:{uid}",
                    "cafebotify_valid_until",
                )
                cleared_legacy_sub += 1

        except Exception:
            pass

        try:
            attr_key = f"user:{uid}:broadcast_attribution"
            raw_attr = await r.get(attr_key)

            if raw_attr:
                attr_data = json.loads(raw_attr)

                if str(attr_data.get("cafe_id") or "").strip() == cafe_id:
                    pipe.delete(attr_key)
                    cleared_broadcast_attr += 1

        except Exception:
            pass

    await pipe.execute()

    logger.warning(
        "CAFE WIPE completed: actor_id=%s reason=%s cafe_id=%s "
        "deleted_keys=%s fixed_users=%s "
        "cleared_legacy_sub=%s cleared_broadcast_attr=%s",
        actor_id if actor_id is not None else "system",
        reason,
        cafe_id,
        len(keys),
        fixed_users,
        cleared_legacy_sub,
        cleared_broadcast_attr,
    )

    return {
        "deleted_keys": len(keys),
        "fixed_users": fixed_users,
        "cleared_legacy_sub": cleared_legacy_sub,
        "cleared_broadcast_attr": cleared_broadcast_attr,
    }


async def auto_wipe_expired_cafes(bot: Bot) -> int:
    """
    Находит кафе, у которых активная подписка закончилась
    более трёх суток назад, и освобождает их Redis-данные.

    Использует текущую модель подписок:
    k_admin_subscription(cafe_id) +
    cafebotify_valid_until / cafebotify_paid / admin_id.
    """
    r: redis.Redis = bot._redis

    now_ts = int(time.time())
    wipe_before_ts = now_ts - CAFE_AUTO_WIPE_AFTER_EXPIRY_SEC

    wiped_count = 0

    # CAFES остаётся списком 330 постоянных слотов из JSON.
    for cafe_id in CAFES:
        try:
            sub = await r.hgetall(
                k_admin_subscription(cafe_id)
            )

            raw_until = sub.get(
                "cafebotify_valid_until",
                "0",
            )
            raw_paid = sub.get(
                "cafebotify_paid",
                "0",
            )
            raw_admin_id = sub.get(
                "admin_id",
                "0",
            )

            try:
                valid_until = int(raw_until or 0)
            except (TypeError, ValueError):
                valid_until = 0

            paid = str(raw_paid or "0").strip()
            admin_id = str(raw_admin_id or "0").strip()

            # Свободный слот или уже очищенное кафе не трогаем.
            if paid != "1":
                continue

            if admin_id in {"", "0"}:
                continue

            # Подписка ещё действует.
            if valid_until > now_ts:
                continue

            # Истекла меньше 3 суток назад:
            # ещё не освобождаем, оставляем время на продление.
            if valid_until > wipe_before_ts:
                continue

            result = await wipe_cafe_runtime_data(
                r,
                cafe_id=cafe_id,
                actor_id=None,
                reason="subscription_expired_3d",
            )

            wiped_count += 1

            logger.warning(
                "AUTO_WIPE expired cafe: cafe_id=%s valid_until=%s "
                "deleted_keys=%s fixed_users=%s",
                cafe_id,
                valid_until,
                result["deleted_keys"],
                result["fixed_users"],
            )

        except Exception:
            # Ошибка проверки одного слота не должна остановить
            # проверку остальных 329 кафе.
            logger.exception(
                "AUTO_WIPE check failed: cafe_id=%s",
                cafe_id,
            )

    return wiped_count


async def expired_cafes_auto_wipe_loop(bot: Bot) -> None:
    """
    Раз в час проверяет все 330 слотов и освобождает только те кафе,
    где подписка истекла более 3 суток назад.

    Функция auto_wipe_expired_cafes() содержит все условия и сам wipe.
    Этот loop только запускает её по расписанию.
    """
    logger.info("Expired cafes auto-wipe worker started")

    while True:
        try:
            wiped_count = await auto_wipe_expired_cafes(bot)

            if wiped_count:
                logger.warning(
                    "Expired cafes auto-wipe completed: wiped_count=%s",
                    wiped_count,
                )

        except asyncio.CancelledError:
            # Нужен для корректной остановки задачи на deploy/restart.
            raise

        except Exception:
            logger.exception("Expired cafes auto-wipe worker failed")

        await asyncio.sleep(CAFE_AUTO_WIPE_CHECK_EVERY_SEC)


@router.message(Command("wipe_cafe_confirm"))
async def cmd_wipe_cafe_confirm(
    message: Message,
    command: CommandObject,
):
    if not is_superadmin(message.from_user.id):
        await message.answer("🔒 Доступ запрещён.")
        return

    args = (command.args or "").strip().split()

    if len(args) != 2:
        await message.answer(
            "Формат: <code>/wipe_cafe_confirm cafe_001 WIPE</code>"
        )
        return

    cafe_id, confirm_word = args

    if cafe_id not in CAFES:
        await message.answer("Неизвестный cafe_id.")
        return

    if confirm_word != "WIPE":
        await message.answer(
            "Неверное подтверждение. Последнее слово должно быть "
            "<code>WIPE</code>."
        )
        return

    r: redis.Redis = message.bot._redis

    try:
        result = await wipe_cafe_runtime_data(
            r,
            cafe_id=cafe_id,
            actor_id=message.from_user.id,
            reason="manual",
        )

    except Exception:
        logger.exception(
            "SUPERADMIN manual WIPE_CAFE failed: user_id=%s cafe_id=%s",
            message.from_user.id,
            cafe_id,
        )

        await message.answer(
            "❌ Не удалось очистить кафе. Проверьте runtime-логи."
        )
        return

    await message.answer(
        "✅ <b>Очистка завершена</b>\n\n"
        f"Кафе: <code>{html.quote(cafe_id)}</code>\n"
        f"Удалено Redis-ключей: <b>{result['deleted_keys']}</b>\n"
        f"Исправлено пользовательских привязок: "
        f"<b>{result['fixed_users']}</b>\n"
        f"Очищено legacy-подписок user hash: "
        f"<b>{result['cleared_legacy_sub']}</b>\n"
        f"Очищено broadcast attribution: "
        f"<b>{result['cleared_broadcast_attr']}</b>\n\n"
        "Кафе не удалено из config.json и снова доступно как свободный слот."
    )


# =========================================================
# /start
# =========================================================
WELCOME_VARIANTS = [
    "👋 Рад тебя видеть, {name}!",
    "Хороший кофе начинается здесь. Что хотите заказать?",
    "{name}, добро пожаловать!",
    "👋 Привет, {name}! Заглянем за чем-то вкусным? ☕",
    "Добро пожаловать! Что будем готовить для вас сегодня?",
    "{name}, рады видеть вас снова!",
    "👋 Здравствуйте, {name}! Сегодня больше про кофе или десерты?",
    "Привет, {name}! Я помогу быстро оформить заказ.",
]

CHOICE_VARIANTS = [
    "Отличный выбор!",
    "Отличный выбор — <b>{drink}</b>! Сколько кружек готовим? 😊",
    "👍 Люблю {drink} не меньше вас. Сколько штук оформить?",
    "Берём {drink}! Напишите количество — от 1 до 5.",
    "{drink} — классика. Сколько порций для вас сегодня?",
    "Звучит вкусно!",
    "Хороший выбор. Сколько добавить?",
]

FINISH_VARIANTS = [
    "Спасибо за заказ, {name}!",
    "👍 Класс, {name}! Пока мы готовим, можно оформить ещё что-нибудь вкусное.",
    "{name}, Хорошего дня!",
    "☕ Мы приготовим заказ к выбранному времени.",
    "Спасибо, что выбрали нас, {name}! Если что-то нужно — просто напишите сюда.",
    "Принято, {name}. Заглядывайте ещё!",
]

async def send_admin_panel(
    message: Message,
    cafe_id: str,
    cafe: Dict[str, Any],
    menu: Dict[str, int],
):
    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    is_super = is_superadmin(uid)
    cafe = await apply_cafe_profile(r, cafe_id, cafe)

    if not is_super and not await is_subscription_active(r, uid, cafe_id):
        await ensure_subscription_active(message, r, cafe_id)
        return

    admin_id = await get_effective_admin_id(r, cafe_id)

    client_link = await create_start_link(
        message.bot,
        payload=cafe_id,
        encode=False,
    )

    admin_link = await create_start_link(
        message.bot,
        payload=f"admin_{cafe_id}",
        encode=False,
    )

    staff_link = await create_startgroup_link(
        message.bot,
        payload=cafe_id,
        encode=False,
    )

    profile_notice = ""

    try:
        profile = await r.hgetall(k_cafe_profile(cafe_id))

        configured_raw = (
            profile.get("profile_configured")
            or profile.get(b"profile_configured")
            or ""
        )

        if isinstance(configured_raw, bytes):
            configured_raw = configured_raw.decode("utf-8", "ignore")

        configured = str(configured_raw).strip()

        if not is_super and configured != "1":
            profile_notice = (
                "\n⚠️ <b>Профиль кафе ещё не заполнен.</b>\n"
                "Нажмите «🏪 Профиль кафе», чтобы указать название, "
                "адрес и режим работы.\n"
            )

    except Exception:
        logger.exception(
            "Не удалось проверить заполнение профиля кафе: cafe_id=%s",
            cafe_id,
        )

    try:
        sub_key = k_admin_subscription(cafe_id)
        raw_until = await r.hget(sub_key, "cafebotify_valid_until")

        if isinstance(raw_until, bytes):
            raw_until = raw_until.decode("utf-8", "ignore")

        if is_super:
            subline = "\n<b>🛠 Суперадмин (без ограничений)</b>\n"
        else:
            until_ts = int(raw_until) if raw_until else 0

            if until_ts > int(time.time()):
                until_dt = datetime.fromtimestamp(
                    until_ts,
                    tz=MSK_TZ,
                ).strftime("%d.%m.%Y")

                subline = f"\n<b>Подписка до:</b> <b>{until_dt}</b>\n"
            else:
                subline = "\n<b>❌ Подписка просрочена</b>\n"

    except Exception:
        logger.exception(
            "Не удалось получить статус подписки: cafe_id=%s",
            cafe_id,
        )
        subline = "\n<b>❌ Ошибка проверки подписки</b>\n"

    await message.answer(
        "🛠 <b>Админ-панель</b>\n\n"
        f"Кафе: <b>{html.quote(cafe_title(cafe))}</b>\n"
        f"ID: <code>{html.quote(cafe_id)}</code>\n"
        f"admin_id (effective): <code>{admin_id}</code>\n"
        f"{subline}"
        f"{profile_notice}"
        f"{work_status(cafe)}{address_line(cafe)}\n\n"
        "Рабочие ссылки доступны по кнопке «🔗 Ссылки».",
    )

from aiogram.enums import ChatType


def is_group_chat(message: Message) -> bool:
    return message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}


@router.message(CommandStart(deep_link=True))
async def cmd_start_deep(message: Message, command: CommandObject, state: FSMContext):
    await cmd_start(message, command, state)


@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, state: FSMContext):
    if is_group_chat(message):
        return

    await state.clear()
    r: redis.Redis = message.bot._redis

    # ✅ ДЕКОДИРОВАНИЕ + НОВЫЙ ПАРСИНГ
    payload_raw = (command.args or "").strip()
    payload = payload_raw
    if payload_raw:
        try:
            payload = decode_payload(payload_raw)
        except Exception:
            pass  # fallback на сырой payload

    id_from_payload, mode = parse_start_payload(payload)

    uid = message.from_user.id

    # Deep link имеет абсолютный приоритет над ранее сохранённым кафе.
    # Это критично после /bind_paid_draft: клиент мог ранее зайти
    # в START через ссылку другого кафе как обычный посетитель.
    if id_from_payload and id_from_payload in CAFES:
        cafe_id = id_from_payload

        # Сохраняем новый контекст пользователя: после перехода
        # по admin_cafe_002 дальнейшее меню и кнопки относятся к cafe_002.
        await r.set(k_user_cafe(uid), cafe_id)

    else:
        # Обычный /start без валидной deep-link ссылки:
        # используем прежнюю логику выбора кафе.
        cafe_id = await resolve_cafe_id(r, message, id_from_payload)

    parts = (payload or "").split(":")
    if len(parts) == 3 and parts[0] == "bc":
        bc_cafe_id = parts[1].strip()
        broadcast_id = parts[2].strip()

        if bc_cafe_id and broadcast_id:
            try:
                await track_broadcast_click(
                    r=r,
                    user_id=uid,
                    cafe_id=bc_cafe_id,
                    broadcast_id=broadcast_id,
                )
            except Exception:
                logger.exception(
                    "broadcast click tracking failed user_id=%s cafe_id=%s broadcast_id=%s",
                    uid,
                    bc_cafe_id,
                    broadcast_id,
                )

    cafe = cafe_or_default(cafe_id)
    cafe = await apply_cafe_profile(r, cafe_id, cafe)

    menu = await get_menu(r, cafe_id)

    name = html.quote(user_name(message))
    welcome = random.choice(WELCOME_VARIANTS).format(name=name)

    is_admin = await is_cafe_admin(r, uid, cafe_id)
    view_mode = str(await r.get(k_view_mode(uid)) or "admin")

    # deep-link admin/super: если есть права — принудительно админка
    if mode in ("admin", "super"):
        if not is_admin:
            await message.answer("🔒 Админ-доступ запрещён.")
            return

    # Обычный админ не должен получать админ-ссылку
    # при просроченной подписке. Суперадмин проходит всегда.
        if is_superadmin(uid) or await is_subscription_active(r, uid, cafe_id):
            try:
                await send_cafe_links_once(
                    bot=message.bot,
                    r=r,
                    user_id=uid,
                    cafe_id=cafe_id,
                )
            except Exception:
                logger.exception(
                    "Не удалось отправить стартовые ссылки: "
                    "cafe_id=%s user_id=%s",
                    cafe_id,
                    uid,
                )

        await r.set(k_view_mode(uid), "admin")
        await send_admin_panel(message, cafe_id, cafe, menu)
        return

    # ✅ НОВАЯ ЛОГИКА с проверкой подписки + СУПЕРАДМИН
    if is_admin:
        if is_superadmin(uid):
            try:
                await send_cafe_links_once(
                    bot=message.bot,
                    r=r,
                    user_id=uid,
                    cafe_id=cafe_id,
                )
            except Exception:
                logger.exception(
                    "Не удалось отправить стартовые ссылки суперадмину: "
                    "cafe_id=%s user_id=%s",
                    cafe_id,
                    uid,
                )

            await r.set(k_view_mode(uid), "admin")
            await send_admin_panel(message, cafe_id, cafe, menu)
            return
        
        # Обычный админ — проверяем подписку
        try:
            sub_key = k_admin_subscription(cafe_id)
            raw_until = await r.hget(sub_key, "cafebotify_valid_until")
            until_ts = int(raw_until) if raw_until else 0
            
            if until_ts > 0 and until_ts > int(time.time()):
            # Подписка активна.
                try:
                    await send_cafe_links_once(
                        bot=message.bot,
                        r=r,
                        user_id=uid,
                        cafe_id=cafe_id,
                    )
                except Exception:
                    logger.exception(
                        "Не удалось отправить стартовые ссылки админу: "
                        "cafe_id=%s user_id=%s",
                        cafe_id,
                        uid,
                    )

                await r.set(k_view_mode(uid), "admin")
                await send_admin_panel(message, cafe_id, cafe, menu)
                return
            else:
                # Подписка просрочена — кнопка продления
                renew_kb = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text=BTN_RENEW_30)],  # ✅ ИСПРАВЛЕНО: BTN_RENEW30
                        [KeyboardButton(text=BTN_RENEW_360)], # ✅ ИСПРАВЛЕНО: BTN_RENEW360
                        [KeyboardButton(text=BTN_BACK)],
                    ],
                    resize_keyboard=True,
                    one_time_keyboard=True,
                )
                await message.answer(
                    f"🔒 <b>{cafe_title(cafe)}</b>\\n\\n"
                    "❌ Подписка просрочена.\n"
                    "Оплатите для доступа к админ-панели:",
                    reply_markup=renew_kb,
                )
                return
        except Exception:
            # Ошибка проверки — показываем клиентский интерфейс
            pass

    # дальше клиентский сценарий (без изменений)
    offer_repeat = await should_offer_repeat(r, cafe_id, uid)
    await set_last_seen(r, cafe_id, uid)

    customer_key = k_customer_profile(cafe_id, uid)
    if await r.exists(customer_key):
        try:
            await r.hset(
                customer_key,
                mapping={
                    "return_stage_7_sent_at": 0,
                    "return_stage_14_sent_at": 0,
                },
            )
        except Exception:
            pass

    if not cafe_open(cafe):
        await message.answer(
            closed_message(cafe, menu),
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    if offer_repeat:
        snap = await get_last_order_snapshot(r, cafe_id, uid)
        if snap and isinstance(snap.get("cart"), dict) and snap.get("cart"):
            lines = []
            for d, q in snap["cart"].items():
                try:
                    lines.append(f"• {html.quote(str(d))} × {int(q)}")
                except Exception:
                    continue
            await state.update_data(repeat_offer_snapshot=snap, cafe_id=cafe_id)
            await message.answer(
                f"{welcome}\n\nХотите повторить ваш прошлый заказ?\n\n" + "\n".join(lines),
                reply_markup=kb_repeat_offer(),
            )
            return

    await message.answer(
        f"{welcome}\n\n"
        f"🏪 <b>{html.quote(cafe_title(cafe))}</b>\n"
        f"{work_status(cafe)}"
        f"{address_line(cafe)}\n\n"
        "Чтобы добавить в корзину: нажмите напиток → выберите количество.\n"
        "Корзина — «🛒 Корзина».",
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )


# =========================================================
# Client: repeat
# =========================================================
@router.message(F.text == BTN_REPEAT_NO)
async def repeat_no(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    await state.update_data(repeat_offer_snapshot=None)
    await message.answer("Ок.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))


@router.message(F.text == BTN_REPEAT_LAST)
async def repeat_last(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    data = await state.get_data()
    snap = data.get("repeat_offer_snapshot") or await get_last_order_snapshot(r, cafe_id, message.from_user.id)

    if not snap or not isinstance(snap.get("cart"), dict) or not snap.get("cart"):
        await message.answer("Не нашёл последний заказ.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    cart = {}
    for k, v in snap["cart"].items():
        try:
            cart[str(k)] = int(v)
        except Exception:
            continue

    filtered = {d: q for d, q in cart.items() if d in menu and q > 0}
    if not filtered:
        await message.answer(
            "Позиции из прошлого заказа сейчас отсутствуют в меню.",
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    await state.update_data(cart=filtered)
    await show_cart(message, state)


# =========================================================
# Admin: renew subscription (point 5) — real paths
# =========================================================
@router.message(F.text == BTN_RENEW_SUB)
async def renew_sub_entry(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("🔒 Доступно только администратору.")
        return

    if not DEMO_PAY_BASE:
        await message.answer("⚙️ DEMO_PAY_BASE не настроен.")
        return

    await message.answer("Выберите срок продления:", reply_markup=kb_renew_sub())


@router.message(F.text.in_({BTN_RENEW_30, BTN_RENEW_360}))
async def renew_sub_choose(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    uid = message.from_user.id

    if not await is_cafe_admin(r, uid, cafe_id):
        await message.answer("🔒 Доступно только администратору.")
        return

    if not DEMO_PAY_BASE:
        await message.answer("⚙️ DEMO_PAY_BASE не настроен.")
        return

    if message.text == BTN_RENEW_360:
        days = 360
        path = "pay-year"
        btn_text = "💳 Оплатить 360 дней"
    else:
        days = 30
        path = "pay-month"
        btn_text = "💳 Оплатить 30 дней"

    pay_base = DEMO_PAY_BASE.rstrip("/")
    pay_url = f"{pay_base}/{path}?cafe_id={cafe_id}&admin_id={uid}"

    admin_link = await create_start_link(
        message.bot,
        payload=f"admin:{cafe_id}",
        encode=True,
    )

    pay_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=btn_text, url=pay_url)],
            [InlineKeyboardButton(text="🛠 Открыть админ-панель", url=admin_link)],
        ]
    )

    await message.answer(
        f"💳 <b>Продление подписки</b>\n\n"
        f"Кафе: <code>{html.quote(cafe_id)}</code>\n"
        f"Тариф: <b>{days} дней</b>\n\n"
        "1) Нажми кнопку оплаты.\n"
        "2) После оплаты вернись в админ-панель по кнопке ниже.",
        reply_markup=pay_kb,
        disable_web_page_preview=True,
    )


from aiogram.filters import StateFilter
from aiogram import F

@router.message(StateFilter(None), F.text == BTN_BACK)
async def back_from_renew_sub(message: Message):
    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    is_admin = await is_cafe_admin(r, uid, cafe_id)
    view_mode = str(await r.get(k_view_mode(uid)) or "admin")  # "admin" | "client"

    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    if is_admin and view_mode != "client":
        await send_admin_panel(message, cafe_id, cafe, menu)
        return

    await message.answer("Хорошо 😊 Выберите что-нибудь из меню.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))


# =========================================================
# Client: info
# =========================================================
@router.message(F.text == BTN_CALL)
async def call_phone(message: Message):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    await message.answer(
        f"📞 <b>Телефон:</b> <code>{html.quote(cafe_phone(cafe))}</code>",
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )


@router.message(F.text == BTN_HOURS)
async def show_hours(message: Message):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    msk_time = get_moscow_time().strftime("%H:%M")
    await message.answer(
        f"🕐 <b>Сейчас:</b> {msk_time} (МСК)\n{work_status(cafe)}{address_line(cafe)}",
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )


# =========================================================
# Client: cart show/clear/cancel
# =========================================================
async def show_cart(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)

    cart = get_cart(await state.get_data())
    await state.set_state(OrderStates.cart_view)
    await state.update_data(cart=cart)
    await message.answer(cart_text(cart, menu), reply_markup=kb_cart(menu, bool(cart)))


@router.message(F.text == BTN_CART)
async def cart_button(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if not cafe_open(cafe):
        await message.answer(
            closed_message(cafe, menu),
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return
    await show_cart(message, state)


@router.message(F.text == BTN_CLEAR_CART)
async def clear_cart(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    await state.update_data(cart={})
    await show_cart(message, state)


@router.message(F.text == BTN_CANCEL_ORDER)
async def cancel_order(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    await state.clear()
    await message.answer("❌ Заказ отменён.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))


# =========================================================
# Client: cart edit
# =========================================================
@router.message(F.text == BTN_EDIT_CART)
async def edit_cart(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    cart = get_cart(await state.get_data())
    if not cart:
        r: redis.Redis = message.bot._redis
        cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
        menu = await get_menu(r, cafe_id)
        is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

        await message.answer("Корзина пустая.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return
    await state.set_state(OrderStates.cart_edit_pick_item)
    await message.answer("Выберите позицию:", reply_markup=kb_cart_pick_item(cart))


@router.message(StateFilter(OrderStates.cart_edit_pick_item))
async def pick_item_to_edit(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    text = (message.text or "").strip()
    if text in {BTN_CANCEL, BTN_CART}:
        await show_cart(message, state)
        return

    cart = get_cart(await state.get_data())
    if text not in cart:
        await message.answer("Выберите позицию кнопкой.", reply_markup=kb_cart_pick_item(cart))
        return

    await state.set_state(OrderStates.cart_edit_pick_action)
    await state.update_data(edit_item=text)
    await message.answer(f"Что сделать с <b>{html.quote(text)}</b>?", reply_markup=kb_cart_edit_actions())


@router.message(StateFilter(OrderStates.cart_edit_pick_action))
async def cart_edit_action(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    action = (message.text or "").strip()
    if action == BTN_CANCEL:
        await show_cart(message, state)
        return

    data = await state.get_data()
    cart = get_cart(data)
    item = str(data.get("edit_item") or "")

    if action == CART_ACT_DONE:
        await show_cart(message, state)
        return

    if not item or item not in cart:
        await show_cart(message, state)
        return

    if action == CART_ACT_PLUS:
        cart[item] = int(cart.get(item, 0)) + 1
    elif action == CART_ACT_MINUS:
        cart[item] = int(cart.get(item, 0)) - 1
        if cart[item] <= 0:
            cart.pop(item, None)
    elif action == CART_ACT_DEL:
        cart.pop(item, None)
    else:
        await message.answer("Выберите действие кнопкой.", reply_markup=kb_cart_edit_actions())
        return

    await state.update_data(cart=cart)
    await show_cart(message, state)


@router.message(F.text == BTN_BROADCAST)
async def broadcast_entry_message(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("Нет доступа.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    await state.clear()
    await state.set_state(BroadcastStates.waiting_for_action)
    await show_broadcast_menu(message, r, cafe_id)


@router.message(StateFilter(BroadcastStates.waiting_for_action))
async def broadcast_choose_action_message(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    text = (message.text or "").strip()
    draft = await get_broadcast_draft(r, cafe_id)

    if text in (BROADCAST_BACK, BTN_BACK):
        await state.clear()
        await clear_broadcast_draft(r, cafe_id)
        await message.answer(".", reply_markup=kb_admin_main(is_superadmin(uid)))
        return

    if text == BROADCAST_EDIT_TEXT:
        await state.set_state(BroadcastStates.waiting_for_text)
        await message.answer("Введите текст рассылки.", reply_markup=kb_broadcast_input())
        return

    if text == BROADCAST_EDIT_URL:
        await state.set_state(BroadcastStates.waiting_for_url)
        await message.answer(
            "Введите ссылку https://... или нажмите «Пропустить».",
            reply_markup=kb_broadcast_input(),
            disable_web_page_preview=True,
        )
        return

    if text == BROADCAST_STATS:
        last_id = str(await r.get(k_broadcast_last(cafe_id)) or "").strip()
        if not last_id:
            await message.answer("Рассылок пока не было.", reply_markup=kb_broadcast_manage())
            return

        meta = await get_broadcast_meta(r, cafe_id, last_id)
        stats = await get_broadcast_stats(r, cafe_id, last_id)
        await message.answer(
            broadcast_stats_text(cafe_id, last_id, meta, stats),
            reply_markup=kb_broadcast_manage(),
        )
        return

    if text == BROADCAST_SEND:
        if not draft.get("text"):
            await message.answer("Сначала добавьте текст рассылки.", reply_markup=kb_broadcast_manage())
            return

        active_id = str(await r.get(k_broadcast_active(cafe_id)) or "").strip()
        if active_id:
            meta = await get_broadcast_meta(r, cafe_id, active_id)
            if str(meta.get("status") or "") == "running":
                await message.answer(
                    f"Уже идёт рассылка <code>{html.quote(active_id)}</code>.",
                    reply_markup=kb_broadcast_manage(),
                )
                return

        left = await get_broadcast_weekly_left(r, cafe_id)
        if left > 0:
            await message.answer(
                "Рассылку можно запускать только 1 раз в 7 дней.\n"
                f"До следующего запуска осталось: <b>{html.quote(format_broadcast_wait(left))}</b>.",
                reply_markup=kb_broadcast_manage(),
            )
            return

        broadcast_id = await create_broadcast(
            r=r,
            cafe_id=cafe_id,
            created_by=uid,
            text=str(draft.get("text") or ""),
            url=str(draft.get("url") or ""),
        )
        await clear_broadcast_draft(r, cafe_id)

        asyncio.create_task(run_broadcast_send(message.bot, cafe_id, broadcast_id))

        await message.answer(
            f"🚀 Рассылка <code>{html.quote(broadcast_id)}</code> запущена.",
            reply_markup=kb_broadcast_manage(),
        )
        return

    await message.answer("Выберите действие.", reply_markup=kb_broadcast_manage())


@router.message(StateFilter(BroadcastStates.waiting_for_text))
async def broadcast_set_text_message(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    text = (message.text or "").strip()

    if text in (BROADCAST_BACK, BTN_BACK):
        await state.set_state(BroadcastStates.waiting_for_action)
        await show_broadcast_menu(message, r, cafe_id)
        return

    if not text:
        await message.answer("Текст не должен быть пустым.", reply_markup=kb_broadcast_input())
        return

    if len(text) > 3000:
        await message.answer("Слишком длинно. До 3000 символов.", reply_markup=kb_broadcast_input())
        return

    draft = await get_broadcast_draft(r, cafe_id)
    draft["text"] = text
    await set_broadcast_draft(r, cafe_id, draft)

    await state.set_state(BroadcastStates.waiting_for_action)
    await show_broadcast_menu(message, r, cafe_id)


@router.message(StateFilter(BroadcastStates.waiting_for_url))
async def broadcast_set_url_message(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    text = (message.text or "").strip()

    if text in (BROADCAST_BACK, BTN_BACK):
        await state.set_state(BroadcastStates.waiting_for_action)
        await show_broadcast_menu(message, r, cafe_id)
        return

    draft = await get_broadcast_draft(r, cafe_id)

    if text == BROADCAST_CANCEL:
        draft["url"] = ""
        await set_broadcast_draft(r, cafe_id, draft)
        await state.set_state(BroadcastStates.waiting_for_action)
        await show_broadcast_menu(message, r, cafe_id)
        return

    if not is_valid_http_url(text):
        await message.answer(
            "Нужна ссылка, начинающаяся с http:// или https://",
            reply_markup=kb_broadcast_input(),
        )
        return

    draft["url"] = text
    await set_broadcast_draft(r, cafe_id, draft)

    await state.set_state(BroadcastStates.waiting_for_action)
    await show_broadcast_menu(message, r, cafe_id)


# =========================================================
# Client: add item
# =========================================================
async def start_add_item(message: Message, state: FSMContext, cafe_id: str, menu: Dict[str, int], drink: str):
    if is_group_chat(message):
        return

    price = menu.get(drink)
    if price is None:
        r: redis.Redis = message.bot._redis
        is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)
        await message.answer(
            "Не нашёл такую позицию в меню.",
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    cart = get_cart(await state.get_data())
    await state.set_state(OrderStates.waiting_for_quantity)
    await state.update_data(current_drink=drink, cart=cart)

    choice_text = random.choice(CHOICE_VARIANTS)
    try:
        choice_text = choice_text.format(drink=html.quote(drink))
    except Exception:
        pass

    await message.answer(
        f"{choice_text}\n\n"
        f"🥤 <b>{html.quote(drink)}</b>\n"
        f"💰 {int(price)}₽\n\n"
        "Сколько добавить?",
        reply_markup=kb_qty(),
    )


@router.message(StateFilter(OrderStates.waiting_for_quantity))
async def process_quantity(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if message.text == BTN_CANCEL:
        cart = get_cart(await state.get_data())
        await message.answer(
            "Ок.",
            reply_markup=kb_cart(menu, bool(cart)) if cart else kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    try:
        qty = int((message.text or "")[0])
        if not (1 <= qty <= 5):
            raise ValueError
    except Exception:
        await message.answer("Нажмите 1–5.", reply_markup=kb_qty())
        return

    data = await state.get_data()
    drink = str(data.get("current_drink") or "")
    cart = get_cart(data)

    if not drink or drink not in menu:
        await state.clear()
        await message.answer("Ошибка. Нажмите /start.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    cart[drink] = int(cart.get(drink, 0)) + qty
    await state.update_data(cart=cart)
    await state.set_state(OrderStates.cart_view)

    await message.answer(
        f"✅ Добавил в корзину: <b>{html.quote(drink)}</b> × {qty}\n\n{cart_text(cart, menu)}",
        reply_markup=kb_cart(menu, True),
    )


# =========================================================
# Client: checkout
# =========================================================
@router.message(F.text == BTN_CHECKOUT)
async def checkout(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    user_id = message.from_user.id

    cafe_id = str(
        await r.get(k_user_cafe(user_id))
        or DEFAULT_CAFE_ID
    )
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, user_id, cafe_id)

    if not cafe_open(cafe):
        await message.answer(
            closed_message(cafe, menu),
            reply_markup=kb_client_main(
                menu,
                show_admin_button=is_admin,
            ),
        )
        return

    cart = get_cart(await state.get_data())
    if not cart:
        await message.answer(
            "Корзина пустая.",
            reply_markup=kb_client_main(
                menu,
                show_admin_button=is_admin,
            ),
        )
        return

    # До первого оформления заказа в этом кафе просим пользователя
    # явно подтвердить ознакомление с политикой и обработку данных.
    if not await has_order_pd_consent(r, cafe_id, user_id):
        await state.set_state(OrderStates.waiting_for_confirmation)

        await message.answer(
            "🔐 <b>Перед оформлением заказа</b>\n\n"
            "Бот использует ваш Telegram ID, имя или username, состав заказа "
            "и выбранное время готовности, чтобы передать заказ выбранному кафе "
            "и обеспечить его приготовление и выдачу.\n\n"
            "Нажимая «Согласен(на)», вы подтверждаете ознакомление с Политикой "
            "обработки персональных данных и соглашаетесь на обработку данных "
            "для оформления и исполнения заказа.",
            reply_markup=kb_pd_consent(),
        )
        return

    await show_order_confirmation_after_consent(
        message,
        state,
        cafe_id=cafe_id,
    )


@router.message(StateFilter(OrderStates.waiting_for_confirmation))
async def confirm_order(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if message.text == BTN_CANCEL_ORDER:
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    if message.text == BTN_CART:
        await show_cart(message, state)
        return

    if message.text != BTN_CONFIRM:
        await message.answer("Нажмите «Подтвердить».", reply_markup=kb_confirm())
        return

    await state.set_state(OrderStates.waiting_for_ready_time)
    await message.answer("Когда забрать?", reply_markup=kb_ready_time())


async def send_promo_after_delay(
    bot: Bot,
    user_id: int,
    cafe_id: str,
    delay_sec: int = 60,
):
    logger.info(
        "PROMO task started: cafe_id=%s user_id=%s delay_sec=%s",
        cafe_id,
        user_id,
        delay_sec,
    )

    try:
        await asyncio.sleep(delay_sec)

        logger.info(
            "PROMO delay finished: cafe_id=%s user_id=%s",
            cafe_id,
            user_id,
        )

        r: redis.Redis = bot._redis
        promo = normalize_promo(await get_cafe_promo(r, cafe_id))

        text = str(promo.get("text") or "").strip()
        photo_file_id = str(promo.get("photo_file_id") or "").strip()
        url = str(promo.get("url") or "").strip()
        button_text = (
            str(promo.get("button_text") or "").strip()
            or "Подробнее"
        )

        logger.info(
            "PROMO check: cafe_id=%s user_id=%s enabled=%s "
            "has_text=%s has_photo=%s has_url=%s",
            cafe_id,
            user_id,
            promo.get("enabled"),
            bool(text),
            bool(photo_file_id),
            bool(url),
        )

        if not promo.get("enabled"):
            logger.info(
                "PROMO skipped: disabled cafe_id=%s user_id=%s",
                cafe_id,
                user_id,
            )
            return

        if not text and not photo_file_id:
            logger.warning(
                "PROMO skipped: no text or photo "
                "cafe_id=%s user_id=%s",
                cafe_id,
                user_id,
            )
            return

        reply_markup = None

        if url:
            reply_markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=button_text,
                            url=url,
                        )
                    ]
                ]
            )

        if photo_file_id:
            await bot.send_photo(
                chat_id=user_id,
                photo=photo_file_id,
                caption=text or None,
                reply_markup=reply_markup,
            )
        else:
            await bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )

        logger.info(
            "PROMO sent: cafe_id=%s user_id=%s",
            cafe_id,
            user_id,
        )

    except Exception:
        logger.exception(
            "PROMO failed: cafe_id=%s user_id=%s",
            cafe_id,
            user_id,
        )


async def finalize_order(message: Message, state: FSMContext, ready_in_min: int):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    user_id = message.from_user.id
    cart = get_cart(await state.get_data())
    if not cart:
        await state.clear()
        await message.answer(
            "Корзина пока пустая.",
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    rl = cafe_rate_limit_seconds(cafe)
    last_order = await r.get(k_rate_limit(user_id))
    if last_order and time.time() - float(last_order) < rl:
        await message.answer(
            f"⏳ Подождите {rl} секунд между заказами.",
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        await state.clear()
        return
    await r.setex(k_rate_limit(user_id), rl, str(time.time()))

    total = cart_total(cart, menu)
    order_num = str(int(time.time()))[-6:]
    ready_at_str = (get_moscow_time() + timedelta(minutes=max(0, ready_in_min))).strftime("%H:%M")
    ready_line = "как можно скорее" if ready_in_min <= 0 else f"через {ready_in_min} мин (к {ready_at_str} МСК)"

    await set_last_order_snapshot(
        r,
        cafe_id,
        user_id,
        {"cart": cart, "total": total, "ts": int(time.time())},
    )

    await r.incr(k_stats_total_orders(cafe_id))
    await r.incrby(k_stats_total_revenue(cafe_id), int(total))
    for drink, qty in cart.items():
        qty_i = int(qty)
        price = int(menu.get(drink, 0))
        await r.incrby(k_stats_drink_cnt(cafe_id, drink), qty_i)
        await r.incrby(k_stats_drink_rev(cafe_id, drink), qty_i * price)

    try:
        await track_broadcast_order(
            r=r,
            user_id=user_id,
            order_cafe_id=cafe_id,
            total=int(total),
        )
    except Exception:
        logger.exception(
            "broadcast order attribution failed user_id=%s cafe_id=%s total=%s",
            user_id,
            cafe_id,
            total,
        )

    try:
        await customer_mark_order(
            r,
            cafe_id,
            user_id=user_id,
            first_name=(message.from_user.first_name or ""),
            username=(message.from_user.username or ""),
            cart=cart,
            total_sum=total,
        )
    except Exception:
        pass

    username = (message.from_user.username or "").strip()
    client_name = (
        message.from_user.full_name
        or message.from_user.first_name
        or "Клиент"
    )

    if username:
        client_link = f"https://t.me/{username}"
        client_label = f"@{html.quote(username)}"
    else:
    # Запасной вариант для пользователей без username.
        client_link = f"tg://user?id={user_id}"
        client_label = html.quote(client_name)

    admin_msg = (
        f"🔔 <b>НОВЫЙ ЗАКАЗ #{order_num}</b> | "
        f"{html.quote(cafe_title(cafe))}\n\n"
        f"👤 <a href=\"{client_link}\">{client_label}</a>\n"
        f"<code>{user_id}</code>\n\n"
        f"✍️ <a href=\"{client_link}\">Написать клиенту</a>\n\n"
        + "\n".join(cart_lines(cart, menu))
        + (
            f"\n\n💰 Итого: <b>{total}₽</b>"
            f"\n⏱ Готовность: <b>{html.quote(ready_line)}</b>"
        )
    )

    created_at_ts = int(time.time())

    # Для «как можно скорее» считаем целевую готовность через 15 минут.
    # Для «через N минут» используем выбранное клиентом время.
    effective_ready_in_min = ready_in_min if ready_in_min > 0 else 15
    ready_at_ts = created_at_ts + effective_ready_in_min * 60

    # Более удобный случайный идентификатор — не только последние цифры времени.
    order_id = make_active_order_id()

    # Переписываем заголовок в уже сформированном сообщении:
    # клиент и персонал будут видеть единый ID заказа.
    admin_msg = admin_msg.replace(
        f"НОВЫЙ ЗАКАЗ #{order_num}",
        f"НОВЫЙ ЗАКАЗ #{order_id}",
        1,
    )

    await create_active_order(
        r,
        cafe_id=cafe_id,
        order_id=order_id,
        created_at=created_at_ts,
        ready_at=ready_at_ts,
        order_text=admin_msg,
    )

    await send_order_card(
        message.bot,
        r,
        cafe_id=cafe_id,
        order_id=order_id,
        text=admin_msg,
    )

    # Демонстрационный режим для клиента оставляем прежним:
    # это не рабочая staff-карточка и не должен иметь кнопку «Выполнен».
    await send_admin_demo_to_user(message.bot, user_id, admin_msg)

    finish = random.choice(FINISH_VARIANTS)
    try:
        finish = finish.format(name=html.quote(user_name(message)))
    except Exception:
        pass

    await message.answer(
        "🎉 <b>Заказ принят!</b>\n\n"
        f"🏪 <b>{html.quote(cafe_title(cafe))}</b>\n"
        f"{address_line(cafe)}\n\n"
        f"{cart_text(cart, menu)}\n\n"
        f"⏱ <b>Готовность:</b> {html.quote(ready_line)}\n\n"
        f"{finish}",
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )

    logger.info(
        "PROMO scheduling: cafe_id=%s user_id=%s delay_sec=30",
        cafe_id,
        user_id,
    )
    
    asyncio.create_task(
        send_promo_after_delay(
            bot=message.bot,
            user_id=user_id,
            cafe_id=cafe_id,
            delay_sec=30,
        )
    )
        
    await state.clear()


@router.message(StateFilter(OrderStates.waiting_for_ready_time))
async def ready_time(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    if message.text == BTN_CANCEL:
        await show_cart(message, state)
        return
    if message.text == BTN_READY_NOW:
        await finalize_order(message, state, 0)
        return
    if message.text == BTN_READY_20:
        await finalize_order(message, state, 20)
        return
    await message.answer("Выберите кнопкой.", reply_markup=kb_ready_time())


# =========================================================
# Booking (allowed in non-working hours)
# =========================================================
@router.message(F.text == BTN_BOOKING)
async def booking_start(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    await state.clear()
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    warn = ""
    if not cafe_open(cafe):
        ws, _ = cafe_hours(cafe)
        warn = (
            "\n\n⚠️ <b>Сейчас кафе закрыто.</b>\n"
            f"Администратор увидит заявку и ответит с {ws}:00 по МСК."
        )

    await state.set_state(BookingStates.waiting_for_datetime)
    await message.answer(
        "📅 <b>Давайте забронируем столик</b>\n\n"
        "Напишите дату и время в формате <code>15.02 19:00</code>.\n"
        "Если планы поменялись — просто нажмите «Отмена»." + warn,
        reply_markup=kb_booking_cancel(),
    )


@router.message(StateFilter(BookingStates.waiting_for_datetime))
async def booking_datetime(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer(
            "Окей, ничего не бронируем 😊",
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    m = re.match(r"^\s*(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})\s*$", message.text or "")
    if not m:
        await message.answer(
            "Чуть-чуть не попали в формат 🙈\n"
            "Пример: <code>15.02 19:00</code>",
            reply_markup=kb_booking_cancel(),
        )
        return

    day, month, hour, minute = map(int, m.groups())
    year = get_moscow_time().year
    try:
        dt = datetime(year, month, day, hour, minute, tzinfo=MSK_TZ)
    except Exception:
        await message.answer(
            "Похоже, такая дата/время невозможны.\n"
            "Попробуйте ещё раз, например: <code>15.02 19:00</code>.",
            reply_markup=kb_booking_cancel(),
        )
        return

    await state.update_data(booking_dt=dt.strftime("%d.%m %H:%M"))
    await state.set_state(BookingStates.waiting_for_people)
    await message.answer(
        "Супер! На сколько гостей готовим столик? (от 1 до 10)",
        reply_markup=kb_booking_people(),
    )


@router.message(StateFilter(BookingStates.waiting_for_people))
async def booking_people(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer(
            "Окей, бронирование отменил.",
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    try:
        people = int((message.text or "").strip())
        if not (1 <= people <= 10):
            raise ValueError
    except Exception:
        await message.answer(
            "Нужно число от 1 до 10.\n"
            "Например: <code>2</code> или <code>6</code>.",
            reply_markup=kb_booking_people(),
        )
        return

    await state.update_data(booking_people=people)
    await state.set_state(BookingStates.waiting_for_comment)
    await message.answer(
        "Если есть пожелания (окно, розетка, детский стул и т.п.) — напишите их.\n"
        "Укажите номер телефона для связи.\n"
        "Если без комментариев — отправьте <code>-</code>.",
        reply_markup=kb_booking_cancel(),
    )


@router.message(StateFilter(BookingStates.waiting_for_comment))
async def booking_finish(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer(
            "Окей, бронирование отменил.",
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    data = await state.get_data()
    dt_str = str(data.get("booking_dt") or "—")
    people = int(data.get("booking_people") or 0)
    comment = (message.text or "").strip() or "-"

    booking_id = str(int(time.time()))[-6:]
    user_id = message.from_user.id

    admin_msg = (
        f"📋 <b>НОВАЯ БРОНЬ #{booking_id}</b> | {html.quote(cafe_title(cafe))}\n\n"
        f"<a href=\"tg://user?id={user_id}\">"
        f"{html.quote(message.from_user.username or message.from_user.first_name or 'Клиент')}</a>\n"
        f"<code>{user_id}</code>\n\n"
        f"🗓 {html.quote(dt_str)}\n"
        f"👥 {people} чел.\n"
        f"💬 {html.quote(comment)}\n\n"
        f"✍️ <a href=\"tg://user?id={user_id}\">Написать клиенту</a>"
    )
    await notify_admin(message.bot, r, cafe_id, admin_msg)

    if cafe_open(cafe):
        user_text = (
            "✅ Я передал вашу заявку администратору кафе.\n"
            "Он свяжется с Вами, чтобы подтвердить бронь."
        )
    else:
        ws, _ = cafe_hours(cafe)
        user_text = (
            "✅ Заявка на бронь принята.\n\n"
            "⚠️ Сейчас кафе закрыто, поэтому администратор ответит уже в рабочее время "
            f"(с {ws}:00 по МСК)."
        )

    await message.answer(
        user_text,
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )
    await state.clear()


# =========================================================
# =========================================================
# Admin buttons
# =========================================================
def demo_stats_preview_text() -> str:
    return "📊 <b>Статистика (DEMO-пример)</b>\n\nВсего заказов: <b>128</b>\nВыручка всего: <b>34 560₽</b>"

def demo_menu_edit_preview_text() -> str:
    return "🛠 <b>Управление меню (DEMO-пример)</b>\n\nИзменения доступны только администратору."

@router.message(F.text == BTN_VIEW_CLIENT)
async def back_to_client(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id

    raw_cafe_id = await r.get(k_user_cafe(uid))
    if isinstance(raw_cafe_id, bytes):
        raw_cafe_id = raw_cafe_id.decode("utf-8", "ignore")

    cafe_id = str(raw_cafe_id or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    cafe = await apply_cafe_profile(r, cafe_id, cafe)

    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, uid, cafe_id)

    await r.set(k_view_mode(uid), "client")
    await state.clear()

    if not cafe_open(cafe):
        await message.answer(
            closed_message(cafe, menu),
            reply_markup=kb_client_main(menu, show_admin_button=is_admin),
        )
        return

    await message.answer(
        "Ок, переключил в клиентский режим.\n\n"
        f"🏪 <b>{html.quote(cafe_title(cafe))}</b>\n"
        f"{work_status(cafe)}"
        f"{address_line(cafe)}\n\n"
        "Чтобы добавить в корзину: нажмите напиток → выберите количество.\n"
        "Корзина — «🛒 Корзина».",
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )

@router.message(F.text == BTN_VIEW_ADMIN)
async def back_to_admin(message: Message):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    await r.set(k_view_mode(message.from_user.id), "admin")
    await message.answer("Ок. Переключил в админ-режим.\nНажмите /start, чтобы открыть админ-панель.")


@router.message(F.text == BTN_LINKS)
async def admin_links_button(message: Message):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("🔒 Доступно только администратору.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    await send_admin_panel(message, cafe_id, cafe, menu)


@router.message(F.text == BTN_ADMIN_INFO)
async def admin_info_button_message(message: Message):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("Нет доступа.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    await message.answer(
        "🧾 <b>Справка админа кафе</b>\n"
        "• «Статистика» — покажет продажи и выручку.\n"
        "• «Меню» — добавление/изменение/удаление позиций.\n"
        "• «Группа персонала» — привязка staff-группы.\n"
        "• «Ссылки» — ссылки для клиента/админа/staff.\n"
        "• «Реклама» — возможность коммерческой рекламы или рекламы своего кафе.\n"
        "• «Продлить» — продление подписки.\n\n"
        "Подробнее о сервисе — на сайте:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="CafeBotify", url="https://cafebotify.tilda.ws")]
            ]
        ),
        disable_web_page_preview=True,
    )


@router.message(F.text == BTN_SUB_INFO)
async def sub_info_button_message(message: Message):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id

    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)
    if not await is_cafe_admin(r, uid, cafe_id):
        await message.answer("Эта информация доступна только админам кафе.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    sub_key = k_admin_subscription(cafe_id)
    rawuntil = await r.hget(sub_key, "cafebotify_valid_until")
    untilts = int(rawuntil) if rawuntil else 0

    if untilts <= 0:
        await message.answer(
            "⏳ <b>Срок действия подписки</b>\n"
            "Подписка не активна.\n\n"
            "Чтобы продлить: нажми <b>💳 Продлить подписку</b>.",
            reply_markup=kb_admin_main(is_superadmin(uid)),
        )
        return

    untildt = datetime.fromtimestamp(untilts, tz=MSK_TZ)
    days_left = (untildt.date() - get_moscow_time().date()).days
    left_line = f"Осталось: <b>{days_left}</b> дн." if days_left >= 0 else "Подписка истекла."

    await message.answer(
        "⏳ <b>Срок действия подписки</b>\n"
        f"До: <b>{untildt.strftime('%d.%m.%Y')}</b>\n"
        f"{left_line}",
        reply_markup=kb_admin_main(is_superadmin(uid)),
    )


@router.message(F.text == BTN_STAFF_GROUP)
async def admin_staff_group_button(message: Message):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("🔒 Доступно только администратору.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)
    gid = await r.get(k_staff_group(cafe_id))
    gid_line = f"Текущая группа: <code>{gid}</code>\n\n" if gid else "Группа ещё не привязана.\n\n"

    await message.answer(
        "👥 <b>Группа персонала</b>\n\n"
        f"{gid_line}"
        "1) Создайте группу.\n"
        "2) Добавьте в неё бота по ссылке:\n"
        f"{staff_link}\n\n"
        f"3) В группе выполните:\n<code>/bind {html.quote(cafe_id)}</code>\n",
        disable_web_page_preview=True,
    )


@router.message(F.text == BTN_STATS)
async def stats_button(message: Message):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        if DEMO_MODE:
            await message.answer(demo_stats_preview_text())
        else:
            await message.answer("📊 Статистика доступна администратору.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    menu = await get_menu(r, cafe_id)
    total_orders = int(await r.get(k_stats_total_orders(cafe_id)) or 0)
    total_rev = int(await r.get(k_stats_total_revenue(cafe_id)) or 0)

    lines = []
    for drink in menu.keys():
        cnt = int(await r.get(k_stats_drink_cnt(cafe_id, drink)) or 0)
        rev = int(await r.get(k_stats_drink_rev(cafe_id, drink)) or 0)
        lines.append(f"• {html.quote(drink)}: <b>{cnt}</b> шт., <b>{rev}₽</b>")

    text = (
        "📊 <b>Статистика</b>\n\n"
        f"Кафе: <code>{html.quote(cafe_id)}</code>\n"
        f"Всего заказов: <b>{total_orders}</b>\n"
        f"Выручка всего: <b>{total_rev}₽</b>\n\n"
        "<b>По позициям:</b>\n" + "\n".join(lines)
    )
    await message.answer(text)


@router.message(F.text == BTN_CAFE_PROFILE)
async def cafe_profile_entry(message: Message, state: FSMContext):
    if is_group_chat(message):
        await message.answer(
            "🏪 Профиль кафе можно редактировать только в личном чате с ботом."
        )
        return

    r: redis.Redis = message.bot._redis

    raw_cafe_id = await r.get(k_user_cafe(message.from_user.id))
    if isinstance(raw_cafe_id, bytes):
        raw_cafe_id = raw_cafe_id.decode("utf-8", "ignore")

    cafe_id = str(raw_cafe_id or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("🔒 Редактирование профиля доступно только администратору.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    profile = await r.hgetall(k_cafe_profile(cafe_id))

    current_title_raw = (
        profile.get("title")
        or profile.get(b"title")
        or ""
    )

    if isinstance(current_title_raw, bytes):
        current_title_raw = current_title_raw.decode("utf-8", "ignore")

    current_title = str(current_title_raw).strip()

    await state.clear()
    await state.update_data(cafe_id=cafe_id)
    await state.set_state(CafeProfileStates.waiting_for_title)

    prompt = (
        "🏪 <b>Профиль кафе</b>\n\n"
        "Шаг 1 из 4. Введите название кафе.\n\n"
        "Например: <i>Coffee Point</i>"
    )

    if current_title:
        prompt += (
            f"\n\nТекущее название: <b>{html.quote(current_title)}</b>"
            "\nОтправьте новое название."
        )

    await message.answer(
        prompt,
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_CANCEL_CAFE_PROFILE)]],
            resize_keyboard=True,
        ),
    )


@router.message(
    CafeProfileStates.waiting_for_title,
    F.text == BTN_CANCEL_CAFE_PROFILE,
)
@router.message(
    CafeProfileStates.waiting_for_address,
    F.text == BTN_CANCEL_CAFE_PROFILE,
)
@router.message(
    CafeProfileStates.waiting_for_work_start,
    F.text == BTN_CANCEL_CAFE_PROFILE,
)
@router.message(
    CafeProfileStates.waiting_for_work_end,
    F.text == BTN_CANCEL_CAFE_PROFILE,
)
async def cafe_profile_cancel(message: Message, state: FSMContext):
    await state.clear()

    await message.answer(
        "Ввод профиля отменён. Ранее сохранённые данные кафе не изменились.",
        reply_markup=kb_admin_main(
            is_super=is_superadmin(message.from_user.id),
        ),
    )


@router.message(CafeProfileStates.waiting_for_title)
async def cafe_profile_title(message: Message, state: FSMContext):
    title = (message.text or "").strip()

    if len(title) < 2 or len(title) > 80:
        await message.answer(
            "⚠️ Название должно содержать от 2 до 80 символов.\n"
            "Попробуйте ещё раз."
        )
        return

    await state.update_data(title=title)
    await state.set_state(CafeProfileStates.waiting_for_address)

    await message.answer(
        "Шаг 2 из 4. Введите адрес кафе.\n\n"
        "Например: <i>Москва, ул. Тверская, 15</i>"
    )


@router.message(CafeProfileStates.waiting_for_address)
async def cafe_profile_address(message: Message, state: FSMContext):
    address = (message.text or "").strip()

    if len(address) < 5 or len(address) > 180:
        await message.answer(
            "⚠️ Адрес должен содержать от 5 до 180 символов.\n"
            "Попробуйте ещё раз."
        )
        return

    await state.update_data(address=address)
    await state.set_state(CafeProfileStates.waiting_for_work_start)

    await message.answer(
        "Шаг 3 из 4. Во сколько кафе открывается?\n\n"
        "Введите время в формате <code>09:00</code>."
    )


@router.message(CafeProfileStates.waiting_for_work_start)
async def cafe_profile_work_start(message: Message, state: FSMContext):
    work_start = (message.text or "").strip()

    try:
        datetime.strptime(work_start, "%H:%M")
    except ValueError:
        await message.answer(
            "⚠️ Нужен формат времени <code>ЧЧ:ММ</code>.\n"
            "Например: <code>09:00</code>."
        )
        return

    await state.update_data(work_start=work_start)
    await state.set_state(CafeProfileStates.waiting_for_work_end)

    await message.answer(
        "Шаг 4 из 4. Во сколько кафе закрывается?\n\n"
        "Введите время в формате <code>22:00</code>."
    )


@router.message(CafeProfileStates.waiting_for_work_end)
async def cafe_profile_work_end(message: Message, state: FSMContext):
    work_end = (message.text or "").strip()

    try:
        datetime.strptime(work_end, "%H:%M")
    except ValueError:
        await message.answer(
            "⚠️ Нужен формат времени <code>ЧЧ:ММ</code>.\n"
            "Например: <code>22:00</code>."
        )
        return

    data = await state.get_data()

    cafe_id = str(data.get("cafe_id") or "")
    title = str(data.get("title") or "").strip()
    address = str(data.get("address") or "").strip()

    if not cafe_id or not title or not address:
        await state.clear()
        await message.answer(
            "⚠️ Данные формы потеряны. Нажмите «🏪 Профиль кафе» и начните снова."
        )
        return

    r: redis.Redis = message.bot._redis

    work_start = str(data["work_start"]).strip()
    work_end = str(work_end).strip()

    work_start_hour = int(work_start.split(":")[0])
    work_end_hour = int(work_end.split(":")[0])

    if work_start_hour == work_end_hour:
        await message.answer(
            "⚠️ Время открытия и закрытия не должно совпадать.\n"
            "Введите другое время закрытия."
        )
        return

    await r.hset(
        k_cafe_profile(cafe_id),
        mapping={
            "title": title,
            "address": address,
            "work_start": str(work_start_hour),
            "work_end": str(work_end_hour),
            "profile_configured": "1",
            "updated_at": str(int(time.time())),
            "updated_by": str(message.from_user.id),
        },
    )

    await state.clear()

    await message.answer(
        "✅ <b>Профиль кафе сохранён.</b>\n\n"
        f"🏪 <b>{html.quote(title)}</b>\n"
        f"📍 {html.quote(address)}\n"
        f"🕒 {work_start_hour:02d}:00–{work_end_hour:02d}:00\n\n"
        "Изменения сохранены.",
        reply_markup=kb_admin_main(
            is_super=is_superadmin(message.from_user.id),
        ),
    )


@router.message(F.text == BTN_MENU_EDIT)
async def menu_edit_entry(message: Message, state: FSMContext):
    if is_group_chat(message):
        # В группе эту кнопку обрабатывает staff_menu_edit_entry
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        if DEMO_MODE:
            await message.answer(demo_menu_edit_preview_text(), reply_markup=kb_menu_edit())
            await message.answer("🔒 Редактирование доступно только администратору.")
        else:
            await message.answer("🔒 Редактирование доступно только администратору.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    await state.clear()
    await state.set_state(MenuEditStates.waiting_for_action)
    await message.answer("🛠 Управление меню: выберите действие", reply_markup=kb_menu_edit())


@router.message(F.text == BTN_PROMO)
async def promo_entry(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("🔒 Доступно только администратору.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        return

    await state.clear()
    await state.set_state(PromoStates.waiting_for_action)
    await show_promo_menu(message, r, cafe_id)


@router.message(StateFilter(PromoStates.waiting_for_action))
async def promo_choose_action(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    text = (message.text or "").strip()
    promo = normalize_promo(await get_cafe_promo(r, cafe_id))

    if text in {PROMO_BACK, BTN_BACK}:
        await state.clear()
        await message.answer("Ок.", reply_markup=kb_admin_main(is_superadmin(uid)))
        return

    if text == PROMO_EDIT_TEXT:
        await state.set_state(PromoStates.waiting_for_text)
        await message.answer(
            "Отправьте текст рекламы одним сообщением.\n"
            "Можно без текста — тогда нажмите «Пропустить».",
            reply_markup=kb_promo_input(),
        )
        return

    if text == PROMO_EDIT_URL:
        await state.set_state(PromoStates.waiting_for_url)
        await message.answer(
            "Отправьте ссылку в формате https://example.com\n"
            "Или нажмите «Пропустить», чтобы убрать ссылку.",
            reply_markup=kb_promo_input(),
            disable_web_page_preview=True,
        )
        return

    if text == PROMO_EDIT_PHOTO:
        await state.set_state(PromoStates.waiting_for_photo)
        await message.answer(
            "Отправьте картинку одним сообщением.\n"
            "Можно нажать «Пропустить» или «Удалить картинку».",
            reply_markup=kb_promo_photo_input(),
        )
        return

    if text == PROMO_TOGGLE:
        has_content = bool(promo["text"] or promo["url"] or promo["photo_file_id"])
        if not has_content and not promo["enabled"]:
            await message.answer(
                "Сначала добавьте хотя бы текст, ссылку или картинку.",
                reply_markup=kb_promo_manage(),
            )
            return

        promo["enabled"] = not promo["enabled"]
        await set_cafe_promo(r, cafe_id, promo)
        await show_promo_menu(message, r, cafe_id)
        return

    if text == PROMO_DELETE_TEXT:
        promo["text"] = ""
        await set_cafe_promo(r, cafe_id, promo)
        await show_promo_menu(message, r, cafe_id)
        return

    if text == PROMO_DELETE_URL:
        promo["url"] = ""
        await set_cafe_promo(r, cafe_id, promo)
        await show_promo_menu(message, r, cafe_id)
        return

    if text == PROMO_DELETE_PHOTO:
        promo["photo_file_id"] = ""
        await set_cafe_promo(r, cafe_id, promo)
        await show_promo_menu(message, r, cafe_id)
        return

    if text == PROMO_CLEAR:
        await clear_cafe_promo(r, cafe_id)
        await show_promo_menu(message, r, cafe_id)
        return

    if text == PROMO_PREVIEW:
        await send_promo_preview(message, promo)
        await show_promo_menu(message, r, cafe_id)
        return

    await message.answer("Выберите действие кнопкой.", reply_markup=kb_promo_manage())


@router.message(StateFilter(PromoStates.waiting_for_text))
async def promo_set_text(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    text = (message.text or "").strip()

    if text in {PROMO_BACK, BTN_BACK}:
        await state.set_state(PromoStates.waiting_for_action)
        await show_promo_menu(message, r, cafe_id)
        return

    promo = normalize_promo(await get_cafe_promo(r, cafe_id))

    if text == PROMO_SKIP:
        promo["text"] = ""
        await set_cafe_promo(r, cafe_id, promo)
        await state.set_state(PromoStates.waiting_for_action)
        await show_promo_menu(message, r, cafe_id)
        return

    if not text:
        await message.answer(
            "Текст пустой. Отправьте сообщение или нажмите «Пропустить».",
            reply_markup=kb_promo_input(),
        )
        return

    if len(text) > 900:
        await message.answer(
            "Текст слишком длинный. Лучше до 900 символов.",
            reply_markup=kb_promo_input(),
        )
        return

    promo["text"] = text
    await set_cafe_promo(r, cafe_id, promo)
    await state.set_state(PromoStates.waiting_for_action)
    await show_promo_menu(message, r, cafe_id)


@router.message(StateFilter(PromoStates.waiting_for_url))
async def promo_set_url(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    text = (message.text or "").strip()

    if text in {PROMO_BACK, BTN_BACK}:
        await state.set_state(PromoStates.waiting_for_action)
        await show_promo_menu(message, r, cafe_id)
        return

    promo = normalize_promo(await get_cafe_promo(r, cafe_id))

    if text == PROMO_SKIP:
        promo["url"] = ""
        await set_cafe_promo(r, cafe_id, promo)
        await state.set_state(PromoStates.waiting_for_action)
        await show_promo_menu(message, r, cafe_id)
        return

    if not is_valid_http_url(text):
        await message.answer(
            "Ссылка должна начинаться с http:// или https://",
            reply_markup=kb_promo_input(),
            disable_web_page_preview=True,
        )
        return

    promo["url"] = text
    await set_cafe_promo(r, cafe_id, promo)
    await state.set_state(PromoStates.waiting_for_action)
    await show_promo_menu(message, r, cafe_id)


@router.message(StateFilter(PromoStates.waiting_for_photo))
async def promo_set_photo(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r: redis.Redis = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    text = (message.text or "").strip()
    promo = normalize_promo(await get_cafe_promo(r, cafe_id))

    if text in {PROMO_BACK, BTN_BACK}:
        await state.set_state(PromoStates.waiting_for_action)
        await show_promo_menu(message, r, cafe_id)
        return

    if text == PROMO_SKIP:
        await state.set_state(PromoStates.waiting_for_action)
        await show_promo_menu(message, r, cafe_id)
        return

    if text == PROMO_DELETE_PHOTO:
        promo["photo_file_id"] = ""
        await set_cafe_promo(r, cafe_id, promo)
        await state.set_state(PromoStates.waiting_for_action)
        await show_promo_menu(message, r, cafe_id)
        return

    if not message.photo:
        await message.answer(
            "Отправьте именно картинку или нажмите нужную кнопку.",
            reply_markup=kb_promo_photo_input(),
        )
        return

    promo["photo_file_id"] = message.photo[-1].file_id
    await set_cafe_promo(r, cafe_id, promo)
    await state.set_state(PromoStates.waiting_for_action)
    await show_promo_menu(message, r, cafe_id)


@router.message(F.text == BTN_ADMIN_SUPPORT)
async def admin_support_entry(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await message.answer("Доступно только админу кафе.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    active_ticket_id = await r.get(k_support_active(cafe_id, uid))
    if active_ticket_id:
        ticket = await get_support_ticket(r, str(active_ticket_id))
        status = support_status_label(str(ticket.get("status") or SUPPORT_STATUS_NEW))
        await message.answer(
            f"У вас уже есть активное обращение: <b>{html.quote(str(active_ticket_id))}</b>\n"
            f"Статус: <b>{html.quote(status)}</b>\n\n"
            "Сначала дождитесь ответа или закрытия текущего тикета.",
            reply_markup=kb_admin_main(is_super=is_superadmin(uid)),
        )
        return

    await state.clear()
    await message.answer(
        "Выберите тему обращения:",
        reply_markup=support_topic_kb(),
    )


@router.callback_query(F.data.startswith("order_done:"))
async def order_done_callback(callback: CallbackQuery) -> None:
    raw_data = (callback.data or "").strip()
    parts = raw_data.split(":", 2)

    if len(parts) != 3:
        await callback.answer("Некорректные данные заказа.", show_alert=True)
        return

    _, cafe_id, order_id = parts
    cafe_id = cafe_id.strip()
    order_id = order_id.strip()

    if not cafe_id or not order_id:
        await callback.answer("Не удалось определить заказ.", show_alert=True)
        return

    r: redis.Redis = callback.bot._redis
    order_key = k_active_order(cafe_id, order_id)
    order = await r.hgetall(order_key)

    if not order:
        await callback.answer(
            "Заказ не найден или срок его хранения уже истёк.",
            show_alert=True,
        )
        return

    status = _as_text(order.get(b"status") or order.get("status"))
    if status == "done":
        await callback.answer("Заказ уже отмечен как выполненный ✅")
        return

    uid = callback.from_user.id
    superadmin = is_superadmin(uid)
    is_admin = await is_cafe_admin(r, uid, cafe_id)

    # В staff-группе кнопку может нажать любой участник этой
    # привязанной группы — именно там готовят и выдают заказы.
    staff_group_id = await r.get(k_staff_group(cafe_id))
    current_chat_id = callback.message.chat.id if callback.message else 0

    in_bound_staff_group = False
    if staff_group_id:
        try:
            in_bound_staff_group = int(staff_group_id) == int(current_chat_id)
        except (TypeError, ValueError):
            in_bound_staff_group = False

    if not (superadmin or is_admin or in_bound_staff_group):
        await callback.answer(
            "Нет прав, чтобы отметить этот заказ.",
            show_alert=True,
        )
        return

    done_at = int(time.time())
    done_by_name = (
        callback.from_user.full_name
        or callback.from_user.first_name
        or "Сотрудник"
    )

    # Сначала меняем статус: после этого воркер уже не отправит напоминание.
    await r.hset(
        order_key,
        mapping={
            "status": "done",
            "done_at": done_at,
            "done_by_id": uid,
            "done_by_name": done_by_name,
        },
    )

    # Удаляем из очереди, если заказ выполнили раньше ready_at + 15 минут.
    await r.zrem(k_order_reminders(), f"{cafe_id}|{order_id}")

    await callback.answer("Заказ отмечен как выполненный ✅")

    try:
        await update_order_cards_as_done(
            callback.bot,
            r,
            cafe_id=cafe_id,
            order_id=order_id,
            done_by_name=done_by_name,
            done_at=done_at,
        )
    except Exception:
        logger.exception(
            "Не удалось обновить карточки заказа после отметки: "
            "cafe_id=%s order_id=%s",
            cafe_id,
            order_id,
        )


@router.callback_query(F.data == PD_CONSENT_ORDER)
async def pd_consent_order_callback(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    if not callback.message:
        await callback.answer(
            "Не удалось определить сообщение.",
            show_alert=True,
        )
        return

    r: redis.Redis = callback.bot._redis
    user_id = callback.from_user.id

    cafe_id = str(
        await r.get(k_user_cafe(user_id))
        or DEFAULT_CAFE_ID
    )

    await save_order_pd_consent(
        r,
        cafe_id,
        user_id,
    )

    await callback.answer("Согласие сохранено ✅")

    # Убираем кнопки у исходного сообщения, чтобы его не жали повторно.
    try:
        await callback.message.edit_reply_markup(
            reply_markup=None,
        )
    except Exception:
        logger.exception(
            "Не удалось убрать кнопки согласия: user_id=%s cafe_id=%s",
            user_id,
            cafe_id,
        )

    # Важно: callback.message — это объект Message.
    # После согласия показываем обычный экран подтверждения с корзиной.
    await show_order_confirmation_after_consent(
        callback.message,
        state,
        cafe_id=cafe_id,
    )


@router.callback_query(F.data.startswith(SUP_CB_TOPIC))
async def admin_support_pick_topic(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    if not callback.data:
        return

    topic = callback.data[len(SUP_CB_TOPIC):].strip()
    if topic not in SUPPORT_TOPICS:
        await callback.message.answer("Неизвестная тема обращения.")
        return

    r = callback.bot._redis
    uid = callback.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await callback.message.answer("Доступно только админу кафе.")
        return

    await state.update_data(support_topic=topic)
    await state.set_state(SupportStates.waiting_for_topic_message)

    topic_title = SUPPORT_TOPICS.get(topic, topic)
    await callback.message.answer(
        f"Тема: <b>{html.quote(topic_title)}</b>\n\n"
        "Теперь отправьте одним сообщением описание проблемы или вопроса.",
        reply_markup=kb_admin_main(is_super=is_superadmin(uid)),
    )


@router.message(SupportStates.waiting_for_topic_message)
async def admin_support_create_ticket(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    r = message.bot._redis
    uid = message.from_user.id
    cafe_id = str(await r.get(k_user_cafe(uid)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, uid, cafe_id):
        await state.clear()
        await message.answer("Доступно только админу кафе.")
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("Отправьте текстовое описание обращения одним сообщением.")
        return

    data = await state.get_data()
    topic = str(data.get("support_topic") or "").strip()
    if topic not in SUPPORT_TOPICS:
        await state.clear()
        await message.answer("Тема обращения потерялась. Нажмите кнопку поддержки ещё раз.")
        return

    active_ticket_id = await r.get(k_support_active(cafe_id, uid))
    if active_ticket_id:
        ticket = await get_support_ticket(r, str(active_ticket_id))
        status = support_status_label(str(ticket.get("status") or SUPPORT_STATUS_NEW))
        await state.clear()
        await message.answer(
            f"У вас уже есть активное обращение: <b>{html.quote(str(active_ticket_id))}</b>\n"
            f"Статус: <b>{html.quote(status)}</b>",
            reply_markup=kb_admin_main(is_super=is_superadmin(uid)),
        )
        return

    cafe = cafe_or_default(cafe_id)
    ticket = await create_support_ticket(
        r,
        cafe_id=cafe_id,
        cafe_title_text=cafe_title(cafe),
        user_id=uid,
        user_name=message.from_user.full_name or message.from_user.first_name or "",
        username=message.from_user.username or "",
        topic=topic,
        text=text,
    )

    sent_to_admin = False
    if SUPERADMIN_ID:
        try:
            admin_msg = await message.bot.send_message(
                SUPERADMIN_ID,
                render_support_ticket_text(ticket),
                reply_markup=support_admin_ticket_kb(ticket["ticket_id"]),
            )
            await bind_support_admin_message(
                r,
                ticket["ticket_id"],
                chat_id=admin_msg.chat.id,
                message_id=admin_msg.message_id,
            )
            sent_to_admin = True
        except Exception:
            sent_to_admin = False

    await state.clear()

    if sent_to_admin:
        await message.answer(
            f"Обращение <b>{html.quote(ticket['ticket_id'])}</b> отправлено в поддержку.\n"
            "Ожидайте ответа супер-админа.",
            reply_markup=kb_admin_main(is_super=is_superadmin(uid)),
        )
    else:
        await message.answer(
            f"Обращение <b>{html.quote(ticket['ticket_id'])}</b> сохранено, "
            "но отправка супер-админу не удалась.",
            reply_markup=kb_admin_main(is_super=is_superadmin(uid)),
        )


@router.callback_query(F.data.startswith(SUP_CB_INWORK))
async def support_inwork_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    if not is_superadmin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return

    if not callback.data:
        return

    ticket_id = callback.data[len(SUP_CB_INWORK):].strip()
    if not ticket_id:
        return

    r = callback.bot._redis
    ticket = await get_support_ticket(r, ticket_id)
    if not ticket:
        await callback.message.answer("Тикет не найден.")
        return

    if str(ticket.get("status")) == SUPPORT_STATUS_CLOSED:
        await callback.answer("Тикет уже закрыт", show_alert=True)
        return

    ticket = await update_support_ticket(r, ticket_id, status=SUPPORT_STATUS_IN_WORK)

    user_id_raw = ticket.get("user_id")
    try:
        user_id = int(user_id_raw)
    except Exception:
        user_id = 0

    if user_id:
        try:
            topic_title = SUPPORT_TOPICS.get(str(ticket.get("topic") or ""), "Без темы")
            await callback.bot.send_message(
                user_id,
                f"🛠 Обращение <b>{html.quote(ticket_id)}</b> взято в работу.\n"
                f"Тема: <b>{html.quote(topic_title)}</b>",
                reply_markup=kb_admin_main(is_super=False),
            )
        except Exception:
            pass

    try:
        await callback.message.edit_text(
            render_support_ticket_text(ticket),
            reply_markup=support_admin_ticket_kb(ticket_id),
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith(SUP_CB_REPLY))
async def support_reply_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    if not is_superadmin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return

    if not callback.data:
        return

    ticket_id = callback.data[len(SUP_CB_REPLY):].strip()
    if not ticket_id:
        return

    r = callback.bot._redis
    ticket = await get_support_ticket(r, ticket_id)
    if not ticket:
        await callback.message.answer("Тикет не найден.")
        return

    if str(ticket.get("status")) == SUPPORT_STATUS_CLOSED:
        await callback.answer("Тикет уже закрыт", show_alert=True)
        return

    await state.clear()
    await state.update_data(support_reply_ticket_id=ticket_id)
    await state.set_state(SupportStates.waiting_for_superadmin_reply)

    await callback.message.answer(
        f"Введите ответ для тикета <b>{html.quote(ticket_id)}</b> одним сообщением."
    )


@router.message(SupportStates.waiting_for_superadmin_reply)
async def support_reply_message(message: Message, state: FSMContext):
    if is_group_chat(message):
        return

    if not is_superadmin(message.from_user.id):
        await state.clear()
        await message.answer("Недостаточно прав.")
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("Отправьте ответ текстовым сообщением.")
        return

    data = await state.get_data()
    ticket_id = str(data.get("support_reply_ticket_id") or "").strip()
    if not ticket_id:
        await state.clear()
        await message.answer("Тикет не найден в состоянии. Нажмите кнопку ответа заново.")
        return

    r = message.bot._redis
    ticket = await get_support_ticket(r, ticket_id)
    if not ticket:
        await state.clear()
        await message.answer("Тикет не найден.")
        return

    if str(ticket.get("status")) == SUPPORT_STATUS_CLOSED:
        await state.clear()
        await message.answer("Тикет уже закрыт.")
        return

    user_id_raw = ticket.get("user_id")
    cafe_id = str(ticket.get("cafe_id") or "")
    try:
        user_id = int(user_id_raw)
    except Exception:
        await state.clear()
        await message.answer("Не удалось определить получателя ответа.")
        return

    topic_title = SUPPORT_TOPICS.get(str(ticket.get("topic") or ""), "Без темы")

    try:
        await message.bot.send_message(
            user_id,
            f"✉️ <b>Ответ по обращению {html.quote(ticket_id)}</b>\n"
            f"Тема: <b>{html.quote(topic_title)}</b>\n\n"
            f"{html.quote(text)}",
            reply_markup=kb_admin_main(is_super=False),
        )
    except Exception:
        await message.answer("Не удалось отправить ответ админу кафе.")
        return

    ticket = await update_support_ticket(r, ticket_id, status=SUPPORT_STATUS_ANSWERED)

    admin_chat_id = str(ticket.get("superadmin_chat_id") or "").strip()
    admin_message_id = str(ticket.get("superadmin_message_id") or "").strip()
    if admin_chat_id and admin_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=int(admin_chat_id),
                message_id=int(admin_message_id),
                text=render_support_ticket_text(ticket),
                reply_markup=support_admin_ticket_kb(ticket_id),
            )
        except Exception:
            pass

    await state.clear()
    await message.answer(f"Ответ по тикету <b>{html.quote(ticket_id)}</b> отправлен админу кафе.")

    if cafe_id:
        await r.delete(k_support_active(cafe_id, user_id))


@router.callback_query(F.data.startswith(SUP_CB_CLOSE))
async def support_close_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    if not is_superadmin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return

    if not callback.data:
        return

    ticket_id = callback.data[len(SUP_CB_CLOSE):].strip()
    if not ticket_id:
        return

    r = callback.bot._redis
    ticket = await get_support_ticket(r, ticket_id)
    if not ticket:
        await callback.message.answer("Тикет не найден.")
        return

    ticket = await update_support_ticket(r, ticket_id, status=SUPPORT_STATUS_CLOSED)

    try:
        await callback.message.edit_text(
            render_support_ticket_text(ticket),
            reply_markup=support_admin_ticket_kb(ticket_id, closed=True),
        )
    except Exception:
        pass

    cafe_id = str(ticket.get("cafe_id") or "")
    user_id_raw = ticket.get("user_id")
    try:
        user_id = int(user_id_raw)
    except Exception:
        user_id = 0

    if cafe_id and user_id:
        await r.delete(k_support_active(cafe_id, user_id))

    if user_id:
        try:
            await callback.bot.send_message(
                user_id,
                f"✅ Обращение <b>{html.quote(ticket_id)}</b> закрыто.",
                reply_markup=kb_admin_main(is_super=False),
            )
        except Exception:
            pass


@router.message(StateFilter(MenuEditStates.waiting_for_action))
async def menu_edit_choose_action(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    menu = await get_menu(r, cafe_id)

    if message.text == BTN_BACK:
        await state.clear()
        await message.answer("Ок.", reply_markup=kb_admin_main(is_superadmin(message.from_user.id)))
        return

    if message.text == MENU_EDIT_ADD:
        await state.set_state(MenuEditStates.waiting_for_add_name)
        await message.answer("Введите название новой позиции:", reply_markup=kb_menu_edit_cancel())
        return

    if message.text == MENU_EDIT_EDIT:
        await state.set_state(MenuEditStates.pick_edit_item)
        await message.answer("Выберите позицию для изменения цены:", reply_markup=kb_pick_menu_item(menu))
        return

    if message.text == MENU_EDIT_DEL:
        await state.set_state(MenuEditStates.pick_remove_item)
        await message.answer("Выберите позицию для удаления:", reply_markup=kb_pick_menu_item(menu))
        return

    await message.answer("Выберите действие кнопкой.", reply_markup=kb_menu_edit())


@router.message(StateFilter(MenuEditStates.waiting_for_add_name))
async def menu_edit_add_name(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    if message.text == BTN_BACK:
        await state.set_state(MenuEditStates.waiting_for_action)
        await message.answer("Ок.", reply_markup=kb_menu_edit())
        return

    name = (message.text or "").strip()
    if not name:
        await message.answer("Введите название.", reply_markup=kb_menu_edit_cancel())
        return

    await state.update_data(add_name=name)
    await state.set_state(MenuEditStates.waiting_for_add_price)
    await message.answer("Введите цену числом:", reply_markup=kb_menu_edit_cancel())


@router.message(StateFilter(MenuEditStates.waiting_for_add_price))
async def menu_edit_add_price(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    if message.text == BTN_BACK:
        await state.set_state(MenuEditStates.waiting_for_action)
        await message.answer("Ок.", reply_markup=kb_menu_edit())
        return

    try:
        price = int((message.text or "").strip())
        if price <= 0:
            raise ValueError
    except Exception:
        await message.answer("Цена должна быть числом.", reply_markup=kb_menu_edit_cancel())
        return

    data = await state.get_data()
    name = str(data.get("add_name") or "").strip()
    await menu_set_item(message.bot._redis, cafe_id, name, price)
    await state.clear()
    await message.answer("✅ Добавлено.", reply_markup=kb_admin_main(is_superadmin(message.from_user.id)))


@router.message(StateFilter(MenuEditStates.pick_edit_item))
async def menu_pick_edit_item(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    menu = await get_menu(r, cafe_id)

    if message.text == BTN_BACK:
        await state.set_state(MenuEditStates.waiting_for_action)
        await message.answer("Ок.", reply_markup=kb_menu_edit())
        return

    picked = (message.text or "").strip()
    if picked not in menu:
        await message.answer("Выберите позицию кнопкой.", reply_markup=kb_pick_menu_item(menu))
        return

    await state.update_data(edit_name=picked)
    await state.set_state(MenuEditStates.waiting_for_edit_price)
    await message.answer(f"Новая цена для <b>{html.quote(picked)}</b>:", reply_markup=kb_menu_edit_cancel())


@router.message(StateFilter(MenuEditStates.waiting_for_edit_price))
async def menu_edit_price(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    if message.text == BTN_BACK:
        await state.set_state(MenuEditStates.waiting_for_action)
        await message.answer("Ок.", reply_markup=kb_menu_edit())
        return

    try:
        price = int((message.text or "").strip())
        if price <= 0:
            raise ValueError
    except Exception:
        await message.answer("Цена должна быть числом.", reply_markup=kb_menu_edit_cancel())
        return

    data = await state.get_data()
    name = str(data.get("edit_name") or "")
    await menu_set_item(message.bot._redis, cafe_id, name, price)
    await state.clear()
    await message.answer("✅ Цена изменена.", reply_markup=kb_admin_main(is_superadmin(message.from_user.id)))


@router.message(StateFilter(MenuEditStates.pick_remove_item))
async def menu_pick_remove_item(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await state.clear()
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    menu = await get_menu(r, cafe_id)

    if message.text == BTN_BACK:
        await state.set_state(MenuEditStates.waiting_for_action)
        await message.answer("Ок.", reply_markup=kb_menu_edit())
        return

    picked = (message.text or "").strip()
    if picked not in menu:
        await message.answer("Выберите позицию кнопкой.", reply_markup=kb_pick_menu_item(menu))
        return

    await menu_delete_item(message.bot._redis, cafe_id, picked)
    await state.clear()
    await message.answer("🗑 Удалено.", reply_markup=kb_admin_main(is_superadmin(message.from_user.id)))


from aiogram.enums import ChatType

async def resolve_cafe_by_staff_group(r: redis.Redis, chat_id: int) -> Optional[str]:
    """
    По chat_id группы находим cafe_id, к которому она привязана через /bind.
    """
    for cafe_id in CAFES.keys():
        gid = await r.get(k_staff_group(cafe_id))
        if gid and int(gid) == chat_id:
            return cafe_id
    return None


@router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    F.text == BTN_MENU_EDIT,
)
async def staff_menu_edit_entry(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = await resolve_cafe_by_staff_group(r, message.chat.id)
    if not cafe_id:
        await message.answer("Этот чат не привязан ни к одному кафе. Используйте /bind в этом чате.")
        return

    if not await is_cafe_admin(r, message.from_user.id, cafe_id) and not is_superadmin(message.from_user.id):
        if DEMO_MODE:
            await message.answer(demo_menu_edit_preview_text(), reply_markup=kb_menu_edit())
            await message.answer("🔒 Редактирование доступно только администратору.")
        else:
            await message.answer("🔒 Редактирование доступно только администратору.")
        return

    if not await ensure_subscription_active(message, r, cafe_id):
        await state.clear()
        return

    await state.clear()
    await state.set_state(MenuEditStates.waiting_for_action)
    await message.answer("🛠 Управление меню (staff-чат): выберите действие", reply_markup=kb_menu_edit())


@router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    F.text == BTN_LINKS,
)
async def staff_links_button(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = await resolve_cafe_by_staff_group(r, message.chat.id)
    if not cafe_id:
        await message.answer("Этот чат не привязан ни к одному кафе. Используйте /bind в этом чате.")
        return

    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    await send_admin_panel(message, cafe_id, cafe, menu)


@router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    F.text == BTN_SUB_INFO,
)
async def staff_sub_info_button(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = await resolve_cafe_by_staff_group(r, message.chat.id)
    if not cafe_id:
        await message.answer("Этот чат не привязан ни к одному кафе. Используйте /bind в этом чате.")
        return

    sub_key = k_admin_subscription(cafe_id)
    raw_until = await r.hget(sub_key, "cafebotify_valid_until")
    until_ts = int(raw_until) if raw_until else 0

    if until_ts <= 0:
        await message.answer("❌ Подписка не активна.")
        return

    untildt = datetime.fromtimestamp(until_ts, tz=MSK_TZ)
    days_left = (untildt.date() - get_moscow_time().date()).days
    left_line = f"Осталось <b>{days_left} дней</b>." if days_left > 0 else "Истекает сегодня!"

    await message.answer(
        f"🗓️ Подписка до <b>{untildt.strftime('%d.%m.%Y')}</b>.\n{left_line}"
    )


@router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    F.text == BTN_STATS,
)
async def staff_stats_button(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = await resolve_cafe_by_staff_group(r, message.chat.id)
    if not cafe_id:
        await message.answer("Этот чат не привязан ни к одному кафе. Используйте /bind в этом чате.")
        return

    if DEMO_MODE:
        await message.answer(demo_stats_preview_text())
        return

    menu = await get_menu(r, cafe_id)
    total_orders = int(await r.get(k_stats_total_orders(cafe_id)) or 0)
    total_rev = int(await r.get(k_stats_total_revenue(cafe_id)) or 0)

    lines: List[str] = []
    for drink in menu.keys():
        cnt = int(await r.get(k_stats_drink_cnt(cafe_id, drink)) or 0)
        rev = int(await r.get(k_stats_drink_rev(cafe_id, drink)) or 0)
        lines.append(f"{html.quote(drink)} — <b>{cnt}</b> шт., {rev}₽")

    text = (
        f"📊 Статистика по кафе <code>{html.quote(cafe_id)}</code>\n\n"
        f"Всего заказов: <b>{total_orders}</b>\n"
        f"Выручка: <b>{total_rev}₽</b>\n\n"
        + ("\n".join(lines) if lines else "Пока нет заказов.")
    )
    await message.answer(text)

from aiogram.enums import ChatType


@router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    F.text == BTN_LINKS,
)
async def staff_link_for_client(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = await resolve_cafe_by_staff_group(r, message.chat.id)
    if not cafe_id:
        await message.answer("Этот чат не привязан ни к одному кафе. Используйте /bind в этом чате.")
        return

    # чистая клиентская ссылка
    client_link = await create_start_link(message.bot, payload=cafe_id, encode=True)
    await message.answer(
        f"🔗 Ссылка для клиента:\n{client_link}",
        disable_web_page_preview=True,
    )


# =========================================================
# Fallback (drink pick)
# =========================================================
@router.message(F.text)
async def anytextmessage(message: Message, state: FSMContext):
    # в группах ничего клиентского не делаем
    if is_group_chat(message):
        return
        
    r: redis.Redis = message.bot._redis
    uid = message.from_user.id

    raw_cafe_id = await r.get(k_user_cafe(uid))

    if isinstance(raw_cafe_id, bytes):
        raw_cafe_id = raw_cafe_id.decode("utf-8", "ignore")

    cafe_id = str(raw_cafe_id or DEFAULT_CAFE_ID)

    cafe = cafe_or_default(cafe_id)
    cafe = await apply_cafe_profile(r, cafe_id, cafe)

    menu = await get_menu(r, cafe_id)
    text = (message.text or "").strip()

    is_admin = await is_cafe_admin(r, uid, cafe_id)
    view_mode = str(await r.get(k_view_mode(uid)) or "admin")  # "admin" | "client"

    # Если админ в админ-режиме — неизвестный текст возвращает в админку
    if is_admin and view_mode != "client":
        await send_admin_panel(message, cafe_id, cafe, menu)
        return

    # Клиентский режим: старая логика
    if text in menu:
        if not cafe_open(cafe):
            await message.answer(
                closed_message(cafe, menu),
                reply_markup=kb_client_main(menu, show_admin_button=is_admin),
            )
            return
        await start_add_item(message, state, cafe_id, menu, text)
        return

    await message.answer(
        "Пожалуйста, выберите напиток из меню.",
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )


# =========================================================
# Smart return loop
# =========================================================
async def smart_return_check_and_send(bot: Bot):
    if not in_send_window_msk():
        return

    r: redis.Redis = bot.redis
    now_ts = int(time.time())

    for cafe_id in CAFES.keys():
        try:
            ids = await r.smembers(k_customers_set(cafe_id))
            user_ids = [int(x) for x in ids] if ids else []
        except Exception:
            user_ids = []

        for user_id in user_ids:
            customer_key = k_customer_profile(cafe_id, user_id)

            try:
                profile = await r.hgetall(customer_key)
            except Exception:
                profile = {}

            if not profile:
                continue

            if str(profile.get("offers_optout", "0")) == "1":
                continue

            raw_last_seen = await r.get(k_last_seen(cafe_id, user_id))
            if not raw_last_seen:
                continue

            try:
                last_seen_ts = int(float(raw_last_seen))
            except Exception:
                continue

            days_since_seen = (now_ts - last_seen_ts) / 86400.0

            try:
                stage_7_sent_at = int(float(profile.get("return_stage_7_sent_at", "0") or 0))
            except Exception:
                stage_7_sent_at = 0

            try:
                stage_14_sent_at = int(float(profile.get("return_stage_14_sent_at", "0") or 0))
            except Exception:
                stage_14_sent_at = 0

            reply_markup = await kb_return_to_menu(bot, cafe_id)

            if days_since_seen >= RETURN_OFFER_DAYS and stage_14_sent_at == 0:
                text = (
                    "Сегодня для вас:\n"
                    f"-{RETURN_OFFER_DISCOUNT_PERCENT}% на любой напиток\n\n"
                    "Будем рады вас видеть!"
                )
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=text,
                        reply_markup=reply_markup,
                        disable_web_page_preview=True,
                    )
                    await r.hset(
                        customer_key,
                        mapping={
                            "return_stage_14_sent_at": str(now_ts),
                        },
                    )
                except Exception as e:
                    logger.exception(
                        "return offer send failed user_id=%s cafe_id=%s err=%r",
                        user_id,
                        cafe_id,
                        e,
                    )
                continue

            if days_since_seen >= RETURN_REMINDER_DAYS and stage_7_sent_at == 0:
                text = (
                    "Мы давно не виделись ☕\n\n"
                    "Загляните на чашку кофе."
                )
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=text,
                        reply_markup=reply_markup,
                        disable_web_page_preview=True,
                    )
                    await r.hset(
                        customer_key,
                        mapping={
                            "return_stage_7_sent_at": str(now_ts),
                        },
                    )
                except Exception as e:
                    logger.exception(
                        "return reminder send failed user_id=%s cafe_id=%s err=%r",
                        user_id,
                        cafe_id,
                        e,
                    )

async def sub_renewal_check_and_send(bot: Bot):
    # не дёргаем админов ночью, шлём только 10:00–20:00 МСК
    if not in_send_window_msk():
        return
        
    r: redis.Redis = bot._redis  # если у тебя redis хранится как bot.redis
    now_dt = get_moscow_time()
    today = now_dt.date()

    for cafe_id in CAFES.keys():
        sub_key = k_admin_subscription(cafe_id)
        try:
            raw_until = await r.hget(sub_key, "cafebotify_valid_until")
            until_ts = int(raw_until) if raw_until else 0
            if until_ts <= 0:
                continue

            until_dt = datetime.fromtimestamp(until_ts, tz=MSK_TZ)
            days_left = (until_dt.date() - today).days
        except Exception:
            continue

        if days_left not in (7, 3, 1):
            continue

        flags_key = k_cafe_sub_notify(cafe_id)
        flags = await r.hgetall(flags_key)
        flag_field = f"d{days_left}"
        if flags.get(flag_field) == "1":
            continue

        cafe = cafe_or_default(cafe_id)
        admin_id = await get_effective_admin_id(r, cafe_id)
        if not admin_id:
            continue

        until_str = until_dt.strftime("%d.%m.%Y")
        cafe_name = cafe_title(cafe)

        if days_left == 7:
            text = (
                "🗓️ <b>Подписка скоро закончится</b>\n\n"
                f"Кафе: <b>{html.quote(cafe_name)}</b>\n"
                f"Дата окончания: <b>{until_str}</b>\n\n"
                "🔋Всё будет работать до этой даты, но чтобы бот продолжал принимать заказы "
                "и напоминать гостям о вас, продлите подписку заранее.\n\n"
                "Сделать это можно в админ-панели кнопкой <b>«💳 Продлить подписку»</b>."
            )
        elif days_left == 3:
            text = (
                "🪫 <b>Осталось 3 дня подписки</b>\n\n"
                f"Кафе: <b>{html.quote(cafe_name)}</b>\n"
                f"До окончания: <b>{until_str}</b>\n\n"
                "За это время наша система автоматизации уже помогла:\n"
                "⭐ принимать заказы и брони без звонков\n"
                "⭐ напоминать гостям о вас\n"
                "⭐ собирать статистику по продажам\n\n"
                "Если бот экономит вам время и приносит заказы, удобнее всего продлить подписку "
                "сейчас, пока всё работает.\n\n"
                "Откройте админ-панель и нажмите <b>«💳 Продлить подписку»</b> — это займёт меньше минуты."
            )
        else:  # days_left == 1
            text = (
                "⚠️ <b>Завтра доступ к боту будет отключён</b>\n\n"
                f"Кафе: <b>{html.quote(cafe_name)}</b>\n"
                f"Дата окончания: <b>{until_str}</b>\n\n"
                "Очень не хочется, чтобы гости снова стояли в очереди или звонили, "
                "а вы принимали заказы вручную.\n\n"
                "Если вам удобно, продлите подписку прямо сейчас — бот продолжит:\n"
                "⚡ принимать заказы и брони\n"
                "⚡ напоминать постоянным гостям\n"
                "⚡ собирать статистику для вас\n\n"
                "Если решили пока сделать паузу — спасибо, что пользовались ботом. "
                "Мы будем рады, если вы вернётесь позже 🙌"
            )

        try:
            await bot.send_message(admin_id, text)
            await r.hset(flags_key, mapping={flag_field: "1"})
        except Exception:
            # не падаем из-за одного кафе
            continue

async def sub_renewal_loop(bot: Bot):
    while True:
        try:
            await sub_renewal_check_and_send(bot)
        except Exception as e:
            logger.error("sub_renewal_loop error: %r", e, exc_info=True)
        # достаточно раз в час (или раз в день, если хочешь)
        await asyncio.sleep(3600)

async def smart_return_loop(bot: Bot):
    while True:
        try:
            await smart_return_check_and_send(bot)
        except Exception as e:
            logger.error("smart_return_loop: %r", e, exc_info=True)
        await asyncio.sleep(RETURN_CHECK_EVERY_SECONDS)


# =========================================================
# Startup / Webhook
# =========================================================
_smart_task: Optional[asyncio.Task] = None
_sub_task: Optional[asyncio.Task] = None
_order_reminder_task: Optional[asyncio.Task] = None
_expired_cafe_wipe_task: Optional[asyncio.Task] = None

async def on_startup(app: web.Application):
    bot: Bot = app["bot"]

    await set_commands(bot)

    global _smart_task, _sub_task, _order_reminder_task, _expired_cafe_wipe_task

    if _smart_task is None or _smart_task.done():
        _smart_task = asyncio.create_task(
            smart_return_loop(bot)
        )

    if _sub_task is None or _sub_task.done():
        _sub_task = asyncio.create_task(
            sub_renewal_loop(bot)
        )

    if _order_reminder_task is None or _order_reminder_task.done():
        _order_reminder_task = asyncio.create_task(
            unfinished_orders_reminder_worker(bot)
        )

    if (
        _expired_cafe_wipe_task is None
        or _expired_cafe_wipe_task.done()
    ):
        _expired_cafe_wipe_task = asyncio.create_task(
            expired_cafes_auto_wipe_loop(bot)
        )

    await bot.set_webhook(
        WEBHOOK_URL,
        secret_token=WEBHOOK_SECRET,
    )

    logger.info("Webhook set: %s", WEBHOOK_URL)

async def on_shutdown(app: web.Application):
    bot: Bot = app["bot"]
    storage: RedisStorage = app["storage"]
    r: redis.Redis = app["redis"]

    global _smart_task, _sub_task, _order_reminder_task, _expired_cafe_wipe_task

    tasks_to_stop = [
        ("smart_return_loop", _smart_task),
        ("sub_renewal_loop", _sub_task),
        ("unfinished_orders_reminder_worker", _order_reminder_task),
        ("expired_cafes_auto_wipe_loop", _expired_cafe_wipe_task),
    ]

    for task_name, task in tasks_to_stop:
        try:
            if task and not task.done():
                task.cancel()

                try:
                    await task
                except asyncio.CancelledError:
                    pass

                logger.info("Stopped background task: %s", task_name)
        except Exception:
            logger.exception(
                "Failed to stop background task: %s",
                task_name,
            )

    _smart_task = None
    _sub_task = None
    _order_reminder_task = None
    _expired_cafe_wipe_task = None

    try:
        await bot.delete_webhook()
        logger.info("Webhook deleted on shutdown")
    except Exception:
        logger.exception("Failed to delete webhook on shutdown")

    try:
        await storage.close()
        logger.info("FSM Redis storage closed")
    except Exception:
        logger.exception("Failed to close FSM Redis storage")

    try:
        await r.aclose()
        logger.info("Redis client closed")
    except Exception:
        logger.exception("Failed to close Redis client")

    try:
        await bot.session.close()
        logger.info("Bot HTTP session closed")
    except Exception:
        logger.exception("Failed to close bot HTTP session")

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set")
    if not WEBHOOK_SECRET:
        raise RuntimeError("WEBHOOK_SECRET not set")
    if len(WEBHOOK_SECRET) < 32:
        raise RuntimeError(
            "WEBHOOK_SECRET must be at least 32 characters long"
        )
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL not set")
    if not PUBLIC_HOST:
        raise RuntimeError("PUBLIC_HOST not set")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    r = await get_redis_client()
    bot.redis = r
    bot._redis = r

    storage = RedisStorage.from_url(REDIS_URL)
    dp = Dispatcher(storage=storage)
    dp.include_router(router)

    app = web.Application()
    app["bot"] = bot
    app["redis"] = r
    app["storage"] = storage

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    async def healthcheck(request: web.Request):
        return web.json_response({"status": "healthy"})

    app.router.add_get("/", healthcheck)

    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET,
        handle_in_background=True,
    ).register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)  # [web:1]

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info("Server running on 0.0.0.0:%s", PORT)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())




























































































































