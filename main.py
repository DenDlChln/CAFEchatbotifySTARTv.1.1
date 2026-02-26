import os
import json
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
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, BotCommand, ErrorEvent
from aiogram.filters import CommandStart, Command, StateFilter, CommandObject
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from aiogram.utils.deep_linking import create_start_link, create_startgroup_link  # [web:24]
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application  # [web:1]


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
PORT = int(os.getenv("PORT", "10000"))

WEBHOOK_PATH = f"/{WEBHOOK_SECRET}/webhook"
WEBHOOK_URL = f"https://{PUBLIC_HOST}{WEBHOOK_PATH}"

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
        if raw is not None and str(raw).strip() != "":
            return int(raw)
    except Exception:
        pass
    return cafe_admin_id_from_json(cafe_or_default(cafe_id))

async def is_cafe_admin(r: redis.Redis, user_id: int, cafe_id: str) -> bool:
    if is_superadmin(user_id):
        return True
    admin_id = await get_effective_admin_id(r, cafe_id)
    return admin_id != 0 and admin_id == user_id

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
def parse_start_payload(payload: str) -> Tuple[Optional[str], str]:
    p = (payload or "").strip()
    if not p:
        return None, "client"
    if p.startswith("admin:"):
        return p.split("admin:", 1)[1].strip() or None, "admin"
    if p.startswith("super:"):
        return p.split("super:", 1)[1].strip() or None, "super"
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
BTN_BACK = "⬅️ Назад"

MENU_EDIT_ADD = "➕ Добавить позицию"
MENU_EDIT_EDIT = "✏️ Изменить цену"
MENU_EDIT_DEL = "🗑 Удалить позицию"

BTN_VIEW_CLIENT = "⬅️ В клиентский режим"
BTN_VIEW_ADMIN = "🛠 В админ-режим"


# =========================================================
# Keyboards
# =========================================================
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
        [KeyboardButton(text=BTN_VIEW_CLIENT)],
    ]
    if is_super:
        kb.append([KeyboardButton(text="ℹ️ /help_admin")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, is_persistent=True)

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
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN_BACK)]], resize_keyboard=True, one_time_keyboard=True)

def kb_pick_menu_item(menu: Dict[str, int]) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=k)] for k in menu.keys()]
    rows.append([KeyboardButton(text=BTN_BACK)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


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

async def should_offer_repeat(r: redis.Redis, cafe_id: str, user_id: int) -> bool:
    last_seen = await r.get(k_last_seen(cafe_id, user_id))
    last_order = await r.get(k_last_order(cafe_id, user_id))
    if not last_order or not last_seen:
        return False
    try:
        last_seen_dt = datetime.fromtimestamp(float(last_seen), tz=MSK_TZ)
    except Exception:
        return False
    return last_seen_dt.date() != get_moscow_time().date()

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
DEFAULT_RETURN_CYCLE_DAYS = 7
RETURN_COOLDOWN_DAYS = 14
RETURN_CHECK_EVERY_SECONDS = 6 * 60 * 60
RETURN_SEND_FROM_HOUR = 10
RETURN_SEND_TO_HOUR = 20
RETURN_DISCOUNT_PERCENT = 10

def in_send_window_msk() -> bool:
    h = get_moscow_time().hour
    return RETURN_SEND_FROM_HOUR <= h < RETURN_SEND_TO_HOUR

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
    pipe.hset(customer_key, mapping={
        "first_name": first_name or "",
        "username": username or "",
        "last_order_ts": now_ts,
        "last_order_sum": int(total_sum),
        "last_drink": last_drink,
    })
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


# =========================================================
# Admin notify
# =========================================================
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
            await bot.send_message(int(group_id), text, disable_web_page_preview=True)
    except Exception:
        pass

async def send_admin_demo_to_user(bot: Bot, user_id: int, admin_like_text: str):
    if not DEMO_MODE:
        return
    demo_text = "ℹ️ <b>DEMO</b>: так это увидит админ:\n\n" + admin_like_text
    try:
        await bot.send_message(user_id, demo_text, disable_web_page_preview=True)
    except Exception:
        pass


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
        BotCommand(command="help_admin", description="Справка для админа/суперадмина"),
        BotCommand(command="bind", description="Привязать staff-группу (в группе)"),
        BotCommand(command="set_admin", description="Назначить админа кафе (superadmin)"),
        BotCommand(command="unset_admin", description="Сбросить override admin_id (superadmin)"),
    ]
    await bot.set_my_commands(cmds)  # [web:204]

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
    is_super = is_superadmin(uid)

    args = (command.args or "").strip()
    cafe_id = args if args in CAFES else None

    cafes_list = ", ".join(sorted(CAFES.keys())[:30])
    if len(CAFES) > 30:
        cafes_list += f" … (+{len(CAFES)-30})"

    lines: List[str] = []
    lines.append("🧾 <b>Справка админа</b>")
    lines.append(f"Ваш ID: <code>{uid}</code>")
    lines.append(f"Роль: <b>{'SUPERADMIN' if is_super else 'пользователь/админ кафе'}</b>")
    lines.append("")
    lines.append("✅ <b>Базовые команды</b>")
    lines.append("• <code>/myid</code> — показать ваш Telegram ID")
    lines.append("• <code>/whoami</code> — роль и текущее кафе")
    lines.append("• <code>/start admin:cafe_001</code> — открыть админ-панель кафе")
    lines.append("• <code>/bind cafe_001</code> — привязать текущую группу как staff-группу (в группе)")
    lines.append("")
    if is_super:
        lines.append("⭐ <b>Команды супер-админа</b>")
        lines.append("• <code>/set_admin cafe_001 123456789</code> — назначить админа кафе (Redis override)")
        lines.append("• <code>/unset_admin cafe_001</code> — сбросить override admin_id")
        lines.append("")
    lines.append("🏪 <b>Доступные cafe_id</b>")
    lines.append(html.quote(cafes_list))
    lines.append("")
    lines.append("ℹ️ Подсказка: <code>/help_admin cafe_001</code> покажет ссылки для конкретного кафе.")

    if cafe_id:
        cafe = cafe_or_default(cafe_id)
        eff_admin = await get_effective_admin_id(r, cafe_id)
        client_link = await create_start_link(message.bot, payload=cafe_id, encode=True)  # [web:24]
        admin_link = await create_start_link(message.bot, payload=f"admin:{cafe_id}", encode=True)  # [web:24]
        staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)  # [web:24]
        lines.append("")
        lines.append(f"🏪 <b>{html.quote(cafe_title(cafe))}</b> (<code>{html.quote(cafe_id)}</code>)")
        lines.append(f"admin_id (effective): <code>{eff_admin}</code>")
        lines.append("")
        lines.append("🔗 <b>Ссылки</b>")
        lines.append(f"• Клиентам: {client_link}")
        lines.append(f"• Админу: {admin_link}")
        lines.append(f"• В staff-группу: {staff_link}")

    await message.answer("\n".join(lines), disable_web_page_preview=True)

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
    try:
        await r.hdel(k_cafe_profile(cafe_id), "admin_id")
    except Exception:
        pass
    await message.answer(f"✅ Override admin_id сброшен для <code>{html.quote(cafe_id)}</code>.")


# =========================================================
# /bind staff group
# =========================================================
@router.message(Command("bind"))
async def cmd_bind(message: Message, command: CommandObject):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда /bind работает только в группе.")
        return

    cafe_id = (command.args or "").strip()
    if not cafe_id or cafe_id not in CAFES:
        await message.answer("Формат: <code>/bind cafe_001</code>")
        return

    r: redis.Redis = message.bot._redis
    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("Только администратор этого кафе может привязать группу.")
        return

    await r.set(k_staff_group(cafe_id), str(message.chat.id))
    await message.answer(f"✅ Группа привязана к кафе <code>{html.quote(cafe_id)}</code>.")


# =========================================================
# /start
# =========================================================
WELCOME_VARIANTS = [
    "Рад тебя видеть, {name}!",
    "{name}, добро пожаловать!",
    "Привет, {name}!",
]
CHOICE_VARIANTS = [
    "Отличный выбор!",
    "Классика.",
    "Звучит вкусно!",
]
FINISH_VARIANTS = [
    "Спасибо за заказ, {name}!",
    "Принято, {name}. Заглядывай ещё!",
]

async def send_admin_panel(message: Message, cafe_id: str, cafe: Dict[str, Any], menu: Dict[str, int]):
    client_link = await create_start_link(message.bot, payload=cafe_id, encode=True)  # [web:24]
    admin_link = await create_start_link(message.bot, payload=f"admin:{cafe_id}", encode=True)  # [web:24]
    staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)  # [web:24]

    eff_admin = await get_effective_admin_id(message.bot._redis, cafe_id)

    await message.answer(
        "🛠 <b>Админ-панель</b>\n\n"
        f"Кафе: <b>{html.quote(cafe_title(cafe))}</b>\n"
        f"ID: <code>{html.quote(cafe_id)}</code>\n"
        f"admin_id (effective): <code>{eff_admin}</code>\n"
        f"{work_status(cafe)}{address_line(cafe)}\n\n"
        "🔗 <b>Ссылки</b>\n"
        f"• Клиентам: {client_link}\n"
        f"• Админу: {admin_link}\n"
        f"• В staff-группу: {staff_link}\n\n"
        "В staff-группе выполните:\n"
        f"<code>/bind {html.quote(cafe_id)}</code>\n\n"
        "Справка: /help_admin",
        reply_markup=kb_admin_main(is_superadmin(message.from_user.id)),
        disable_web_page_preview=True,
    )

@router.message(CommandStart(deep_link=True))
async def cmd_start_deep(message: Message, command: CommandObject, state: FSMContext):
    await cmd_start(message, command, state)

@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    r: redis.Redis = message.bot._redis

    payload = (command.args or "").strip()
    cafe_id_payload, mode = parse_start_payload(payload)

    cafe_id = await resolve_cafe_id(r, message, cafe_id_payload)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    uid = message.from_user.id
    name = html.quote(user_name(message))
    welcome = random.choice(WELCOME_VARIANTS).format(name=name)

    is_admin = await is_cafe_admin(r, uid, cafe_id)
    view_mode = str(await r.get(k_view_mode(uid)) or "admin")  # "admin" | "client"

    # deep-link admin/super: если есть права — принудительно админка
    if mode in ("admin", "super"):
        if not is_admin:
            await message.answer("🔒 Админ-доступ запрещён.")
            return
        await r.set(k_view_mode(uid), "admin")
        await send_admin_panel(message, cafe_id, cafe, menu)
        return

    # обычный /start: если админ и не переключался в client — показываем админку
    if is_admin and view_mode != "client":
        await send_admin_panel(message, cafe_id, cafe, menu)
        return

    # дальше клиентский сценарий
    offer_repeat = await should_offer_repeat(r, cafe_id, uid)
    await set_last_seen(r, cafe_id, uid)

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
                f"{welcome}\n\nВы давно не заходили. Повторить последний заказ?\n\n" + "\n".join(lines),
                reply_markup=kb_repeat_offer(),
            )
            return

    await message.answer(
        f"{welcome}\n\n🏪 {work_status(cafe)}{address_line(cafe)}\n\n"
        "Чтобы добавить в корзину: нажмите напиток → выберите количество.\n"
        "Корзина — «🛒 Корзина».",
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )


# =========================================================
# Client: repeat
# =========================================================
@router.message(F.text == BTN_REPEAT_NO)
async def repeat_no(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    await state.update_data(repeat_offer_snapshot=None)
    await message.answer("Ок.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))

@router.message(F.text == BTN_REPEAT_LAST)
async def repeat_last(message: Message, state: FSMContext):
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
# Client: info
# =========================================================
@router.message(F.text == BTN_CALL)
async def call_phone(message: Message):
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
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)

    cart = get_cart(await state.get_data())
    await state.set_state(OrderStates.cart_view)
    await state.update_data(cart=cart)
    await message.answer(cart_text(cart, menu), reply_markup=kb_cart(menu, bool(cart)))

@router.message(F.text == BTN_CART)
async def cart_button(message: Message, state: FSMContext):
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
    await state.update_data(cart={})
    await show_cart(message, state)

@router.message(F.text == BTN_CANCEL_ORDER)
async def cancel_order(message: Message, state: FSMContext):
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


# =========================================================
# Client: add item
# =========================================================
async def start_add_item(message: Message, state: FSMContext, cafe_id: str, menu: Dict[str, int], drink: str):
    price = menu.get(drink)
    if price is None:
        r: redis.Redis = message.bot._redis
        is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

        await message.answer("Этой позиции уже нет.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    cart = get_cart(await state.get_data())
    await state.set_state(OrderStates.waiting_for_quantity)
    await state.update_data(current_drink=drink, cart=cart)

    await message.answer(
        f"{random.choice(CHOICE_VARIANTS)}\n\n🥤 <b>{html.quote(drink)}</b>\n💰 <b>{price}₽</b>\n\nСколько добавить?",
        reply_markup=kb_qty(),
    )

@router.message(StateFilter(OrderStates.waiting_for_quantity))
async def process_quantity(message: Message, state: FSMContext):
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
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if not cafe_open(cafe):
        await message.answer(closed_message(cafe, menu), reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    cart = get_cart(await state.get_data())
    if not cart:
        await message.answer("Корзина пустая.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    await state.set_state(OrderStates.waiting_for_confirmation)
    await message.answer("✅ <b>Подтвердите заказ</b>\n\n" + cart_text(cart, menu), reply_markup=kb_confirm())

@router.message(StateFilter(OrderStates.waiting_for_confirmation))
async def confirm_order(message: Message, state: FSMContext):
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

async def finalize_order(message: Message, state: FSMContext, ready_in_min: int):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    user_id = message.from_user.id
    cart = get_cart(await state.get_data())
    if not cart:
        await state.clear()
        await message.answer("Корзина пустая.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
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

    await set_last_order_snapshot(r, cafe_id, user_id, {"cart": cart, "total": total, "ts": int(time.time())})

    await r.incr(k_stats_total_orders(cafe_id))
    await r.incrby(k_stats_total_revenue(cafe_id), int(total))
    for drink, qty in cart.items():
        qty_i = int(qty)
        price = int(menu.get(drink, 0))
        await r.incrby(k_stats_drink_cnt(cafe_id, drink), qty_i)
        await r.incrby(k_stats_drink_rev(cafe_id, drink), qty_i * price)

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

    admin_msg = (
        f"🔔 <b>НОВЫЙ ЗАКАЗ #{order_num}</b> | {html.quote(cafe_title(cafe))}\n\n"
        f"<a href=\"tg://user?id={user_id}\">{html.quote(message.from_user.username or message.from_user.first_name or 'Клиент')}</a>\n"
        f"<code>{user_id}</code>\n\n"
        f"✍️ <a href=\"tg://user?id={user_id}\">Написать клиенту</a>\n\n"
        + "\n".join(cart_lines(cart, menu))
        + f"\n\n💰 Итого: <b>{total}₽</b>\n⏱ Готовность: <b>{html.quote(ready_line)}</b>"
    )

    await notify_admin(message.bot, r, cafe_id, admin_msg)
    await send_admin_demo_to_user(message.bot, user_id, admin_msg)

    finish = random.choice(FINISH_VARIANTS).format(name=html.quote(user_name(message)))
    await message.answer(
        f"🎉 <b>Заказ принят!</b>\n\n{cart_text(cart, menu)}\n\n⏱ Готовность: {html.quote(ready_line)}\n\n{finish}",
        reply_markup=kb_client_main(menu, show_admin_button=is_admin),
    )
    await state.clear()

@router.message(StateFilter(OrderStates.waiting_for_ready_time))
async def ready_time(message: Message, state: FSMContext):
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
    await state.clear()
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    warn = ""
    if not cafe_open(cafe):
        ws, _ = cafe_hours(cafe)
        warn = (
            "\n\n⚠️ <b>Сейчас нерабочее время.</b>\n"
            f"Администратор ответит с началом рабочего дня (с {ws}:00 МСК)."
        )

    await state.set_state(BookingStates.waiting_for_datetime)
    await message.answer(
        "📅 <b>Бронирование</b>\n\n"
        "Напишите дату и время: <code>15.02 19:00</code>\n"
        "Или «Отмена»." + warn,
        reply_markup=kb_booking_cancel(),
    )

@router.message(StateFilter(BookingStates.waiting_for_datetime))
async def booking_datetime(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок, отменил.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    m = re.match(r"^\s*(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})\s*$", message.text or "")
    if not m:
        await message.answer("Формат: <code>15.02 19:00</code>", reply_markup=kb_booking_cancel())
        return

    day, month, hour, minute = map(int, m.groups())
    year = get_moscow_time().year
    try:
        dt = datetime(year, month, day, hour, minute, tzinfo=MSK_TZ)
    except Exception:
        await message.answer("Дата/время некорректны.", reply_markup=kb_booking_cancel())
        return

    await state.update_data(booking_dt=dt.strftime("%d.%m %H:%M"))
    await state.set_state(BookingStates.waiting_for_people)
    await message.answer("Сколько гостей? (1–10)", reply_markup=kb_booking_people())

@router.message(StateFilter(BookingStates.waiting_for_people))
async def booking_people(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок, отменил.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    try:
        people = int((message.text or "").strip())
        if not (1 <= people <= 10):
            raise ValueError
    except Exception:
        await message.answer("Нужно число 1–10.", reply_markup=kb_booking_people())
        return

    await state.update_data(booking_people=people)
    await state.set_state(BookingStates.waiting_for_comment)
    await message.answer("Комментарий (или <code>-</code>):", reply_markup=kb_booking_cancel())

@router.message(StateFilter(BookingStates.waiting_for_comment))
async def booking_finish(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    is_admin = await is_cafe_admin(r, message.from_user.id, cafe_id)

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок, отменил.", reply_markup=kb_client_main(menu, show_admin_button=is_admin))
        return

    data = await state.get_data()
    dt_str = str(data.get("booking_dt") or "—")
    people = int(data.get("booking_people") or 0)
    comment = (message.text or "").strip() or "-"

    booking_id = str(int(time.time()))[-6:]
    user_id = message.from_user.id

    admin_msg = (
        f"📋 <b>НОВАЯ БРОНЬ #{booking_id}</b> | {html.quote(cafe_title(cafe))}\n\n"
        f"<a href=\"tg://user?id={user_id}\">{html.quote(message.from_user.username or message.from_user.first_name or 'Клиент')}</a>\n"
        f"<code>{user_id}</code>\n\n"
        f"✍️ <a href=\"tg://user?id={user_id}\">Написать клиенту</a>\n\n"
        f"🗓 {html.quote(dt_str)}\n👥 {people} чел.\n💬 {html.quote(comment)}"
    )
    await notify_admin(message.bot, r, cafe_id, admin_msg)

    if cafe_open(cafe):
        user_text = "✅ Заявка на бронь отправлена администратору. Он свяжется с вами в Telegram."
    else:
        ws, _ = cafe_hours(cafe)
        user_text = (
            "✅ Заявка на бронь принята.\n\n"
            "⚠️ Сейчас кафе закрыто — администратор ответит в рабочее время "
            f"(с {ws}:00 МСК)."
        )

    await message.answer(user_text, reply_markup=kb_client_main(menu, show_admin_button=is_admin))
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
async def back_to_client(message: Message):
    r: redis.Redis = message.bot._redis
    await r.set(k_view_mode(message.from_user.id), "client")
    await message.answer("Ок. Переключил в клиентский режим.\nНажмите /start, чтобы увидеть интерфейс клиента.")

@router.message(F.text == BTN_VIEW_ADMIN)
async def back_to_admin(message: Message):
    r: redis.Redis = message.bot._redis
    await r.set(k_view_mode(message.from_user.id), "admin")
    await message.answer("Ок. Переключил в админ-режим.\nНажмите /start, чтобы открыть админ-панель.")

@router.message(F.text == BTN_LINKS)
async def admin_links_button(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("🔒 Доступно только администратору.")
        return
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    await send_admin_panel(message, cafe_id, cafe, menu)

@router.message(F.text == BTN_STAFF_GROUP)
async def admin_staff_group_button(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        await message.answer("🔒 Доступно только администратору.")
        return

    staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)  # [web:24]
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
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        if DEMO_MODE:
            await message.answer(demo_stats_preview_text())
        else:
            await message.answer("📊 Статистика доступна администратору.")
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

@router.message(F.text == BTN_MENU_EDIT)
async def menu_edit_entry(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)

    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
        if DEMO_MODE:
            await message.answer(demo_menu_edit_preview_text(), reply_markup=kb_menu_edit())
            await message.answer("🔒 Редактирование доступно только администратору.")
        else:
            await message.answer("🔒 Редактирование доступно только администратору.")
        return

    await state.clear()
    await state.set_state(MenuEditStates.waiting_for_action)
    await message.answer("🛠 Управление меню: выберите действие", reply_markup=kb_menu_edit())

@router.message(StateFilter(MenuEditStates.waiting_for_action))
async def menu_edit_choose_action(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    if not await is_cafe_admin(r, message.from_user.id, cafe_id):
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


# =========================================================
# Fallback (drink pick)
# =========================================================
@router.message(F.text)
async def any_text(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    text = (message.text or "").strip()
    if text in menu:
        if not cafe_open(cafe):
            await message.answer(closed_message(cafe, menu), reply_markup=kb_client_main(menu))
            return
        await start_add_item(message, state, cafe_id, menu, text)
        return

    await message.answer("Нажмите напиток или «🛒 Корзина».", reply_markup=kb_client_main(menu))


# =========================================================
# Smart return loop
# =========================================================
async def smart_return_check_and_send(bot: Bot):
    if not in_send_window_msk():
        return

    r: redis.Redis = bot._redis
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

            if not profile or str(profile.get("offers_opt_out", "0")) == "1":
                continue

            try:
                last_order_ts = int(float(profile.get("last_order_ts", "0") or 0))
            except Exception:
                continue

            days_since = (now_ts - last_order_ts) // 86400
            if days_since < DEFAULT_RETURN_CYCLE_DAYS:
                continue

            try:
                last_trigger_ts = int(float(profile.get("last_trigger_ts", "0") or 0))
            except Exception:
                last_trigger_ts = 0

            if last_trigger_ts and (now_ts - last_trigger_ts) < (RETURN_COOLDOWN_DAYS * 86400):
                continue

            first_name = profile.get("first_name") or "друг"
            favorite = await get_favorite_drink(r, cafe_id, user_id) or profile.get("last_drink") or "напиток"
            promo = promo_code(user_id)

            text = (
                f"{html.quote(str(first_name))}, давно не виделись ☕\n\n"
                f"Ваш любимый <b>{html.quote(str(favorite))}</b> сегодня со скидкой <b>{RETURN_DISCOUNT_PERCENT}%</b>.\n"
                f"Промокод: <code>{promo}</code>\n\n"
                "Сделаем заказ? Нажмите /start."
            )

            try:
                await bot.send_message(user_id, text)
                await r.hset(customer_key, mapping={"last_trigger_ts": str(now_ts)})
            except Exception:
                try:
                    await r.srem(k_customers_set(cafe_id), user_id)
                except Exception:
                    pass

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

async def on_startup(app: web.Application):
    bot: Bot = app["bot"]
    await set_commands(bot)

    global _smart_task
    if _smart_task is None or _smart_task.done():
        _smart_task = asyncio.create_task(smart_return_loop(bot))

    await bot.set_webhook(WEBHOOK_URL, secret_token=WEBHOOK_SECRET)  # [web:1]
    logger.info("Webhook set: %s", WEBHOOK_URL)

async def on_shutdown(app: web.Application):
    bot: Bot = app["bot"]
    storage: RedisStorage = app["storage"]
    r: redis.Redis = app["redis"]

    global _smart_task
    try:
        if _smart_task and not _smart_task.done():
            _smart_task.cancel()
    except Exception:
        pass

    try:
        await bot.delete_webhook()
    except Exception:
        pass
    try:
        await storage.close()
    except Exception:
        pass
    try:
        await r.aclose()
    except Exception:
        pass
    try:
        await bot.session.close()
    except Exception:
        pass


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set")
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL not set")
    if not PUBLIC_HOST:
        raise RuntimeError("PUBLIC_HOST not set")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    r = await get_redis_client()
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

    async def healthcheck(_: web.Request):
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








