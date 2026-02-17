import os
import json
import time
import asyncio
import random
import logging
import re
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, List

import redis.asyncio as redis
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command, StateFilter, CommandObject
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
    ErrorEvent,
)

from aiogram.utils.deep_linking import create_start_link, create_startgroup_link  # [web:24]
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application  # [web:1]


# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("cafebotify")


# -------------------------
# Time
# -------------------------
MSK_TZ = timezone(timedelta(hours=3))

def now_msk() -> datetime:
    return datetime.now(MSK_TZ)

def msk_hm() -> str:
    return now_msk().strftime("%H:%M")


# -------------------------
# Env
# -------------------------
CLIENT_BOT_TOKEN = (os.getenv("CLIENT_BOT_TOKEN") or "").strip()
ADMIN_BOT_TOKEN = (os.getenv("ADMIN_BOT_TOKEN") or "").strip()
REDIS_URL = (os.getenv("REDIS_URL") or "").strip()

PUBLIC_HOST = (os.getenv("PUBLIC_HOST") or "").strip()
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "MySuperHook123").strip()

PORT = int(os.getenv("PORT", "10000"))

CLIENT_WEBHOOK_PATH = f"/{WEBHOOK_SECRET}/client"
ADMIN_WEBHOOK_PATH = f"/{WEBHOOK_SECRET}/admin"

CLIENT_WEBHOOK_URL = f"https://{PUBLIC_HOST}{CLIENT_WEBHOOK_PATH}"
ADMIN_WEBHOOK_URL = f"https://{PUBLIC_HOST}{ADMIN_WEBHOOK_PATH}"


# -------------------------
# Config
# -------------------------
def load_config() -> Dict[str, Any]:
    env_path = (os.getenv("CONFIG_PATH") or "").strip()
    base_dir = Path(__file__).resolve().parent  # /app

    try:
        files = sorted([(x.name, x.stat().st_size) for x in base_dir.iterdir() if x.is_file()])
        logger.info("Files in %s: %s", base_dir, files)
    except Exception as e:
        logger.warning("Cannot list files in %s: %r", base_dir, e)

    candidates = []
    if env_path:
        candidates.append(env_path)
    candidates += ["config_330_template.json", "config.json"]

    tried: List[str] = []
    last_err: Optional[Exception] = None

    for name in candidates:
        if not name:
            continue
        p = Path(name)
        if not p.is_absolute():
            p = base_dir / p
        tried.append(str(p))

        if not p.exists() or not p.is_file():
            continue

        raw = p.read_text(encoding="utf-8", errors="replace")
        if not raw.strip():
            last_err = ValueError(f"Config file is empty: {p} (size={p.stat().st_size} bytes)")
            logger.error("%r", last_err)
            continue

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            preview = raw.strip()[:200]
            last_err = ValueError(f"Config is not valid JSON: {p}. Preview: {preview}")
            logger.error("%r", last_err)
            continue

        if not isinstance(data, dict):
            last_err = ValueError(f"config root must be object: {p}")
            logger.error("%r", last_err)
            continue
        if "cafes" not in data or not isinstance(data["cafes"], dict):
            last_err = ValueError(f"config.cafes must be object: {p}")
            logger.error("%r", last_err)
            continue

        logger.info("CONFIG loaded: %s (cafes=%d)", p, len(data["cafes"]))
        return data

    msg = "Config load failed. Tried: " + ", ".join(tried)
    if last_err:
        raise RuntimeError(msg) from last_err
    raise FileNotFoundError(msg)


CONFIG = load_config()
CAFES: Dict[str, Dict[str, Any]] = CONFIG["cafes"]
DEFAULT_CAFE_ID: str = str(CONFIG.get("default_cafe_id") or next(iter(CAFES.keys())))
SUPERADMIN_ID: int = int(CONFIG.get("superadmin_id") or 0)


def cafe_or_default(cafe_id: Optional[str]) -> Dict[str, Any]:
    if cafe_id and cafe_id in CAFES:
        return CAFES[cafe_id]
    return CAFES[DEFAULT_CAFE_ID]

def is_superadmin(user_id: int) -> bool:
    return bool(SUPERADMIN_ID) and user_id == SUPERADMIN_ID


# -------------------------
# Redis keys (multi-tenant)
# -------------------------
def k_user_cafe(user_id: int) -> str:
    return f"user:{user_id}:cafe_id"

def k_staff_group(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:staff_group_id"

def k_cafe_menu(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:menu"  # hash: drink -> price

def k_stats_total_orders(cafe_id: str) -> str:
    return f"stats:{cafe_id}:total_orders"

def k_stats_total_revenue(cafe_id: str) -> str:
    return f"stats:{cafe_id}:total_revenue"

def k_stats_drink_cnt(cafe_id: str, drink: str) -> str:
    return f"stats:{cafe_id}:drink:{drink}:cnt"

def k_stats_drink_rev(cafe_id: str, drink: str) -> str:
    return f"stats:{cafe_id}:drink:{drink}:rev"

def k_rl(user_id: int) -> str:
    return f"rate_limit:{user_id}"

def k_last_seen(user_id: int) -> str:
    return f"lastseen:{user_id}"

def k_last_order(user_id: int) -> str:
    return f"lastorder:{user_id}"

def k_customers_set(cafe_id: str) -> str:
    return f"customers:{cafe_id}:set"

def k_customer_profile(cafe_id: str, user_id: int) -> str:
    return f"customer:{cafe_id}:{user_id}:profile"  # hash

def k_customer_drinks(cafe_id: str, user_id: int) -> str:
    return f"customer:{cafe_id}:{user_id}:drinks"  # hash: drink -> cnt


# -------------------------
# Cafe helpers
# -------------------------
def parse_work_hours(cafe: Dict[str, Any]) -> Tuple[int, int]:
    feat = cafe.get("features") or {}
    ws = int(feat.get("work_start", cafe.get("work_start", 9)))
    we = int(feat.get("work_end", cafe.get("work_end", 21)))
    return ws, we

def is_cafe_open(cafe: Dict[str, Any]) -> bool:
    ws, we = parse_work_hours(cafe)
    return ws <= now_msk().hour < we

def work_status_line(cafe: Dict[str, Any]) -> str:
    ws, we = parse_work_hours(cafe)
    if is_cafe_open(cafe):
        return f"🟢 Открыто до {we}:00 (МСК)"
    return f"🔴 Закрыто\n🕐 Открываемся: {ws}:00 (МСК)"

def rate_limit_seconds(cafe: Dict[str, Any]) -> int:
    feat = cafe.get("features") or {}
    try:
        return int(feat.get("rate_limit_seconds", 60))
    except Exception:
        return 60

def orders_enabled(cafe: Dict[str, Any]) -> bool:
    feat = cafe.get("features") or {}
    return bool(feat.get("orders_enabled", True))

def booking_enabled(cafe: Dict[str, Any]) -> bool:
    feat = cafe.get("features") or {}
    return bool(feat.get("booking_enabled", True))

def staff_group_enabled(cafe: Dict[str, Any]) -> bool:
    feat = cafe.get("features") or {}
    return bool(feat.get("staff_group_enabled", True))

def cafe_title(cafe: Dict[str, Any]) -> str:
    return str(cafe.get("title") or "Кафе")

def cafe_phone(cafe: Dict[str, Any]) -> str:
    return str(cafe.get("phone") or "")

def cafe_address(cafe: Dict[str, Any]) -> str:
    return str(cafe.get("address") or "")

def cafe_admin_id(cafe: Dict[str, Any]) -> int:
    try:
        return int(cafe.get("admin_id") or 0)
    except Exception:
        return 0

def username_of(message: Message) -> str:
    if not message.from_user:
        return "гость"
    return message.from_user.first_name or "гость"


# -------------------------
# Menu I/O
# -------------------------
async def get_menu(r: redis.Redis, cafe_id: str) -> Dict[str, int]:
    data = await r.hgetall(k_cafe_menu(cafe_id))
    if data:
        out: Dict[str, int] = {}
        for k, v in data.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                continue
        if out:
            return out

    # fallback to config; also seed redis for future edits
    base = cafe_or_default(cafe_id).get("menu") or {}
    seed: Dict[str, str] = {}
    out: Dict[str, int] = {}
    if isinstance(base, dict):
        for k, v in base.items():
            try:
                out[str(k)] = int(v)
                seed[str(k)] = str(int(v))
            except Exception:
                continue
    if seed:
        await r.hset(k_cafe_menu(cafe_id), mapping=seed)
    return out

async def set_menu_item(r: redis.Redis, cafe_id: str, drink: str, price: int) -> None:
    await r.hset(k_cafe_menu(cafe_id), mapping={drink: str(int(price))})

async def del_menu_item(r: redis.Redis, cafe_id: str, drink: str) -> None:
    await r.hdel(k_cafe_menu(cafe_id), drink)


# -------------------------
# Tenant resolve
# -------------------------
async def resolve_cafe_id(r: redis.Redis, message: Message, payload: Optional[str]) -> str:
    uid = message.from_user.id if message.from_user else 0

    if payload:
        payload = payload.strip()
        if payload in CAFES:
            await r.set(k_user_cafe(uid), payload)
            return payload

    cid = await r.get(k_user_cafe(uid))
    if cid and str(cid) in CAFES:
        return str(cid)

    await r.set(k_user_cafe(uid), DEFAULT_CAFE_ID)
    return DEFAULT_CAFE_ID


# -------------------------
# Keyboards (client)
# -------------------------
BTNCALL = "📞 Связаться с кафе"
BTNHOURS = "⏰ Режим работы"
BTNBOOKING = "📋 Бронирование / столики"

BTNCART = "🛒 Корзина"
BTNCHECKOUT = "✅ Оформить заказ"
BTNCLEARCART = "🧹 Очистить корзину"
BTNEDITCART = "✏️ Изменить корзину"
BTNCANCELORDER = "❌ Отменить заказ"

BTNCONFIRM = "Подтвердить"
BTNCANCEL = "Отмена"
BTNBACK = "Назад"

BTNREADYNOW = "Как можно быстрее"
BTNREADY20 = "Через 20 минут"

BTNREPEATLAST = "Повторить вчерашний заказ"
BTNREPEATNO = "Нет, спасибо"

CARTACTPLUS = "+1"
CARTACTMINUS = "-1"
CARTACTDEL = "Удалить"
CARTACTDONE = "Готово"

def kb_main(menu: Dict[str, int]) -> ReplyKeyboardMarkup:
    rows: List[List[KeyboardButton]] = []
    for drink in menu.keys():
        rows.append([KeyboardButton(text=drink)])
    rows.append([KeyboardButton(text=BTNCART), KeyboardButton(text=BTNCHECKOUT), KeyboardButton(text=BTNBOOKING)])
    rows.append([KeyboardButton(text=BTNCALL), KeyboardButton(text=BTNHOURS)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)

def kb_cart(has_items: bool, menu: Dict[str, int]) -> ReplyKeyboardMarkup:
    rows: List[List[KeyboardButton]] = []
    rows.append([KeyboardButton(text=BTNCART), KeyboardButton(text=BTNCHECKOUT)])
    if has_items:
        rows.append([KeyboardButton(text=BTNEDITCART), KeyboardButton(text=BTNCLEARCART), KeyboardButton(text=BTNCANCELORDER)])
    else:
        rows.append([KeyboardButton(text=BTNCANCELORDER)])
    for drink in menu.keys():
        rows.append([KeyboardButton(text=drink)])
    rows.append([KeyboardButton(text=BTNBOOKING)])
    rows.append([KeyboardButton(text=BTNCALL), KeyboardButton(text=BTNHOURS)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)

def kb_qty() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text=BTNCANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_confirm() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTNCONFIRM), KeyboardButton(text=BTNCART), KeyboardButton(text=BTNCANCELORDER)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_ready_time() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTNREADYNOW), KeyboardButton(text=BTNREADY20), KeyboardButton(text=BTNCANCEL)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_repeat_offer() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTNREPEATLAST), KeyboardButton(text=BTNREPEATNO)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_pick_cart_item(cart: Dict[str, int]) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=k)] for k in cart.keys()]
    rows.append([KeyboardButton(text=BTNCANCEL), KeyboardButton(text=BTNCART)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)

def kb_cart_edit_actions() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=CARTACTPLUS), KeyboardButton(text=CARTACTMINUS)],
            [KeyboardButton(text=CARTACTDEL), KeyboardButton(text=CARTACTDONE)],
            [KeyboardButton(text=BTNCANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_booking_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTNCANCEL)]], resize_keyboard=True, one_time_keyboard=True)

def kb_booking_people() -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=str(i)) for i in range(1, 6)], [KeyboardButton(text=str(i)) for i in range(6, 11)]]
    rows.append([KeyboardButton(text=BTNCANCEL)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


# -------------------------
# Keyboards (admin)
# -------------------------
BTNADMIN_MENU = "🧾 Меню"
BTNADMIN_STATS = "📊 Статистика"
BTNADMIN_STAFF = "👥 Группа персонала"
BTNADMIN_LINKS = "🔗 Ссылки"
BTNADMIN_BACK = "⬅️ Назад"

MENUEDIT_ADD = "➕ Добавить"
MENUEDIT_EDIT = "✏️ Изменить цену"
MENUEDIT_DEL = "🗑 Удалить"

def kb_admin_home() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTNADMIN_MENU), KeyboardButton(text=BTNADMIN_STATS)],
            [KeyboardButton(text=BTNADMIN_STAFF), KeyboardButton(text=BTNADMIN_LINKS)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )

def kb_admin_menu_edit() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENUEDIT_ADD), KeyboardButton(text=MENUEDIT_EDIT), KeyboardButton(text=MENUEDIT_DEL)],
            [KeyboardButton(text=BTNADMIN_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_admin_back() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTNADMIN_BACK)]], resize_keyboard=True, one_time_keyboard=True)

def kb_pick_menu_item(menu: Dict[str, int]) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=k)] for k in menu.keys()]
    rows.append([KeyboardButton(text=BTNADMIN_BACK)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)


# -------------------------
# Client FSM (from DEMO)
# -------------------------
class ClientOrderStates(StatesGroup):
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


# -------------------------
# Admin FSM
# -------------------------
class AdminStates(StatesGroup):
    home = State()
    menu_action = State()
    menu_add_name = State()
    menu_add_price = State()
    menu_pick_edit_item = State()
    menu_edit_price = State()
    menu_pick_del_item = State()
    bind_wait_cafe_id = State()


# -------------------------
# Smart return settings (from DEMO)
# -------------------------
DEFAULT_RETURN_CYCLE_DAYS = 7
RETURN_COOLDOWN_DAYS = 30
RETURN_CHECK_EVERY_SECONDS = 6 * 60 * 60
RETURN_SEND_FROM_HOUR = 10
RETURN_SEND_TO_HOUR = 20
RETURN_DISCOUNT_PERCENT = 10

def in_send_window_msk() -> bool:
    h = now_msk().hour
    return RETURN_SEND_FROM_HOUR <= h < RETURN_SEND_TO_HOUR

def promocode(user_id: int) -> str:
    return f"CB{user_id % 100000:05d}{int(time.time()) % 10000:04d}"


# -------------------------
# Text variants
# -------------------------
WELCOME_VARIANTS = [
    "Привет, {name}! ☕️",
    "{name}, добро пожаловать!",
    "Привет, {name}! Готовим вкусный кофе.",
]
CHOICE_VARIANTS = [
    "Отличный выбор!",
    "Супер!",
    "Классика.",
]
FINISH_VARIANTS = [
    "Спасибо, {name}! Ждём снова.",
    "Заказ принят, {name}.",
]


# -------------------------
# Common: notify admin-bot + staff group
# -------------------------
async def notify_admins(client_bot: Bot, cafe_id: str, text: str) -> None:
    cafe = cafe_or_default(cafe_id)
    admin_id = cafe_admin_id(cafe)
    if admin_id:
        try:
            # пишем админам через админ-бот (не через client)
            admin_bot: Bot = client_bot._admin_bot
            await admin_bot.send_message(admin_id, text, disable_web_page_preview=True)
        except Exception:
            pass

    # staff group (если привязана)
    try:
        r: redis.Redis = client_bot._redis
        gid = await r.get(k_staff_group(cafe_id))
        if gid:
            await client_bot.send_message(int(gid), text, disable_web_page_preview=True)
    except Exception:
        pass


# -------------------------
# Client helpers: cart
# -------------------------
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
        lines.append(f"{html.quote(d)} × {q} — <b>{p*q}</b> р")
    return lines

def cart_text(cart: Dict[str, int], menu: Dict[str, int]) -> str:
    if not cart:
        return "🛒 <b>Корзина пуста</b>"
    total = cart_total(cart, menu)
    return "🛒 <b>Корзина</b>\n\n" + "\n".join(cart_lines(cart, menu)) + f"\n\nИтого: <b>{total}</b> р"


# -------------------------
# Client: repeat last order snapshot
# -------------------------
async def set_last_seen(r: redis.Redis, user_id: int) -> None:
    await r.set(k_last_seen(user_id), str(time.time()))

async def should_offer_repeat(r: redis.Redis, user_id: int) -> bool:
    last_seen = await r.get(k_last_seen(user_id))
    last_order = await r.get(k_last_order(user_id))
    if not last_seen or not last_order:
        return False
    try:
        dt = datetime.fromtimestamp(float(last_seen), tz=MSK_TZ)
    except Exception:
        return False
    return dt.date() != now_msk().date()

async def get_last_order_snapshot(r: redis.Redis, user_id: int) -> Optional[dict]:
    raw = await r.get(k_last_order(user_id))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

async def set_last_order_snapshot(r: redis.Redis, user_id: int, snapshot: dict) -> None:
    await r.set(k_last_order(user_id), json.dumps(snapshot, ensure_ascii=False))


# -------------------------
# Client: customer profile for smart return
# -------------------------
async def customer_mark_order(r: redis.Redis, cafe_id: str, user_id: int, firstname: str, username: str, cart: Dict[str, int], total_sum: int) -> None:
    nowts = int(time.time())
    prof_key = k_customer_profile(cafe_id, user_id)
    drinks_key = k_customer_drinks(cafe_id, user_id)

    lastdrink = next(iter(cart.keys()), "")
    pipe = r.pipeline()
    pipe.sadd(k_customers_set(cafe_id), user_id)
    pipe.hsetnx(prof_key, "first_order_ts", nowts)
    pipe.hsetnx(prof_key, "offers_optout", 0)
    pipe.hsetnx(prof_key, "last_trigger_ts", 0)
    pipe.hset(
        prof_key,
        mapping={
            "firstname": firstname or "",
            "username": username or "",
            "last_order_ts": nowts,
            "last_order_sum": int(total_sum),
            "last_drink": lastdrink,
            "total_orders": 1,
        },
    )
    pipe.hincrby(prof_key, "total_orders", 1)
    pipe.hincrby(prof_key, "total_spent", int(total_sum))
    for drink, qty in cart.items():
        pipe.hincrby(drinks_key, drink, int(qty))
    await pipe.execute()

async def get_favorite_drink(r: redis.Redis, cafe_id: str, user_id: int) -> Optional[str]:
    data = await r.hgetall(k_customer_drinks(cafe_id, user_id))
    best_name = None
    best_cnt = -1
    for k, v in data.items():
        try:
            cnt = int(v)
            if cnt > best_cnt:
                best_cnt = cnt
                best_name = str(k)
        except Exception:
            continue
    return best_name


# -------------------------
# Routers
# -------------------------
client_router = Router()
admin_router = Router()


@client_router.error()
async def client_error(event: ErrorEvent):
    logger.critical("CLIENT update error: %r", event.exception, exc_info=True)

@admin_router.error()
async def admin_error(event: ErrorEvent):
    logger.critical("ADMIN update error: %r", event.exception, exc_info=True)


# -------------------------
# CLIENT bot handlers
# -------------------------
@client_router.message(CommandStart(deep_link=True))
async def client_start_deep(message: Message, command: CommandObject, state: FSMContext):
    await client_start(message, command, state)

@client_router.message(CommandStart())
async def client_start(message: Message, command: CommandObject, state: FSMContext):
    r: redis.Redis = message.bot._redis
    await state.clear()

    payload = (command.args or "").strip()
    cafe_id = await resolve_cafe_id(r, message, payload if payload else None)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    name = html.quote(username_of(message))
    welcome = random.choice(WELCOME_VARIANTS).format(name=name)

    offer_repeat = await should_offer_repeat(r, message.from_user.id)
    await set_last_seen(r, message.from_user.id)

    if not is_cafe_open(cafe):
        await message.answer(
            f"🔒 <b>{html.quote(cafe_title(cafe))} сейчас закрыто</b>\n\n{work_status_line(cafe)}\n\n"
            f"📍 {html.quote(cafe_address(cafe))}\n"
            f"📞 <code>{html.quote(cafe_phone(cafe))}</code>\n",
            reply_markup=kb_main(menu),
        )
        return

    if offer_repeat:
        snap = await get_last_order_snapshot(r, message.from_user.id)
        if snap and isinstance(snap.get("cart"), dict) and snap.get("cart"):
            cart_preview = get_cart({"cart": snap.get("cart")})
            lines = [f"{html.quote(d)} × {q}" for d, q in cart_preview.items()]
            await state.update_data(repeat_offer_snapshot=snap)
            await message.answer(
                f"{welcome}\n\nХочешь повторить вчерашний заказ?\n" + "\n".join(lines),
                reply_markup=kb_repeat_offer(),
            )
            return

    await message.answer(
        f"{welcome}\n\n{work_status_line(cafe)}\n🕐 <i>Московское время: {msk_hm()}</i>\n\nВыбери напиток:",
        reply_markup=kb_main(menu),
    )

@client_router.message(F.text == BTNREPEATNO)
async def client_repeat_no(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    await state.update_data(repeat_offer_snapshot=None)
    await message.answer("Ок. Выбирай напиток:", reply_markup=kb_main(menu))

@client_router.message(F.text == BTNREPEATLAST)
async def client_repeat_last(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)

    data = await state.get_data()
    snap = data.get("repeat_offer_snapshot") or await get_last_order_snapshot(r, message.from_user.id)
    if not snap or not isinstance(snap.get("cart"), dict) or not snap.get("cart"):
        await message.answer("Не нашёл прошлый заказ.", reply_markup=kb_main(menu))
        return

    cart = {}
    for k, v in snap["cart"].items():
        try:
            if k in menu and int(v) > 0:
                cart[str(k)] = int(v)
        except Exception:
            continue
    if not cart:
        await message.answer("Похоже, прошлый заказ больше не актуален (меню изменилось).", reply_markup=kb_main(menu))
        return

    await state.update_data(cart=cart)
    await show_cart(message, state)

@client_router.message(F.text == BTNCALL)
async def client_call(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    await message.answer(f"📞 <b>{html.quote(cafe_title(cafe))}</b>\n☎️ <code>{html.quote(cafe_phone(cafe))}</code>", reply_markup=kb_main(menu))

@client_router.message(F.text == BTNHOURS)
async def client_hours(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    await message.answer(f"⏰ <b>Режим работы</b>\n\n{work_status_line(cafe)}\n📍 {html.quote(cafe_address(cafe))}", reply_markup=kb_main(menu))


async def show_cart(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)

    cart = get_cart(await state.get_data())
    await state.set_state(ClientOrderStates.cart_view)
    await state.update_data(cart=cart)
    await message.answer(cart_text(cart, menu), reply_markup=kb_cart(bool(cart), menu))

@client_router.message(F.text == BTNCART)
async def client_cart(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)
    if not is_cafe_open(cafe):
        await message.answer(
            f"🔒 <b>{html.quote(cafe_title(cafe))} сейчас закрыто</b>\n\n{work_status_line(cafe)}",
            reply_markup=kb_main(menu),
        )
        return
    await show_cart(message, state)

@client_router.message(F.text == BTNCLEARCART)
async def client_clear_cart(message: Message, state: FSMContext):
    await state.update_data(cart={})
    await show_cart(message, state)

@client_router.message(F.text == BTNCANCELORDER)
async def client_cancel_order(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    await state.clear()
    await message.answer("Ок.", reply_markup=kb_main(menu))

@client_router.message(F.text == BTNEDITCART)
async def client_edit_cart(message: Message, state: FSMContext):
    data = await state.get_data()
    cart = get_cart(data)
    if not cart:
        await show_cart(message, state)
        return
    await state.set_state(ClientOrderStates.cart_edit_pick_item)
    await message.answer("Выбери напиток для изменения:", reply_markup=kb_pick_cart_item(cart))

@client_router.message(StateFilter(ClientOrderStates.cart_edit_pick_item))
async def client_pick_item_to_edit(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if text in (BTNCANCEL, BTNCART):
        await show_cart(message, state)
        return

    cart = get_cart(await state.get_data())
    if text not in cart:
        await message.answer("Выбери напиток кнопкой.", reply_markup=kb_pick_cart_item(cart))
        return

    await state.set_state(ClientOrderStates.cart_edit_pick_action)
    await state.update_data(edit_item=text)
    await message.answer(f"Что сделать с <b>{html.quote(text)}</b>?", reply_markup=kb_cart_edit_actions())

@client_router.message(StateFilter(ClientOrderStates.cart_edit_pick_action))
async def client_cart_edit_action(message: Message, state: FSMContext):
    action = (message.text or "").strip()
    if action == BTNCANCEL:
        await show_cart(message, state)
        return

    data = await state.get_data()
    cart = get_cart(data)
    item = str(data.get("edit_item") or "")

    if action == CARTACTDONE:
        await show_cart(message, state)
        return

    if not item or item not in cart:
        await show_cart(message, state)
        return

    if action == CARTACTPLUS:
        cart[item] = int(cart.get(item, 0)) + 1
    elif action == CARTACTMINUS:
        cart[item] = int(cart.get(item, 0)) - 1
        if cart[item] <= 0:
            cart.pop(item, None)
    elif action == CARTACTDEL:
        cart.pop(item, None)
    else:
        await message.answer("Выбери действие кнопкой.", reply_markup=kb_cart_edit_actions())
        return

    await state.update_data(cart=cart)
    await show_cart(message, state)

@client_router.message(F.text == BTNBOOKING)
async def client_booking_start(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    await state.clear()
    if not booking_enabled(cafe):
        await message.answer("Бронирование временно недоступно.", reply_markup=kb_main(menu))
        return
    if not is_cafe_open(cafe):
        await message.answer(f"🔒 <b>{html.quote(cafe_title(cafe))} сейчас закрыто</b>\n\n{work_status_line(cafe)}", reply_markup=kb_main(menu))
        return

    await state.set_state(BookingStates.waiting_for_datetime)
    await message.answer("Напиши дату и время, например: <code>15.02 19:00</code>", reply_markup=kb_booking_cancel())

@client_router.message(StateFilter(BookingStates.waiting_for_datetime))
async def client_booking_datetime(message: Message, state: FSMContext):
    if (message.text or "").strip() == BTNCANCEL:
        r: redis.Redis = message.bot._redis
        cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
        menu = await get_menu(r, cafe_id)
        await state.clear()
        await message.answer("Ок.", reply_markup=kb_main(menu))
        return

    m = re.match(r"^(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})$", (message.text or "").strip())
    if not m:
        await message.answer("Формат: <code>15.02 19:00</code>", reply_markup=kb_booking_cancel())
        return

    day, month, hour, minute = map(int, m.groups())
    year = now_msk().year
    try:
        dt = datetime(year, month, day, hour, minute, tzinfo=MSK_TZ)
    except Exception:
        await message.answer("Некорректная дата/время.", reply_markup=kb_booking_cancel())
        return

    await state.update_data(booking_dt=dt.strftime("%d.%m %H:%M"))
    await state.set_state(BookingStates.waiting_for_people)
    await message.answer("Сколько гостей? (1–10)", reply_markup=kb_booking_people())

@client_router.message(StateFilter(BookingStates.waiting_for_people))
async def client_booking_people(message: Message, state: FSMContext):
    if (message.text or "").strip() == BTNCANCEL:
        r: redis.Redis = message.bot._redis
        cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
        menu = await get_menu(r, cafe_id)
        await state.clear()
        await message.answer("Ок.", reply_markup=kb_main(menu))
        return

    try:
        people = int((message.text or "").strip())
        if not (1 <= people <= 10):
            raise ValueError
    except Exception:
        await message.answer("Выбери 1–10.", reply_markup=kb_booking_people())
        return

    await state.update_data(booking_people=people)
    await state.set_state(BookingStates.waiting_for_comment)
    await message.answer("Комментарий (или <code>-</code>):", reply_markup=kb_booking_cancel())

@client_router.message(StateFilter(BookingStates.waiting_for_comment))
async def client_booking_finish(message: Message, state: FSMContext):
    if (message.text or "").strip() == BTNCANCEL:
        r: redis.Redis = message.bot._redis
        cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
        menu = await get_menu(r, cafe_id)
        await state.clear()
        await message.answer("Ок.", reply_markup=kb_main(menu))
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    data = await state.get_data()
    dtstr = str(data.get("booking_dt") or "")
    people = int(data.get("booking_people") or 0)
    comment = (message.text or "").strip() or "-"

    booking_id = str(int(time.time()))[-6:]
    uid = message.from_user.id
    guest_name = message.from_user.username or message.from_user.first_name or "Гость"
    user_link = f'<a href="tg://user?id={uid}">{html.quote(guest_name)}</a>'

    admin_msg = (
        f"📋 <b>БРОНЬ #{booking_id}</b>\n"
        f"🏠 {html.quote(cafe_title(cafe))} (id=<code>{html.quote(cafe_id)}</code>)\n\n"
        f"👤 {user_link} (<code>{uid}</code>)\n"
        f"🕒 {html.quote(dtstr)}\n"
        f"👥 {people}\n"
        f"💬 {html.quote(comment)}\n"
    )
    await notify_admins(message.bot, cafe_id, admin_msg)

    await state.clear()
    await message.answer("✅ Заявка на бронь отправлена администратору.", reply_markup=kb_main(menu))


async def start_add_item(message: Message, state: FSMContext, drink: str):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    if not orders_enabled(cafe):
        await message.answer("Заказы временно недоступны.", reply_markup=kb_main(menu))
        return
    if not is_cafe_open(cafe):
        await message.answer(f"🔒 <b>{html.quote(cafe_title(cafe))} сейчас закрыто</b>\n\n{work_status_line(cafe)}", reply_markup=kb_main(menu))
        return

    price = menu.get(drink)
    if price is None:
        await message.answer("Не нашёл напиток в меню.", reply_markup=kb_main(menu))
        return

    cart = get_cart(await state.get_data())
    await state.set_state(ClientOrderStates.waiting_for_quantity)
    await state.update_data(current_drink=drink, cart=cart)
    await message.answer(
        f"{random.choice(CHOICE_VARIANTS)}\n\n<b>{html.quote(drink)}</b> — <b>{price}</b> р\n\nСколько (1–5)?",
        reply_markup=kb_qty(),
    )

@client_router.message(StateFilter(ClientOrderStates.waiting_for_quantity))
async def client_process_qty(message: Message, state: FSMContext):
    if (message.text or "").strip() == BTNCANCEL:
        await show_cart(message, state)
        return

    try:
        qty = int((message.text or "").strip())
        if not (1 <= qty <= 5):
            raise ValueError
    except Exception:
        await message.answer("Выбери 1–5.", reply_markup=kb_qty())
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)

    data = await state.get_data()
    drink = str(data.get("current_drink") or "")
    cart = get_cart(data)

    if not drink or drink not in menu:
        await state.clear()
        await message.answer("Не могу добавить. Нажми /start.", reply_markup=kb_main(menu))
        return

    cart[drink] = int(cart.get(drink, 0)) + int(qty)
    await state.update_data(cart=cart)
    await show_cart(message, state)

@client_router.message(F.text == BTNCHECKOUT)
async def client_checkout(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    if not orders_enabled(cafe):
        await message.answer("Заказы временно недоступны.", reply_markup=kb_main(menu))
        return
    if not is_cafe_open(cafe):
        await message.answer(f"🔒 <b>{html.quote(cafe_title(cafe))} сейчас закрыто</b>\n\n{work_status_line(cafe)}", reply_markup=kb_main(menu))
        return

    cart = get_cart(await state.get_data())
    if not cart:
        await message.answer("Корзина пуста.", reply_markup=kb_main(menu))
        return

    await state.set_state(ClientOrderStates.waiting_for_confirmation)
    await message.answer(cart_text(cart, menu) + "\n\nПодтвердить заказ?", reply_markup=kb_confirm())

@client_router.message(StateFilter(ClientOrderStates.waiting_for_confirmation))
async def client_confirm(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)

    if (message.text or "").strip() == BTNCANCELORDER:
        await state.clear()
        await message.answer("Ок.", reply_markup=kb_main(menu))
        return
    if (message.text or "").strip() == BTNCART:
        await show_cart(message, state)
        return
    if (message.text or "").strip() != BTNCONFIRM:
        await message.answer("Нажми «Подтвердить».", reply_markup=kb_confirm())
        return

    await state.set_state(ClientOrderStates.waiting_for_ready_time)
    await message.answer("Когда приготовить?", reply_markup=kb_ready_time())

async def finalize_order(message: Message, state: FSMContext, ready_in_min: int):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = cafe_or_default(cafe_id)
    menu = await get_menu(r, cafe_id)

    uid = message.from_user.id
    cart = get_cart(await state.get_data())
    if not cart:
        await state.clear()
        await message.answer("Корзина пуста.", reply_markup=kb_main(menu))
        return

    rl = rate_limit_seconds(cafe)
    last = await r.get(k_rl(uid))
    if last and time.time() - float(last) < rl:
        await message.answer(f"⏳ Подожди {rl} секунд между заказами.", reply_markup=kb_main(menu))
        await state.clear()
        return
    await r.setex(k_rl(uid), rl, str(time.time()))

    total = cart_total(cart, menu)
    order_num = str(int(time.time()))[-6:]

    ready_at = now_msk() + timedelta(minutes=max(0, ready_in_min))
    ready_line = "как можно быстрее" if ready_in_min == 0 else f"через {ready_in_min} мин (к {ready_at.strftime('%H:%M')})"

    # snapshot for repeat offer
    await set_last_order_snapshot(r, uid, {"cart": cart, "total": total, "ts": int(time.time())})

    # stats
    await r.incr(k_stats_total_orders(cafe_id))
    await r.incrby(k_stats_total_revenue(cafe_id), int(total))
    for drink, qty in cart.items():
        price = int(menu.get(drink, 0))
        await r.incrby(k_stats_drink_cnt(cafe_id, drink), int(qty))
        await r.incrby(k_stats_drink_rev(cafe_id, drink), int(qty) * price)

    # smart return profile
    try:
        await customer_mark_order(
            r, cafe_id, uid,
            firstname=(message.from_user.first_name or ""),
            username=(message.from_user.username or ""),
            cart=cart,
            total_sum=total,
        )
    except Exception:
        pass

    guest_name = message.from_user.username or message.from_user.first_name or "Клиент"
    user_link = f'<a href="tg://user?id={uid}">{html.quote(guest_name)}</a>'
    order_lines = "\n".join([f"{html.quote(d)} × {q}" for d, q in cart.items()])

    admin_msg = (
        f"🔔 <b>ЗАКАЗ #{order_num}</b>\n"
        f"🏠 {html.quote(cafe_title(cafe))} (id=<code>{html.quote(cafe_id)}</code>)\n\n"
        f"👤 {user_link} (<code>{uid}</code>)\n\n"
        f"{order_lines}\n\n"
        f"💰 Итого: <b>{total}</b> р\n"
        f"⏱ Готовность: <b>{html.quote(ready_line)}</b>\n"
    )
    await notify_admins(message.bot, cafe_id, admin_msg)

    finish = random.choice(FINISH_VARIANTS).format(name=html.quote(username_of(message)))
    await message.answer(
        f"✅ <b>Заказ #{order_num} принят!</b>\n\n{order_lines}\n\nИтого: <b>{total}</b> р\nГотовность: <b>{html.quote(ready_line)}</b>\n\n{finish}",
        reply_markup=kb_main(menu),
    )
    await state.clear()

@client_router.message(StateFilter(ClientOrderStates.waiting_for_ready_time))
async def client_ready_time(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if text == BTNCANCEL:
        await show_cart(message, state)
        return
    if text == BTNREADYNOW:
        await finalize_order(message, state, 0)
        return
    if text == BTNREADY20:
        await finalize_order(message, state, 20)
        return
    await message.answer("Выбери вариант кнопкой.", reply_markup=kb_ready_time())

@client_router.message(F.text)
async def client_fallback_text(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(k_user_cafe(message.from_user.id)) or DEFAULT_CAFE_ID)
    menu = await get_menu(r, cafe_id)
    text = (message.text or "").strip()
    if text in menu:
        await start_add_item(message, state, text)
        return
    await message.answer("Выбери пункт меню кнопками.", reply_markup=kb_main(menu))


# -------------------------
# ADMIN bot handlers
# -------------------------
async def admin_resolve_cafe(message: Message, payload: Optional[str]) -> Optional[str]:
    """
    Админский бот: вход в контекст кафе через deep-link payload.
    payload:
      - "cafe_001"  => открыть админку кафе
    """
    if not payload:
        return None
    payload = payload.strip()
    if payload in CAFES:
        return payload
    return None

async def admin_is_cafe_admin(user_id: int, cafe_id: str) -> bool:
    cafe = cafe_or_default(cafe_id)
    return is_superadmin(user_id) or (cafe_admin_id(cafe) == user_id and cafe_admin_id(cafe) != 0)

async def admin_set_user_cafe(r: redis.Redis, user_id: int, cafe_id: str) -> None:
    await r.set(k_user_cafe(user_id), cafe_id)

async def admin_get_user_cafe(r: redis.Redis, user_id: int) -> str:
    cid = await r.get(k_user_cafe(user_id))
    if cid and str(cid) in CAFES:
        return str(cid)
    return DEFAULT_CAFE_ID

@admin_router.message(CommandStart(deep_link=True))
async def admin_start_deep(message: Message, command: CommandObject, state: FSMContext):
    await admin_start(message, command, state)

@admin_router.message(CommandStart())
async def admin_start(message: Message, command: CommandObject, state: FSMContext):
    r: redis.Redis = message.bot._redis
    await state.clear()

    payload = (command.args or "").strip()
    cafe_id = await admin_resolve_cafe(message, payload) if payload else None
    if cafe_id:
        if not await admin_is_cafe_admin(message.from_user.id, cafe_id):
            await message.answer("Доступ запрещён (ты не админ этого кафе).")
            return
        await admin_set_user_cafe(r, message.from_user.id, cafe_id)

    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    cafe = cafe_or_default(cafe_id)

    if not await admin_is_cafe_admin(message.from_user.id, cafe_id):
        await message.answer(
            "Это админ-бот.\n\n"
            "Чтобы войти как администратор кафе, открой админ-ссылку (её выдаёт суперадмин) "
            "или попроси назначить твой Telegram ID в поле admin_id для нужного cafe_id."
        )
        return

    await state.set_state(AdminStates.home)
    await message.answer(
        f"🛠 <b>Админ-панель</b>\nКафе: <b>{html.quote(cafe_title(cafe))}</b>\nID: <code>{html.quote(cafe_id)}</code>",
        reply_markup=kb_admin_home(),
    )

@admin_router.message(Command("myid"))
async def admin_myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@admin_router.message(F.text == BTNADMIN_LINKS)
async def admin_links(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    if not await admin_is_cafe_admin(message.from_user.id, cafe_id):
        await message.answer("Доступ запрещён.")
        return

    client_bot: Bot = message.bot._client_bot
    client_link = await create_start_link(client_bot, payload=cafe_id, encode=True)  # [web:24]
    staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)  # [web:24]
    admin_link = await create_start_link(message.bot, payload=cafe_id, encode=True)  # [web:24]

    await message.answer(
        "🔗 <b>Ссылки</b>\n\n"
        f"1) Клиентская ссылка:\n{client_link}\n\n"
        f"2) Добавить админ-бота в staff-группу:\n{staff_link}\n\n"
        f"3) Админ-ссылка:\n{admin_link}\n\n",
        disable_web_page_preview=True,
        reply_markup=kb_admin_home(),
    )

@admin_router.message(F.text == BTNADMIN_STAFF)
async def admin_staff(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    if not await admin_is_cafe_admin(message.from_user.id, cafe_id):
        await message.answer("Доступ запрещён.")
        return

    if not staff_group_enabled(cafe_or_default(cafe_id)):
        await message.answer("Staff-группа отключена в features.", reply_markup=kb_admin_home())
        return

    staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)  # [web:24]
    gid = await r.get(k_staff_group(cafe_id))
    gid_line = f"Текущая группа: <code>{gid}</code>\n\n" if gid else "Группа ещё не привязана.\n\n"
    await message.answer(
        "👥 <b>Группа персонала</b>\n\n"
        f"{gid_line}"
        "1) Создайте группу.\n"
        "2) Добавьте в неё админ-бота по ссылке:\n"
        f"{staff_link}\n\n"
        f"3) В группе выполните:\n<code>/bind {html.quote(cafe_id)}</code>\n",
        disable_web_page_preview=True,
        reply_markup=kb_admin_home(),
    )

@admin_router.message(Command("bind"))
async def admin_bind_group(message: Message, command: CommandObject):
    """
    Команда выполняется в группе персонала, где находится админ-бот.
    """
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда /bind работает только в группе.")
        return

    cafe_id = (command.args or "").strip()
    if not cafe_id or cafe_id not in CAFES:
        await message.answer("Формат: <code>/bind cafe_001</code>")
        return

    if not await admin_is_cafe_admin(message.from_user.id, cafe_id):
        await message.answer("Только администратор этого кафе может привязать группу.")
        return

    r: redis.Redis = message.bot._redis
    await r.set(k_staff_group(cafe_id), str(message.chat.id))
    await message.answer(f"✅ Группа привязана к кафе <code>{html.quote(cafe_id)}</code>.")

@admin_router.message(F.text == BTNADMIN_STATS)
async def admin_stats(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    if not await admin_is_cafe_admin(message.from_user.id, cafe_id):
        await message.answer("Доступ запрещён.")
        return

    total_orders = int(await r.get(k_stats_total_orders(cafe_id)) or 0)
    total_rev = int(await r.get(k_stats_total_revenue(cafe_id)) or 0)

    menu = await get_menu(r, cafe_id)
    lines = [
        f"📊 <b>Статистика</b>\nКафе: <code>{html.quote(cafe_id)}</code>\n\n"
        f"Всего заказов: <b>{total_orders}</b>\n"
        f"Выручка: <b>{total_rev}</b> р\n"
    ]
    for drink in menu.keys():
        cnt = int(await r.get(k_stats_drink_cnt(cafe_id, drink)) or 0)
        rev = int(await r.get(k_stats_drink_rev(cafe_id, drink)) or 0)
        if cnt > 0:
            lines.append(f"{html.quote(drink)}: <b>{cnt}</b> шт, <b>{rev}</b> р")
    await message.answer("\n".join(lines), reply_markup=kb_admin_home())

@admin_router.message(F.text == BTNADMIN_MENU)
async def admin_menu_entry(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    if not await admin_is_cafe_admin(message.from_user.id, cafe_id):
        await message.answer("Доступ запрещён.")
        return

    menu = await get_menu(r, cafe_id)
    preview = "\n".join([f"• {html.quote(k)} — <b>{v}</b> р" for k, v in menu.items()]) or "Меню пустое."
    await state.set_state(AdminStates.menu_action)
    await message.answer("🧾 <b>Меню</b>\n\n" + preview + "\n\nВыберите действие:", reply_markup=kb_admin_menu_edit())

@admin_router.message(StateFilter(AdminStates.menu_action))
async def admin_menu_choose_action(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    if not await admin_is_cafe_admin(message.from_user.id, cafe_id):
        await state.clear()
        await message.answer("Доступ запрещён.")
        return

    text = (message.text or "").strip()
    menu = await get_menu(r, cafe_id)

    if text == BTNADMIN_BACK:
        await state.set_state(AdminStates.home)
        await message.answer("Админ-панель:", reply_markup=kb_admin_home())
        return

    if text == MENUEDIT_ADD:
        await state.set_state(AdminStates.menu_add_name)
        await message.answer("Напиши название напитка:", reply_markup=kb_admin_back())
        return

    if text == MENUEDIT_EDIT:
        if not menu:
            await message.answer("Меню пустое.", reply_markup=kb_admin_menu_edit())
            return
        await state.set_state(AdminStates.menu_pick_edit_item)
        await message.answer("Выбери напиток:", reply_markup=kb_pick_menu_item(menu))
        return

    if text == MENUEDIT_DEL:
        if not menu:
            await message.answer("Меню пустое.", reply_markup=kb_admin_menu_edit())
            return
        await state.set_state(AdminStates.menu_pick_del_item)
        await message.answer("Что удалить?", reply_markup=kb_pick_menu_item(menu))
        return

    await message.answer("Выбери действие кнопкой.", reply_markup=kb_admin_menu_edit())

@admin_router.message(StateFilter(AdminStates.menu_add_name))
async def admin_menu_add_name(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if text == BTNADMIN_BACK:
        await state.set_state(AdminStates.menu_action)
        await message.answer("Выберите действие:", reply_markup=kb_admin_menu_edit())
        return
    if not text:
        await message.answer("Название пустое. Введите ещё раз:", reply_markup=kb_admin_back())
        return
    await state.update_data(add_name=text)
    await state.set_state(AdminStates.menu_add_price)
    await message.answer("Цена (число):", reply_markup=kb_admin_back())

@admin_router.message(StateFilter(AdminStates.menu_add_price))
async def admin_menu_add_price(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if text == BTNADMIN_BACK:
        await state.set_state(AdminStates.menu_action)
        await message.answer("Выберите действие:", reply_markup=kb_admin_menu_edit())
        return
    try:
        price = int(text)
        if price <= 0:
            raise ValueError
    except Exception:
        await message.answer("Цена должна быть числом > 0:", reply_markup=kb_admin_back())
        return

    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    name = str((await state.get_data()).get("add_name") or "").strip()
    await set_menu_item(r, cafe_id, name, price)

    await state.set_state(AdminStates.menu_action)
    await message.answer(f"✅ Добавлено: <b>{html.quote(name)}</b> = <b>{price}</b> р", reply_markup=kb_admin_menu_edit())

@admin_router.message(StateFilter(AdminStates.menu_pick_edit_item))
async def admin_menu_pick_edit_item(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    menu = await get_menu(r, cafe_id)

    text = (message.text or "").strip()
    if text == BTNADMIN_BACK:
        await state.set_state(AdminStates.menu_action)
        await message.answer("Выберите действие:", reply_markup=kb_admin_menu_edit())
        return
    if text not in menu:
        await message.answer("Выбери напиток кнопкой.", reply_markup=kb_pick_menu_item(menu))
        return

    await state.update_data(edit_name=text)
    await state.set_state(AdminStates.menu_edit_price)
    await message.answer(f"Новая цена для <b>{html.quote(text)}</b>:", reply_markup=kb_admin_back())

@admin_router.message(StateFilter(AdminStates.menu_edit_price))
async def admin_menu_edit_price(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if text == BTNADMIN_BACK:
        await state.set_state(AdminStates.menu_action)
        await message.answer("Выберите действие:", reply_markup=kb_admin_menu_edit())
        return
    try:
        price = int(text)
        if price <= 0:
            raise ValueError
    except Exception:
        await message.answer("Цена должна быть числом > 0:", reply_markup=kb_admin_back())
        return

    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    name = str((await state.get_data()).get("edit_name") or "").strip()
    if not name:
        await state.set_state(AdminStates.menu_action)
        await message.answer("Не выбрано что менять.", reply_markup=kb_admin_menu_edit())
        return

    await set_menu_item(r, cafe_id, name, price)
    await state.set_state(AdminStates.menu_action)
    await message.answer(f"✅ Обновлено: <b>{html.quote(name)}</b> = <b>{price}</b> р", reply_markup=kb_admin_menu_edit())

@admin_router.message(StateFilter(AdminStates.menu_pick_del_item))
async def admin_menu_del_item(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = await admin_get_user_cafe(r, message.from_user.id)
    menu = await get_menu(r, cafe_id)

    text = (message.text or "").strip()
    if text == BTNADMIN_BACK:
        await state.set_state(AdminStates.menu_action)
        await message.answer("Выберите действие:", reply_markup=kb_admin_menu_edit())
        return
    if text not in menu:
        await message.answer("Выбери напиток кнопкой.", reply_markup=kb_pick_menu_item(menu))
        return

    await del_menu_item(r, cafe_id, text)
    await state.set_state(AdminStates.menu_action)
    await message.answer(f"✅ Удалено: <b>{html.quote(text)}</b>", reply_markup=kb_admin_menu_edit())


# -------------------------
# Smart return loop (client side)
# -------------------------
async def smart_return_check_and_send(client_bot: Bot):
    if not in_send_window_msk():
        return

    r: redis.Redis = client_bot._redis
    for cafe_id in CAFES.keys():
        try:
            ids = await r.smembers(k_customers_set(cafe_id))
            ids_int = [int(x) for x in ids] if ids else []
        except Exception:
            ids_int = []

        nowts = int(time.time())
        for user_id in ids_int:
            prof_key = k_customer_profile(cafe_id, user_id)
            try:
                profile = await r.hgetall(prof_key)
            except Exception:
                profile = {}

            if not profile:
                continue
            if str(profile.get("offers_optout", "0")) == "1":
                continue

            try:
                last_order_ts = int(float(profile.get("last_order_ts", 0) or 0))
            except Exception:
                continue

            cycle_days = DEFAULT_RETURN_CYCLE_DAYS
            days_since = (nowts - last_order_ts) / 86400
            if days_since < cycle_days:
                continue

            try:
                last_trigger_ts = int(float(profile.get("last_trigger_ts", 0) or 0))
            except Exception:
                last_trigger_ts = 0

            if last_trigger_ts and (nowts - last_trigger_ts) < RETURN_COOLDOWN_DAYS * 86400:
                continue

            firstname = profile.get("firstname") or ""
            fav = await get_favorite_drink(r, cafe_id, user_id) or profile.get("last_drink") or "кофе"
            code = promocode(user_id)

            text = (
                f"{html.quote(str(firstname) or 'Привет')}!\n\n"
                f"Скучаем 🙂\n"
                f"Твой любимый напиток: <b>{html.quote(str(fav))}</b>\n"
                f"Скидка <b>{RETURN_DISCOUNT_PERCENT}%</b> по промокоду: <code>{html.quote(code)}</code>\n\n"
                f"Открой бота и сделай заказ."
            )

            try:
                await client_bot.send_message(user_id, text, disable_web_page_preview=True)
                await r.hset(prof_key, mapping={"last_trigger_ts": str(nowts)})
            except Exception:
                # если пользователь недоступен — удалим из сета
                try:
                    await r.srem(k_customers_set(cafe_id), user_id)
                except Exception:
                    pass

async def smart_return_loop(client_bot: Bot):
    while True:
        try:
            await smart_return_check_and_send(client_bot)
        except Exception as e:
            logger.error("smart_return_loop error: %r", e, exc_info=True)
        await asyncio.sleep(RETURN_CHECK_EVERY_SECONDS)


# -------------------------
# Webhook apps startup/shutdown
# -------------------------
async def setup_bot_common(bot: Bot, webhook_url: str, webhook_secret: str):
    await bot.set_webhook(webhook_url, secret_token=webhook_secret)  # [web:1]

async def app_startup(app: web.Application):
    client_bot: Bot = app["client_bot"]
    admin_bot: Bot = app["admin_bot"]

    await setup_bot_common(client_bot, CLIENT_WEBHOOK_URL, WEBHOOK_SECRET)
    await setup_bot_common(admin_bot, ADMIN_WEBHOOK_URL, WEBHOOK_SECRET)

    # commands
    await client_bot.set_my_commands([
        BotCommand(command="start", description="Запуск"),
    ])
    await admin_bot.set_my_commands([
        BotCommand(command="start", description="Админ-панель"),
        BotCommand(command="myid", description="Показать мой Telegram ID"),
        BotCommand(command="bind", description="Привязать staff-группу к кафе"),
    ])

    # background task
    app["smart_task"] = asyncio.create_task(smart_return_loop(client_bot))

    logger.info("Client webhook: %s", CLIENT_WEBHOOK_URL)
    logger.info("Admin webhook:  %s", ADMIN_WEBHOOK_URL)

async def app_shutdown(app: web.Application):
    client_bot: Bot = app["client_bot"]
    admin_bot: Bot = app["admin_bot"]
    storage_client: RedisStorage = app["storage_client"]
    storage_admin: RedisStorage = app["storage_admin"]
    r: redis.Redis = app["redis"]

    task: Optional[asyncio.Task] = app.get("smart_task")
    if task and not task.done():
        task.cancel()

    for bot in (client_bot, admin_bot):
        try:
            await bot.delete_webhook()
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass

    try:
        await storage_client.close()
    except Exception:
        pass
    try:
        await storage_admin.close()
    except Exception:
        pass
    try:
        await r.aclose()
    except Exception:
        pass


# -------------------------
# Main
# -------------------------
async def main():
    if not CLIENT_BOT_TOKEN:
        raise RuntimeError("CLIENT_BOT_TOKEN not set")
    if not ADMIN_BOT_TOKEN:
        raise RuntimeError("ADMIN_BOT_TOKEN not set")
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL not set")
    if not PUBLIC_HOST:
        raise RuntimeError("PUBLIC_HOST not set")

    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()

    # bots
    client_bot = Bot(token=CLIENT_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    admin_bot = Bot(token=ADMIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    # shared redis refs
    client_bot._redis = r
    admin_bot._redis = r

    # cross refs for notifications/links
    client_bot._admin_bot = admin_bot
    admin_bot._client_bot = client_bot

    # storages (separate namespaces to avoid FSM collisions between bots)
    storage_client = RedisStorage.from_url(REDIS_URL, key_builder=None)
    storage_admin = RedisStorage.from_url(REDIS_URL, key_builder=None)

    dp_client = Dispatcher(storage=storage_client)
    dp_admin = Dispatcher(storage=storage_admin)

    dp_client.include_router(client_router)
    dp_admin.include_router(admin_router)

    app = web.Application()
    app["redis"] = r
    app["client_bot"] = client_bot
    app["admin_bot"] = admin_bot
    app["storage_client"] = storage_client
    app["storage_admin"] = storage_admin

    app.on_startup.append(app_startup)
    app.on_shutdown.append(app_shutdown)

    async def health(_: web.Request):
        return web.json_response({"status": "ok"})

    app.router.add_get("/", health)

    # register BOTH webhooks on one aiohttp app
    SimpleRequestHandler(dp_client, client_bot, secret_token=WEBHOOK_SECRET, handle_in_background=True).register(
        app, path=CLIENT_WEBHOOK_PATH
    )
    SimpleRequestHandler(dp_admin, admin_bot, secret_token=WEBHOOK_SECRET, handle_in_background=True).register(
        app, path=ADMIN_WEBHOOK_PATH
    )

    setup_application(app, dp_client, bot=client_bot)  # [web:1]
    setup_application(app, dp_admin, bot=admin_bot)    # [web:1]

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info("Server running on 0.0.0.0:%s", PORT)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
