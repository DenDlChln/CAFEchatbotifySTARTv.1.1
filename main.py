import os
import json
import time
import asyncio
import random
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, Any as AnyType

import redis.asyncio as redis
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command, StateFilter, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
    ChatMemberUpdated,
    ErrorEvent,
)
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter
from aiogram.filters import IS_NOT_MEMBER, IS_MEMBER
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
# Time / constants
# -------------------------
MSK_TZ = timezone(timedelta(hours=3))

def now_msk() -> datetime:
    return datetime.now(MSK_TZ)

DEFAULT_RATE_LIMIT_SECONDS = 60


# -------------------------
# Env
# -------------------------
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
REDIS_URL = (os.getenv("REDIS_URL") or "").strip()

WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "cafebot123").strip()
PUBLIC_HOST = (os.getenv("PUBLIC_HOST") or "cafebotify-start-denvyd.amvera.io").strip()

PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_PATH = f"/{WEBHOOK_SECRET}/webhook"
WEBHOOK_URL = f"https://{PUBLIC_HOST}{WEBHOOK_PATH}"


# -------------------------
# Config loader (robust)
# -------------------------
def load_config() -> Dict[str, Any]:
    env_path = (os.getenv("CONFIG_PATH") or "").strip()
    base_dir = Path(__file__).resolve().parent  # /app

    # diagnostics: files in /app
    try:
        files = sorted([(x.name, x.stat().st_size) for x in base_dir.iterdir() if x.is_file()])
        logger.info("Files in %s: %s", base_dir, files)
    except Exception as e:
        logger.warning("Cannot list files in %s: %r", base_dir, e)

    candidates = []
    if env_path:
        candidates.append(env_path)
    candidates += ["config_330_template.json", "config.json"]

    tried = []
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
CHATS_TO_CAFE: Dict[str, str] = CONFIG.get("chats_to_cafe", {}) if isinstance(CONFIG.get("chats_to_cafe", {}), dict) else {}

def cafe_or_default(cafe_id: Optional[str]) -> Dict[str, Any]:
    if cafe_id and cafe_id in CAFES:
        return CAFES[cafe_id]
    return CAFES[DEFAULT_CAFE_ID]

def is_superadmin(user_id: int) -> bool:
    return bool(SUPERADMIN_ID) and user_id == SUPERADMIN_ID


# -------------------------
# Router + error handler
# -------------------------
router = Router()

@router.error()
async def error_handler(event: ErrorEvent):
    logger.critical("Update handling error: %r", event.exception, exc_info=True)


# -------------------------
# States
# -------------------------
class OrderStates(StatesGroup):
    waiting_for_quantity = State()
    waiting_for_confirmation = State()
    waiting_for_booking_info = State()


# -------------------------
# Redis keys
# -------------------------
def rl_key(user_id: int) -> str:
    return f"rate_limit:{user_id}"

def user_cafe_key(user_id: int) -> str:
    return f"user_cafe:{user_id}"

def group_cafe_key(chat_id: int) -> str:
    return f"group_cafe:{chat_id}"

def stats_total_orders_key(cafe_id: str) -> str:
    return f"stats:{cafe_id}:total_orders"

def stats_drink_key(cafe_id: str, drink: str) -> str:
    return f"stats:{cafe_id}:drink:{drink}"

def cafe_profile_key(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:profile"

def cafe_menu_key(cafe_id: str) -> str:
    return f"cafe:{cafe_id}:menu"


# -------------------------
# Cafe logic
# -------------------------
def menu_of(cafe: Dict[str, Any]) -> Dict[str, int]:
    menu = cafe.get("menu") or {}
    out: Dict[str, int] = {}
    if isinstance(menu, dict):
        for k, v in menu.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                continue
    return out

def cafe_hours(cafe: Dict[str, Any]) -> Tuple[int, int]:
    feat = cafe.get("features") or {}
    ws = int(feat.get("work_start", cafe.get("work_start", 9)))
    we = int(feat.get("work_end", cafe.get("work_end", 21)))
    return ws, we

def cafe_rate_limit(cafe: Dict[str, Any]) -> int:
    feat = cafe.get("features") or {}
    try:
        return int(feat.get("rate_limit_seconds", DEFAULT_RATE_LIMIT_SECONDS))
    except Exception:
        return DEFAULT_RATE_LIMIT_SECONDS

def cafe_open(cafe: Dict[str, Any]) -> bool:
    ws, we = cafe_hours(cafe)
    return ws <= now_msk().hour < we

def work_status(cafe: Dict[str, Any]) -> str:
    ws, we = cafe_hours(cafe)
    if cafe_open(cafe):
        return f"🟢 Открыто до {we}:00 (МСК)"
    return f"🔴 Закрыто\n🕐 Открываемся: {ws}:00 (МСК)"

def user_name(m: Message) -> str:
    if not m.from_user:
        return "друг"
    return m.from_user.first_name or "друг"

def closed_message(cafe: Dict[str, Any]) -> str:
    m = menu_of(cafe)
    menu_text = " • ".join([f"<b>{html.quote(d)}</b> {p}р" for d, p in m.items()]) if m else "Меню ещё настраивается."
    return (
        f"🔒 <b>{html.quote(str(cafe.get('title','Кафе')))} сейчас закрыто</b>\n\n"
        f"{work_status(cafe)}\n\n"
        f"☕️ <b>Меню:</b>\n{menu_text}\n\n"
        f"📍 <b>Адрес:</b> {html.quote(str(cafe.get('address','')))}\n"
        f"📞 <b>Телефон:</b> <code>{html.quote(str(cafe.get('phone','')))}</code>\n"
    )

def is_admin_of_cafe(user_id: int, cafe: Dict[str, Any]) -> bool:
    admin_id = int(cafe.get("admin_id") or 0)
    return (admin_id and user_id == admin_id) or is_superadmin(user_id)


# -------------------------
# Redis overrides
# -------------------------
async def apply_overrides(r: redis.Redis, cafe_id: str, base: Dict[str, Any]) -> Dict[str, Any]:
    cafe = dict(base)

    prof = await r.hgetall(cafe_profile_key(cafe_id))
    if prof:
        for k in ("title", "phone", "address", "city", "timezone"):
            if prof.get(k):
                cafe[k] = str(prof[k])
        if prof.get("admin_id"):
            try:
                cafe["admin_id"] = int(prof["admin_id"])
            except Exception:
                pass

        feat = dict(cafe.get("features") or {})
        for hk in ("work_start", "work_end", "rate_limit_seconds"):
            if prof.get(hk):
                try:
                    feat[hk] = int(prof[hk])
                except Exception:
                    pass
        cafe["features"] = feat

    menu = await r.hgetall(cafe_menu_key(cafe_id))
    if menu:
        new_menu: Dict[str, int] = {}
        for k, v in menu.items():
            try:
                new_menu[str(k)] = int(v)
            except Exception:
                continue
        if new_menu:
            cafe["menu"] = new_menu

    return cafe

async def get_cafe_by_id_effective(r: redis.Redis, cafe_id: str) -> Dict[str, Any]:
    return await apply_overrides(r, cafe_id, cafe_or_default(cafe_id))


# -------------------------
# Tenant resolve
# -------------------------
async def get_cafe_for_message(message: Message, r: redis.Redis) -> Tuple[str, Dict[str, Any]]:
    if message.chat.type in ("group", "supergroup"):
        gid = await r.get(group_cafe_key(message.chat.id))
        cafe_id = str(gid) if gid else DEFAULT_CAFE_ID
        return cafe_id, await get_cafe_by_id_effective(r, cafe_id)

    mapped = CHATS_TO_CAFE.get(str(message.chat.id))
    if mapped:
        return mapped, await get_cafe_by_id_effective(r, mapped)

    uid = message.from_user.id if message.from_user else 0
    cid = await r.get(user_cafe_key(uid))
    cafe_id = str(cid) if cid else DEFAULT_CAFE_ID
    return cafe_id, await get_cafe_by_id_effective(r, cafe_id)


# -------------------------
# Keyboards
# -------------------------
BTN_CALL = "📞 Связаться с кафе"
BTN_HOURS = "⏰ Режим работы"
BTN_BOOK = "📋 Бронирование / столики"

BTN_ADMIN_LINKS = "Мои ссылки"
BTN_ADMIN_GROUP = "Подключить группу"
BTN_ADMIN_STATS = "Статистика"
BTN_ADMIN_OPEN_MENU = "Открыть меню"

BTN_CANCEL = "Отмена"
BTN_MENU = "Меню"
BTN_CONFIRM = "Подтвердить"

def kb_guest(cafe: Dict[str, Any]) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=d)] for d in menu_of(cafe).keys()]
    rows.append([KeyboardButton(text=BTN_CALL), KeyboardButton(text=BTN_HOURS)])
    rows.append([KeyboardButton(text=BTN_BOOK)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def kb_info() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CALL), KeyboardButton(text=BTN_HOURS)],
            [KeyboardButton(text=BTN_BOOK)],
        ],
        resize_keyboard=True,
    )

def kb_admin() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADMIN_LINKS)],
            [KeyboardButton(text=BTN_ADMIN_GROUP)],
            [KeyboardButton(text=BTN_ADMIN_STATS)],
            [KeyboardButton(text=BTN_ADMIN_OPEN_MENU)],
        ],
        resize_keyboard=True,
    )

def kb_qty() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def kb_confirm() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CONFIRM), KeyboardButton(text=BTN_MENU)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# -------------------------
# Text variants
# -------------------------
WELCOME_VARIANTS = [
    "Привет, {name}! Заходи по‑домашнему — подберём кофе под настроение.",
    "{name}, добро пожаловать! Выбирай напиток — приготовим с заботой ☕️",
    "{name}, привет! Устроим вкусную паузу?",
]
CHOICE_VARIANTS = [
    "Отличный выбор 👍",
    "Классика, которая никогда не подводит.",
    "Супер! Один из хитов меню.",
]
FINISH_VARIANTS = [
    "Спасибо за заказ, {name}! Будем рады видеть тебя снова.",
    "Заказ принят, {name}. Пусть этот кофе сделает день лучше.",
]


# -------------------------
# Admin screen (3 deep links)
# -------------------------
async def send_admin_screen(message: Message, cafe_id: str, cafe: Dict[str, Any]) -> None:
    # encode=True is required if payload contains ":" or any non [A-Za-z0-9_-] [web:24]
    admin_link = await create_start_link(message.bot, payload=f"admin:{cafe_id}", encode=True)   # [web:24]
    staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)        # [web:24]
    guest_link = await create_start_link(message.bot, payload=cafe_id, encode=True)             # [web:24]

    text = (
        f"🛠 <b>Режим администратора</b>\n"
        f"Кафе: <b>{html.quote(str(cafe.get('title','Кафе')))}</b> (id=<code>{html.quote(cafe_id)}</code>)\n\n"
        f"1️⃣ <b>Ссылка админа</b>:\n{admin_link}\n\n"
        f"2️⃣ <b>Ссылка для группы персонала</b>:\n{staff_link}\n\n"
        f"3️⃣ <b>Ссылка для клиентов</b>:\n{guest_link}\n\n"
        f"После добавления бота в группу напишите там:\n<code>/bind {html.quote(cafe_id)}</code>"
    )
    await message.answer(text, reply_markup=kb_admin(), disable_web_page_preview=True)


# -------------------------
# Commands setup
# -------------------------
async def set_commands(bot: Bot) -> None:
    commands = [
        BotCommand(command="start", description="Запуск бота"),
        BotCommand(command="myid", description="Показать мой Telegram ID"),
        BotCommand(command="stats", description="Статистика (админ)"),
        BotCommand(command="bind", description="Привязать группу к кафе (в группе)"),
        BotCommand(command="ping", description="Проверка (pong)"),

        BotCommand(command="init_cafe", description="Инициализировать кафе (суперадмин)"),
        BotCommand(command="init_cafe_json", description="Инициализировать кафе JSON (суперадмин)"),
        BotCommand(command="set_profile_json", description="Профиль кафе JSON (суперадмин)"),
        BotCommand(command="set_admin", description="Назначить admin_id (суперадмин)"),
        BotCommand(command="set_menu_set", description="Добавить/обновить напиток (суперадмин)"),
        BotCommand(command="set_menu_del", description="Удалить напиток (суперадмин)"),
        BotCommand(command="cafe", description="Показать кафе (суперадмин)"),
        BotCommand(command="export_cafe", description="Экспорт кафе JSON (суперадмин)"),
    ]
    await bot.set_my_commands(commands)


# -------------------------
# Basic commands
# -------------------------
@router.message(Command("ping"))
async def ping(message: Message):
    await message.answer("pong")

@router.message(Command("myid"))
async def myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")


# -------------------------
# JSON helper commands for onboarding
# -------------------------
def _parse_cafe_json_args(args: str) -> Tuple[str, Dict[str, Any]]:
    args = (args or "").strip()
    cafe_id, rest = args.split(maxsplit=1)
    payload = json.loads(rest)
    if not isinstance(payload, dict):
        raise ValueError("json must be object")
    return cafe_id, payload

def _normalize_profile_payload(p: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k in ("title", "phone", "address", "city", "timezone"):
        if k in p and p[k] is not None:
            out[k] = str(p[k])
    for k in ("admin_id", "work_start", "work_end", "rate_limit_seconds"):
        if k in p and p[k] is not None:
            out[k] = str(int(p[k]))
    return out

def _normalize_menu_payload(p: AnyType) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(p, dict):
        return out
    for k, v in p.items():
        try:
            out[str(k)] = str(int(v))
        except Exception:
            continue
    return out


# -------------------------
# Superadmin onboarding commands
# -------------------------
async def _ensure_default_menu(r: redis.Redis, cafe_id: str) -> None:
    if await r.hlen(cafe_menu_key(cafe_id)):
        return
    base_menu = cafe_or_default(cafe_id).get("menu") or {"Капучино": 250, "Латте": 270}
    menu_map = _normalize_menu_payload(base_menu)
    if menu_map:
        await r.hset(cafe_menu_key(cafe_id), mapping=menu_map)

@router.message(Command("init_cafe"))
async def init_cafe_cmd(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        return
    cafe_id = (command.args or "").strip()
    if not cafe_id or cafe_id not in CAFES:
        await message.answer("Формат: <code>/init_cafe cafe_001</code>")
        return
    r: redis.Redis = message.bot._redis
    await _ensure_default_menu(r, cafe_id)
    cafe = await get_cafe_by_id_effective(r, cafe_id)
    await send_admin_screen(message, cafe_id, cafe)

@router.message(Command("init_cafe_json"))
async def init_cafe_json_cmd(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        return
    try:
        cafe_id, payload = _parse_cafe_json_args(command.args or "")
    except Exception:
        await message.answer(
            "Формат:\n<code>/init_cafe_json cafe_001 {\"title\":\"...\",\"phone\":\"...\",\"address\":\"...\",\"admin_id\":123,\"menu\":{\"Капучино\":250}}</code>"
        )
        return
    if cafe_id not in CAFES:
        await message.answer("Неизвестный cafe_id.")
        return

    r: redis.Redis = message.bot._redis

    try:
        prof_map = _normalize_profile_payload(payload)
    except Exception:
        await message.answer("Неверные типы в JSON (admin_id/work_start/work_end/rate_limit_seconds должны быть числами).")
        return
    if prof_map:
        await r.hset(cafe_profile_key(cafe_id), mapping=prof_map)

    if "menu" in payload:
        menu_map = _normalize_menu_payload(payload.get("menu"))
        if menu_map:
            await r.hset(cafe_menu_key(cafe_id), mapping=menu_map)
        else:
            await _ensure_default_menu(r, cafe_id)
    else:
        await _ensure_default_menu(r, cafe_id)

    cafe = await get_cafe_by_id_effective(r, cafe_id)
    await send_admin_screen(message, cafe_id, cafe)

@router.message(Command("set_profile_json"))
async def set_profile_json_cmd(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        return
    try:
        cafe_id, payload = _parse_cafe_json_args(command.args or "")
    except Exception:
        await message.answer(
            "Формат:\n<code>/set_profile_json cafe_001 {\"title\":\"...\",\"phone\":\"...\",\"address\":\"...\",\"admin_id\":123}</code>"
        )
        return
    if cafe_id not in CAFES:
        await message.answer("Неизвестный cafe_id.")
        return

    try:
        prof_map = _normalize_profile_payload(payload)
    except Exception:
        await message.answer("Неверные типы в JSON.")
        return
    if not prof_map:
        await message.answer("JSON пустой или без поддерживаемых полей.")
        return

    r: redis.Redis = message.bot._redis
    await r.hset(cafe_profile_key(cafe_id), mapping=prof_map)

    cafe = await get_cafe_by_id_effective(r, cafe_id)
    await message.answer(
        f"✅ Профиль обновлён: <code>{html.quote(cafe_id)}</code> → <b>{html.quote(str(cafe.get('title','Кафе')))}</b>"
    )

@router.message(Command("set_admin"))
async def set_admin_cmd(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
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
    except Exception:
        await message.answer("admin_id должен быть числом.")
        return

    r: redis.Redis = message.bot._redis
    await r.hset(cafe_profile_key(cafe_id), mapping={"admin_id": str(admin_id)})
    await message.answer(f"✅ admin_id установлен: <code>{html.quote(cafe_id)}</code> → <code>{admin_id}</code>")

@router.message(Command("set_menu_set"))
async def set_menu_set_cmd(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        return
    raw = (command.args or "").strip()
    if not raw:
        await message.answer('Формат: <code>/set_menu_set cafe_001 "Капучино" 250</code>')
        return
    tokens = raw.split()
    if len(tokens) < 3:
        await message.answer('Формат: <code>/set_menu_set cafe_001 "Капучино" 250</code>')
        return

    cafe_id = tokens[0]
    if cafe_id not in CAFES:
        await message.answer("Неизвестный cafe_id.")
        return
    try:
        price = int(tokens[-1])
    except Exception:
        await message.answer("Цена должна быть числом.")
        return

    drink = raw[len(cafe_id):].strip()
    drink = drink.rsplit(" ", 1)[0].strip().strip('"').strip("'")
    if not drink:
        await message.answer("Название напитка пустое.")
        return

    r: redis.Redis = message.bot._redis
    await r.hset(cafe_menu_key(cafe_id), mapping={drink: str(price)})
    await message.answer(f"✅ Меню: <code>{html.quote(cafe_id)}</code> → {html.quote(drink)} = <b>{price}</b> р")

@router.message(Command("set_menu_del"))
async def set_menu_del_cmd(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        return
    raw = (command.args or "").strip()
    if not raw:
        await message.answer('Формат: <code>/set_menu_del cafe_001 "Латте"</code>')
        return
    parts = raw.split(maxsplit=1)
    if len(parts) != 2:
        await message.answer('Формат: <code>/set_menu_del cafe_001 "Латте"</code>')
        return

    cafe_id, drink = parts[0], parts[1].strip().strip('"').strip("'")
    if cafe_id not in CAFES or not drink:
        await message.answer('Формат: <code>/set_menu_del cafe_001 "Латте"</code>')
        return

    r: redis.Redis = message.bot._redis
    await r.hdel(cafe_menu_key(cafe_id), drink)
    await message.answer(f"✅ Удалено: <code>{html.quote(cafe_id)}</code> → {html.quote(drink)}")

@router.message(Command("cafe"))
async def cafe_info(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        return
    cafe_id = (command.args or "").strip() or DEFAULT_CAFE_ID
    if cafe_id not in CAFES:
        await message.answer("Неизвестный cafe_id.")
        return
    r: redis.Redis = message.bot._redis
    cafe = await get_cafe_by_id_effective(r, cafe_id)
    ws, we = cafe_hours(cafe)
    await message.answer(
        f"🏠 <b>{html.quote(str(cafe.get('title','Кафе')))}</b>\n"
        f"id: <code>{html.quote(cafe_id)}</code>\n"
        f"admin_id: <code>{int(cafe.get('admin_id') or 0)}</code>\n"
        f"phone: <code>{html.quote(str(cafe.get('phone','')))}</code>\n"
        f"address: {html.quote(str(cafe.get('address','')))}\n"
        f"hours: {ws}:00–{we}:00\n"
        f"menu items: <b>{len(menu_of(cafe))}</b>",
    )

@router.message(Command("export_cafe"))
async def export_cafe_cmd(message: Message, command: CommandObject):
    if not is_superadmin(message.from_user.id):
        return
    cafe_id = (command.args or "").strip() or DEFAULT_CAFE_ID
    if cafe_id not in CAFES:
        await message.answer("Неизвестный cafe_id.")
        return
    r: redis.Redis = message.bot._redis
    prof = await r.hgetall(cafe_profile_key(cafe_id))
    menu = await r.hgetall(cafe_menu_key(cafe_id))
    out = {"cafe_id": cafe_id, "profile": prof or {}, "menu": menu or {}}
    await message.answer("<code>" + html.quote(json.dumps(out, ensure_ascii=False)) + "</code>")


# -------------------------
# Admin buttons
# -------------------------
@router.message(F.text == BTN_ADMIN_OPEN_MENU)
async def admin_open_menu(message: Message, state: FSMContext):
    await state.clear()
    r: redis.Redis = message.bot._redis
    _, cafe = await get_cafe_for_message(message, r)

    if not cafe_open(cafe):
        await message.answer(closed_message(cafe), reply_markup=kb_info())
        return
    await message.answer("Открываю гостевое меню:", reply_markup=kb_guest(cafe))

@router.message(F.text == BTN_ADMIN_LINKS)
async def admin_links(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(user_cafe_key(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = await get_cafe_by_id_effective(r, cafe_id)

    if not is_admin_of_cafe(message.from_user.id, cafe):
        await message.answer("Доступно только администратору кафе.")
        return
    await send_admin_screen(message, cafe_id, cafe)

@router.message(F.text == BTN_ADMIN_GROUP)
async def admin_group_help(message: Message):
    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(user_cafe_key(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = await get_cafe_by_id_effective(r, cafe_id)

    if not is_admin_of_cafe(message.from_user.id, cafe):
        await message.answer("Доступно только администратору кафе.")
        return

    staff_link = await create_startgroup_link(message.bot, payload=cafe_id, encode=True)  # [web:24]
    await message.answer(
        "👥 <b>Подключение группы персонала</b>\n\n"
        "1) Создайте группу (например «Кафе — персонал»).\n"
        "2) Добавьте туда бота по ссылке:\n"
        f"{staff_link}\n\n"
        f"3) В группе напишите:\n<code>/bind {html.quote(cafe_id)}</code>\n",
        disable_web_page_preview=True,
    )

@router.message(F.text == BTN_ADMIN_STATS)
async def admin_stats_button(message: Message, state: FSMContext):
    await stats_cmd(message, state)


# -------------------------
# Group events + bind
# -------------------------
@router.my_chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def bot_added_to_group(event: ChatMemberUpdated, bot: Bot):
    if event.chat.type not in ("group", "supergroup"):
        return
    await bot.send_message(
        event.chat.id,
        "✅ Бот добавлен в группу персонала.\n\n"
        "Чтобы привязать группу к кафе, напишите:\n"
        "<code>/bind cafe_001</code>\n\n"
        "Команду должен выполнить администратор кафе.",
    )

@router.message(Command("bind"))
async def bind_group(message: Message, command: CommandObject):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда /bind работает только в группе персонала.")
        return

    cafe_id = (command.args or "").strip()
    if not cafe_id or cafe_id not in CAFES:
        await message.answer("Формат: <code>/bind cafe_001</code>")
        return

    r: redis.Redis = message.bot._redis
    cafe = await get_cafe_by_id_effective(r, cafe_id)

    if not is_admin_of_cafe(message.from_user.id, cafe):
        await message.answer("Только администратор этого кафе может привязать группу.")
        return

    await r.set(group_cafe_key(message.chat.id), cafe_id)
    await message.answer(f"Группа привязана к кафе: <b>{html.quote(str(cafe.get('title','Кафе')))}</b>")


# -------------------------
# Start flow
# -------------------------
async def start_common(message: Message, state: FSMContext, payload: Optional[str]):
    await state.clear()
    r: redis.Redis = message.bot._redis

    uid = message.from_user.id
    payload = (payload or "").strip() or None

    # admin deep link: admin:<cafe_id>
    if payload and payload.startswith("admin:"):
        cafe_id = payload.split("admin:", 1)[1].strip()
        if cafe_id in CAFES:
            cafe = await get_cafe_by_id_effective(r, cafe_id)
            if is_admin_of_cafe(uid, cafe):
                await r.set(user_cafe_key(uid), cafe_id)
                await send_admin_screen(message, cafe_id, cafe)
                return
            await message.answer("Доступ к админ-ссылке запрещён (не администратор кафе).")
            return

    # guest deep link: <cafe_id>
    if payload and payload in CAFES:
        await r.set(user_cafe_key(uid), payload)
        cafe_id = payload
        cafe = await get_cafe_by_id_effective(r, cafe_id)
    else:
        cafe_id, cafe = await get_cafe_for_message(message, r)
        existing = await r.get(user_cafe_key(uid))
        if not existing:
            await r.set(user_cafe_key(uid), cafe_id)

    # if admin (even without link)
    if is_admin_of_cafe(uid, cafe):
        await send_admin_screen(message, cafe_id, cafe)
        return

    name = html.quote(user_name(message))
    welcome = random.choice(WELCOME_VARIANTS).format(name=name)
    msk = now_msk().strftime("%H:%M")

    if cafe_open(cafe):
        await message.answer(
            f"{welcome}\n\n"
            f"<b>{html.quote(str(cafe.get('title','Кафе')))}</b>\n"
            f"🕐 <i>Московское время: {msk}</i>\n"
            f"{work_status(cafe)}\n\n"
            f"☕️ <b>Выберите напиток:</b>",
            reply_markup=kb_guest(cafe),
        )
    else:
        await message.answer(closed_message(cafe), reply_markup=kb_info())

@router.message(CommandStart(deep_link=True))
async def start_deep(message: Message, command: CommandObject, state: FSMContext):
    await start_common(message, state, (command.args or "").strip())

@router.message(CommandStart())
async def start_plain(message: Message, state: FSMContext):
    await start_common(message, state, None)


# -------------------------
# Stats
# -------------------------
@router.message(Command("stats"))
async def stats_cmd(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id, cafe = await get_cafe_for_message(message, r)
    if not is_admin_of_cafe(message.from_user.id, cafe):
        return

    total = int(await r.get(stats_total_orders_key(cafe_id)) or 0)
    lines = [
        f"📊 <b>Статистика</b>\n"
        f"Кафе: <b>{html.quote(str(cafe.get('title','Кафе')))}</b>\n\n"
        f"Всего заказов: <b>{total}</b>\n"
    ]
    for drink in menu_of(cafe).keys():
        cnt = int(await r.get(stats_drink_key(cafe_id, drink)) or 0)
        if cnt > 0:
            lines.append(f"{html.quote(drink)}: {cnt}")
    await message.answer("\n".join(lines))


# -------------------------
# Booking
# -------------------------
IGNORED_BOOKING_TEXTS = {
    BTN_CALL, BTN_HOURS, BTN_BOOK,
    BTN_ADMIN_LINKS, BTN_ADMIN_GROUP, BTN_ADMIN_STATS, BTN_ADMIN_OPEN_MENU,
    BTN_MENU, BTN_CANCEL, BTN_CONFIRM,
}

async def booking_start(message: Message, state: FSMContext, cafe: Dict[str, Any]):
    await state.set_state(OrderStates.waiting_for_booking_info)
    await message.answer(
        "📋 <b>Бронирование столика</b>\n\n"
        "Напиши одним сообщением:\n"
        "• дату и время (например: <i>сегодня в 19:30</i>)\n"
        "• количество гостей (например: <i>на 3 человека</i>)\n\n"
        "Я передам заявку администратору, и он свяжется с тобой в Telegram.",
        reply_markup=kb_info(),
    )

@router.message(StateFilter(OrderStates.waiting_for_booking_info), F.text)
async def booking_step(message: Message, state: FSMContext):
    if (message.text or "").strip() in IGNORED_BOOKING_TEXTS:
        await message.answer(
            "Напиши, пожалуйста, одним сообщением дату/время и количество гостей.\n"
            "Пример: <i>завтра в 19:30, на 4 человека</i>",
            reply_markup=kb_info(),
        )
        return

    r: redis.Redis = message.bot._redis
    cafe_id = str(await r.get(user_cafe_key(message.from_user.id)) or DEFAULT_CAFE_ID)
    cafe = await get_cafe_by_id_effective(r, cafe_id)

    uid = message.from_user.id
    guest_name = message.from_user.username or message.from_user.first_name or "Гость"
    user_link = f'<a href="tg://user?id={uid}">{html.quote(guest_name)}</a>'
    booking_text = html.quote((message.text or "").strip())

    admin_id = int(cafe.get("admin_id") or 0)
    if admin_id:
        admin_msg = (
            f"📋 <b>ЗАЯВКА НА БРОНЬ</b>\n"
            f"🏠 Кафе: <b>{html.quote(str(cafe.get('title','Кафе')))}</b> (id=<code>{html.quote(cafe_id)}</code>)\n\n"
            f"👤 Гость: {user_link}\n"
            f"🆔 ID: <code>{uid}</code>\n\n"
            f"📝 <b>Текст:</b>\n{booking_text}\n"
        )
        await message.bot.send_message(admin_id, admin_msg, disable_web_page_preview=True)

    await message.answer("👌 Заявка отправлена администратору. Он свяжется с тобой в Telegram.", reply_markup=kb_guest(cafe))
    await state.clear()


# -------------------------
# Orders
# -------------------------
QTY_MAP = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5}

@router.message(StateFilter(None), F.text)
async def guest_entry(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text:
        return

    r: redis.Redis = message.bot._redis
    cafe_id, cafe = await get_cafe_for_message(message, r)

    if text == BTN_BOOK:
        await booking_start(message, state, cafe)
        return

    if text == BTN_CALL:
        await message.answer(
            "📞 <b>Связаться с кафе</b>\n\n"
            f"🏠 <b>{html.quote(str(cafe.get('title','Кафе')))}</b>\n"
            f"☎️ <code>{html.quote(str(cafe.get('phone','')))}</code>\n",
            reply_markup=kb_guest(cafe) if cafe_open(cafe) else kb_info(),
        )
        return

    if text == BTN_HOURS:
        await message.answer(
            "⏰ <b>Режим работы</b>\n\n"
            f"{work_status(cafe)}\n"
            f"📍 <b>Адрес:</b> {html.quote(str(cafe.get('address','')))}\n",
            reply_markup=kb_guest(cafe) if cafe_open(cafe) else kb_info(),
        )
        return

    menu = menu_of(cafe)
    if text not in menu:
        return

    if not cafe_open(cafe):
        await message.answer(closed_message(cafe), reply_markup=kb_info())
        return

    drink = text
    price = int(menu[drink])

    await state.set_state(OrderStates.waiting_for_quantity)
    await state.set_data({"drink": drink, "price": price, "cafe_id": cafe_id})

    choice = random.choice(CHOICE_VARIANTS)
    await message.answer(
        f"{choice}\n\n"
        f"☕️ <b>{html.quote(drink)}</b>\n"
        f"💰 <b>{price} р</b>\n\n"
        f"<b>Сколько порций нужно?</b>",
        reply_markup=kb_qty(),
    )

@router.message(StateFilter(OrderStates.waiting_for_quantity))
async def qty_step(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    cafe_id = str((await state.get_data()).get("cafe_id") or DEFAULT_CAFE_ID)
    cafe = await get_cafe_by_id_effective(r, cafe_id)

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("❌ Заказ отменён.", reply_markup=kb_guest(cafe) if cafe_open(cafe) else kb_info())
        return

    qty = QTY_MAP.get((message.text or "").strip())
    if not qty:
        await message.answer("Выберите количество кнопкой ниже.", reply_markup=kb_qty())
        return

    data = await state.get_data()
    drink = str(data["drink"])
    price = int(data["price"])
    total = price * qty

    await state.set_state(OrderStates.waiting_for_confirmation)
    await state.update_data(quantity=qty, total=total)

    await message.answer(
        f"✨ Проверим заказ:\n\n"
        f"• Напиток: <b>{html.quote(drink)}</b>\n"
        f"• Количество: <b>{qty}</b>\n"
        f"• Итого: <b>{total} р</b>\n\n"
        f"Если всё верно — нажимай «{BTN_CONFIRM}».",
        reply_markup=kb_confirm(),
    )

@router.message(StateFilter(OrderStates.waiting_for_confirmation))
async def confirm_step(message: Message, state: FSMContext):
    r: redis.Redis = message.bot._redis
    data = await state.get_data()
    cafe_id = str(data.get("cafe_id") or DEFAULT_CAFE_ID)
    cafe = await get_cafe_by_id_effective(r, cafe_id)

    if message.text == BTN_MENU:
        await state.clear()
        await message.answer("☕️ Меню:", reply_markup=kb_guest(cafe))
        return

    if message.text != BTN_CONFIRM:
        await message.answer(f"Нажмите «{BTN_CONFIRM}» или «{BTN_MENU}».", reply_markup=kb_confirm())
        return

    uid = message.from_user.id
    rate_limit = cafe_rate_limit(cafe)

    last = await r.get(rl_key(uid))
    if last and time.time() - float(last) < rate_limit:
        await message.answer(
            f"⏳ Заказ уже оформляли недавно.\nНовый можно оформить через {rate_limit} секунд.",
            reply_markup=kb_guest(cafe),
        )
        await state.clear()
        return

    await r.setex(rl_key(uid), rate_limit, str(time.time()))

    drink = str(data["drink"])
    qty = int(data["quantity"])
    total = int(data["total"])

    await r.incr(stats_total_orders_key(cafe_id))
    await r.incr(stats_drink_key(cafe_id, drink))

    order_num = str(int(time.time()))[-6:]
    guest_name = message.from_user.username or message.from_user.first_name or "Клиент"
    user_link = f'<a href="tg://user?id={uid}">{html.quote(guest_name)}</a>'

    admin_id = int(cafe.get("admin_id") or 0)
    if admin_id:
        admin_text = (
            f"🔔 <b>НОВЫЙ ЗАКАЗ #{order_num}</b>\n"
            f"🏠 Кафе: <b>{html.quote(str(cafe.get('title','Кафе')))}</b> (id=<code>{html.quote(cafe_id)}</code>)\n\n"
            f"👤 Гость: {user_link}\n"
            f"🆔 ID: <code>{uid}</code>\n\n"
            f"☕️ {html.quote(drink)}\n"
            f"🔢 Количество: {qty}\n"
            f"💰 Сумма: <b>{total} р</b>\n"
        )
        await message.bot.send_message(admin_id, admin_text, disable_web_page_preview=True)

    finish = random.choice(FINISH_VARIANTS).format(name=html.quote(user_name(message)))
    await message.answer(
        f"✅ <b>Заказ #{order_num} принят!</b>\n\n"
        f"• {html.quote(drink)} × {qty}\n"
        f"• К оплате: <b>{total}р</b>\n\n"
        f"{finish}",
        reply_markup=kb_guest(cafe),
    )
    await state.clear()


# -------------------------
# Webhook app
# -------------------------
async def app_startup(app: web.Application):
    bot: Bot = app["bot"]
    logger.info("Startup: webhook url=%s", WEBHOOK_URL)

    await set_commands(bot)
    await bot.set_webhook(WEBHOOK_URL, secret_token=WEBHOOK_SECRET)  # [web:39]

    try:
        info = await bot.get_webhook_info()
        logger.info(
            "Webhook info: url=%s pending=%s last_error=%s",
            info.url,
            info.pending_update_count,
            info.last_error_message,
        )
    except Exception as e:
        logger.warning("get_webhook_info failed: %r", e)

async def app_shutdown(app: web.Application):
    bot: Bot = app["bot"]
    storage: RedisStorage = app["storage"]
    r: redis.Redis = bot._redis

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
    logger.info("Shutdown complete")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set")
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL not set")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    storage = RedisStorage.from_url(REDIS_URL)
    dp = Dispatcher(storage=storage)
    dp.include_router(router)

    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()
    bot._redis = r  # single redis client for all handlers

    app = web.Application()
    app["bot"] = bot
    app["dp"] = dp
    app["storage"] = storage
    app.on_startup.append(app_startup)
    app.on_shutdown.append(app_shutdown)

    async def healthcheck(_: web.Request):
        return web.json_response({"status": "ok"})

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
