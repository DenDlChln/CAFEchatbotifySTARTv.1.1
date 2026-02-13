import os
import json
import logging
import asyncio
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

import redis.asyncio as redis
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
    ChatMemberUpdated,
    ErrorEvent,
)
from aiogram.filters import CommandStart, Command, StateFilter, CommandObject
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from aiogram.utils.deep_linking import create_start_link, create_startgroup_link
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter
from aiogram.filters import IS_NOT_MEMBER, IS_MEMBER


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

MSK_TZ = timezone(timedelta(hours=3))
RATE_LIMIT_SECONDS = 60


def get_moscow_time() -> datetime:
    return datetime.now(MSK_TZ)


# -------------------------
# CONFIG (multi-cafe) + DIAGNOSTIC
# -------------------------

def load_config_file() -> Dict[str, Any]:
    path = os.getenv("CONFIG_PATH", "config.json")

    logger.info("=== IMPORT MARK: MULTI-CAFE DIAG LOADED ===")
    logger.info(f"CONFIG_PATH={path}")
    try:
        logger.info(f"CWD={os.getcwd()}")
    except Exception as e:
        logger.info(f"CWD error: {e}")

    try:
        logger.info("DIR=" + ", ".join(os.listdir(".")))
    except Exception as e:
        logger.info(f"DIR list error: {e}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            cafes_count = len(data.get("cafes", [])) if isinstance(data, dict) else "n/a"
            logger.info(f"CONFIG loaded cafes={cafes_count}")
            if not isinstance(data, dict):
                raise ValueError("config root must be object/dict")
            return data
    except FileNotFoundError as e:
        logger.error(f"CONFIG not found: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"CONFIG JSON invalid: {e}")
    except Exception as e:
        logger.error(f"CONFIG load error: {e}")

    return {}


CONFIG = load_config_file()
CAFES = CONFIG.get("cafes", [])
if not isinstance(CAFES, list):
    CAFES = []

DEFAULT_CAFE = {
    "id": "default_cafe",
    "name": "Кофейня (дефолт)",
    "phone": "+7 900 000-00-00",
    "address": "Адрес не указан",
    "admin_chat_id": 0,
    "work_start": 9,
    "work_end": 21,
    "menu": {
        "Капучино": 250,
        "Латте": 270,
    },
}

normalized: list[Dict[str, Any]] = []
for cafe in CAFES:
    if not isinstance(cafe, dict):
        continue
    c = dict(DEFAULT_CAFE)
    c.update(cafe)
    c["id"] = str(c.get("id", DEFAULT_CAFE["id"])).strip()
    c["name"] = str(c.get("name", DEFAULT_CAFE["name"]))
    c["phone"] = str(c.get("phone", DEFAULT_CAFE["phone"]))
    c["address"] = str(c.get("address", DEFAULT_CAFE["address"]))
    c["admin_chat_id"] = int(c.get("admin_chat_id", 0))
    c["work_start"] = int(c.get("work_start", DEFAULT_CAFE["work_start"]))
    c["work_end"] = int(c.get("work_end", DEFAULT_CAFE["work_end"]))
    c["menu"] = dict(c.get("menu", DEFAULT_CAFE["menu"]))
    if c["id"]:
        normalized.append(c)

if not normalized:
    normalized = [DEFAULT_CAFE]

CAFES = normalized
CAFES_BY_ID = {c["id"]: c for c in CAFES}
DEFAULT_CAFE_ID = CAFES[0]["id"]

SUPERADMIN_ID = int(CONFIG.get("superadmin_id") or 0)


# -------------------------
# ENV / WEBHOOK
# -------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "cafebot123")
HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

WEBHOOK_PATH = f"/{WEBHOOK_SECRET}/webhook"
WEBHOOK_URL = f"https://{HOSTNAME}{WEBHOOK_PATH}" if HOSTNAME else None

router = Router()


# -------------------------
# Global error handler
# -------------------------

@router.error()
async def on_error(event: ErrorEvent):
    logger.critical("UNHANDLED ERROR in handler: %r", event.exception, exc_info=True)


# -------------------------
# FSM
# -------------------------

class OrderStates(StatesGroup):
    waiting_for_quantity = State()
    waiting_for_confirmation = State()
    waiting_for_booking_info = State()  # бронирование


# -------------------------
# Redis helpers
# -------------------------

async def get_redis_client():
    client = redis.from_url(REDIS_URL)
    try:
        await client.ping()
        return client
    except Exception:
        await client.aclose()
        raise


def _rate_limit_key(user_id: int) -> str:
    return f"rate_limit:{user_id}"


def _user_cafe_key(user_id: int) -> str:
    return f"user_cafe:{user_id}"


def _group_cafe_key(chat_id: int) -> str:
    return f"group_cafe:{chat_id}"


async def get_user_cafe_id(user_id: int) -> Optional[str]:
    r = await get_redis_client()
    try:
        v = await r.get(_user_cafe_key(user_id))
    finally:
        await r.aclose()
    if not v:
        return None
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)


async def set_user_cafe_id(user_id: int, cafe_id: str) -> None:
    r = await get_redis_client()
    try:
        await r.set(_user_cafe_key(user_id), cafe_id)
    finally:
        await r.aclose()


async def set_group_cafe_id(chat_id: int, cafe_id: str) -> None:
    r = await get_redis_client()
    try:
        await r.set(_group_cafe_key(chat_id), cafe_id)
    finally:
        await r.aclose()


async def get_group_cafe_id(chat_id: int) -> Optional[str]:
    r = await get_redis_client()
    try:
        v = await r.get(_group_cafe_key(chat_id))
    finally:
        await r.aclose()
    if not v:
        return None
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)


def get_cafe_or_default(cafe_id: Optional[str]) -> Dict[str, Any]:
    if cafe_id and cafe_id in CAFES_BY_ID:
        return CAFES_BY_ID[cafe_id]
    return CAFES_BY_ID[DEFAULT_CAFE_ID]


async def get_cafe_for_user(user_id: int) -> Dict[str, Any]:
    cafe_id = await get_user_cafe_id(user_id)
    return get_cafe_or_default(cafe_id)


def is_cafe_open(cafe: Dict[str, Any]) -> bool:
    ws = int(cafe["work_start"])
    we = int(cafe["work_end"])
    return ws <= get_moscow_time().hour < we


def get_work_status(cafe: Dict[str, Any]) -> str:
    ws = int(cafe["work_start"])
    we = int(cafe["work_end"])
    h = get_moscow_time().hour
    if ws <= h < we:
        remaining = max(0, we - h)
        return f"🟢 Открыто (ещё {remaining} ч.)"
    return f"🔴 Закрыто\n🕐 Открываемся: {ws}:00 (МСК)"


# -------------------------
# Keyboards
# -------------------------

def create_menu_keyboard(cafe: Dict[str, Any]) -> ReplyKeyboardMarkup:
    keyboard = [[KeyboardButton(text=drink)] for drink in cafe["menu"].keys()]
    keyboard.append([
        KeyboardButton(text="📞 Связаться с кафе"),
        KeyboardButton(text="⏰ Режим работы"),
    ])
    keyboard.append([
        KeyboardButton(text="📋 Бронирование / столики"),
    ])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def create_info_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📞 Связаться с кафе"), KeyboardButton(text="⏰ Режим работы")],
            [KeyboardButton(text="📋 Бронирование / столики")],
        ],
        resize_keyboard=True,
    )


def create_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Мои ссылки")],
            [KeyboardButton(text="Подключить группу")],
            [KeyboardButton(text="Статистика")],
            [KeyboardButton(text="Открыть меню")],
        ],
        resize_keyboard=True,
    )


def create_quantity_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def create_confirm_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Подтвердить"), KeyboardButton(text="Меню")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# -------------------------
# Warm texts
# -------------------------

WELCOME_VARIANTS = [
    "Привет, {name}! Заходи по‑домашнему — подберём кофе под настроение.",
    "{name}, рад тебя видеть в нашей кофейне. Устроим вкусную паузу?",
    "{name}, добро пожаловать! Уже слышу шипение кофемашины — выбирай, что приготовить ☕️",
    "Заходи, {name}! Сейчас самое время сделать день чуть приятнее.",
    "{name}, привет! Давай придумаем идеальный напиток именно для тебя.",
]

CHOICE_VARIANTS = [
    "Отличный выбор — гости часто берут именно это 👍",
    "Классика, которая никогда не подводит.",
    "Тот самый вариант, который бариста выбирают для себя 😉",
    "Супер! Это один из хитов нашего меню.",
    "Вот это вкус! Любители хорошего кофе тебя поймут.",
    "Смело и по‑делу — такой выбор обычно делают ценители.",
    "{name}, видно, что ты разбираешься в напитках.",
    "Звучит очень вкусно, уже представляю аромат ☕️",
]

FINISH_VARIANTS = [
    "Спасибо за заказ, {name}! Готовим с заботой — приходи за настроением ещё.",
    "Заказ принят, {name}. Пусть этот кофе сделает день чуточку лучше.",
    "Готовим твой напиток, {name}. Будем рады видеть тебя снова.",
    "{name}, спасибо, что выбираешь нас. Всегда ждём на кофе‑паузу.",
    "Заказ в работе. Если захочется повторить — я всегда на связи ☕️",
]


def get_user_name(message: Message) -> str:
    if message.from_user is None:
        return "друг"
    return message.from_user.first_name or "друг"


def get_closed_message(cafe: Dict[str, Any]) -> str:
    menu_text = " • ".join([f"<b>{drink}</b> {price}р" for drink, price in cafe["menu"].items()])
    return (
        f"🔒 <b>{cafe['name']} сейчас закрыто</b>\n\n"
        f"{get_work_status(cafe)}\n\n"
        f"☕️ <b>Меню на паузе, но можно присмотреться:</b>\n{menu_text}\n\n"
        f"📍 <b>Адрес:</b> {cafe.get('address', 'адрес не указан')}\n"
        f"📞 Связаться:\n<code>{cafe['phone']}</code>\n\n"
        f"<i>Напиши /start или воспользуйся кнопками ниже, когда захочешь заглянуть ещё.</i>"
    )


def is_admin_of_cafe(user_id: int, cafe: Dict[str, Any]) -> bool:
    return user_id == int(cafe["admin_chat_id"]) or (SUPERADMIN_ID and user_id == SUPERADMIN_ID)


# -------------------------
# Debug command
# -------------------------

@router.message(Command("ping"))
async def ping(message: Message):
    await message.answer("pong")


# -------------------------
# Group events + /bind
# -------------------------

@router.my_chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def bot_added_to_group(event: ChatMemberUpdated, bot: Bot):
    if event.chat.type not in ("group", "supergroup"):
        return
    await bot.send_message(
        event.chat.id,
        "✅ Бот добавлен в группу персонала.\n\n"
        "Чтобы привязать её к конкретному кафе, напишите здесь:\n"
        "<code>/bind cafe_roma</code>\n\n"
        "Команду должен выполнить администратор кафе.",
    )


@router.message(Command("bind"))
async def bind_group(message: Message, command: CommandObject):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда /bind работает только в группе персонала.")
        return

    cafe_id = (command.args or "").strip()
    if not cafe_id:
        await message.answer("Формат: <code>/bind cafe_roma</code>")
        return

    if cafe_id not in CAFES_BY_ID:
        await message.answer("Неизвестный cafe_id. Пример: <code>/bind cafe_roma</code>")
        return

    cafe = CAFES_BY_ID[cafe_id]
    if message.from_user.id != int(cafe["admin_chat_id"]) and (not SUPERADMIN_ID or message.from_user.id != SUPERADMIN_ID):
        await message.answer("Только администратор этого кафе может привязать группу.")
        return

    await set_group_cafe_id(message.chat.id, cafe_id)
    await message.answer(f"Группа привязана к кафе: <b>{cafe['name']}</b>")


# -------------------------
# Admin screens
# -------------------------

async def send_admin_start_screen(message: Message, cafe: Dict[str, Any]):
    guest_link = await create_start_link(message.bot, payload=cafe["id"], encode=False)
    staff_link = await create_startgroup_link(message.bot, payload=cafe["id"], encode=False)

    text = (
        f"🛠 <b>Режим администратора</b>\n"
        f"Кафе: <b>{cafe['name']}</b> (id=<code>{cafe['id']}</code>)\n\n"
        f"1️⃣ Гостевая ссылка (QR на столы):\n{guest_link}\n\n"
        f"2️⃣ Ссылка для группы персонала:\n{staff_link}\n\n"
        f"3️⃣ После добавления бота в группу напишите там:\n"
        f"<code>/bind {cafe['id']}</code>\n\n"
        f"Нажмите «Открыть меню», чтобы посмотреть сценарий гостя."
    )
    await message.answer(text, reply_markup=create_admin_keyboard(), disable_web_page_preview=True)


# -------------------------
# START handlers
# -------------------------

async def _start_common(message: Message, state: FSMContext, incoming_cafe_id: Optional[str]):
    await state.clear()
    user_id = message.from_user.id

    if incoming_cafe_id:
        if incoming_cafe_id not in CAFES_BY_ID:
            await message.answer("Ссылка устарела или кафе не найдено. Попросите актуальную ссылку у заведения.")
            return
        await set_user_cafe_id(user_id, incoming_cafe_id)
        cafe = CAFES_BY_ID[incoming_cafe_id]
    else:
        cafe = await get_cafe_for_user(user_id)
        if not await get_user_cafe_id(user_id):
            await set_user_cafe_id(user_id, cafe["id"])

    logger.info(f"/start user={user_id} cafe={cafe['id']} incoming={incoming_cafe_id}")

    if is_admin_of_cafe(user_id, cafe):
        await send_admin_start_screen(message, cafe)
        return

    name = get_user_name(message)
    msk_time = get_moscow_time().strftime("%H:%M")
    welcome = random.choice(WELCOME_VARIANTS).format(name=name)

    if is_cafe_open(cafe):
        await message.answer(
            f"{welcome}\n\n"
            f"<b>{cafe['name']}</b>\n"
            f"🕐 <i>Московское время: {msk_time}</i>\n"
            f"{get_work_status(cafe)}\n\n"
            f"☕️ <b>Выберите напиток:</b>",
            reply_markup=create_menu_keyboard(cafe),
        )
    else:
        await message.answer(get_closed_message(cafe), reply_markup=create_info_keyboard())


@router.message(CommandStart(deep_link=True))
async def start_with_payload(message: Message, command: CommandObject, state: FSMContext):
    incoming = (command.args or "").strip() or None
    await _start_common(message, state, incoming)


@router.message(CommandStart())
async def start_plain(message: Message, state: FSMContext):
    await _start_common(message, state, None)


# -------------------------
# Admin buttons
# -------------------------

@router.message(F.text == "Открыть меню")
async def open_menu_as_guest(message: Message, state: FSMContext):
    await state.clear()
    cafe = await get_cafe_for_user(message.from_user.id)

    name = get_user_name(message)
    msk_time = get_moscow_time().strftime("%H:%M")
    welcome = random.choice(WELCOME_VARIANTS).format(name=name)

    if is_cafe_open(cafe):
        await message.answer(
            f"{welcome}\n\n"
            f"<b>{cafe['name']}</b>\n"
            f"🕐 <i>Московское время: {msk_time}</i>\n"
            f"{get_work_status(cafe)}\n\n"
            f"☕️ <b>Выберите напиток:</b>",
            reply_markup=create_menu_keyboard(cafe),
        )
    else:
        await message.answer(get_closed_message(cafe), reply_markup=create_info_keyboard())


@router.message(F.text == "Мои ссылки")
async def my_links_button(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    if not is_admin_of_cafe(message.from_user.id, cafe):
        await message.answer("Доступно только администратору кафе.")
        return
    await send_admin_start_screen(message, cafe)


@router.message(F.text == "Подключить группу")
async def group_help_button(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    if not is_admin_of_cafe(message.from_user.id, cafe):
        await message.answer("Доступно только администратору кафе.")
        return

    staff_link = await create_startgroup_link(message.bot, payload=cafe["id"], encode=False)
    text = (
        "👥 <b>Подключение группы персонала</b>\n\n"
        "1) Создайте группу (например «Кафе — персонал»).\n"
        "2) Добавьте туда бота по ссылке:\n"
        f"{staff_link}\n\n"
        f"3) В группе напишите:\n<code>/bind {cafe['id']}</code>\n"
    )
    await message.answer(text, disable_web_page_preview=True)


@router.message(F.text == "Статистика")
async def stats_button(message: Message):
    await stats_command(message)


# -------------------------
# Ordering
# -------------------------

QUANTITY_MAP = {
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
}


@router.message(StateFilter(OrderStates.waiting_for_quantity))
async def process_quantity(message: Message, state: FSMContext):
    cafe = await get_cafe_for_user(message.from_user.id)

    if message.text == "Отмена":
        await state.clear()
        await message.answer(
            "❌ Заказ отменён. Если захочешь, всегда можно начать заново — я рядом.",
            reply_markup=create_menu_keyboard(cafe) if is_cafe_open(cafe) else create_info_keyboard(),
        )
        return

    quantity = QUANTITY_MAP.get(message.text)
    if quantity:
        data = await state.get_data()
        drink, price = data["drink"], int(data["price"])
        total = price * quantity

        await state.set_state(OrderStates.waiting_for_confirmation)
        await state.update_data(quantity=quantity, total=total)

        await message.answer(
            f"✨ Проверим заказ:\n\n"
            f"• Напиток: <b>{drink}</b>\n"
            f"• Количество: <b>{quantity}</b>\n"
            f"• Итого: <b>{total} р</b>\n\n"
            f"Если всё верно — нажимай «Подтвердить».",
            reply_markup=create_confirm_keyboard(),
        )
    else:
        await message.answer("Пожалуйста, выбери количество кнопкой ниже ☟", reply_markup=create_quantity_keyboard())


@router.message(StateFilter(OrderStates.waiting_for_confirmation))
async def process_confirmation(message: Message, state: FSMContext):
    cafe = await get_cafe_for_user(message.from_user.id)
    user_id = message.from_user.id

    if message.text == "Подтвердить":
        try:
            r_client = await get_redis_client()
            last_order = await r_client.get(_rate_limit_key(user_id))
            if last_order and time.time() - float(last_order) < RATE_LIMIT_SECONDS:
                await message.answer(
                    f"⏳ Уже оформляли заказ совсем недавно.\n"
                    f"Новый можно оформить через {RATE_LIMIT_SECONDS} секунд — так бариста успеют всё приготовить без спешки.",
                    reply_markup=create_menu_keyboard(cafe),
                )
                await r_client.aclose()
                return

            await r_client.setex(_rate_limit_key(user_id), RATE_LIMIT_SECONDS, time.time())
            await r_client.aclose()
        except Exception:
            pass

        data = await state.get_data()
        drink = data["drink"]
        quantity = int(data["quantity"])
        total = int(data["total"])

        order_id = f"order:{int(time.time())}:{user_id}"
        order_num = order_id.split(":")[-1]
        user_name = message.from_user.username or message.from_user.first_name or "Клиент"

        try:
            r_client = await get_redis_client()
            await r_client.hset(
                order_id,
                mapping={
                    "user_id": user_id,
                    "username": user_name,
                    "drink": drink,
                    "quantity": quantity,
                    "total": total,
                    "timestamp": datetime.now().isoformat(),
                    "cafe_id": cafe["id"],
                },
            )
            await r_client.expire(order_id, 86400)
            await r_client.incr(f"stats:{cafe['id']}:total_orders")
            await r_client.incr(f"stats:{cafe['id']}:drink:{drink}")
            await r_client.aclose()
        except Exception:
            pass

        user_link = f'<a href="tg://user?id={user_id}">{user_name}</a>'
        admin_message = (
            f"🔔 <b>НОВЫЙ ЗАКАЗ #{order_num}</b>\n"
            f"🏠 Кафе: <b>{cafe['name']}</b> (id={cafe['id']})\n\n"
            f"👤 Гость: {user_link}\n"
            f"🆔 ID: <code>{user_id}</code>\n\n"
            f"☕️ {drink}\n"
            f"🔢 Количество: {quantity}\n"
            f"💰 Сумма: <b>{total} р</b>\n\n"
            f"Нажми на имя, чтобы открыть чат и ответить гостю."
        )

        await message.bot.send_message(int(cafe["admin_chat_id"]), admin_message, disable_web_page_preview=True)

        finish_text = random.choice(FINISH_VARIANTS).format(name=get_user_name(message))
        await message.answer(
            f"✅ <b>Заказ #{order_num} принят!</b>\n\n"
            f"• {drink} × {quantity}\n"
            f"• К оплате: <b>{total}р</b>\n\n"
            f"{finish_text}",
            reply_markup=create_menu_keyboard(cafe),
        )
        await state.clear()
        return

    if message.text == "Меню":
        await state.clear()
        await message.answer("☕️ Открываю меню:", reply_markup=create_menu_keyboard(cafe))
        return

    await message.answer("Чтобы продолжить, нажми кнопку «Подтвердить» или «Меню».", reply_markup=create_confirm_keyboard())


# -------------------------
# Booking
# -------------------------

# тексты-кнопки, которые не должны считаться заявкой
IGNORED_BOOKING_TEXTS = {
    "📞 Связаться с кафе",
    "⏰ Режим работы",
    "📋 Бронирование / столики",
    "Открыть меню",
    "Мои ссылки",
    "Подключить группу",
    "Статистика",
    "Меню",
    "Отмена",
}


async def start_booking(message: Message, state: FSMContext, cafe: Dict[str, Any]):
    await state.set_state(OrderStates.waiting_for_booking_info)
    name = get_user_name(message)
    msk_time = get_moscow_time().strftime("%H:%M")

    text = (
        f"📋 <b>Бронирование столика</b>\n\n"
        f"{name}, сейчас {msk_time} (МСК).\n\n"
        "Напиши одним сообщением:\n"
        "• дату и время (например: <i>сегодня в 19:30</i>)\n"
        "• количество гостей (например: <i>на 3 человека</i>)\n\n"
        "Я передам заявку администратору и он свяжется с тобой в Telegram, чтобы подтвердить бронь."
    )
    await message.answer(text, reply_markup=create_info_keyboard())


@router.message(StateFilter(OrderStates.waiting_for_booking_info), F.text)
async def process_booking(message: Message, state: FSMContext):
    # защита от случайных нажатий кнопок
    if message.text in IGNORED_BOOKING_TEXTS:
        await message.answer(
            "Напиши, пожалуйста, одним сообщением:\n"
            "• дату и время\n"
            "• на сколько человек нужен столик\n\n"
            "Пример: <i>завтра в 19:30, на 4 человека</i>",
            reply_markup=create_info_keyboard(),
        )
        return

    cafe = await get_cafe_for_user(message.from_user.id)
    user_id = message.from_user.id
    user_name = message.from_user.username or message.from_user.first_name or "Гость"
    user_link = f'<a href="tg://user?id={user_id}">{user_name}</a>'

    booking_text = message.text.strip()

    admin_msg = (
        f"📋 <b>НОВАЯ ЗАЯВКА НА БРОНЬ</b>\n"
        f"🏠 Кафе: <b>{cafe['name']}</b> (id={cafe['id']})\n\n"
        f"👤 Гость: {user_link}\n"
        f"🆔 ID: <code>{user_id}</code>\n\n"
        f"📝 <b>Пожелание гостя:</b>\n{booking_text}\n\n"
        f"Ответь гостю в личных сообщениях и подтверди бронь.\n"
        f"При необходимости уточни время и посадку."
    )
    await message.bot.send_message(int(cafe["admin_chat_id"]), admin_msg, disable_web_page_preview=True)

    await message.answer(
        "👌 Заявка отправлена администратору.\n"
        "Он свяжется с тобой в Telegram, подтвердит бронь и подскажет по посадке.",
        reply_markup=create_menu_keyboard(cafe),
    )
    await state.clear()


# -------------------------
# Info buttons
# -------------------------

@router.message(StateFilter(None), F.text == "📞 Связаться с кафе")
async def call_phone(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    name = get_user_name(message)

    phone = cafe.get("phone", "номер не указан")
    cafe_name = cafe.get("name", "наше кафе")

    text = (
        f"📞 <b>Связаться с кафе</b>\n\n"
        f"{name}, вот контакт для связи:\n\n"
        f"🏠 <b>{cafe_name}</b>\n"
        f"☎️ <code>{phone}</code>\n\n"
        f"Если хочешь, можешь сразу коротко написать сюда, по какому вопросу будем на связи "
        f"(бронь, заказ навынос, уточнение по меню и т.п.) — я передам сообщение администратору."
    )

    await message.answer(text, reply_markup=create_menu_keyboard(cafe))


@router.message(StateFilter(None), F.text == "⏰ Режим работы")
async def show_hours(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    name = get_user_name(message)
    msk_time = get_moscow_time().strftime("%H:%M")

    status = get_work_status(cafe)
    cafe_name = cafe.get("name", "наше кафе")
    address = cafe.get("address", "адрес уточняется")

    text = (
        f"⏰ <b>Режим работы кафе</b>\n\n"
        f"{name}, сейчас {msk_time} (МСК).\n"
        f"{status}\n\n"
        f"🏠 <b>{cafe_name}</b>\n"
        f"📍 <b>Адрес:</b> {address}\n\n"
        f"Можешь сразу выбрать напиток или оставить бронь столика на удобное время."
    )

    await message.answer(
        text,
        reply_markup=create_menu_keyboard(cafe) if is_cafe_open(cafe) else create_info_keyboard(),
    )


# -------------------------
# General text handler (drinks + booking entry)
# -------------------------

@router.message(StateFilter(None), F.text)
async def drink_selected(message: Message, state: FSMContext):
    if await state.get_state() is not None:
        return

    if not message.text:
        return

    cafe = await get_cafe_for_user(message.from_user.id)
    menu = cafe["menu"]

    if message.text == "📋 Бронирование / столики":
        await start_booking(message, state, cafe)
        return

    if message.text not in menu:
        return

    if not is_cafe_open(cafe):
        await message.answer(get_closed_message(cafe), reply_markup=create_info_keyboard())
        return

    drink = message.text
    price = int(menu[drink])

    await state.set_state(OrderStates.waiting_for_quantity)
    await state.set_data({"drink": drink, "price": price, "cafe_id": cafe["id"]})

    choice_text = random.choice(CHOICE_VARIANTS).format(name=get_user_name(message))
    await message.answer(
        f"{choice_text}\n\n"
        f"☕️ <b>{drink}</b>\n"
        f"💰 <b>{price} р</b>\n\n"
        f"<b>Сколько порций нужно?</b>",
        reply_markup=create_quantity_keyboard(),
    )


# -------------------------
# Commands
# -------------------------

@router.message(Command("stats"))
async def stats_command(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    if not is_admin_of_cafe(message.from_user.id, cafe):
        return

    try:
        r_client = await get_redis_client()
        total_orders = int(await r_client.get(f"stats:{cafe['id']}:total_orders") or 0)
        stats_text = (
            f"📊 <b>Статистика заказов</b>\n"
            f"Кафе: <b>{cafe['name']}</b> (id={cafe['id']})\n\n"
            f"Всего заказов: <b>{total_orders}</b>\n\n"
        )
        for drink in cafe["menu"].keys():
            count = int(await r_client.get(f"stats:{cafe['id']}:drink:{drink}") or 0)
            if count > 0:
                stats_text += f"{drink}: {count}\n"
        await r_client.aclose()
        await message.answer(stats_text)
    except Exception:
        await message.answer("Ошибка статистики")


@router.message(Command("links"))
async def links_command(message: Message):
    if not SUPERADMIN_ID or message.from_user.id != SUPERADMIN_ID:
        return

    parts = ["🔗 <b>Ссылки всех кафе</b>\n"]
    for cafe in CAFES:
        guest_link = await create_start_link(message.bot, payload=cafe["id"], encode=False)
        staff_link = await create_startgroup_link(message.bot, payload=cafe["id"], encode=False)
        parts.append(
            f"<b>{cafe['name']}</b> (id={cafe['id']}):\n"
            f"• Гости: {guest_link}\n"
            f"• Персонал: {staff_link}\n"
        )
    await message.answer("\n".join(parts), disable_web_page_preview=True)


@router.message(Command("myid"))
async def myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")


# -------------------------
# Startup / Webhook
# -------------------------

async def set_bot_commands(bot: Bot) -> None:
    commands = [
        BotCommand(command="start", description="Запуск бота"),
        BotCommand(command="ping", description="Проверка (pong)"),
        BotCommand(command="myid", description="Показать мой Telegram ID"),
        BotCommand(command="stats", description="Статистика (админ)"),
        BotCommand(command="bind", description="Привязать группу к кафе (в группе)"),
        BotCommand(command="links", description="Ссылки всех кафе (суперадмин)"),
    ]
    await bot.set_my_commands(commands)


async def on_startup(bot: Bot) -> None:
    logger.info("=== BUILD MARK: MULTI-CAFE MAIN v7 (booking fix + hours) ===")
    logger.info(f"Cafes loaded: {len(CAFES)}")
    for c in CAFES:
        logger.info(f"CFG cafe={c['id']} admin={c['admin_chat_id']}")

    if WEBHOOK_URL:
        logger.info(f"Webhook target: {WEBHOOK_URL}")

    try:
        r_test = redis.from_url(REDIS_URL)
        await r_test.ping()
        await r_test.aclose()
        logger.info("Redis connected")
    except Exception as e:
        logger.error(f"Redis error: {e}")

    try:
        await set_bot_commands(bot)
        logger.info("Commands set")
    except Exception as e:
        logger.error(f"set_my_commands error: {e}")

    if WEBHOOK_URL:
        try:
            await bot.set_webhook(WEBHOOK_URL, secret_token=WEBHOOK_SECRET)
            logger.info("Webhook set")
        except Exception as e:
            logger.error(f"Webhook error: {e}")
    else:
        logger.warning("WEBHOOK_URL is None (no RENDER_EXTERNAL_HOSTNAME). Webhook not set.")

    try:
        for cafe in CAFES:
            guest = await create_start_link(bot, payload=cafe["id"], encode=False)
            staff = await create_startgroup_link(bot, payload=cafe["id"], encode=False)
            logger.info(f"LINK guest [{cafe['id']}]: {guest}")
            logger.info(f"LINK staff  [{cafe['id']}]: {staff}")
    except Exception as e:
        logger.error(f"Link generation error: {e}")


async def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not found")
        return
    if not REDIS_URL:
        logger.error("REDIS_URL not found")
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    storage = RedisStorage.from_url(REDIS_URL)
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    dp.startup.register(on_startup)

    app = web.Application()

    async def healthcheck(request: web.Request):
        return web.json_response({"status": "healthy", "bot": "ready"})

    app.router.add_get("/", healthcheck)

    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET,
        handle_in_background=True,
    ).register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)

    async def _on_shutdown(a: web.Application):
        try:
            await bot.delete_webhook()
        except Exception:
            pass
        try:
            await storage.close()
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass
        logger.info("Shutdown complete")

    app.on_shutdown.append(_on_shutdown)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info(f"Server running on 0.0.0.0:{PORT}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
