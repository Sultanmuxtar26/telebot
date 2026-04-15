
import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ============================================================
# UNIVERSAL ADMIN PRO MAX BUSINESS v6
# ============================================================
import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "973453261"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "xisoblaydi_majburBot").lstrip("@")
DB_NAME = os.getenv("DB_NAME", "/data/universal_admin_v6.db")

EXTRA_SLOT_PRICE_UZS = int(os.getenv("EXTRA_SLOT_PRICE_UZS", "30000"))
DEFAULT_REFERRALS = 0
DEFAULT_FORCE_DELETE_SECONDS = 20
DEFAULT_DUPLICATE_WINDOW_MINUTES = 60
DEFAULT_MAX_POST_LEN = 1200
DEFAULT_SLOT_INTERVAL_MINUTES = 60
DEFAULT_WELCOME_TEXT = """Assalomu alaykum, {name}!

Siz <b>{group}</b> guruhiga qo'shildingiz.
Bu guruhda mavzuga oid e'lon yozish mumkin.
Spam, begona reklama va taqiqlangan mavzular yozmang."""

LINK_RE = re.compile(
    r"""(?ix)
    (
        https?://
        | www\.
        | t\.me/
        | telegram\.me/
        | tg://
        | (?:[a-z0-9-]+\.)+(?:com|net|org|info|biz|site|online|store|shop|top|xyz|io|co|app|dev|me|uz|ru|kz|kg|tj|tm|ua|de|fr|uk|us)(?:/|\b)
    )
    """
)
TG_USERNAME_RE = re.compile(r"(?<!\w)@[A-Za-z0-9_]{5,32}\b")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

storage = MemoryStorage()
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)

conn = sqlite3.connect(DB_NAME, check_same_thread=False)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
scheduler_task: Optional[asyncio.Task] = None


# ============================================================
# DATABASE / MIGRATIONS
# ============================================================
def ensure_column(table: str, column: str, definition: str) -> None:
    info = cur.execute(f"PRAGMA table_info({table})").fetchall()
    names = {row["name"] for row in info}
    if column not in names:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        conn.commit()


cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    full_name TEXT,
    username TEXT,
    referred_by INTEGER,
    referrals_count INTEGER DEFAULT 0,
    is_blocked INTEGER DEFAULT 0,
    is_owner_approved INTEGER DEFAULT 0,
    can_post_until TEXT,
    is_group_admin_client INTEGER DEFAULT 0,
    paid_slot_quota INTEGER DEFAULT 0,
    created_at TEXT,
    last_seen TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS groups (
    chat_id INTEGER PRIMARY KEY,
    title TEXT,
    owner_admin_id INTEGER,
    is_active INTEGER DEFAULT 1,
    posting_mode TEXT DEFAULT 'referrals',
    required_invites INTEGER DEFAULT 0,
    required_referrals INTEGER DEFAULT 0,
    forced_channel TEXT DEFAULT '',
    forced_text TEXT DEFAULT '',
    forced_text_delete_seconds INTEGER DEFAULT 20,
    welcome_enabled INTEGER DEFAULT 1,
    welcome_text TEXT DEFAULT '',
    last_welcome_message_id INTEGER DEFAULT 0,
    anti_link_enabled INTEGER DEFAULT 0,
    duplicate_window_minutes INTEGER DEFAULT 60,
    max_post_len INTEGER DEFAULT 1200,
    created_at TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS group_members_added (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER,
    inviter_id INTEGER,
    invited_user_id INTEGER,
    created_at TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS group_link_allowlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER,
    domain TEXT,
    created_at TEXT
)
""")
cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_group_link_allowlist_unique ON group_link_allowlist(chat_id, domain)")

cur.execute("""
CREATE TABLE IF NOT EXISTS message_fingerprints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER,
    user_id INTEGER,
    fp TEXT,
    created_at TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS ad_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    full_name TEXT,
    username TEXT,
    service_type TEXT,
    text TEXT,
    status TEXT DEFAULT 'new',
    created_at TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS slot_purchase_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    full_name TEXT,
    username TEXT,
    requested_slots INTEGER DEFAULT 1,
    unit_price INTEGER DEFAULT 0,
    discount_percent INTEGER DEFAULT 0,
    total_price INTEGER DEFAULT 0,
    status TEXT DEFAULT 'new',
    created_at TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS recurring_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_user_id INTEGER,
    chat_id INTEGER,
    slot_no INTEGER DEFAULT 1,
    message_text TEXT,
    interval_minutes INTEGER DEFAULT 60,
    is_paid INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    next_run_at TEXT,
    last_run_at TEXT,
    last_error TEXT DEFAULT '',
    created_at TEXT
)
""")

ensure_column("users", "paid_slot_quota", "INTEGER DEFAULT 0")
ensure_column("groups", "welcome_text", "TEXT DEFAULT ''")
ensure_column("groups", "forced_text", "TEXT DEFAULT ''")
ensure_column("groups", "forced_text_delete_seconds", f"INTEGER DEFAULT {DEFAULT_FORCE_DELETE_SECONDS}")
ensure_column("groups", "anti_link_enabled", "INTEGER DEFAULT 0")
ensure_column("groups", "duplicate_window_minutes", f"INTEGER DEFAULT {DEFAULT_DUPLICATE_WINDOW_MINUTES}")
ensure_column("groups", "max_post_len", f"INTEGER DEFAULT {DEFAULT_MAX_POST_LEN}")
ensure_column("groups", "required_invites", "INTEGER DEFAULT 0")
ensure_column("groups", "required_referrals", "INTEGER DEFAULT 0")
ensure_column("groups", "forced_channel", "TEXT DEFAULT ''")
ensure_column("groups", "posting_mode", "TEXT DEFAULT 'referrals'")
ensure_column("groups", "welcome_enabled", "INTEGER DEFAULT 1")
ensure_column("groups", "last_welcome_message_id", "INTEGER DEFAULT 0")
ensure_column("users", "referrals_count", "INTEGER DEFAULT 0")
ensure_column("users", "is_owner_approved", "INTEGER DEFAULT 0")
ensure_column("users", "is_group_admin_client", "INTEGER DEFAULT 0")

cur.execute(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_group_members_unique ON group_members_added(chat_id, inviter_id, invited_user_id)"
)
cur.execute(
    "CREATE INDEX IF NOT EXISTS idx_message_fp_lookup ON message_fingerprints(chat_id, user_id, fp)"
)
cur.execute(
    "CREATE INDEX IF NOT EXISTS idx_slots_due ON recurring_slots(is_active, next_run_at)"
)
conn.commit()


# ============================================================
# HELPERS
# ============================================================
def now_dt() -> datetime:
    return datetime.now()


def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def fmt_money(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")


def ensure_setting(key: str, default: str) -> None:
    row = cur.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    if row is None:
        cur.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (key, default))
        conn.commit()


def get_setting(key: str, default: str = "") -> str:
    row = cur.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    conn.commit()


def init_settings() -> None:
    ensure_setting("welcome_text", "Assalomu alaykum! Bu universal admin-bot.")
    ensure_setting("global_posting_mode", "referrals")
    ensure_setting("default_required_referrals", str(DEFAULT_REFERRALS))
    ensure_setting("owner_contact", "@yourusername")
    ensure_setting("extra_slot_price", str(EXTRA_SLOT_PRICE_UZS))
    ensure_setting("public_users_count_threshold", "50000")


init_settings()


async def safe_delete(chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def delete_message_later(chat_id: int, message_id: int, seconds: int = 5) -> None:
    await asyncio.sleep(max(1, int(seconds)))
    await safe_delete(chat_id, message_id)


def schedule_delete(chat_id: int, message_id: int, seconds: int = 5) -> None:
    asyncio.create_task(delete_message_later(chat_id, message_id, seconds))


async def reply_temp(message: Message, text: str, seconds: int = 8, reply_markup=None) -> None:
    sent = await message.answer(text, reply_markup=reply_markup)
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        await safe_delete(message.chat.id, message.message_id)
        schedule_delete(sent.chat.id, sent.message_id, seconds)


def get_users_count() -> int:
    row = cur.execute("SELECT COUNT(*) c FROM users").fetchone()
    return int(row["c"] if row else 0)


def should_hide_public_users_count() -> bool:
    threshold = int(get_setting("public_users_count_threshold", "500") or 500)
    return get_users_count() >= max(1, threshold)


def build_home_text(user_id: int, owner_show_users_count: bool = False) -> str:
    users_count = get_users_count()
    if user_id == OWNER_ID and owner_show_users_count:
        users_line = f"👥 Foydalanuvchilar soni: <b>{users_count}</b>"
    else:
        users_line = (
            "👥 Foydalanuvchilar soni: <b>yashirin</b>"
            if should_hide_public_users_count()
            else f"👥 Foydalanuvchilar soni: <b>{users_count}</b>"
        )

    extra = ""
    if user_id == OWNER_ID:
        extra = "\n\nOwner bu raqamni pastdagi tugma bilan ochishi yoki yashirishi mumkin."

    return (
        "Assalomu alaykum!\n\n"
        "<b>Universal admin-bot</b>\n"
        f"{users_line}\n\n"
        "Bu yerda siz referral, slot, reklama va guruhlaringizni boshqarishingiz mumkin."
        f"{extra}"
    )


def add_or_update_user(
    user_id: int,
    full_name: str,
    username: str = "",
    referred_by: Optional[int] = None,
) -> None:
    row = cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row:
        cur.execute(
            "UPDATE users SET full_name=?, username=?, last_seen=? WHERE user_id=?",
            (full_name, username, now_str(), user_id),
        )
    else:
        cur.execute(
            """
            INSERT INTO users
            (user_id, full_name, username, referred_by, created_at, last_seen, paid_slot_quota)
            VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (user_id, full_name, username, referred_by, now_str(), now_str()),
        )
        if referred_by and referred_by != user_id:
            ref_row = cur.execute("SELECT * FROM users WHERE user_id=?", (referred_by,)).fetchone()
            if ref_row:
                cur.execute(
                    "UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id=?",
                    (referred_by,),
                )
    conn.commit()


def get_user(user_id: int):
    return cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()


def get_group(chat_id: int):
    return cur.execute("SELECT * FROM groups WHERE chat_id=? AND is_active=1", (chat_id,)).fetchone()


def add_group(chat_id: int, title: str, owner_admin_id: int) -> None:
    exists = cur.execute("SELECT 1 FROM groups WHERE chat_id=?", (chat_id,)).fetchone()
    if exists:
        cur.execute(
            """
            UPDATE groups
            SET title=?, owner_admin_id=?, is_active=1
            WHERE chat_id=?
            """,
            (title, owner_admin_id, chat_id),
        )
    else:
        cur.execute(
            """
            INSERT INTO groups
            (
                chat_id, title, owner_admin_id, is_active, posting_mode,
                required_invites, required_referrals, forced_channel, forced_text,
                forced_text_delete_seconds, welcome_enabled, welcome_text,
                last_welcome_message_id, anti_link_enabled, duplicate_window_minutes,
                max_post_len, created_at
            )
            VALUES (?, ?, ?, 1, ?, 0, ?, '', '', ?, 1, ?, 0, 0, ?, ?, ?)
            """,
            (
                chat_id,
                title,
                owner_admin_id,
                get_setting("global_posting_mode", "referrals"),
                int(get_setting("default_required_referrals", str(DEFAULT_REFERRALS))),
                DEFAULT_FORCE_DELETE_SECONDS,
                DEFAULT_WELCOME_TEXT,
                DEFAULT_DUPLICATE_WINDOW_MINUTES,
                DEFAULT_MAX_POST_LEN,
                now_str(),
            ),
        )
    cur.execute("UPDATE users SET is_group_admin_client=1 WHERE user_id=?", (owner_admin_id,))
    conn.commit()


def count_user_added_members(chat_id: int, user_id: int) -> int:
    row = cur.execute(
        "SELECT COUNT(*) c FROM group_members_added WHERE chat_id=? AND inviter_id=?",
        (chat_id, user_id),
    ).fetchone()
    return int(row["c"] if row else 0)


def add_member_credit(chat_id: int, inviter_id: int, invited_user_id: int) -> bool:
    if inviter_id == invited_user_id:
        return False
    exists = cur.execute(
        "SELECT 1 FROM group_members_added WHERE chat_id=? AND inviter_id=? AND invited_user_id=?",
        (chat_id, inviter_id, invited_user_id),
    ).fetchone()
    if exists:
        return False
    cur.execute(
        "INSERT INTO group_members_added (chat_id, inviter_id, invited_user_id, created_at) VALUES (?, ?, ?, ?)",
        (chat_id, inviter_id, invited_user_id, now_str()),
    )
    conn.commit()
    return True


def add_manual_member_credit(chat_id: int, inviter_id: int, invited_user_id: int) -> bool:
    return add_member_credit(chat_id, inviter_id, invited_user_id)


def normalize_text(text: str) -> str:
    return " ".join((text or "").strip().lower().split())


def normalize_domain(value: str) -> str:
    v = (value or "").strip().lower()
    v = re.sub(r"^https?://", "", v)
    v = re.sub(r"^www\.", "", v)
    v = v.split("/", 1)[0].strip()
    return v


def get_group_allow_domains(chat_id: int) -> set[str]:
    rows = cur.execute(
        "SELECT domain FROM group_link_allowlist WHERE chat_id=? ORDER BY domain ASC",
        (chat_id,),
    ).fetchall()
    return {normalize_domain(r["domain"]) for r in rows if normalize_domain(r["domain"])}


def add_group_allow_domain(chat_id: int, domain: str) -> str:
    d = normalize_domain(domain)
    if not d:
        return ""
    cur.execute(
        "INSERT OR IGNORE INTO group_link_allowlist (chat_id, domain, created_at) VALUES (?, ?, ?)",
        (chat_id, d, now_str()),
    )
    conn.commit()
    return d


def delete_group_allow_domain(chat_id: int, domain: str) -> str:
    d = normalize_domain(domain)
    cur.execute("DELETE FROM group_link_allowlist WHERE chat_id=? AND domain=?", (chat_id, d))
    conn.commit()
    return d


def clear_group_allow_domains(chat_id: int) -> None:
    cur.execute("DELETE FROM group_link_allowlist WHERE chat_id=?", (chat_id,))
    conn.commit()


def extract_domains_from_text(text: str) -> set[str]:
    t = (text or "").strip()
    if not t:
        return set()

    domains = set()
    for m in LINK_RE.finditer(t):
        raw = m.group(0).strip().lower()
        candidate = raw
        if raw.startswith("tg://"):
            domains.add("tg")
            continue
        if raw.startswith("www."):
            candidate = "http://" + raw
        elif not re.match(r"^[a-z]+://", raw):
            candidate = "http://" + raw
        try:
            parsed = urlparse(candidate)
            host = parsed.netloc or parsed.path.split("/")[0]
        except Exception:
            host = raw
        host = normalize_domain(host)
        if host:
            domains.add(host)
    return domains


def has_forbidden_link(text: str, allowed_domains: Optional[set[str]] = None) -> bool:
    t = (text or "").strip()
    if not t:
        return False

    allowed = {normalize_domain(x) for x in (allowed_domains or set()) if normalize_domain(x)}
    for domain in extract_domains_from_text(t):
        if domain == "tg":
            return True
        if domain in allowed or any(domain.endswith("." + ad) for ad in allowed):
            continue
        return True

    return bool(TG_USERNAME_RE.search(t))


def is_duplicate_recent(chat_id: int, user_id: int, text: str, minutes: int) -> bool:
    fp = normalize_text(text)
    row = cur.execute(
        "SELECT created_at FROM message_fingerprints WHERE chat_id=? AND user_id=? AND fp=? ORDER BY id DESC LIMIT 1",
        (chat_id, user_id, fp),
    ).fetchone()
    if not row:
        return False
    old = parse_dt(row["created_at"])
    if not old:
        return False
    return (now_dt() - old).total_seconds() <= minutes * 60


def save_fingerprint(chat_id: int, user_id: int, text: str) -> None:
    fp = normalize_text(text)
    cur.execute(
        "INSERT INTO message_fingerprints (chat_id, user_id, fp, created_at) VALUES (?, ?, ?, ?)",
        (chat_id, user_id, fp, now_str()),
    )
    conn.commit()


def get_second_level_referrals_count(user_id: int) -> int:
    row = cur.execute(
        """
        SELECT COUNT(*)
        FROM users
        WHERE referred_by IN (
            SELECT user_id FROM users WHERE referred_by=?
        )
        """,
        (user_id,),
    ).fetchone()
    return int(row[0] if row else 0)


def get_discount_percent(user_id: int) -> int:
    user = get_user(user_id)
    refs = int(user["referrals_count"] or 0) if user else 0
    return 20 if refs >= 10 else 0


def get_free_slot_quota(user_id: int) -> int:
    bonus = 1 if get_second_level_referrals_count(user_id) >= 1 else 0
    return 1 + bonus


def get_paid_slot_quota(user_id: int) -> int:
    user = get_user(user_id)
    return int(user["paid_slot_quota"] or 0) if user else 0


def get_total_slot_quota(user_id: int) -> int:
    return get_free_slot_quota(user_id) + get_paid_slot_quota(user_id)


def count_active_slots(user_id: int) -> int:
    row = cur.execute(
        "SELECT COUNT(*) c FROM recurring_slots WHERE owner_user_id=? AND is_active=1",
        (user_id,),
    ).fetchone()
    return int(row["c"] if row else 0)


def next_slot_no(user_id: int) -> int:
    row = cur.execute(
        "SELECT COALESCE(MAX(slot_no), 0) m FROM recurring_slots WHERE owner_user_id=?",
        (user_id,),
    ).fetchone()
    return int(row["m"] or 0) + 1


def user_can_create_slot(user_id: int) -> tuple[bool, int, int]:
    active = count_active_slots(user_id)
    quota = get_total_slot_quota(user_id)
    return active < quota, active, quota


def create_recurring_slot(user_id: int, chat_id: int, text: str, interval_minutes: int) -> int:
    free_slots = get_free_slot_quota(user_id)
    active_rows = cur.execute(
        """
        SELECT * FROM recurring_slots
        WHERE owner_user_id=? AND is_active=1
        ORDER BY id ASC
        """,
        (user_id,),
    ).fetchall()
    is_paid = 1 if len(active_rows) >= free_slots else 0
    slot_no = next_slot_no(user_id)
    next_run = now_dt() + timedelta(minutes=interval_minutes)
    cur.execute(
        """
        INSERT INTO recurring_slots
        (owner_user_id, chat_id, slot_no, message_text, interval_minutes, is_paid, is_active, next_run_at, last_run_at, last_error, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 1, ?, '', '', ?)
        """,
        (user_id, chat_id, slot_no, text, interval_minutes, is_paid, next_run.strftime("%Y-%m-%d %H:%M:%S"), now_str()),
    )
    conn.commit()
    return int(cur.lastrowid)


def stop_slot(slot_id: int, user_id: int) -> bool:
    row = cur.execute(
        "SELECT 1 FROM recurring_slots WHERE id=? AND owner_user_id=? AND is_active=1",
        (slot_id, user_id),
    ).fetchone()
    if not row:
        return False
    cur.execute("UPDATE recurring_slots SET is_active=0 WHERE id=?", (slot_id,))
    conn.commit()
    return True


def calc_slot_price(user_id: int, requested_slots: int = 1) -> tuple[int, int, int]:
    unit_price = int(get_setting("extra_slot_price", str(EXTRA_SLOT_PRICE_UZS)))
    discount = get_discount_percent(user_id)
    discounted = unit_price
    if discount:
        discounted = int(round(unit_price * (100 - discount) / 100))
    total = discounted * max(1, requested_slots)
    return unit_price, discount, total


def create_slot_purchase_request(user_id: int, requested_slots: int) -> int:
    user = get_user(user_id)
    unit_price, discount, total = calc_slot_price(user_id, requested_slots)
    cur.execute(
        """
        INSERT INTO slot_purchase_requests
        (user_id, full_name, username, requested_slots, unit_price, discount_percent, total_price, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'new', ?)
        """,
        (
            user_id,
            user["full_name"] if user else str(user_id),
            user["username"] if user else "",
            requested_slots,
            unit_price,
            discount,
            total,
            now_str(),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def grant_paid_slots(user_id: int, count: int) -> None:
    cur.execute(
        "UPDATE users SET paid_slot_quota = COALESCE(paid_slot_quota, 0) + ? WHERE user_id=?",
        (count, user_id),
    )
    conn.commit()


def render_welcome_text(template: str, user_name: str, group_name: str) -> str:
    return (template or DEFAULT_WELCOME_TEXT).replace("{name}", user_name).replace("{group}", group_name)


def user_add_ref_status(chat_id: int, user_id: int) -> tuple[int, int, int, int]:
    group = get_group(chat_id)
    user = get_user(user_id)
    add_count = count_user_added_members(chat_id, user_id)
    refs = int(user["referrals_count"] or 0) if user else 0
    need_add = int(group["required_invites"] or 0) if group else 0
    need_ref = int(group["required_referrals"] or 0) if group else 0
    return add_count, refs, need_add, need_ref


def access_debug_text(chat_id: int, user_id: int) -> str:
    add_count, refs, need_add, need_ref = user_add_ref_status(chat_id, user_id)
    add_left = max(0, need_add - add_count)
    ref_left = max(0, need_ref - refs)
    return (
        "🧪 Holat\n"
        f"👥 Add: <b>{add_count}</b> / {need_add}\n"
        f"🔗 Ref: <b>{refs}</b> / {need_ref}\n"
        f"⏳ Qolgan add: <b>{add_left}</b>\n"
        f"⏳ Qolgan ref: <b>{ref_left}</b>"
    )


def build_access_denied_text(full_name: str, chat_id: int, user_id: int) -> str:
    add_count, refs, need_add, need_ref = user_add_ref_status(chat_id, user_id)
    add_left = max(0, need_add - add_count)
    ref_left = max(0, need_ref - refs)
    safe_name = full_name or "Foydalanuvchi"

    if need_add > 0 and need_ref == 0:
        return (
            f"❌ Kechirasiz, <b>{safe_name}</b>!\n\n"
            f"Siz guruhga yozish uchun avval <b>{need_add}</b> ta odam qo'shishingiz kerak.\n"
            f"Hozir sizda: <b>{add_count}</b> ta.\n"
            f"Yana kerak: <b>{add_left}</b> ta.\n\n"
            f"🤖 Bot: https://t.me/{BOT_USERNAME}"
        )

    if need_ref > 0 and need_add == 0:
        return (
            f"❌ Kechirasiz, <b>{safe_name}</b>!\n\n"
            f"Siz guruhga yozish uchun avval <b>{need_ref}</b> ta referral yig'ishingiz kerak.\n"
            f"Hozir sizda: <b>{refs}</b> ta.\n"
            f"Yana kerak: <b>{ref_left}</b> ta.\n\n"
            f"🤖 Bot: https://t.me/{BOT_USERNAME}"
        )

    if need_add > 0 and need_ref > 0:
        return (
            f"❌ Kechirasiz, <b>{safe_name}</b>!\n\n"
            f"Siz guruhga yozish uchun quyidagilardan bittasini bajarishingiz kerak:\n"
            f"• <b>{need_add}</b> ta odam qo'shish (sizda: <b>{add_count}</b>, yana: <b>{add_left}</b>)\n"
            f"yoki\n"
            f"• <b>{need_ref}</b> ta referral yig'ish (sizda: <b>{refs}</b>, yana: <b>{ref_left}</b>)\n\n"
            f"🤖 Bot: https://t.me/{BOT_USERNAME}"
        )

    return (
        f"❌ Kechirasiz, <b>{safe_name}</b>!\n\n"
        f"Sizda guruhga yozish uchun ruxsat hali ochilmagan.\n\n"
        f"🤖 Bot: https://t.me/{BOT_USERNAME}"
    )


async def send_temp_notice(chat_id: int, text: str, seconds: int = DEFAULT_FORCE_DELETE_SECONDS, reply_markup=None):
    try:
        sent = await bot.send_message(
            chat_id,
            text,
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
    except Exception:
        return None

    schedule_delete(sent.chat.id, sent.message_id, max(5, int(seconds or DEFAULT_FORCE_DELETE_SECONDS)))
    return sent


def extract_target_user_id(message: Message) -> Optional[int]:
    parts = (message.text or "").split(maxsplit=1)
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user.id
    if len(parts) > 1:
        raw = parts[1].strip()
        if raw.lstrip("-").isdigit():
            return int(raw)
    return None


def parse_slot_command_payload(message: Message) -> tuple[Optional[int], Optional[str]]:
    text = message.text or ""
    parts = text.split(maxsplit=2)
    if len(parts) < 2:
        return None, None
    interval_raw = parts[1].strip()
    if not interval_raw.isdigit():
        return None, None
    interval = int(interval_raw)
    slot_text = None
    if message.reply_to_message and (message.reply_to_message.text or message.reply_to_message.caption):
        slot_text = message.reply_to_message.text or message.reply_to_message.caption
    elif len(parts) >= 3:
        slot_text = parts[2].strip()
    return interval, slot_text


def can_user_post_in_group(chat_id: int, user_id: int) -> bool:
    group = get_group(chat_id)
    user = get_user(user_id)
    if not group or not user:
        return True
    if user["is_blocked"] == 1:
        return False
    if user_id == OWNER_ID or user["is_owner_approved"] == 1:
        return True

    mode = group["posting_mode"] or get_setting("global_posting_mode", "referrals")
    if mode == "free":
        return True
    if mode == "owner_only":
        return False

    need_add = int(group["required_invites"] or 0)
    need_ref = int(group["required_referrals"] or 0)

    if need_add == 0 and need_ref == 0:
        return True

    user_add = count_user_added_members(chat_id, user_id)
    user_ref = int(user["referrals_count"] or 0)
    return (need_add > 0 and user_add >= need_add) or (need_ref > 0 and user_ref >= need_ref)


def get_all_active_groups(search_query: str = "") -> list[sqlite3.Row]:
    search_query = (search_query or "").strip()
    if search_query:
        like = f"%{search_query.lower()}%"
        return cur.execute(
            """
            SELECT * FROM groups
            WHERE is_active=1
              AND (
                  LOWER(title) LIKE ?
                  OR CAST(chat_id AS TEXT) LIKE ?
                  OR CAST(owner_admin_id AS TEXT) LIKE ?
              )
            ORDER BY title COLLATE NOCASE ASC, created_at DESC
            """,
            (like, f"%{search_query}%", f"%{search_query}%"),
        ).fetchall()

    return cur.execute(
        """
        SELECT * FROM groups
        WHERE is_active=1
        ORDER BY title COLLATE NOCASE ASC, created_at DESC
        """
    ).fetchall()


def truncate_text(value: str, max_len: int = 38) -> str:
    value = (value or "").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


def owner_groups_page_text(page: int, page_size: int = 12, search_query: str = "") -> str:
    rows = get_all_active_groups(search_query)
    total = len(rows)
    if total == 0:
        return "<b>Ulangan chatlar</b>\n\nHozircha aktiv chat yo'q."

    pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, pages - 1))
    start = page * page_size
    subset = rows[start : start + page_size]

    parts = []
    for idx, row in enumerate(subset, start=start + 1):
        parts.append(
            f"{idx}. <b>{row['title']}</b>\n"
            f"ID: <code>{row['chat_id']}</code>\n"
            f"Admin: <code>{row['owner_admin_id']}</code>"
        )

    search_line = f"\n🔎 Qidiruv: <b>{search_query}</b>" if search_query else ""

    return (
        "<b>Ulangan chatlar</b>\n\n"
        f"Jami: <b>{total}</b> ta chat\n"
        f"Sahifa: <b>{page + 1}/{pages}</b>{search_line}\n\n"
        + "\n\n".join(parts)
    )


def owner_groups_nav_kb(page: int, page_size: int = 12, search_query: str = "") -> InlineKeyboardMarkup:
    total = len(get_all_active_groups(search_query))
    pages = max(1, (total + page_size - 1) // page_size) if total else 1
    page = max(0, min(page, pages - 1))
    rows = []
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"og:{page - 1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"og:{page + 1}"))
        if nav:
            rows.append(nav)
    search_row = [InlineKeyboardButton(text="🔎 Qidirish", callback_data="og:search")]
    if search_query:
        search_row.append(InlineKeyboardButton(text="🧹 Tozalash", callback_data="og:clear"))
    rows.append(search_row)
    rows.append([InlineKeyboardButton(text="🔙 Owner panel", callback_data="owner_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def owner_broadcast_preview_label(message: Message) -> str:
    preview = (message.text or message.caption or "").strip()
    if preview:
        return truncate_text(preview.replace("\n", " "), 160)
    content_type = getattr(message, "content_type", "message")
    return f"<{content_type}>"


def owner_broadcast_keyboard(page: int, selected_ids: list[int], page_size: int = 8, search_query: str = "") -> InlineKeyboardMarkup:
    rows_data = get_all_active_groups(search_query)
    total = len(rows_data)
    pages = max(1, (total + page_size - 1) // page_size) if total else 1
    page = max(0, min(page, pages - 1))
    start = page * page_size
    subset = rows_data[start : start + page_size]

    keyboard = []
    selected_set = set(int(x) for x in selected_ids)

    for row in subset:
        mark = "✅" if int(row["chat_id"]) in selected_set else "⬜️"
        keyboard.append([
            InlineKeyboardButton(
                text=f"{mark} {truncate_text(row['title'], 34)}",
                callback_data=f"ob:t:{int(row['chat_id'])}:{page}",
            )
        ])

    if pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"ob:p:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{pages}", callback_data="ob:noop"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"ob:p:{page + 1}"))
        keyboard.append(nav)

    keyboard.append([
        InlineKeyboardButton(text="☑️ Sahifadagilar", callback_data=f"ob:sp:{page}"),
        InlineKeyboardButton(text="🧹 Tanlov", callback_data="ob:cl"),
    ])
    search_row = [InlineKeyboardButton(text="🔎 Qidirish", callback_data="ob:search")]
    if search_query:
        search_row.append(InlineKeyboardButton(text="🗑 Qidiruvni tozalash", callback_data="ob:clearsearch"))
    keyboard.append(search_row)
    keyboard.append([InlineKeyboardButton(text="📤 Hammasiga yuborish", callback_data="ob:all")])
    keyboard.append([InlineKeyboardButton(text="✅ Tanlanganlarga yuborish", callback_data="ob:send")])
    keyboard.append([InlineKeyboardButton(text="❌ Bekor qilish", callback_data="ob:back")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def render_owner_broadcast_menu(target_message: Message, state: FSMContext, page: int = 0, notice: str = "") -> None:
    data = await state.get_data()
    search_query = (data.get("owner_broadcast_search", "") or "").strip()
    rows = get_all_active_groups(search_query)
    total = len(rows)
    selected_ids = [int(x) for x in data.get("owner_broadcast_selected_ids", [])]
    preview = data.get("owner_broadcast_preview", "")

    if total == 0:
        await target_message.edit_text(
            "<b>Owner e'loni</b>\n\nAktiv chatlar topilmadi.",
            reply_markup=owner_menu(),
        )
        return

    pages = max(1, (total + 7) // 8)
    page = max(0, min(page, pages - 1))
    start = page * 8
    subset = rows[start : start + 8]

    lines = []
    selected_set = set(selected_ids)
    for row in subset:
        mark = "✅" if int(row["chat_id"]) in selected_set else "⬜️"
        lines.append(f"{mark} <b>{row['title']}</b>")

    search_line = f"\n🔎 Qidiruv: <b>{search_query}</b>" if search_query else ""
    text = (
        "<b>Owner e'loni</b>\n\n"
        f"Tayyor xabar: {preview}\n"
        f"Tanlangan chatlar: <b>{len(selected_ids)}</b> / <b>{total}</b>\n"
        f"Sahifa: <b>{page + 1}/{pages}</b>{search_line}\n\n"
        + "\n".join(lines)
    )
    if notice:
        text += f"\n\n{notice}"

    await state.update_data(owner_broadcast_page=page)
    await target_message.edit_text(
        text,
        reply_markup=owner_broadcast_keyboard(page, selected_ids, search_query=search_query),
    )


async def send_owner_broadcast(source_chat_id: int, source_message_id: int, target_chat_ids: list[int]) -> tuple[int, int, list[str]]:
    success = 0
    failed = 0
    failed_titles = []

    rows = {int(r["chat_id"]): r["title"] for r in get_all_active_groups()}
    for chat_id in target_chat_ids:
        try:
            await bot.copy_message(chat_id=chat_id, from_chat_id=source_chat_id, message_id=source_message_id)
            success += 1
        except Exception as e:
            failed += 1
            title = rows.get(int(chat_id), str(chat_id))
            failed_titles.append(f"• {title} — {str(e)[:80]}")
        await asyncio.sleep(0.05)

    return success, failed, failed_titles[:10]


# ============================================================
# STATES
# ============================================================
class WelcomeTextState(StatesGroup):
    waiting_text = State()


class ForcedTextState(StatesGroup):
    waiting_text = State()


class AdRequestState(StatesGroup):
    waiting_service = State()
    waiting_text = State()


class OwnerBroadcastState(StatesGroup):
    waiting_message = State()
    selecting_groups = State()
    waiting_search = State()


class OwnerGroupsSearchState(StatesGroup):
    waiting_query = State()


# ============================================================
# KEYBOARDS
# ============================================================
def main_menu(user_id: int, owner_show_users_count: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="👤 Mening kabinetim", callback_data="cabinet")],
        [InlineKeyboardButton(text="🔗 Referral havolam", callback_data="my_ref")],
        [InlineKeyboardButton(text="♻️ Slotlar", callback_data="slots_menu")],
        [InlineKeyboardButton(text="📢 Reklama berish", callback_data="order_ad")],
        [InlineKeyboardButton(text="📋 Mening guruhlarim", callback_data="my_groups")],
        [InlineKeyboardButton(text="📘 Qollanma", callback_data="guide")],
    ]
    if user_id == OWNER_ID:
        buttons.append([
            InlineKeyboardButton(
                text="🙈 Users sonini yashirish" if owner_show_users_count else "👥 Users sonini ko'rish",
                callback_data="home_users:hide" if owner_show_users_count else "home_users:show",
            )
        ])
        buttons.append([InlineKeyboardButton(text="🛠 OWNER PANEL", callback_data="owner_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def owner_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Statistika", callback_data="owner_stats")],
            [InlineKeyboardButton(text="📋 Ulangan chatlar", callback_data="owner_groups")],
            [InlineKeyboardButton(text="📣 Owner e'loni", callback_data="owner_broadcast")],
            [InlineKeyboardButton(text="📩 Reklama so'rovlari", callback_data="owner_requests")],
            [InlineKeyboardButton(text="♻️ Slot so'rovlari", callback_data="owner_slot_requests")],
            [InlineKeyboardButton(text="🔙 Orqaga", callback_data="owner_back")],
        ]
    )


def group_panel_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Add 1", callback_data=f"gp:add:1:{chat_id}"),
                InlineKeyboardButton(text="Add 3", callback_data=f"gp:add:3:{chat_id}"),
                InlineKeyboardButton(text="Add OFF", callback_data=f"gp:add:off:{chat_id}"),
            ],
            [
                InlineKeyboardButton(text="Ref 0", callback_data=f"gp:ref:0:{chat_id}"),
                InlineKeyboardButton(text="Ref 1", callback_data=f"gp:ref:1:{chat_id}"),
                InlineKeyboardButton(text="Ref 2", callback_data=f"gp:ref:2:{chat_id}"),
            ],
            [
                InlineKeyboardButton(text="Welcome ON/OFF", callback_data=f"gp:welcome_toggle:{chat_id}"),
                InlineKeyboardButton(text="Welcome text", callback_data=f"gp:welcome_text:{chat_id}"),
            ],
            [
                InlineKeyboardButton(text="Anti-link ON/OFF", callback_data=f"gp:anti_link:{chat_id}"),
                InlineKeyboardButton(text="Forced text", callback_data=f"gp:forced_text:{chat_id}"),
            ],
            [
                InlineKeyboardButton(text="Debug", callback_data=f"gp:debug:{chat_id}"),
                InlineKeyboardButton(text="Qollanma", callback_data=f"gp:help:{chat_id}"),
            ],
        ]
    )


def blocked_post_kb(chat_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="✅ Tekshirish", callback_data=f"check_add:{chat_id}")]]
    if BOT_USERNAME:
        rows.append([InlineKeyboardButton(text="🤖 Botga o'tish", url=f"https://t.me/{BOT_USERNAME}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ============================================================
# CHECKS
# ============================================================
async def is_real_group_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
    except Exception:
        return False


async def has_group_admin_rights(message: Message) -> bool:
    if message.from_user and message.from_user.id == OWNER_ID:
        return True
    if message.from_user:
        return await is_real_group_admin(message.chat.id, message.from_user.id)
    return False


async def callback_group_admin_ok(callback: CallbackQuery, chat_id: int) -> bool:
    if callback.from_user.id == OWNER_ID:
        return True
    return await is_real_group_admin(chat_id, callback.from_user.id)


async def is_bot_admin(chat_id: int) -> bool:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id, me.id)
        return member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
    except Exception:
        return False


async def check_forced_channel_membership(user_id: int, channel_username: str) -> bool:
    if not channel_username:
        return True
    try:
        member = await bot.get_chat_member(channel_username, user_id)
        return member.status not in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]
    except Exception:
        return True


# ============================================================
# PRIVATE FLOW
# ============================================================
@dp.message(Command("start"))
async def start_cmd(message: Message, command: CommandObject):
    user_id = message.from_user.id
    full_name = message.from_user.full_name
    username = message.from_user.username or ""
    referred_by = None

    if command.args and command.args.startswith("ref_"):
        try:
            referred_by = int(command.args.split("_", 1)[1])
        except Exception:
            referred_by = None

    existing = get_user(user_id)
    add_or_update_user(user_id, full_name, username, referred_by if not existing else None)

    await message.answer(build_home_text(user_id), reply_markup=main_menu(user_id))


@dp.callback_query(F.data == "cabinet")
async def cabinet_cb(callback: CallbackQuery):
    u = get_user(callback.from_user.id)
    refs = int(u["referrals_count"] or 0) if u else 0
    second = get_second_level_referrals_count(callback.from_user.id)
    discount = get_discount_percent(callback.from_user.id)
    active_slots = count_active_slots(callback.from_user.id)
    free_slots = get_free_slot_quota(callback.from_user.id)
    paid_slots = get_paid_slot_quota(callback.from_user.id)
    rows = cur.execute(
        "SELECT COUNT(*) c FROM groups WHERE owner_admin_id=? AND is_active=1",
        (callback.from_user.id,),
    ).fetchone()
    text = (
        "<b>Mening kabinetim</b>\n\n"
        f"👤 Ism: {callback.from_user.full_name}\n"
        f"🆔 ID: <code>{callback.from_user.id}</code>\n"
        f"🔗 1-daraja referral: <b>{refs}</b>\n"
        f"🪜 2-daraja referral: <b>{second}</b>\n"
        f"💸 Chegirma: <b>{discount}%</b>\n"
        f"💬 Guruhlar soni: <b>{rows['c']}</b>\n"
        f"♻️ Aktiv slotlar: <b>{active_slots}</b>\n"
        f"🎁 Bepul slotlar: <b>{free_slots}</b>\n"
        f"💳 Pullik slotlar: <b>{paid_slots}</b>"
    )
    await callback.message.edit_text(text, reply_markup=main_menu(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data == "my_ref")
async def my_ref_cb(callback: CallbackQuery):
    text = f"<b>Sizning referral havolangiz:</b>\n\nhttps://t.me/{BOT_USERNAME}?start=ref_{callback.from_user.id}"
    await callback.message.edit_text(text, reply_markup=main_menu(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data == "slots_menu")
async def slots_menu_cb(callback: CallbackQuery):
    active = count_active_slots(callback.from_user.id)
    free_slots = get_free_slot_quota(callback.from_user.id)
    paid_slots = get_paid_slot_quota(callback.from_user.id)
    unit_price, discount, total = calc_slot_price(callback.from_user.id, 1)
    effective_price = total
    second_bonus = "bor" if get_second_level_referrals_count(callback.from_user.id) >= 1 else "yo'q"
    text = (
        "<b>♻️ Slotlar bo'limi</b>\n\n"
        f"Faol slotlar: <b>{active}</b>\n"
        f"Bepul slotlar: <b>{free_slots}</b>\n"
        f"Pullik slotlar: <b>{paid_slots}</b>\n"
        f"2-daraja referral bonusi: <b>{second_bonus}</b>\n"
        f"Qo'shimcha slot narxi: <b>{fmt_money(unit_price)} so'm</b>\n"
        f"Sizning chegirmangiz: <b>{discount}%</b>\n"
        f"Siz uchun narx: <b>{fmt_money(effective_price)} so'm</b>\n\n"
        "Yaratish: guruhda /newslot 60 matn\n"
        "Yoki xabarga reply qilib: /newslot 60\n"
        "Ro'yxat: /listslots\n"
        "To'xtatish: /stopslot ID\n"
        "Statistika: /slotstats\n"
        "Qo'shimcha slot so'rovi: /buy_slot 1"
    )
    await callback.message.edit_text(text, reply_markup=main_menu(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data == "my_groups")
async def my_groups_cb(callback: CallbackQuery):
    rows = cur.execute(
        "SELECT * FROM groups WHERE owner_admin_id=? AND is_active=1 ORDER BY created_at DESC",
        (callback.from_user.id,),
    ).fetchall()

    if not rows:
        text = "Sizda ulangan guruh yo'q.\n\nBotni guruhga admin qiling va /register_group yuboring."
    else:
        parts = []
        for r in rows[:30]:
            parts.append(
                f"• {r['title']}\n"
                f"ID: <code>{r['chat_id']}</code>\n"
                f"Add: {r['required_invites']} | Ref: {r['required_referrals']}"
            )
        text = "<b>Mening guruhlarim</b>\n\n" + "\n\n".join(parts)

    await callback.message.edit_text(text, reply_markup=main_menu(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data == "guide")
async def guide_cb(callback: CallbackQuery):
    text = (
        "<b>Qollanma</b>\n\n"
        "• /register_group — guruhni ulash\n"
        "• /panel — admin panel\n"
        "• /set @kanal — majburiy kanal\n"
        "• /plus USER_ID — qo'lda +1 add\n"
        "• /debugadd — holatni tekshirish\n"
        "• /welcome matn — admin o'z welcome matnini yozadi\n"
        "• /forced matn — ogohlantirish matni\n"
        "• /dupe 60 / /dupe off — dublikat post cheklovi\n"
        "• /maxlen 500 / /maxlen off — post uzunligi limiti\n"
        "• /newslot 60 matn — 60 daqiqada takroriy xabar\n"
        "• /listslots — slotlar ro'yxati\n"
        "• /mystats — oddiy user statistikasi\n"
        "• /buy_slot 1 — qo'shimcha slot uchun ownerga so'rov"
    )
    await callback.message.edit_text(text, reply_markup=main_menu(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data == "order_ad")
async def order_ad_cb(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdRequestState.waiting_service)
    discount = get_discount_percent(callback.from_user.id)
    await callback.message.edit_text(
        "Qaysi xizmat kerak?\n\n"
        "1) Oddiy pullik e'lon\n"
        "2) VIP/TOP reklama\n"
        "3) Ko'p guruhga tarqatish\n\n"
        f"Sizning hozirgi chegirmangiz: <b>{discount}%</b>\n"
        "Javobni matn qilib yuboring.",
        reply_markup=main_menu(callback.from_user.id),
    )
    await callback.answer()


@dp.message(AdRequestState.waiting_service)
async def ad_request_service(message: Message, state: FSMContext):
    await state.update_data(service=message.text or "Xizmat")
    await state.set_state(AdRequestState.waiting_text)
    await message.answer("Endi reklama matnini yuboring.")


@dp.message(AdRequestState.waiting_text)
async def ad_request_text(message: Message, state: FSMContext):
    data = await state.get_data()
    service = data.get("service", "Xizmat")
    discount = get_discount_percent(message.from_user.id)

    cur.execute(
        """
        INSERT INTO ad_requests (user_id, full_name, username, service_type, text, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'new', ?)
        """,
        (
            message.from_user.id,
            message.from_user.full_name,
            message.from_user.username or "",
            f"{service} | discount {discount}%",
            message.text or "",
            now_str(),
        ),
    )
    conn.commit()

    req_id = cur.lastrowid
    try:
        await bot.send_message(
            OWNER_ID,
            (
                "<b>Yangi reklama so'rovi</b>\n\n"
                f"ID: <code>{req_id}</code>\n"
                f"User: {message.from_user.full_name}\n"
                f"Username: @{message.from_user.username if message.from_user.username else 'yoq'}\n"
                f"Chegirma: {discount}%\n"
                f"Service: {service}\n\n"
                f"Matn:\n{message.text or ''}"
            ),
        )
    except Exception:
        pass

    await state.clear()
    await message.answer("✅ So'rov qabul qilindi.", reply_markup=main_menu(message.from_user.id))


# ============================================================
# OWNER PANEL
# ============================================================
@dp.callback_query(F.data == "owner_panel")
async def owner_panel_cb(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Siz owner emassiz", show_alert=True)

    users_count = cur.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
    groups_count = cur.execute("SELECT COUNT(*) c FROM groups WHERE is_active=1").fetchone()["c"]
    active_slots = cur.execute("SELECT COUNT(*) c FROM recurring_slots WHERE is_active=1").fetchone()["c"]
    text = (
        "<b>OWNER PANEL</b>\n\n"
        f"👥 Users: <b>{users_count}</b>\n"
        f"💬 Groups: <b>{groups_count}</b>\n"
        f"♻️ Active slots: <b>{active_slots}</b>"
    )
    await callback.message.edit_text(text, reply_markup=owner_menu())
    await callback.answer()


@dp.callback_query(F.data == "owner_stats")
async def owner_stats_cb(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Ruxsat yo'q", show_alert=True)

    req_count = cur.execute("SELECT COUNT(*) c FROM ad_requests WHERE status='new'").fetchone()["c"]
    slot_req_count = cur.execute("SELECT COUNT(*) c FROM slot_purchase_requests WHERE status='new'").fetchone()["c"]
    text = (
        "<b>Statistika</b>\n\n"
        f"Yangi reklama so'rovlari: <b>{req_count}</b>\n"
        f"Yangi slot so'rovlari: <b>{slot_req_count}</b>"
    )
    await callback.message.edit_text(text, reply_markup=owner_menu())
    await callback.answer()


@dp.callback_query(F.data == "owner_requests")
async def owner_requests_cb(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Ruxsat yo'q", show_alert=True)

    rows = cur.execute("SELECT * FROM ad_requests ORDER BY id DESC LIMIT 15").fetchall()
    if not rows:
        text = "So'rovlar yo'q"
    else:
        text = "<b>So'rovlar</b>\n\n" + "\n\n".join(
            [f"ID: <code>{r['id']}</code>\n{r['full_name']} | {r['service_type']}" for r in rows]
        )
    await callback.message.edit_text(text, reply_markup=owner_menu())
    await callback.answer()


@dp.callback_query(F.data == "owner_slot_requests")
async def owner_slot_requests_cb(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Ruxsat yo'q", show_alert=True)

    rows = cur.execute("SELECT * FROM slot_purchase_requests ORDER BY id DESC LIMIT 15").fetchall()
    if not rows:
        text = "Slot so'rovlari yo'q"
    else:
        parts = []
        for r in rows:
            parts.append(
                f"ID: <code>{r['id']}</code>\n"
                f"User: {r['full_name']} ({r['user_id']})\n"
                f"Slots: {r['requested_slots']} | Discount: {r['discount_percent']}%\n"
                f"Total: {fmt_money(r['total_price'])} so'm | Status: {r['status']}"
            )
        text = "<b>Slot so'rovlari</b>\n\n" + "\n\n".join(parts)
    await callback.message.edit_text(text, reply_markup=owner_menu())
    await callback.answer()


@dp.callback_query(F.data == "owner_back")
async def owner_back_cb(callback: CallbackQuery):
    await callback.message.edit_text(build_home_text(callback.from_user.id), reply_markup=main_menu(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data.startswith("home_users:"))
async def home_users_toggle_cb(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Faqat owner uchun", show_alert=True)

    action = callback.data.split(":", 1)[1]
    show = action == "show"
    await callback.message.edit_text(
        build_home_text(callback.from_user.id, owner_show_users_count=show),
        reply_markup=main_menu(callback.from_user.id, owner_show_users_count=show),
    )
    await callback.answer()


@dp.callback_query(F.data == "owner_groups")
async def owner_groups_cb(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Ruxsat yo'q", show_alert=True)
    search_query = get_setting("owner_groups_search", "")
    await callback.message.edit_text(
        owner_groups_page_text(0, search_query=search_query),
        reply_markup=owner_groups_nav_kb(0, search_query=search_query),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("og:"))
async def owner_groups_nav_cb(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Ruxsat yo'q", show_alert=True)

    action = callback.data.split(":", 1)[1]
    if action == "search":
        await state.set_state(OwnerGroupsSearchState.waiting_query)
        await callback.message.edit_text(
            "<b>Chat qidirish</b>\n\n"
            "Endi chat nomidan bir parcha yuboring.\n"
            "Masalan: <code>rent</code> yoki <code>Toshkent</code>.\n\n"
            "Qaytish uchun: /start yoki OWNER PANEL.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Owner panel", callback_data="owner_panel")]]
            ),
        )
        return await callback.answer()

    if action == "clear":
        set_setting("owner_groups_search", "")
        await callback.message.edit_text(owner_groups_page_text(0), reply_markup=owner_groups_nav_kb(0))
        return await callback.answer("Qidiruv tozalandi")

    try:
        page = int(action)
    except Exception:
        page = 0

    search_query = get_setting("owner_groups_search", "")
    await callback.message.edit_text(
        owner_groups_page_text(page, search_query=search_query),
        reply_markup=owner_groups_nav_kb(page, search_query=search_query),
    )
    await callback.answer()


@dp.message(OwnerGroupsSearchState.waiting_query)
async def owner_groups_search_message(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID or message.chat.type != ChatType.PRIVATE:
        return

    query = (message.text or "").strip()
    if query == "0":
        query = ""

    set_setting("owner_groups_search", query)
    await state.clear()
    await message.answer(
        owner_groups_page_text(0, search_query=query),
        reply_markup=owner_groups_nav_kb(0, search_query=query),
    )


@dp.callback_query(F.data == "owner_broadcast")
async def owner_broadcast_cb(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Ruxsat yo'q", show_alert=True)
    await state.clear()
    await state.set_state(OwnerBroadcastState.waiting_message)
    await callback.message.edit_text(
        "<b>Owner e'loni</b>\n\n"
        "Endi private chatga yuboriladigan e'lonni yuboring.\n"
        "Matn, rasm+caption, video yoki boshqa xabar turi ham bo'lishi mumkin.\n\n"
        "Bot shu xabarni tanlangan chatlarga nusxa qilib yuboradi.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Owner panel", callback_data="owner_panel")]]
        ),
    )
    await callback.answer()


@dp.message(OwnerBroadcastState.waiting_message)
async def owner_broadcast_waiting_message(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID or message.chat.type != ChatType.PRIVATE:
        return

    rows = get_all_active_groups()
    if not rows:
        await state.clear()
        return await message.answer("Aktiv chatlar topilmadi.", reply_markup=owner_menu())

    await state.update_data(
        owner_broadcast_source_chat_id=message.chat.id,
        owner_broadcast_source_message_id=message.message_id,
        owner_broadcast_preview=owner_broadcast_preview_label(message),
        owner_broadcast_selected_ids=[],
        owner_broadcast_page=0,
        owner_broadcast_search="",
    )
    await state.set_state(OwnerBroadcastState.selecting_groups)
    menu = await message.answer("Tanlash menyusi yuklanmoqda...")
    await render_owner_broadcast_menu(menu, state, page=0)


@dp.message(OwnerBroadcastState.waiting_search)
async def owner_broadcast_search_message(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID or message.chat.type != ChatType.PRIVATE:
        return

    query = (message.text or "").strip()
    if query == "0":
        query = ""

    await state.update_data(owner_broadcast_search=query)
    await state.set_state(OwnerBroadcastState.selecting_groups)
    menu = await message.answer("Qidiruv natijalari yuklanmoqda...")
    await render_owner_broadcast_menu(menu, state, page=0, notice="🔎 Qidiruv yangilandi." if query else "🧹 Qidiruv tozalandi.")


@dp.callback_query(F.data.startswith("ob:"))
async def owner_broadcast_actions_cb(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Ruxsat yo'q", show_alert=True)

    data = await state.get_data()
    if not data.get("owner_broadcast_source_message_id"):
        await state.clear()
        await callback.message.edit_text("Owner e'loni bekor qilindi yoki vaqti tugadi.", reply_markup=owner_menu())
        return await callback.answer()

    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    selected = [int(x) for x in data.get("owner_broadcast_selected_ids", [])]
    page = int(data.get("owner_broadcast_page", 0) or 0)
    search_query = (data.get("owner_broadcast_search", "") or "").strip()
    all_rows = get_all_active_groups()
    filtered_rows = get_all_active_groups(search_query)
    page_size = 8

    if action == "noop":
        return await callback.answer()

    if action == "back":
        await state.clear()
        await callback.message.edit_text("Owner panel", reply_markup=owner_menu())
        return await callback.answer()

    if action == "search":
        await state.set_state(OwnerBroadcastState.waiting_search)
        await callback.message.edit_text(
            "<b>Owner e'loni uchun chat qidirish</b>\n\n"
            "Chat nomidan bir parcha yuboring.\n"
            "Masalan: <code>rent</code> yoki <code>Toshkent</code>.\n"
            "Qidiruvni o'chirish uchun: <code>0</code>",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Owner panel", callback_data="owner_panel")]]
            ),
        )
        return await callback.answer()

    if action == "clearsearch":
        await state.update_data(owner_broadcast_search="")
        await render_owner_broadcast_menu(callback.message, state, page=0, notice="🧹 Qidiruv tozalandi.")
        return await callback.answer("Qidiruv tozalandi")

    if action == "p":
        try:
            page = int(parts[2])
        except Exception:
            page = 0
        await render_owner_broadcast_menu(callback.message, state, page=page)
        return await callback.answer()

    if action == "t":
        try:
            chat_id = int(parts[2])
            page = int(parts[3])
        except Exception:
            return await callback.answer("Xato", show_alert=True)
        if chat_id in selected:
            selected.remove(chat_id)
        else:
            selected.append(chat_id)
        await state.update_data(owner_broadcast_selected_ids=selected)
        await render_owner_broadcast_menu(callback.message, state, page=page)
        return await callback.answer("Belgi yangilandi")

    if action == "sp":
        try:
            page = int(parts[2])
        except Exception:
            page = 0
        start = page * page_size
        subset_ids = [int(r["chat_id"]) for r in filtered_rows[start : start + page_size]]
        selected_set = set(selected)
        for chat_id in subset_ids:
            selected_set.add(chat_id)
        selected = sorted(selected_set)
        await state.update_data(owner_broadcast_selected_ids=selected)
        await render_owner_broadcast_menu(callback.message, state, page=page, notice="✅ Sahifadagi chatlar belgilandi.")
        return await callback.answer("Sahifa belgilandi")

    if action == "cl":
        await state.update_data(owner_broadcast_selected_ids=[])
        await render_owner_broadcast_menu(callback.message, state, page=0, notice="🧹 Tanlov tozalandi.")
        return await callback.answer("Tozalandi")

    source_chat_id = int(data["owner_broadcast_source_chat_id"])
    source_message_id = int(data["owner_broadcast_source_message_id"])

    if action == "all":
        target_chat_ids = [int(r["chat_id"]) for r in all_rows]
        await callback.message.edit_text("⏳ Owner e'loni barcha chatlarga yuborilmoqda...")
        success, failed, failed_titles = await send_owner_broadcast(source_chat_id, source_message_id, target_chat_ids)
        await state.clear()
        result_text = (
            "<b>Owner e'loni yuborildi</b>\n\n"
            f"Rejim: <b>Hammasiga</b>\n"
            f"✅ Yuborildi: <b>{success}</b>\n"
            f"❌ Xato: <b>{failed}</b>"
        )
        if failed_titles:
            result_text += "\n\n" + "\n".join(failed_titles)
        await callback.message.edit_text(result_text, reply_markup=owner_menu())
        return await callback.answer("Yuborildi")

    if action == "send":
        if not selected:
            return await callback.answer("Avval kamida 1 ta chatni tanlang", show_alert=True)
        await callback.message.edit_text("⏳ Owner e'loni tanlangan chatlarga yuborilmoqda...")
        success, failed, failed_titles = await send_owner_broadcast(source_chat_id, source_message_id, selected)
        await state.clear()
        result_text = (
            "<b>Owner e'loni yuborildi</b>\n\n"
            f"Rejim: <b>Tanlangan chatlar</b>\n"
            f"✅ Yuborildi: <b>{success}</b>\n"
            f"❌ Xato: <b>{failed}</b>"
        )
        if failed_titles:
            result_text += "\n\n" + "\n".join(failed_titles)
        await callback.message.edit_text(result_text, reply_markup=owner_menu())
        return await callback.answer("Yuborildi")

    await callback.answer()


# ============================================================
# GROUP COMMANDS
# ============================================================
@dp.message(Command("register_group"))
async def register_group_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await message.answer("Faqat guruh admini ro'yxatdan o'tkaza oladi")

    uid = message.from_user.id
    add_or_update_user(uid, message.from_user.full_name, message.from_user.username or "")
    add_group(message.chat.id, message.chat.title or str(message.chat.id), uid)

    sent = await message.answer("✅ Guruh ro'yxatdan o'tdi", reply_markup=group_panel_kb(message.chat.id))
    await safe_delete(message.chat.id, message.message_id)
    schedule_delete(sent.chat.id, sent.message_id, 45)


@dp.message(Command("panel"))
async def panel_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await message.answer("Faqat admin ishlata oladi")

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    dupe_minutes = int(group['duplicate_window_minutes'] or 0)
    max_len = int(group['max_post_len'] or 0)

    text = (
        "<b>🛠 Admin panel</b>\n\n"
        f"Guruh: {message.chat.title}\n"
        f"Add talabi: <b>{int(group['required_invites'] or 0)}</b>\n"
        f"Referral talabi: <b>{int(group['required_referrals'] or 0)}</b>\n"
        f"Majburiy kanal: <b>{group['forced_channel'] or 'yoq'}</b>\n"
        f"Welcome: <b>{'ON' if int(group['welcome_enabled'] or 0) else 'OFF'}</b>\n"
        f"Anti-link: <b>{'ON' if int(group['anti_link_enabled'] or 0) else 'OFF'}</b>\n"
        f"Whitelist: <b>{len(get_group_allow_domains(message.chat.id))}</b>\n"
        f"Dupe: <b>{'OFF' if dupe_minutes <= 0 else f'ON {dupe_minutes} min'}</b>\n"
        f"Maxlen: <b>{'OFF' if max_len <= 0 else f'ON {max_len}'}</b>"
    )
    sent = await message.answer(text, reply_markup=group_panel_kb(message.chat.id))
    await safe_delete(message.chat.id, message.message_id)
    schedule_delete(sent.chat.id, sent.message_id, 60)


@dp.callback_query(F.data.startswith("gp:"))
async def group_panel_callback(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    action = parts[1]

    if action in ["add", "ref"]:
        value = parts[2]
        chat_id = int(parts[3])
    else:
        chat_id = int(parts[2])

    if not await callback_group_admin_ok(callback, chat_id):
        return await callback.answer("Faqat admin uchun", show_alert=True)

    group = get_group(chat_id)
    if not group:
        return await callback.answer("Avval /register_group qiling", show_alert=True)

    if action == "add":
        new_val = 0 if value == "off" else int(value)
        cur.execute("UPDATE groups SET required_invites=? WHERE chat_id=?", (new_val, chat_id))
        conn.commit()
        await callback.answer("Add yangilandi")

    elif action == "ref":
        cur.execute("UPDATE groups SET required_referrals=? WHERE chat_id=?", (int(value), chat_id))
        conn.commit()
        await callback.answer("Referral yangilandi")

    elif action == "welcome_toggle":
        new_val = 0 if int(group["welcome_enabled"] or 0) == 1 else 1
        cur.execute("UPDATE groups SET welcome_enabled=? WHERE chat_id=?", (new_val, chat_id))
        conn.commit()
        await callback.answer("Welcome almashtirildi")

    elif action == "anti_link":
        new_val = 0 if int(group["anti_link_enabled"] or 0) == 1 else 1
        cur.execute("UPDATE groups SET anti_link_enabled=? WHERE chat_id=?", (new_val, chat_id))
        conn.commit()
        await callback.answer("Anti-link almashtirildi")

    elif action == "welcome_text":
        await state.set_state(WelcomeTextState.waiting_text)
        await state.update_data(welcome_chat_id=chat_id)
        await callback.message.edit_text(
            "Yangi welcome matnini yuboring.\n\n{name} va {group} ishlaydi."
        )
        await callback.answer()
        return

    elif action == "forced_text":
        await state.set_state(ForcedTextState.waiting_text)
        await state.update_data(forced_chat_id=chat_id)
        await callback.message.edit_text(
            "Yangi ogohlantirish matnini yuboring.\n\n0 yuborsangiz, matn o'chadi."
        )
        await callback.answer()
        return

    elif action == "debug":
        dupe_minutes = int(group['duplicate_window_minutes'] or 0)
        max_len = int(group['max_post_len'] or 0)
        await callback.message.edit_text(
            (
                "<b>Debug</b>\n\n"
                f"Add: <b>{group['required_invites']}</b>\n"
                f"Ref: <b>{group['required_referrals']}</b>\n"
                f"Welcome: <b>{'ON' if int(group['welcome_enabled'] or 0) else 'OFF'}</b>\n"
                f"Anti-link: <b>{'ON' if int(group['anti_link_enabled'] or 0) else 'OFF'}</b>\n"
                f"Whitelist: <b>{len(get_group_allow_domains(chat_id))}</b>\n"
                f"Dupe: <b>{'OFF' if dupe_minutes <= 0 else f'ON {dupe_minutes} min'}</b>\n"
                f"Maxlen: <b>{'OFF' if max_len <= 0 else f'ON {max_len}'}</b>\n"
                f"Forced channel: <b>{group['forced_channel'] or 'yoq'}</b>"
            ),
            reply_markup=group_panel_kb(chat_id),
        )
        await callback.answer()
        return

    elif action == "help":
        await callback.message.edit_text(
            "Add/Ref, /add 5, /ref 5, Welcome, Anti-link, /linkguard, /allowlink, Forced text, /plus USER_ID, /debugadd, /set @kanal, /welcome, /forced, /dupe, /maxlen, /newslot, /listslots, /stopslot",
            reply_markup=group_panel_kb(chat_id),
        )
        await callback.answer()
        return

    group = get_group(chat_id)
    dupe_minutes = int(group['duplicate_window_minutes'] or 0)
    max_len = int(group['max_post_len'] or 0)
    await callback.message.edit_text(
        (
            "<b>🛠 Admin panel</b>\n\n"
            f"Add: <b>{group['required_invites']}</b>\n"
            f"Ref: <b>{group['required_referrals']}</b>\n"
            f"Welcome: <b>{'ON' if int(group['welcome_enabled'] or 0) else 'OFF'}</b>\n"
            f"Anti-link: <b>{'ON' if int(group['anti_link_enabled'] or 0) else 'OFF'}</b>\n"
            f"Whitelist: <b>{len(get_group_allow_domains(chat_id))}</b>\n"
            f"Dupe: <b>{'OFF' if dupe_minutes <= 0 else f'ON {dupe_minutes} min'}</b>\n"
            f"Maxlen: <b>{'OFF' if max_len <= 0 else f'ON {max_len}'}</b>"
        ),
        reply_markup=group_panel_kb(chat_id),
    )
    await callback.answer()


@dp.message(WelcomeTextState.waiting_text)
async def save_welcome_text(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get("welcome_chat_id")
    if message.chat.id != chat_id:
        return await reply_temp(message, "Matnni aynan shu guruhga yuboring", 8)
    cur.execute("UPDATE groups SET welcome_text=? WHERE chat_id=?", (message.text or "", chat_id))
    conn.commit()
    await state.clear()
    sent = await message.answer("✅ Welcome matni saqlandi", reply_markup=group_panel_kb(chat_id))
    await safe_delete(message.chat.id, message.message_id)
    schedule_delete(sent.chat.id, sent.message_id, 20)


@dp.message(ForcedTextState.waiting_text)
async def save_forced_text(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get("forced_chat_id")
    if message.chat.id != chat_id:
        return await reply_temp(message, "Matnni aynan shu guruhga yuboring", 8)
    val = "" if (message.text or "").strip() == "0" else (message.text or "")
    cur.execute("UPDATE groups SET forced_text=? WHERE chat_id=?", (val, chat_id))
    conn.commit()
    await state.clear()
    sent = await message.answer("✅ Ogohlantirish matni saqlandi", reply_markup=group_panel_kb(chat_id))
    await safe_delete(message.chat.id, message.message_id)
    schedule_delete(sent.chat.id, sent.message_id, 20)


@dp.message(Command("welcome"))
async def welcome_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        current = group["welcome_text"] or DEFAULT_WELCOME_TEXT
        return await reply_temp(
            message,
            "Hozirgi welcome matn:\n\n"
            f"{current}\n\n"
            "Yangi matn uchun: /welcome Sizning matningiz\n"
            "{name} va {group} ishlaydi.",
            25,
        )

    new_text = parts[1].strip()
    cur.execute("UPDATE groups SET welcome_text=? WHERE chat_id=?", (new_text, message.chat.id))
    conn.commit()
    await reply_temp(message, "✅ Welcome matni yangilandi", 8)


@dp.message(Command("forced"))
async def forced_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        current = group["forced_text"] or "yoq"
        return await reply_temp(
            message,
            f"Hozirgi forced text:\n\n{current}\n\nYangi matn: /forced Sizning matningiz\nO'chirish: /forced 0",
            25,
        )

    new_text = parts[1].strip()
    new_text = "" if new_text == "0" else new_text
    cur.execute("UPDATE groups SET forced_text=? WHERE chat_id=?", (new_text, message.chat.id))
    conn.commit()
    await reply_temp(message, "✅ Forced text yangilandi", 8)


@dp.message(Command("mystats"))
async def mystats_cmd(message: Message):
    user = get_user(message.from_user.id)
    refs = int(user["referrals_count"] or 0) if user else 0
    second = get_second_level_referrals_count(message.from_user.id)
    discount = get_discount_percent(message.from_user.id)
    free_slots = get_free_slot_quota(message.from_user.id)
    paid_slots = get_paid_slot_quota(message.from_user.id)

    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        add_count, refs_group, need_add, need_ref = user_add_ref_status(message.chat.id, message.from_user.id)
        text = (
            "<b>Mening holatim</b>\n\n"
            f"👥 Add: <b>{add_count}</b> / {need_add}\n"
            f"🔗 Ref: <b>{refs_group}</b> / {need_ref}\n"
            f"🪜 2-daraja ref: <b>{second}</b>\n"
            f"💸 Chegirma: <b>{discount}%</b>\n"
            f"♻️ Bepul slotlar: <b>{free_slots}</b>\n"
            f"💳 Pullik slotlar: <b>{paid_slots}</b>"
        )
        return await reply_temp(message, text, 15)

    text = (
        "<b>Mening kabinet statistikam</b>\n\n"
        f"🔗 1-daraja referral: <b>{refs}</b>\n"
        f"🪜 2-daraja referral: <b>{second}</b>\n"
        f"💸 Chegirma: <b>{discount}%</b>\n"
        f"🎁 Bepul slotlar: <b>{free_slots}</b>\n"
        f"💳 Pullik slotlar: <b>{paid_slots}</b>"
    )
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@dp.message(F.text.regexp(r"(?i)^/add(?:@\w+)?(?:\s+.+)?$"))
async def add_value_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        current = int(group["required_invites"] or 0)
        return await reply_temp(
            message,
            "Add limitni qo'lda sozlash\n\n"
            f"Hozirgi qiymat: <b>{current}</b>\n"
            "Misol: /add 5\n"
            "O'chirish: /add off",
            20,
        )

    raw = (parts[1] or "").strip().lower()
    if raw == "off":
        value = 0
    else:
        try:
            value = int(raw)
        except Exception:
            return await reply_temp(message, "To'g'ri format: /add 5 yoki /add off", 10)
        if value < 0:
            return await reply_temp(message, "Qiymat 0 dan kichik bo'lmasin", 10)
        if value > 1000:
            return await reply_temp(message, "Juda katta qiymat. Maksimum: 1000", 10)

    cur.execute("UPDATE groups SET required_invites=? WHERE chat_id=?", (value, message.chat.id))
    conn.commit()
    if value == 0:
        await reply_temp(message, "✅ Add talabi o'chirildi", 8)
    else:
        await reply_temp(message, f"✅ Add talabi saqlandi: <b>{value}</b>", 8)


@dp.message(F.text.regexp(r"(?i)^/ref(?:@\w+)?(?:\s+.+)?$"))
async def ref_value_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        current = int(group["required_referrals"] or 0)
        return await reply_temp(
            message,
            "Referral limitni qo'lda sozlash\n\n"
            f"Hozirgi qiymat: <b>{current}</b>\n"
            "Misol: /ref 5\n"
            "O'chirish: /ref off",
            20,
        )

    raw = (parts[1] or "").strip().lower()
    if raw == "off":
        value = 0
    else:
        try:
            value = int(raw)
        except Exception:
            return await reply_temp(message, "To'g'ri format: /ref 5 yoki /ref off", 10)
        if value < 0:
            return await reply_temp(message, "Qiymat 0 dan kichik bo'lmasin", 10)
        if value > 1000:
            return await reply_temp(message, "Juda katta qiymat. Maksimum: 1000", 10)

    cur.execute("UPDATE groups SET required_referrals=? WHERE chat_id=?", (value, message.chat.id))
    conn.commit()
    if value == 0:
        await reply_temp(message, "✅ Referral talabi o'chirildi", 8)
    else:
        await reply_temp(message, f"✅ Referral talabi saqlandi: <b>{value}</b>", 8)


@dp.message(Command("plus"))
async def plus_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    target_user_id = extract_target_user_id(message)
    if not target_user_id:
        return await reply_temp(message, "Reply qiling yoki /plus USER_ID yuboring", 8)

    try:
        member = await bot.get_chat_member(message.chat.id, target_user_id)
        target_user = member.user
        add_or_update_user(target_user.id, target_user.full_name, target_user.username or "")
        target_name = target_user.full_name
    except Exception:
        target_name = str(target_user_id)
        if not get_user(target_user_id):
            add_or_update_user(target_user_id, f"User {target_user_id}", "")

    manual_fake_id = int(f"{target_user_id}{now_dt().strftime('%S%f')[:4]}")
    added = add_manual_member_credit(message.chat.id, target_user_id, manual_fake_id)
    total = count_user_added_members(message.chat.id, target_user_id)

    if added:
        await reply_temp(message, f"✅ +1 add berildi. Jami: <b>{total}</b>\nUser: <b>{target_name}</b>", 10)
    else:
        await reply_temp(message, f"ℹ️ Oldin berilgan add. Jami: <b>{total}</b>\nUser: <b>{target_name}</b>", 10)


@dp.message(Command("debugadd"))
async def debugadd_cmd(message: Message):
    target_user_id = extract_target_user_id(message) or message.from_user.id
    target_name = str(target_user_id)

    try:
        member = await bot.get_chat_member(message.chat.id, target_user_id)
        target_name = member.user.full_name
    except Exception:
        pass

    await reply_temp(message, f"<b>{target_name}</b>\n" + access_debug_text(message.chat.id, target_user_id), 20)


@dp.message(Command("set"))
async def set_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        current = group["forced_channel"] or "yoq"
        return await reply_temp(message, f"Hozirgi kanal: <b>{current}</b>\nMisol: /set @kanal\nO'chirish: /set 0", 20)

    value = parts[1].strip()
    value = "" if value == "0" else value
    cur.execute("UPDATE groups SET forced_channel=? WHERE chat_id=?", (value, message.chat.id))
    conn.commit()
    await reply_temp(message, "✅ Kanal saqlandi", 8)






@dp.message(Command("linkguard"))
async def linkguard_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)
    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        status = "ON" if int(group["anti_link_enabled"] or 0) == 1 else "OFF"
        allow_count = len(get_group_allow_domains(message.chat.id))
        return await reply_temp(message, f"Linkguard: <b>{status}</b>\nWhitelist: <b>{allow_count}</b> ta\n\nMisol: /linkguard on yoki /linkguard off", 14)

    arg = parts[1].strip().lower()
    if arg in {"on", "1", "enable"}:
        cur.execute("UPDATE groups SET anti_link_enabled=1 WHERE chat_id=?", (message.chat.id,))
        conn.commit()
        return await reply_temp(message, "✅ Linkguard yoqildi", 8)
    if arg in {"off", "0", "disable"}:
        cur.execute("UPDATE groups SET anti_link_enabled=0 WHERE chat_id=?", (message.chat.id,))
        conn.commit()
        return await reply_temp(message, "✅ Linkguard o'chirildi", 8)

    await reply_temp(message, "Misol: /linkguard on yoki /linkguard off", 10)


@dp.message(Command("allowlink"))
async def allowlink_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)
    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) == 1:
        return await reply_temp(message, "Misollar:\n/allowlink list\n/allowlink add olx.uz\n/allowlink del olx.uz\n/allowlink clear", 18)

    action = parts[1].strip().lower()
    if action == "list":
        domains = sorted(get_group_allow_domains(message.chat.id))
        if not domains:
            return await reply_temp(message, "Whitelist bo'sh", 10)
        return await reply_temp(message, "Whitelist:\n" + "\n".join(f"• {d}" for d in domains[:100]), 20)

    if action == "clear":
        clear_group_allow_domains(message.chat.id)
        return await reply_temp(message, "✅ Whitelist tozalandi", 8)

    if len(parts) < 3:
        return await reply_temp(message, "Domain yozing. Masalan: /allowlink add olx.uz", 10)

    domain = normalize_domain(parts[2])
    if not domain:
        return await reply_temp(message, "Domain noto'g'ri. Masalan: olx.uz", 10)

    if action == "add":
        saved = add_group_allow_domain(message.chat.id, domain)
        return await reply_temp(message, f"✅ Ruxsat berildi: <b>{saved}</b>", 8)
    if action in {"del", "delete", "remove"}:
        deleted = delete_group_allow_domain(message.chat.id, domain)
        return await reply_temp(message, f"✅ O'chirildi: <b>{deleted}</b>", 8)

    await reply_temp(message, "Misollar:\n/allowlink list\n/allowlink add olx.uz\n/allowlink del olx.uz\n/allowlink clear", 18)


@dp.message(Command("dupe"))
async def dupe_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        current = int(group["duplicate_window_minutes"] or 0)
        current_text = f"{current} daqiqa" if current > 0 else "off"
        return await reply_temp(
            message,
            "Dublikat post cheklovi.\n\n"
            f"Hozirgi holat: <b>{current_text}</b>\n"
            "Yoqish: /dupe 60\n"
            "O'chirish: /dupe off\n\n"
            "Bir xil yoki juda o'xshash post shu vaqt ichida qayta yuborilsa, bot uni o'chiradi.",
            25,
        )

    value = parts[1].strip().lower()
    if value in {"off", "0", "yoq", "yo'q"}:
        minutes = 0
    else:
        try:
            minutes = int(value)
        except Exception:
            return await reply_temp(message, "To'g'ri format: /dupe 60 yoki /dupe off", 10)
        if minutes < 0:
            return await reply_temp(message, "Daqiqa 0 dan kichik bo'lmasin", 10)
        if minutes > 10080:
            return await reply_temp(message, "Juda katta qiymat. Maksimum: 10080 daqiqa", 10)

    cur.execute("UPDATE groups SET duplicate_window_minutes=? WHERE chat_id=?", (minutes, message.chat.id))
    conn.commit()

    if minutes == 0:
        await reply_temp(message, "✅ Dublikat post cheklovi o'chirildi", 8)
    else:
        await reply_temp(message, f"✅ Dublikat post cheklovi saqlandi: <b>{minutes}</b> daqiqa", 8)


@dp.message(Command("maxlen"))
async def maxlen_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        current = int(group["max_post_len"] or 0)
        current_text = str(current) if current > 0 else "off"
        return await reply_temp(
            message,
            "Post uzunligi limiti.\n\n"
            f"Hozirgi limit: <b>{current_text}</b>\n"
            "Yoqish: /maxlen 500\n"
            "O'chirish: /maxlen off\n\n"
            "Agar post matni yoki rasm/video caption shu limitdan uzun bo'lsa, bot postni o'chiradi.",
            25,
        )

    value = parts[1].strip().lower()
    if value in {"off", "0", "yoq", "yo'q"}:
        limit = 0
    else:
        try:
            limit = int(value)
        except Exception:
            return await reply_temp(message, "To'g'ri format: /maxlen 500 yoki /maxlen off", 10)
        if limit < 0:
            return await reply_temp(message, "Limit 0 dan kichik bo'lmasin", 10)
        if limit > 4096:
            return await reply_temp(message, "Juda katta limit. Maksimum: 4096", 10)

    cur.execute("UPDATE groups SET max_post_len=? WHERE chat_id=?", (limit, message.chat.id))
    conn.commit()

    if limit == 0:
        await reply_temp(message, "✅ Post uzunligi limiti o'chirildi", 8)
    else:
        await reply_temp(message, f"✅ Post uzunligi limiti saqlandi: <b>{limit}</b> belgi", 8)


@dp.message(Command("newslot"))
async def newslot_cmd(message: Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await message.answer("Bu buyruq guruh ichida ishlaydi")
    if not await has_group_admin_rights(message):
        return await reply_temp(message, "Faqat admin ishlata oladi", 8)

    group = get_group(message.chat.id)
    if not group:
        return await reply_temp(message, "Avval /register_group qiling", 8)

    interval_minutes, slot_text = parse_slot_command_payload(message)
    if interval_minutes is None or not slot_text:
        return await reply_temp(
            message,
            "Foydalanish:\n"
            "/newslot 60 Bu xabar har 60 daqiqada takrorlanadi\n"
            "yoki xabarga reply qilib: /newslot 60",
            20,
        )

    if interval_minutes < 5:
        return await reply_temp(message, "Interval kamida 5 daqiqa bo'lishi kerak.", 10)

    allowed, active, quota = user_can_create_slot(message.from_user.id)
    if not allowed:
        unit_price, discount, total = calc_slot_price(message.from_user.id, 1)
        req_id = create_slot_purchase_request(message.from_user.id, 1)
        owner_contact = get_setting("owner_contact", "@yourusername")
        try:
            await bot.send_message(
                OWNER_ID,
                (
                    "<b>Yangi slot so'rovi</b>\n\n"
                    f"Request ID: <code>{req_id}</code>\n"
                    f"User ID: <code>{message.from_user.id}</code>\n"
                    f"Ism: {message.from_user.full_name}\n"
                    f"Username: @{message.from_user.username if message.from_user.username else 'yoq'}\n"
                    f"Base price: {fmt_money(unit_price)} so'm\n"
                    f"Discount: {discount}%\n"
                    f"To'lov: {fmt_money(total)} so'm\n\n"
                    f"To'lovdan keyin owner buyruq beradi:\n/grantslot {message.from_user.id} 1"
                ),
            )
        except Exception:
            pass
        return await reply_temp(
            message,
            (
                "♻️ Hozircha bo'sh slot qolmagan.\n"
                f"Faol slotlar: <b>{active}</b> / <b>{quota}</b>\n"
                f"Siz uchun qo'shimcha slot narxi: <b>{fmt_money(total)} so'm</b>\n"
                f"Chegirma: <b>{discount}%</b>\n"
                f"Owner bilan aloqa: {owner_contact}\n"
                "So'rov ownerga yuborildi."
            ),
            20,
        )

    slot_id = create_recurring_slot(message.from_user.id, message.chat.id, slot_text, interval_minutes)
    await reply_temp(
        message,
        (
            f"✅ Slot yaratildi. ID: <code>{slot_id}</code>\n"
            f"Interval: <b>{interval_minutes}</b> daqiqa\n"
            f"Guruh: <b>{message.chat.title}</b>"
        ),
        12,
    )


@dp.message(Command("listslots"))
async def listslots_cmd(message: Message):
    owner_id = message.from_user.id
    rows = cur.execute(
        """
        SELECT * FROM recurring_slots
        WHERE owner_user_id=? AND is_active=1
        ORDER BY id DESC
        LIMIT 20
        """,
        (owner_id,),
    ).fetchall()
    if not rows:
        return await reply_temp(message, "Aktiv slotlar yo'q.", 12) if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP] else await message.answer("Aktiv slotlar yo'q.", reply_markup=main_menu(owner_id))

    parts = []
    for r in rows:
        preview = (r["message_text"] or "").replace("\n", " ")
        if len(preview) > 45:
            preview = preview[:45] + "..."
        parts.append(
            f"ID <code>{r['id']}</code> | chat <code>{r['chat_id']}</code>\n"
            f"Har {r['interval_minutes']} daq | {'PULLIK' if int(r['is_paid'] or 0) else 'BEPUL'}\n"
            f"{preview}"
        )
    text = "<b>Aktiv slotlar</b>\n\n" + "\n\n".join(parts)
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await reply_temp(message, text, 25)
    await message.answer(text, reply_markup=main_menu(owner_id))


@dp.message(Command("stopslot"))
async def stopslot_cmd(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1 or not parts[1].strip().isdigit():
        return await reply_temp(message, "Misol: /stopslot 12", 8) if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP] else await message.answer("Misol: /stopslot 12")
    slot_id = int(parts[1].strip())
    ok = stop_slot(slot_id, message.from_user.id)
    if ok:
        if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
            return await reply_temp(message, f"🛑 Slot to'xtatildi: <code>{slot_id}</code>", 8)
        return await message.answer(f"🛑 Slot to'xtatildi: {slot_id}", reply_markup=main_menu(message.from_user.id))
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await reply_temp(message, "Bunday aktiv slot topilmadi.", 8)
    await message.answer("Bunday aktiv slot topilmadi.", reply_markup=main_menu(message.from_user.id))


@dp.message(Command("slotstats"))
async def slotstats_cmd(message: Message):
    active = count_active_slots(message.from_user.id)
    free_slots = get_free_slot_quota(message.from_user.id)
    paid_slots = get_paid_slot_quota(message.from_user.id)
    discount = get_discount_percent(message.from_user.id)
    second = get_second_level_referrals_count(message.from_user.id)
    text = (
        "<b>Slot statistikasi</b>\n\n"
        f"Faol: <b>{active}</b>\n"
        f"Bepul: <b>{free_slots}</b>\n"
        f"Pullik: <b>{paid_slots}</b>\n"
        f"2-daraja ref bonus: <b>{'bor' if second >= 1 else 'yoq'}</b>\n"
        f"Chegirma: <b>{discount}%</b>"
    )
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await reply_temp(message, text, 15)
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@dp.message(Command("buy_slot"))
async def buy_slot_cmd(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    count = 1
    if len(parts) > 1 and parts[1].strip().isdigit():
        count = max(1, int(parts[1].strip()))
    add_or_update_user(message.from_user.id, message.from_user.full_name, message.from_user.username or "")
    req_id = create_slot_purchase_request(message.from_user.id, count)
    unit_price, discount, total = calc_slot_price(message.from_user.id, count)
    owner_contact = get_setting("owner_contact", "@yourusername")

    try:
        await bot.send_message(
            OWNER_ID,
            (
                "<b>Yangi slot so'rovi</b>\n\n"
                f"Request ID: <code>{req_id}</code>\n"
                f"User ID: <code>{message.from_user.id}</code>\n"
                f"Ism: {message.from_user.full_name}\n"
                f"Username: @{message.from_user.username if message.from_user.username else 'yoq'}\n"
                f"Slots: {count}\n"
                f"Base price: {fmt_money(unit_price)} so'm\n"
                f"Discount: {discount}%\n"
                f"To'lov: {fmt_money(total)} so'm\n\n"
                f"To'lovdan keyin owner buyruq beradi:\n/grantslot {message.from_user.id} {count}"
            ),
        )
    except Exception:
        pass

    text = (
        f"✅ Slot so'rovi yuborildi.\n\n"
        f"Slots: <b>{count}</b>\n"
        f"Base price: <b>{fmt_money(unit_price)} so'm</b>\n"
        f"Chegirma: <b>{discount}%</b>\n"
        f"Jami: <b>{fmt_money(total)} so'm</b>\n"
        f"Aloqa: {owner_contact}"
    )
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await reply_temp(message, text, 20)
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@dp.message(Command("grantslot"))
async def grantslot_cmd(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        return await message.answer("Misol: /grantslot USER_ID 1")
    user_id = int(parts[1])
    count = 1
    if len(parts) >= 3 and parts[2].isdigit():
        count = max(1, int(parts[2]))
    if not get_user(user_id):
        add_or_update_user(user_id, f"User {user_id}", "")
    grant_paid_slots(user_id, count)
    cur.execute(
        "UPDATE slot_purchase_requests SET status='approved' WHERE user_id=? AND status='new'",
        (user_id,),
    )
    conn.commit()
    await message.answer(f"✅ User {user_id} ga {count} ta pullik slot berildi.")
    try:
        await bot.send_message(
            user_id,
            f"✅ Sizga owner tomonidan {count} ta pullik slot berildi.\nEndi /newslot bilan slot yaratishingiz mumkin."
        )
    except Exception:
        pass


# ============================================================
# NEW MEMBERS / WELCOME
# ============================================================
@dp.message(F.new_chat_members)
async def track_new_members(message: Message):
    if not message.from_user:
        return

    await safe_delete(message.chat.id, message.message_id)

    inviter = message.from_user
    add_or_update_user(inviter.id, inviter.full_name, inviter.username or "")
    group = get_group(message.chat.id)

    if group:
        old_id = int(group["last_welcome_message_id"] or 0)
        if old_id:
            try:
                await bot.delete_message(message.chat.id, old_id)
            except Exception:
                pass

    welcome_msg_id = 0

    for new_member in message.new_chat_members:
        add_or_update_user(new_member.id, new_member.full_name, new_member.username or "")

        if not new_member.is_bot and inviter.id != new_member.id:
            add_member_credit(message.chat.id, inviter.id, new_member.id)

        if group and int(group["welcome_enabled"] or 0) == 1:
            text = render_welcome_text(
                group["welcome_text"] or DEFAULT_WELCOME_TEXT,
                new_member.full_name,
                message.chat.title or "Guruh",
            )
            try:
                sent = await bot.send_message(message.chat.id, text)
                welcome_msg_id = sent.message_id
            except Exception:
                pass

    conn.commit()

    if welcome_msg_id and group:
        cur.execute("UPDATE groups SET last_welcome_message_id=? WHERE chat_id=?", (welcome_msg_id, message.chat.id))
        conn.commit()


@dp.callback_query(F.data.startswith("check_add:"))
async def check_add_callback(callback: CallbackQuery):
    chat_id = int(callback.data.split(":")[1])
    add_count, refs, need_add, need_ref = user_add_ref_status(chat_id, callback.from_user.id)

    if can_user_post_in_group(chat_id, callback.from_user.id):
        await callback.answer("✅ Ruxsat bor. Endi yozishingiz mumkin", show_alert=True)
        return

    add_left = max(0, need_add - add_count)
    ref_left = max(0, need_ref - refs)
    await callback.answer(
        f"❌ Hali ruxsat yo'q. Qolgan add: {add_left}, qolgan ref: {ref_left}. Admin /plus USER_ID berishi mumkin.",
        show_alert=True,
    )


# ============================================================
# GROUP MODERATION
# ============================================================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def group_moderation(message: Message):
    if message.text and message.text.startswith("/"):
        return
    if message.new_chat_members or message.left_chat_member or not message.from_user:
        return

    group = get_group(message.chat.id)
    if not group or not await is_bot_admin(message.chat.id):
        return

    if await is_real_group_admin(message.chat.id, message.from_user.id) or message.from_user.id == OWNER_ID:
        return

    add_or_update_user(message.from_user.id, message.from_user.full_name, message.from_user.username or "")
    text = message.text or message.caption or ""
    ttl = int(group["forced_text_delete_seconds"] or DEFAULT_FORCE_DELETE_SECONDS)

    if not await check_forced_channel_membership(message.from_user.id, group["forced_channel"] or ""):
        try:
            await message.delete()
        except Exception:
            pass

        warn_text = f"❌ Avval majburiy kanalga a'zo bo'ling: {group['forced_channel'] or 'kanal kiritilmagan'}"
        if group["forced_text"]:
            warn_text += f"\n\n{group['forced_text']}"
        await send_temp_notice(message.chat.id, warn_text, ttl, reply_markup=blocked_post_kb(message.chat.id))
        return

    if int(group["max_post_len"] or 0) > 0 and len(text) > int(group["max_post_len"] or 0):
        try:
            await message.delete()
        except Exception:
            pass
        await send_temp_notice(
            message.chat.id,
            f"❌ Xabar juda uzun. Limit: {int(group['max_post_len'] or 0)} ta belgi.",
            ttl,
        )
        return

    allowed_domains = get_group_allow_domains(message.chat.id)
    if int(group["anti_link_enabled"] or 0) == 1 and has_forbidden_link(text, allowed_domains):
        try:
            await message.delete()
        except Exception:
            pass

        if allowed_domains:
            allow_text = "\nRuxsat berilgan domenlar: " + ", ".join(sorted(allowed_domains)[:8])
        else:
            allow_text = ""
        warn_text = "❌ Chet link, @username yoki tashqi reklama yuborish taqiqlangan." + allow_text
        if group["forced_text"]:
            warn_text += f"\n\n{group['forced_text']}"
        await send_temp_notice(message.chat.id, warn_text, ttl)
        return

    if text and is_duplicate_recent(
        message.chat.id,
        message.from_user.id,
        text,
        int(group["duplicate_window_minutes"] or 0),
    ):
        try:
            await message.delete()
        except Exception:
            pass
        await send_temp_notice(
            message.chat.id,
            f"❌ Bir xil e'lonni {int(group['duplicate_window_minutes'] or 0)} daqiqa ichida qayta yuborib bo'lmaydi.",
            ttl,
        )
        return

    if can_user_post_in_group(message.chat.id, message.from_user.id):
        if text:
            save_fingerprint(message.chat.id, message.from_user.id, text)
        return

    try:
        await message.delete()
    except Exception:
        return
    warn_text = build_access_denied_text(
        message.from_user.full_name,
        message.chat.id,
        message.from_user.id,
    )
    if group["forced_text"]:
        warn_text += f"\n\n{group['forced_text']}"

    await send_temp_notice(
        message.chat.id,
        warn_text,
        ttl,
        reply_markup=blocked_post_kb(message.chat.id),
    )


# ============================================================
# RECURRING SCHEDULER
# ============================================================
async def recurring_scheduler_loop():
    await asyncio.sleep(5)
    while True:
        try:
            rows = cur.execute(
                """
                SELECT * FROM recurring_slots
                WHERE is_active=1 AND next_run_at IS NOT NULL AND next_run_at<=?
                ORDER BY next_run_at ASC
                LIMIT 20
                """,
                (now_str(),),
            ).fetchall()

            for row in rows:
                next_time = now_dt() + timedelta(minutes=int(row["interval_minutes"] or DEFAULT_SLOT_INTERVAL_MINUTES))
                try:
                    await bot.send_message(int(row["chat_id"]), row["message_text"] or "")
                    cur.execute(
                        """
                        UPDATE recurring_slots
                        SET last_run_at=?, next_run_at=?, last_error=''
                        WHERE id=?
                        """,
                        (now_str(), next_time.strftime("%Y-%m-%d %H:%M:%S"), row["id"]),
                    )
                except Exception as e:
                    cur.execute(
                        """
                        UPDATE recurring_slots
                        SET last_error=?, next_run_at=?
                        WHERE id=?
                        """,
                        (str(e)[:500], next_time.strftime("%Y-%m-%d %H:%M:%S"), row["id"]),
                    )
                conn.commit()
        except Exception as e:
            logging.exception(f"scheduler error: {e}")

        await asyncio.sleep(20)


# ============================================================
# OTHER COMMANDS
# ============================================================
@dp.message(Command("id"))
async def id_cmd(message: Message):
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await reply_temp(
            message,
            f"Chat ID: <code>{message.chat.id}</code>\nSizning ID: <code>{message.from_user.id}</code>",
            12,
        )
    await message.answer(
        f"Chat ID: <code>{message.chat.id}</code>\nSizning ID: <code>{message.from_user.id}</code>"
    )


@dp.message(Command("help"))
async def help_cmd(message: Message):
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await reply_temp(
            message,
            "Foydalanuvchi: /mystats, /debugadd, /id\n"
            "Admin: /register_group, /panel, /add, /ref, /set, /linkguard, /allowlink, /welcome, /forced, /dupe, /maxlen, /plus, /newslot, /listslots, /stopslot, /slotstats",
            20,
        )
    await message.answer(
        "Asosiy buyruqlar: /start, /help, /id, /buy_slot, /listslots, /slotstats\n"
        "Guruh admini bo'lsangiz: /register_group, /panel, /add, /ref, /set, /welcome, /forced, /dupe, /maxlen, /plus, /newslot\n"
        "Owner bo'lsangiz: owner panel orqali ulangan chatlar va owner e'loni bo'limidan foydalaning."
    )


@dp.message(Command("adminhelp"))
async def adminhelp_cmd(message: Message):
    text = (
        "Admin buyruqlar:\n"
        "/register_group — guruhni ulash\n"
        "/panel — tugmali panel\n"
        "/set @kanal — majburiy kanal\n"
        "/welcome matn — welcome sozlash\n"
        "/forced matn — ogohlantirish matni\n"
        "/add 5 | /add off — qo'lda add talabi\n"
        "/ref 5 | /ref off — qo'lda referral talabi\n"
        "/dupe 60 | /dupe off — dublikat post cheklovi\n"
        "/maxlen 500 | /maxlen off — post uzunligi limiti\n"
        "/plus USER_ID — qo'lda +1 add\n"
        "/debugadd [USER_ID] — tekshirish\n"
        "/newslot 60 matn — takroriy xabar\n"
        "/listslots — slotlar ro'yxati\n"
        "/stopslot ID — slotni to'xtatish\n"
        "/slotstats — slot holati\n"
        "/buy_slot 1 — qo'shimcha slot so'rovi\n\n"
        "Owner:\n"
        "/grantslot USER_ID 1 — slot berish\n"
        "Owner panel — ulangan chatlar ro'yxati va tanlab yuborish"
    )
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return await reply_temp(message, text, 30)
    await message.answer(text)


# ============================================================
# STARTUP
# ============================================================
async def on_startup():
    global BOT_USERNAME, scheduler_task

    if not BOT_TOKEN or BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("BOT_TOKEN bo'sh. .env yoki fayl ichiga haqiqiy token kiriting.")

    try:
        me = await bot.get_me()
        if BOT_USERNAME == "YOUR_BOT_USERNAME" or not BOT_USERNAME:
            BOT_USERNAME = me.username
    except Exception:
        pass

    try:
        await bot.set_my_commands(
            [
                BotCommand(command="start", description="Botni ishga tushirish"),
                BotCommand(command="help", description="Yordam"),
                BotCommand(command="adminhelp", description="Admin yordam"),
                BotCommand(command="buy_slot", description="Slot sotib olish so'rovi"),
                BotCommand(command="slotstats", description="Slot statistikasi"),
                BotCommand(command="listslots", description="Slotlar ro'yxati"),
                BotCommand(command="id", description="ID"),
            ],
            scope=BotCommandScopeAllPrivateChats(),
        )
        await bot.set_my_commands(
            [
                BotCommand(command="help", description="Yordam"),
                BotCommand(command="mystats", description="Mening holatim"),
                BotCommand(command="debugadd", description="Add/ref tekshiruv"),
                BotCommand(command="add", description="Add talabini sozlash"),
                BotCommand(command="ref", description="Referral talabini sozlash"),
                BotCommand(command="linkguard", description="Link himoyasi"),
                BotCommand(command="allowlink", description="Ruxsatli linklar"),
                BotCommand(command="dupe", description="Dublikat cheklovi"),
                BotCommand(command="maxlen", description="Post uzunligi limiti"),
                BotCommand(command="newslot", description="Takroriy xabar sloti"),
                BotCommand(command="listslots", description="Slotlar ro'yxati"),
                BotCommand(command="stopslot", description="Slotni to'xtatish"),
                BotCommand(command="slotstats", description="Slot statistikasi"),
                BotCommand(command="id", description="ID"),
            ],
            scope=BotCommandScopeAllGroupChats(),
        )
    except Exception as e:
        logging.warning(f"set_my_commands xato: {e}")

    if scheduler_task is None or scheduler_task.done():
        scheduler_task = asyncio.create_task(recurring_scheduler_loop())

    logging.info("Universal Admin BUSINESS v6.4 ishga tushdi")


async def main():
    await on_startup()
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot to'xtatildi")
