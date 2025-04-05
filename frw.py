import os
import sqlite3
import threading
import logging
import re
import uuid
from datetime import datetime, timedelta
import sys
import types
import filetype
import asyncio
import time
import signal
import random

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, CallbackQueryHandler, ConversationHandler, Filters
)
from telethon import TelegramClient
from telethon.tl.types import PeerChannel
from telethon.errors import SessionPasswordNeededError, FloodWaitError, ChatSendMediaForbiddenError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
import pytz

# Constants
CLIENT_TIMEOUT = 30
CHECK_TASKS_INTERVAL = 60

# Fake 'imghdr' module for Python 3.11 compatibility
imghdr_module = types.ModuleType('imghdr')
def what(file, h=None):
    """Determine the file type based on its header."""
    buf = file.read(32) if hasattr(file, 'read') else open(file, 'rb').read(32) if isinstance(file, str) else file[:32]
    kind = filetype.guess(buf)
    return kind.extension if kind else None
imghdr_module.what = what
sys.modules['imghdr'] = imghdr_module

# Configuration from environment variables with validation
def load_env_var(name, required=True, cast=str):
    """Load an environment variable with type casting and validation."""
    value = os.environ.get(name)
    if required and not value:
        raise ValueError(f"Environment variable {name} is not set.")
    return cast(value) if value else None

API_ID = load_env_var('API_ID', cast=int)
API_HASH = load_env_var('API_HASH')
BOT_TOKEN = load_env_var('BOT_TOKEN')
ADMIN_IDS = [int(id_) for id_ in load_env_var('ADMIN_IDS', False, str).split(',') if id_] if load_env_var('ADMIN_IDS', False) else []

# Ensure data directories exist
if not os.path.exists("./data/sessions"):
    os.makedirs("./data/sessions")

# Database setup with persistent storage
db = sqlite3.connect('./data/telegram_bot.db', check_same_thread=False)
cursor = db.cursor()
db_lock = threading.RLock()

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()  # Output to stdout
    ]
)

# Signal handler for graceful shutdown
def shutdown(signum, frame):
    """Handle shutdown signals to close resources gracefully."""
    logging.info("Shutting down...")
    db.close()
    for client, loop, lock in userbots.values():
        asyncio.run_coroutine_threadsafe(client.disconnect(), loop)
        loop.call_soon_threadsafe(loop.stop)
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)

# Bot setup
updater = Updater(BOT_TOKEN)
dp = updater.dispatcher

# Userbots management
userbots = {}
userbots_lock = threading.Lock()

# Conversation states
(
    WAITING_FOR_CODE, WAITING_FOR_PHONE, WAITING_FOR_API_ID, WAITING_FOR_API_HASH,
    WAITING_FOR_CODE_USERBOT, WAITING_FOR_PASSWORD, WAITING_FOR_SUB_DETAILS,
    WAITING_FOR_GROUP_URLS, WAITING_FOR_MESSAGE_LINK, WAITING_FOR_START_TIME,
    WAITING_FOR_TARGET_GROUP, WAITING_FOR_FOLDER_CHOICE, WAITING_FOR_FOLDER_NAME,
    WAITING_FOR_FOLDER_SELECTION, TASK_SETUP, WAITING_FOR_LANGUAGE,
    WAITING_FOR_EXTEND_CODE, WAITING_FOR_EXTEND_DAYS,
    WAITING_FOR_ADD_USERBOTS_CODE, WAITING_FOR_ADD_USERBOTS_COUNT, SELECT_TARGET_GROUPS,
    WAITING_FOR_USERBOT_SELECTION, WAITING_FOR_GROUP_LINKS, WAITING_FOR_FOLDER_ACTION,
    WAITING_FOR_PRIMARY_MESSAGE_LINK, WAITING_FOR_FALLBACK_MESSAGE_LINK
) = range(26)

# Translations dictionary
translations = {
    'en': {
        'welcome': "Welcome! To activate your account, please send your invitation code now (e.g., a565ae57).",
        'invalid_code': "Invalid or expired code.",
        'client_menu': "Client Menu (Code: {code})\nAssigned Userbots: {count}\nSubscription ends: {end_date}\n",
        'set_language': "Set Language",
        'select_language': "Select your preferred language:",
        'language_set': "Language set to {lang}.",
        'account_activated': "Account activated! Your userbots will join target groups as you add them.",
        'setup_tasks': "Setup Tasks",
        'manage_folders': "Manage Folders",
        'back_to_menu': "Back to Menu",
        'select_target_groups': "Select Target Groups",
        'select_folder': "Select Folder",
        'send_to_all_groups': "Send to All Groups",
        'join_target_groups': "Join Target Groups",
        'logs': "Logs",
    },
    'uk': {},
    'pl': {},
    'lt': {},
    'ru': {}
}

def get_text(user_id, key, **kwargs):
    """Retrieve translated text based on user's language preference."""
    with db_lock:
        cursor.execute("SELECT language FROM clients WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        lang = result[0] if result else 'en'
    text = translations.get(lang, translations['en']).get(key, translations['en'].get(key, key))
    return text.format(**kwargs)

# Database initialization with persistence
try:
    with db_lock:
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS clients (
                invitation_code TEXT PRIMARY KEY,
                user_id INTEGER UNIQUE,
                subscription_end INTEGER NOT NULL,
                dedicated_userbots TEXT,
                folder_name TEXT,
                forwards_count INTEGER DEFAULT 0,
                groups_reached INTEGER DEFAULT 0,
                total_messages_sent INTEGER DEFAULT 0,
                language TEXT DEFAULT 'en'
            );

            CREATE TABLE IF NOT EXISTS userbots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone_number TEXT UNIQUE NOT NULL,
                session_file TEXT NOT NULL,
                status TEXT CHECK(status IN ('active', 'inactive')) DEFAULT 'active',
                assigned_client TEXT,
                api_id INTEGER NOT NULL,
                api_hash TEXT NOT NULL,
                username TEXT
            );

            CREATE TABLE IF NOT EXISTS target_groups (
                group_id INTEGER,
                group_name TEXT,
                group_link TEXT,
                added_by TEXT,
                folder_id INTEGER,
                PRIMARY KEY (group_id, added_by)
            );

            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                created_by TEXT NOT NULL,
                UNIQUE(name, created_by)
            );

            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                event TEXT NOT NULL,
                details TEXT
            );

            CREATE TABLE IF NOT EXISTS userbot_settings (
                client_id INTEGER,
                userbot_phone TEXT,
                message_link TEXT,
                fallback_message_link TEXT,
                start_time INTEGER,
                repetition_interval INTEGER,
                status TEXT CHECK(status IN ('active', 'inactive')) DEFAULT 'active',
                folder_id INTEGER,
                send_to_all_groups INTEGER DEFAULT 0,
                last_run INTEGER,
                PRIMARY KEY (client_id, userbot_phone)
            );
        ''')
        db.commit()
except sqlite3.Error as e:
    print(f"Database setup failed: {e}")
    raise

# Time zone setup
lithuania_tz = pytz.timezone('Europe/Vilnius')
utc_tz = pytz.utc

# Async helper functions
async def async_connect_and_check(client, phone):
    await client.connect()
    return "already_authorized" if await client.is_user_authorized() else await client.send_code_request(phone)

async def async_sign_in(client, phone, code):
    await client.sign_in(phone, code)

async def async_sign_in_with_password(client, password):
    await client.sign_in(password=password)

async def async_disconnect(client):
    await client.disconnect()

async def create_client(session_file, api_id, api_hash):
    client = TelegramClient(session_file, api_id, api_hash, timeout=CLIENT_TIMEOUT)
    return client

# Utility functions
def is_admin(user_id):
    return user_id in ADMIN_IDS

def notify_admins(bot, message_text):
    for admin_id in ADMIN_IDS:
        bot.send_message(admin_id, message_text)

def log_event(event, details):
    timestamp = int(datetime.now(utc_tz).timestamp())
    with db_lock:
        cursor.execute("INSERT INTO logs (timestamp, event, details) VALUES (?, ?, ?)", (timestamp, event, details))
        db.commit()
    logging.info(f"{event}: {details}")

def get_current_lithuanian_time():
    return datetime.now(lithuania_tz).strftime('%d/%m/%y %H:%M')

def parse_lithuanian_time(time_str):
    now = datetime.now(lithuania_tz)
    try:
        time_obj = datetime.strptime(time_str, '%H:%M')
        time_obj = lithuania_tz.localize(time_obj.replace(year=now.year, month=now.month, day=now.day))
        if time_obj < now:
            time_obj += timedelta(days=1)
        return int(time_obj.astimezone(utc_tz).timestamp())
    except ValueError:
        return None

def format_lithuanian_time(timestamp):
    return datetime.fromtimestamp(timestamp, utc_tz).astimezone(lithuania_tz).strftime('%H:%M') if timestamp else "Not set"

def format_interval(minutes):
    if minutes is None:
        return "Not set"
    if minutes % 60 == 0:
        hours = minutes // 60
        return f"Every {hours} hour{'s' if hours > 1 else ''}"
    return f"Every {minutes} minute{'s' if minutes > 1 else ''}"

def parse_telegram_url(url):
    if url.startswith("https://t.me/"):
        path = url[len("https://t.me/"):].strip()
        if path.startswith("+") or path.startswith("joinchat/"):
            return "private", path[1:] if path.startswith("+") else path[len("joinchat/"):]
        elif path.startswith("addlist/"):
            return "addlist", path[len("addlist/"):]
        return "public", path.split('/')[0]
    raise ValueError("Invalid Telegram URL")

async def get_message_from_link(client, link):
    logging.info(f"Parsing message link: {link}")
    parts = link.split('/')
    if link.startswith("https://t.me/c/") and len(parts) == 6 and parts[4].isdigit() and parts[5].isdigit():
        group_id = -1000000000000 - int(parts[4])
        message_id = int(parts[5])
        return PeerChannel(group_id), message_id
    elif link.startswith("https://t.me/") and len(parts) == 5 and parts[4].isdigit():
        try:
            chat = await client.get_entity(parts[3])
            return chat, int(parts[4])
        except Exception as e:
            logging.error(f"Failed to get entity for {parts[3]}: {e}")
            raise ValueError(f"Failed to get entity: {e}")
    logging.error(f"Invalid message link: {link}")
    raise ValueError("Invalid message link")

async def get_chat_from_link(client, link):
    if link.startswith("https://t.me/+"):
        updates = await client(ImportChatInviteRequest(link[len("https://t.me/+"):]))
        return updates.chats[0].id
    elif link.startswith("https://t.me/c/"):
        match = re.search(r'https://t.me/c/(\d+)/\d+', link)
        if match:
            return -1000000000000 - int(match.group(1))
    elif link.startswith("https://t.me/"):
        chat = await client.get_entity(link[len("https://t.me/"):].split('/')[0])
        return chat.id
    raise ValueError("Invalid link")

def get_userbot_client(phone_number):
    """Retrieve or create a TelegramClient instance for a userbot."""
    try:
        with db_lock:
            cursor.execute("SELECT api_id, api_hash, session_file FROM userbots WHERE phone_number = ?", (phone_number,))
            result = cursor.fetchone()
        if result:
            api_id, api_hash, session_file = result
            with userbots_lock:
                if phone_number not in userbots:
                    loop = asyncio.new_event_loop()
                    def run_loop():
                        asyncio.set_event_loop(loop)
                        loop.run_forever()
                    thread = threading.Thread(target=run_loop, daemon=True)
                    thread.start()
                    future = asyncio.run_coroutine_threadsafe(
                        create_client(session_file, api_id, api_hash), loop
                    )
                    client = future.result()
                    async def create_lock():
                        return asyncio.Lock()
                    lock_future = asyncio.run_coroutine_threadsafe(create_lock(), loop)
                    lock = lock_future.result()
                    userbots[phone_number] = (client, loop, lock)
                return userbots[phone_number]
        return None, None, None
    except Exception as e:
        log_event("Get Userbot Client Error", f"Phone: {phone_number}, Error: {e}")
        return None, None, None

async def check_membership(client, group_id):
    try:
        permissions = await client.get_permissions(PeerChannel(group_id), client._self_id)
        if permissions:
            return True
    except Exception:
        return False
    return False

async def add_and_join_group(client, group_url, folder_id, added_by, phone):
    max_retries = 5
    base_delay = 5  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            logging.info(f"Attempt {attempt + 1}: Joining group {group_url}")
            group_type, identifier = parse_telegram_url(group_url)
            if group_type == "addlist":
                return False, "Addlist links are not supported. Use individual group links."

            entity = await client.get_entity(identifier)
            if group_type == "private":
                updates = await client(ImportChatInviteRequest(identifier))
                chat = updates.chats[0]
            else:
                chat = entity

            group_id, group_name = chat.id, chat.title

            is_member = await check_membership(client, group_id)
            if is_member:
                logging.info(f"Already a member of group {group_name}")
                with db_lock:
                    cursor.execute("SELECT group_id FROM target_groups WHERE group_id = ? AND added_by = ?", (group_id, added_by))
                    if not cursor.fetchone():
                        cursor.execute("INSERT INTO target_groups (group_id, group_name, group_link, added_by, folder_id) VALUES (?, ?, ?, ?, ?)",
                                       (group_id, group_name, group_url, added_by, folder_id))
                        db.commit()
                return True, f"Already a member of {group_name} (ID: {group_id})"
            else:
                await client(JoinChannelRequest(group_id))
                is_member_after_join = await check_membership(client, group_id)
                if is_member_after_join:
                    logging.info(f"Successfully joined group: {group_name}")
                    with db_lock:
                        cursor.execute("INSERT INTO target_groups (group_id, group_name, group_link, added_by, folder_id) VALUES (?, ?, ?, ?, ?)",
                                       (group_id, group_name, group_url, added_by, folder_id))
                        db.commit()
                    return True, f"Successfully joined {group_name} (ID: {group_id})"
                else:
                    logging.info(f"Join request pending for group: {group_name}")
                    return False, f"Join request pending for {group_name} (ID: {group_id})"
        except FloodWaitError as e:
            if attempt < max_retries - 1:
                wait_time = min(e.seconds, 60) + (base_delay * (2 ** attempt))  # Exponential backoff with cap
                logging.warning(f"Flood wait error on attempt {attempt + 1}: Waiting {wait_time} seconds before retrying...")
                await asyncio.sleep(wait_time)
                continue
            else:
                return False, f"Flood wait error after {max_retries} attempts: {str(e)}"
        except ValueError as e:
            return False, f"Value error: {str(e)}"
        except telethon.errors.ChannelPrivateError:
            return False, "The group is private and cannot be joined."
        except telethon.errors.InviteHashExpiredError:
            return False, "The invite link has expired."
        except Exception as e:
            logging.error(f"Unexpected error joining group {group_url}: {e}")
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)
                logging.warning(f"Retrying in {wait_time} seconds due to unexpected error...")
                await asyncio.sleep(wait_time)
                continue
            else:
                return False, f"Unexpected error after {max_retries} attempts: {str(e)}"
    return False, f"Max retries ({max_retries}) reached. Could not join the group."

async def join_groups(client, urls, folder_id, phone, added_by):
    semaphore = asyncio.Semaphore(5)
    async def wrapped_add_and_join(url):
        async with semaphore:
            try:
                return await asyncio.wait_for(add_and_join_group(client, url, folder_id, added_by, phone), timeout=30)
            except asyncio.TimeoutError:
                return False, "Timeout - Join operation took too long"
    tasks = [wrapped_add_and_join(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results

async def add_groups_to_folder(client, urls, folder_id, added_by):
    results = []
    for url in urls:
        try:
            group_type, identifier = parse_telegram_url(url)
            if group_type == "addlist":
                results.append((url, False, "Addlist links not supported"))
                continue
            entity = await client.get_entity(identifier)
            chat = entity
            group_id, group_name = chat.id, chat.title
            with db_lock:
                cursor.execute("INSERT OR IGNORE INTO target_groups (group_id, group_name, group_link, added_by, folder_id) VALUES (?, ?, ?, ?, ?)",
                               (group_id, group_name, url, added_by, folder_id))
                db.commit()
            results.append((url, True, f"Added {group_name} to folder"))
        except Exception as e:
            results.append((url, False, str(e)))
    return results

async def join_target_groups(client, lock, folder_id, phone):
    async with lock:
        try:
            await client.start()
            with db_lock:
                cursor.execute("SELECT group_id, group_name FROM target_groups WHERE folder_id = ?", (folder_id,))
                groups = cursor.fetchall()
            if groups:
                tasks = [client(JoinChannelRequest(PeerChannel(group_id))) for group_id, _ in groups]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success_count = 0
                errors = []
                for (group_id, group_name), result in zip(groups, results):
                    if isinstance(result, Exception):
                        errors.append(f"Error joining group {group_name}: {result}")
                        log_event("Group Join Error", f"Phone: {phone}, Group: {group_name}, Error: {result}")
                    else:
                        log_event("Userbot Joined Group", f"Phone: {phone}, Group: {group_name} (ID: {group_id})")
                        success_count += 1
                await client.disconnect()
                return success_count, len(groups), errors
            else:
                return 0, 0, ["No target groups found."]
        except Exception as e:
            log_event("Join Error", f"Phone: {phone}, Error: {e}")
            print(f"Error in join_target_groups for {phone}: {e}")
            return 0, 0, [f"Error joining target groups: {e}"]

# Handlers
def start(update: Update, context):
    """Handle the /start command to activate the account or show client menu."""
    try:
        user_id = update.effective_user.id
        logging.info(f"Start command received from user {user_id}")
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        if result and result[0]:
            logging.info(f"User {user_id} has userbots, redirecting to client menu")
            return client_menu(update, context)
        else:
            if 'prompted_for_code' not in context.user_data:
                update.message.reply_text(get_text(user_id, 'welcome'))
                context.user_data['prompted_for_code'] = True
            else:
                update.message.reply_text(get_text(user_id, 'welcome'))
            return WAITING_FOR_CODE
    except Exception as e:
        log_event("Start Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text("An error occurred. Please try again.")
        return ConversationHandler.END

async def join_existing_target_groups(client, lock, user_id, phone):
    async with lock:
        try:
            await client.start()
            with db_lock:
                cursor.execute("SELECT folder_name FROM clients WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
                folder_name = result[0] if result else None
                query = "SELECT group_id, group_name FROM target_groups WHERE added_by = ? AND folder_id = (SELECT id FROM folders WHERE name = ?)"
                cursor.execute(query, (str(user_id), folder_name))
                groups = cursor.fetchall()
            if groups:
                tasks = [client(JoinChannelRequest(PeerChannel(group_id))) for group_id, _ in groups]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success_count = 0
                for (group_id, group_name), result in zip(groups, results):
                    if isinstance(result, Exception):
                        log_event("Group Join Error", f"User: {user_id}, Phone: {phone}, Group: {group_name}, Error: {result}")
                    else:
                        log_event("Userbot Joined Group", f"Phone: {phone}, Group: {group_name} (ID: {group_id})")
                        success_count += 1
                await client.disconnect()
                return success_count, len(groups)
            else:
                return 0, 0
        except Exception as e:
            log_event("Join Error", f"User: {user_id}, Phone: {phone}, Error: {e}")
            print(f"Error in join_existing_target_groups for user {user_id}: {e}")
            return 0, 0

def admin_panel(update: Update, context):
    """Display the admin panel for authorized users."""
    try:
        if not is_admin(update.effective_user.id):
            update.message.reply_text("Unauthorized")
            return ConversationHandler.END
        keyboard = [
            [InlineKeyboardButton("Add Userbot", callback_data="admin_add_userbot")],
            [InlineKeyboardButton("Remove Userbot", callback_data="admin_remove_userbot")],
            [InlineKeyboardButton("Add Target Group", callback_data="admin_add_group")],
            [InlineKeyboardButton("Remove Target Group", callback_data="admin_remove_group")],
            [InlineKeyboardButton("Generate Invitation", callback_data="admin_generate_invite")],
            [InlineKeyboardButton("View Subscriptions", callback_data="admin_view_subs")],
            [InlineKeyboardButton("View Logs", callback_data="admin_view_logs")],
            [InlineKeyboardButton("Extend Subscription", callback_data="admin_extend_sub")],
            [InlineKeyboardButton("Add Userbots to Client", callback_data="admin_add_userbots")],
        ]
        markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text("Admin Panel:", reply_markup=markup)
        return ConversationHandler.END
    except Exception as e:
        log_event("Admin Panel Error", f"User: {update.effective_user.id}, Error: {e}")
        update.message.reply_text("An error occurred in the admin panel.")
        return ConversationHandler.END

async def get_username_from_phone(client, phone):
    try:
        if not client.is_connected():
            await client.connect()
        me = await client.get_me()
        username = me.username if me.username else None
        return username
    except Exception as e:
        logging.error(f"Failed to get username for {phone}: {e}")
        return None

def client_menu(update: Update, context):
    """Show the client menu with userbot and subscription details."""
    try:
        user_id = update.effective_user.id
        with db_lock:
            cursor.execute("SELECT invitation_code, dedicated_userbots, subscription_end FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        if not result:
            update.message.reply_text(get_text(user_id, 'invalid_code'))
            return ConversationHandler.END
        code, userbots_str, sub_end = result
        end_date = datetime.fromtimestamp(sub_end).strftime('%Y-%m-%d')
        userbot_phones = userbots_str.split(",") if userbots_str else []
        message = get_text(user_id, 'client_menu', code=code, count=len(userbot_phones), end_date=end_date)
        for i, phone in enumerate(userbot_phones, 1):
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            message += f"{i}. {display_name}\n"
        keyboard = [
            [InlineKeyboardButton(get_text(user_id, 'setup_tasks'), callback_data="client_setup_tasks")],
            [InlineKeyboardButton(get_text(user_id, 'manage_folders'), callback_data="client_manage_folders")],
            [InlineKeyboardButton(get_text(user_id, 'join_target_groups'), callback_data="client_join_target_groups")],
            [InlineKeyboardButton("Already Joined Groups", callback_data="client_joined_groups")],
            [InlineKeyboardButton(get_text(user_id, 'logs'), callback_data="client_view_logs")],
            [InlineKeyboardButton(get_text(user_id, 'set_language'), callback_data="client_set_language")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(message, reply_markup=markup)
        return ConversationHandler.END
    except Exception as e:
        log_event("Client Menu Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text("An error occurred. Please try again or contact support.")
        return ConversationHandler.END

def handle_callback(update: Update, context):
    """Handle all callback queries from inline keyboards."""
    try:
        query = update.callback_query
        query.answer()
        data = query.data
        user_id = query.from_user.id

        if data == "client_join_target_groups":
            with db_lock:
                cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
            if result and result[0]:
                userbot_phones = result[0].split(",")
                keyboard = [[InlineKeyboardButton("All Userbots", callback_data="join_all_userbots")]]
                for phone in userbot_phones:
                    with db_lock:
                        cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                        result = cursor.fetchone()
                        username = result[0] if result and result[0] else None
                    display_name = f"@{username}" if username else f"{phone} (no username set)"
                    keyboard.append([InlineKeyboardButton(display_name, callback_data=f"join_userbot_{phone}")])
                keyboard.append([InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")])
                markup = InlineKeyboardMarkup(keyboard)
                query.edit_message_text("Select a userbot to join groups:", reply_markup=markup)
                return WAITING_FOR_USERBOT_SELECTION
            else:
                query.edit_message_text("No userbots assigned.")
                return ConversationHandler.END

        elif data.startswith("join_userbot_") or data == "join_all_userbots":
            context.user_data['selected_userbot'] = data.split("_")[-1] if data != "join_all_userbots" else "all"
            keyboard = [[InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Send the list of group links (one per line):", reply_markup=markup)
            return WAITING_FOR_GROUP_LINKS

        elif data == "client_joined_groups":
            with db_lock:
                cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
            if result and result[0]:
                userbot_phones = result[0].split(",")
                keyboard = []
                for phone in userbot_phones:
                    with db_lock:
                        cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                        result = cursor.fetchone()
                        username = result[0] if result and result[0] else None
                    display_name = f"@{username}" if username else f"{phone} (no username set)"
                    keyboard.append([InlineKeyboardButton(display_name, callback_data=f"view_joined_{phone}")])
                keyboard.append([InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")])
                markup = InlineKeyboardMarkup(keyboard)
                query.edit_message_text("Select a userbot to view joined groups:", reply_markup=markup)
                return ConversationHandler.END
            else:
                query.edit_message_text("No userbots assigned.")
                return ConversationHandler.END

        elif data.startswith("view_joined_"):
            phone = data.split("_")[2]
            client, loop, lock = get_userbot_client(phone)
            if not client:
                query.edit_message_text(f"Failed to initialize userbot {phone}.")
                return ConversationHandler.END
            async def get_joined_groups():
                async with lock:
                    await client.start()
                    try:
                        dialogs = await client.get_dialogs()
                        groups = [dialog for dialog in dialogs if dialog.is_group]
                        group_links = []
                        for group in groups:
                            if group.entity.username:
                                link = f"https://t.me/{group.entity.username}"
                            else:
                                link = f"Private group: {group.entity.title}"
                            group_links.append(link)
                        return group_links
                    finally:
                        await client.disconnect()
            group_links = asyncio.run_coroutine_threadsafe(get_joined_groups(), loop).result()
            if group_links:
                message = "Joined groups:\n" + "\n".join(group_links)
            else:
                message = "No joined groups found."
            keyboard = [[InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return ConversationHandler.END

        elif data == "client_view_logs":
            with db_lock:
                cursor.execute("SELECT total_messages_sent, groups_reached FROM clients WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
            if result:
                total_sent, groups_reached = result
                message = f"**Logs**\nTotal Messages Sent: {total_sent}\nGroups Reached: {groups_reached}"
                keyboard = [[InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")]]
                markup = InlineKeyboardMarkup(keyboard)
                query.edit_message_text(message, reply_markup=markup)
            else:
                query.edit_message_text("No logs available.")
            return ConversationHandler.END

        elif data == "client_manage_folders":
            keyboard = [
                [InlineKeyboardButton("Create New Folder", callback_data="create_new_folder")],
                [InlineKeyboardButton("Edit Existing Folder", callback_data="edit_existing_folder")],
                [InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Manage Folders:", reply_markup=markup)
            return WAITING_FOR_FOLDER_ACTION

        elif data == "create_new_folder":
            keyboard = [[InlineKeyboardButton("Back to Manage Folders", callback_data="client_manage_folders")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Enter the name for the new folder:", reply_markup=markup)
            return WAITING_FOR_FOLDER_NAME

        elif data == "edit_existing_folder":
            with db_lock:
                cursor.execute("SELECT id, name FROM folders WHERE created_by = ?", (str(user_id),))
                folders = cursor.fetchall()
            if not folders:
                query.edit_message_text("No folders available. Create a new folder first.")
                return ConversationHandler.END
            keyboard = [[InlineKeyboardButton(f[1], callback_data=f"edit_folder_{f[0]}")] for f in folders]
            keyboard.append([InlineKeyboardButton("Back to Manage Folders", callback_data="client_manage_folders")])
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Select a folder to edit:", reply_markup=markup)
            return WAITING_FOR_FOLDER_SELECTION

        elif data.startswith("edit_folder_"):
            folder_id = int(data.split("_")[2])
            context.user_data['selected_folder_id'] = folder_id
            with db_lock:
                cursor.execute("SELECT name FROM folders WHERE id = ?", (folder_id,))
                result = cursor.fetchone()
                folder_name = result[0] if result else "Not set"
                cursor.execute("SELECT group_link FROM target_groups WHERE folder_id = ?", (folder_id,))
                existing_groups = [row[0] for row in cursor.fetchall()]
            message = f"Folder: {folder_name}\nExisting groups:\n- " + "\n- ".join(existing_groups) if existing_groups else "No groups in this folder."
            keyboard = [
                [InlineKeyboardButton("Update Entire List", callback_data="update_folder_list")],
                [InlineKeyboardButton("Add New Links", callback_data="add_to_folder_list")],
                [InlineKeyboardButton("Back to Manage Folders", callback_data="client_manage_folders")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return WAITING_FOR_FOLDER_ACTION

        elif data in ["update_folder_list", "add_to_folder_list"]:
            context.user_data['folder_action'] = data
            keyboard = [[InlineKeyboardButton("Back to Manage Folders", callback_data="client_manage_folders")]]
            markup = InlineKeyboardMarkup(keyboard)
            action_text = "update" if data == "update_folder_list" else "add to"
            query.edit_message_text(f"Send the group links to {action_text} the folder (one per line):", reply_markup=markup)
            return WAITING_FOR_GROUP_LINKS

        elif data == "admin_add_group":
            keyboard = [
                [InlineKeyboardButton("Add to Existing Folder", callback_data="add_to_existing")],
                [InlineKeyboardButton("Create New Folder", callback_data="create_new_folder_admin")],
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Choose an option:", reply_markup=markup)
            return WAITING_FOR_FOLDER_CHOICE

        elif data == "create_new_folder_admin":
            keyboard = [[InlineKeyboardButton("Back to Admin Panel", callback_data="admin_panel")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Enter the name for the new folder:", reply_markup=markup)
            return WAITING_FOR_FOLDER_NAME

        elif data == "add_to_existing":
            with db_lock:
                cursor.execute("SELECT id, name FROM folders WHERE created_by = ?", (str(user_id),))
                folders = cursor.fetchall()
            if not folders:
                query.edit_message_text("No folders available. Create a new folder first.")
                return ConversationHandler.END
            keyboard = [[InlineKeyboardButton(f[1], callback_data=f"folder_{f[0]}")] for f in folders]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Select a folder:", reply_markup=markup)
            return WAITING_FOR_FOLDER_SELECTION

        elif data == "admin_panel":
            return admin_panel(update, context)

        elif data.startswith("folder_"):
            folder_id = int(data.split("_")[1])
            with db_lock:
                cursor.execute("SELECT name FROM folders WHERE id = ?", (folder_id,))
                result = cursor.fetchone()
                folder_name = result[0] if result else "Not set"
                cursor.execute("SELECT group_link FROM target_groups WHERE folder_id = ?", (folder_id,))
                existing_groups = [row[0] for row in cursor.fetchall()]
            context.user_data['folder_id'] = folder_id
            context.user_data['folder_name'] = folder_name
            message = (f"Selected folder: {folder_name}\nExisting groups:\n- " + "\n- ".join(existing_groups) if existing_groups 
                       else f"Selected folder: {folder_name}\nNo existing groups.") + "\n\nEnter additional target group URLs (one per line):"
            keyboard = [[InlineKeyboardButton("Back to Admin Panel", callback_data="admin_panel")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return WAITING_FOR_GROUP_URLS

        elif data == "admin_add_userbot":
            keyboard = [[InlineKeyboardButton("Back to Admin Panel", callback_data="admin_panel")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Enter userbot phone number (e.g., +1234567890):", reply_markup=markup)
            return WAITING_FOR_PHONE

        elif data == "admin_remove_userbot":
            with db_lock:
                cursor.execute("SELECT phone_number, username FROM userbots")
                userbots_list = cursor.fetchall()
            if not userbots_list:
                query.edit_message_text("No userbots available.")
                return ConversationHandler.END
            keyboard = []
            for phone, username in userbots_list:
                display_name = f"@{username}" if username else f"{phone} (no username set)"
                keyboard.append([InlineKeyboardButton(display_name, callback_data=f"remove_ub_{phone}")])
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Select userbot to remove:", reply_markup=markup)
            return ConversationHandler.END

        elif data.startswith("remove_ub_"):
            phone = data.split("_")[2]
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            with db_lock:
                cursor.execute("DELETE FROM userbots WHERE phone_number = ?", (phone,))
                db.commit()
            with userbots_lock:
                if phone in userbots:
                    client, loop, _ = userbots.pop(phone)
                    asyncio.run_coroutine_threadsafe(client.disconnect(), loop)
                    loop.call_soon_threadsafe(loop.stop)
            log_event("Userbot Removed", f"Phone: {phone}")
            notify_admins(context.bot, f"Userbot {display_name} removed.")
            query.edit_message_text(f"Userbot {display_name} removed.")
            return ConversationHandler.END

        elif data == "admin_remove_group":
            with db_lock:
                cursor.execute("SELECT group_id, group_name FROM target_groups WHERE added_by = ?", (str(user_id),))
                groups = cursor.fetchall()
            if not groups:
                query.edit_message_text("No target groups available.")
                return ConversationHandler.END
            keyboard = [[InlineKeyboardButton(g[1], callback_data=f"remove_group_{g[0]}")] for g in groups]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Select group to remove:", reply_markup=markup)
            return ConversationHandler.END

        elif data.startswith("remove_group_"):
            group_id = int(data.split("_")[2])
            with db_lock:
                cursor.execute("DELETE FROM target_groups WHERE group_id = ? AND added_by = ?", (group_id, str(user_id)))
                db.commit()
            log_event("Group Removed", f"Group ID: {group_id}, By: {user_id}")
            query.edit_message_text("Group removed.")
            return ConversationHandler.END

        elif data == "admin_generate_invite":
            keyboard = [[InlineKeyboardButton("Back to Admin Panel", callback_data="admin_panel")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Enter subscription details (e.g., 30day 4acc [folder_name]):", reply_markup=markup)
            return WAITING_FOR_SUB_DETAILS

        elif data == "admin_view_subs":
            with db_lock:
                cursor.execute("SELECT user_id, invitation_code, subscription_end, folder_name FROM clients")
                subs = cursor.fetchall()
            if not subs:
                query.edit_message_text("No active subscriptions.")
                return ConversationHandler.END
            msg = "Subscriptions:\n"
            for s in subs:
                end_date = datetime.fromtimestamp(s[2]).strftime('%Y-%m-%d')
                msg += f"User {s[0]} | Code: {s[1]} | Ends: {end_date} | Folder: {s[3] or 'None'}\n"
            query.edit_message_text(msg)
            return ConversationHandler.END

        elif data == "admin_view_logs":
            with db_lock:
                cursor.execute("SELECT timestamp, event, details FROM logs ORDER BY timestamp DESC LIMIT 10")
                logs = cursor.fetchall()
            msg = "Recent Logs:\n"
            for log in logs:
                date = datetime.fromtimestamp(log[0]).strftime('%Y-%m-%d %H:%M')
                msg += f"{date} | {log[1]} | {log[2]}\n"
            query.edit_message_text(msg)
            return ConversationHandler.END

        elif data == "admin_extend_sub":
            keyboard = [[InlineKeyboardButton("Back to Admin Panel", callback_data="admin_panel")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Enter the client's activation code to extend their subscription:", reply_markup=markup)
            return WAITING_FOR_EXTEND_CODE

        elif data == "admin_add_userbots":
            keyboard = [[InlineKeyboardButton("Back to Admin Panel", callback_data="admin_panel")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Enter the client's activation code to add more userbots:", reply_markup=markup)
            return WAITING_FOR_ADD_USERBOTS_CODE

        elif data == "client_setup_tasks":
            with db_lock:
                cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
            if result and result[0]:
                userbot_phones = result[0].split(",")
                message = "Select a userbot to configure:\n"
                keyboard = []
                for i, phone in enumerate(userbot_phones, 1):
                    with db_lock:
                        cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                        result = cursor.fetchone()
                        username = result[0] if result and result[0] else None
                    display_name = f"@{username}" if username else f"{phone} (no username set)"
                    message += f"{i}. {display_name}\n"
                    keyboard.append([InlineKeyboardButton(display_name, callback_data=f"edit_task_{phone}")])
                keyboard.append([InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")])
                markup = InlineKeyboardMarkup(keyboard)
                query.edit_message_text(message, reply_markup=markup)
            return ConversationHandler.END

        elif data.startswith("edit_task_"):
            phone = data.split("_")[2]
            context.user_data['setting_phone'] = phone
            task_config = context.user_data.get(f'task_config_{phone}', {})
            with db_lock:
                cursor.execute("SELECT message_link, fallback_message_link, start_time, repetition_interval, status, folder_id, send_to_all_groups FROM userbot_settings WHERE client_id = ? AND userbot_phone = ?", (user_id, phone))
                settings = cursor.fetchone()
                logging.info(f"Retrieved settings for {phone}: {settings}")
            if settings:
                task_config.update({
                    'message_link': settings[0],
                    'fallback_message_link': settings[1],
                    'start_time': settings[2],
                    'repetition_interval': settings[3],
                    'status': settings[4],
                    'folder_id': settings[5],
                    'send_to_all_groups': settings[6]
                })
            if 'folder_id' not in task_config:
                task_config['folder_id'] = None
            if 'message_link' not in task_config:
                task_config['message_link'] = None
            if 'fallback_message_link' not in task_config:
                task_config['fallback_message_link'] = None
            if 'start_time' not in task_config:
                task_config['start_time'] = None
            if 'repetition_interval' not in task_config:
                task_config['repetition_interval'] = None
            if 'status' not in task_config:
                task_config['status'] = 'inactive'
            if 'send_to_all_groups' not in task_config:
                task_config['send_to_all_groups'] = 0
            context.user_data[f'task_config_{phone}'] = task_config
            with db_lock:
                cursor.execute("SELECT name FROM folders WHERE id = ?", (task_config['folder_id'],))
                result = cursor.fetchone()
                folder_name = result[0] if result else "Not set"
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            message = (f"Task Settings for {display_name}:\n"
                       f"Primary Message: {task_config['message_link'] or 'Not set'}\n"
                       f"Fallback Message: {task_config['fallback_message_link'] or 'Not set'}\n"
                       f"Start Time: {format_lithuanian_time(task_config['start_time'])}\n"
                       f"Interval: {format_interval(task_config['repetition_interval'])}\n"
                       f"Target: {'All Groups' if task_config['send_to_all_groups'] else folder_name}\n"
                       f"Status: {task_config['status']}")
            keyboard = [
                [InlineKeyboardButton("Set Message", callback_data=f"set_message_{phone}")],
                [InlineKeyboardButton("Set Time", callback_data=f"set_time_{phone}")],
                [InlineKeyboardButton("Set Interval", callback_data=f"set_interval_{phone}")],
                [InlineKeyboardButton(get_text(user_id, 'select_target_groups'), callback_data=f"select_target_groups_{phone}")],
                [InlineKeyboardButton(f"{'Deactivate' if task_config['status'] == 'active' else 'Activate'}", callback_data=f"toggle_status_{phone}")],
                [InlineKeyboardButton("Save", callback_data=f"save_task_{phone}"), InlineKeyboardButton("Cancel", callback_data="cancel_task")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return TASK_SETUP

        elif data.startswith("select_target_groups_"):
            phone = data.split("_")[3]
            context.user_data['setting_phone'] = phone
            keyboard = [
                [InlineKeyboardButton(get_text(user_id, 'select_folder'), callback_data=f"set_folder_{phone}")],
                [InlineKeyboardButton(get_text(user_id, 'send_to_all_groups'), callback_data=f"send_to_all_groups_{phone}")],
                [InlineKeyboardButton("Back to Task Setup", callback_data=f"back_to_task_setup_{phone}")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Choose target groups option:", reply_markup=markup)
            return SELECT_TARGET_GROUPS

        elif data.startswith("send_to_all_groups_"):
            phone = data.split("_")[4]
            task_config = context.user_data[f'task_config_{phone}']
            task_config['send_to_all_groups'] = 1
            task_config['folder_id'] = None
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            message = (f"Task Settings for {display_name}:\n"
                       f"Primary Message: {task_config['message_link'] or 'Not set'}\n"
                       f"Fallback Message: {task_config['fallback_message_link'] or 'Not set'}\n"
                       f"Start Time: {format_lithuanian_time(task_config['start_time'])}\n"
                       f"Interval: {format_interval(task_config['repetition_interval'])}\n"
                       f"Target: All Groups\n"
                       f"Status: {task_config['status']}")
            keyboard = [
                [InlineKeyboardButton("Set Message", callback_data=f"set_message_{phone}")],
                [InlineKeyboardButton("Set Time", callback_data=f"set_time_{phone}")],
                [InlineKeyboardButton("Set Interval", callback_data=f"set_interval_{phone}")],
                [InlineKeyboardButton(get_text(user_id, 'select_target_groups'), callback_data=f"select_target_groups_{phone}")],
                [InlineKeyboardButton(f"{'Deactivate' if task_config['status'] == 'active' else 'Activate'}", callback_data=f"toggle_status_{phone}")],
                [InlineKeyboardButton("Save", callback_data=f"save_task_{phone}"), InlineKeyboardButton("Cancel", callback_data="cancel_task")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return TASK_SETUP

        elif data.startswith("set_folder_"):
            phone = data.split("_")[2]
            context.user_data['setting_phone'] = phone
            with db_lock:
                cursor.execute("SELECT id, name FROM folders WHERE created_by = ?", (str(user_id),))
                folders = cursor.fetchall()
            if not folders:
                query.edit_message_text("No folders available. Create one via 'Manage Folders' first.")
                return TASK_SETUP
            keyboard = [[InlineKeyboardButton(f[1], callback_data=f"select_folder_{phone}_{f[0]}")] for f in folders]
            keyboard.append([InlineKeyboardButton("Back to Task Setup", callback_data=f"back_to_task_setup_{phone}")])
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Select a folder for forwarding:", reply_markup=markup)
            return SELECT_TARGET_GROUPS

        elif data.startswith("select_folder_"):
            parts = data.split("_")
            phone, folder_id = parts[2], int(parts[3])
            task_config = context.user_data[f'task_config_{phone}']
            task_config['folder_id'] = folder_id
            task_config['send_to_all_groups'] = 0
            with db_lock:
                cursor.execute("SELECT name FROM folders WHERE id = ?", (folder_id,))
                result = cursor.fetchone()
                folder_name = result[0] if result else "Not set"
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            message = (f"Task Settings for {display_name}:\n"
                       f"Primary Message: {task_config['message_link'] or 'Not set'}\n"
                       f"Fallback Message: {task_config['fallback_message_link'] or 'Not set'}\n"
                       f"Start Time: {format_lithuanian_time(task_config['start_time'])}\n"
                       f"Interval: {format_interval(task_config['repetition_interval'])}\n"
                       f"Target: {folder_name}\n"
                       f"Status: {task_config['status']}")
            keyboard = [
                [InlineKeyboardButton("Set Message", callback_data=f"set_message_{phone}")],
                [InlineKeyboardButton("Set Time", callback_data=f"set_time_{phone}")],
                [InlineKeyboardButton("Set Interval", callback_data=f"set_interval_{phone}")],
                [InlineKeyboardButton(get_text(user_id, 'select_target_groups'), callback_data=f"select_target_groups_{phone}")],
                [InlineKeyboardButton(f"{'Deactivate' if task_config['status'] == 'active' else 'Activate'}", callback_data=f"toggle_status_{phone}")],
                [InlineKeyboardButton("Save", callback_data=f"save_task_{phone}"), InlineKeyboardButton("Cancel", callback_data="cancel_task")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return TASK_SETUP

        elif data.startswith("set_message_"):
            phone = data.split("_")[2]
            context.user_data['setting_phone'] = phone
            keyboard = [[InlineKeyboardButton("Back to Task Setup", callback_data=f"back_to_task_setup_{phone}")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Send the primary message link (e.g., https://t.me/c/123456789/10):", reply_markup=markup)
            return WAITING_FOR_PRIMARY_MESSAGE_LINK

        elif data.startswith("set_time_"):
            phone = data.split("_")[2]
            context.user_data['setting_phone'] = phone
            keyboard = [[InlineKeyboardButton("Back to Task Setup", callback_data=f"back_to_task_setup_{phone}")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Enter start time (HH:MM, e.g., 17:30):", reply_markup=markup)
            return WAITING_FOR_START_TIME

        elif data.startswith("set_interval_"):
            phone = data.split("_")[2]
            context.user_data['setting_phone'] = phone
            hours = [1, 2, 3, 6, 12, 24]
            minutes = [5, 10, 15, 30]
            keyboard = [
                *[[InlineKeyboardButton(f"{h}h", callback_data=f"interval_{phone}_h_{h}")] for h in hours],
                *[[InlineKeyboardButton(f"{m}m", callback_data=f"interval_{phone}_m_{m}")] for m in minutes],
                [InlineKeyboardButton("Back to Task Setup", callback_data=f"back_to_task_setup_{phone}")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("Select repetition interval:", reply_markup=markup)
            return TASK_SETUP

        elif data.startswith("interval_"):
            parts = data.split("_")
            phone, unit, value = parts[1], parts[2], int(parts[3])
            interval = value * 60 if unit == 'h' else value
            context.user_data[f'task_config_{phone}']['repetition_interval'] = interval
            task_config = context.user_data[f'task_config_{phone}']
            with db_lock:
                cursor.execute("SELECT name FROM folders WHERE id = ?", (task_config['folder_id'],))
                result = cursor.fetchone()
                folder_name = result[0] if result else "Not set"
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            message = (f"Task Settings for {display_name}:\n"
                       f"Primary Message: {task_config['message_link'] or 'Not set'}\n"
                       f"Fallback Message: {task_config['fallback_message_link'] or 'Not set'}\n"
                       f"Start Time: {format_lithuanian_time(task_config['start_time'])}\n"
                       f"Interval: {format_interval(task_config['repetition_interval'])}\n"
                       f"Target: {'All Groups' if task_config['send_to_all_groups'] else folder_name}\n"
                       f"Status: {task_config['status']}")
            keyboard = [
                [InlineKeyboardButton("Set Message", callback_data=f"set_message_{phone}")],
                [InlineKeyboardButton("Set Time", callback_data=f"set_time_{phone}")],
                [InlineKeyboardButton("Set Interval", callback_data=f"set_interval_{phone}")],
                [InlineKeyboardButton(get_text(user_id, 'select_target_groups'), callback_data=f"select_target_groups_{phone}")],
                [InlineKeyboardButton(f"{'Deactivate' if task_config['status'] == 'active' else 'Activate'}", callback_data=f"toggle_status_{phone}")],
                [InlineKeyboardButton("Save", callback_data=f"save_task_{phone}"), InlineKeyboardButton("Cancel", callback_data="cancel_task")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return TASK_SETUP

        elif data.startswith("toggle_status_"):
            phone = data.split("_")[2]
            task_config = context.user_data[f'task_config_{phone}']
            task_config['status'] = 'inactive' if task_config['status'] == 'active' else 'active'
            with db_lock:
                cursor.execute("SELECT name FROM folders WHERE id = ?", (task_config['folder_id'],))
                result = cursor.fetchone()
                folder_name = result[0] if result else "Not set"
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            message = (f"Task Settings for {display_name}:\n"
                       f"Primary Message: {task_config['message_link'] or 'Not set'}\n"
                       f"Fallback Message: {task_config['fallback_message_link'] or 'Not set'}\n"
                       f"Start Time: {format_lithuanian_time(task_config['start_time'])}\n"
                       f"Interval: {format_interval(task_config['repetition_interval'])}\n"
                       f"Target: {'All Groups' if task_config['send_to_all_groups'] else folder_name}\n"
                       f"Status: {task_config['status']}")
            keyboard = [
                [InlineKeyboardButton("Set Message", callback_data=f"set_message_{phone}")],
                [InlineKeyboardButton("Set Time", callback_data=f"set_time_{phone}")],
                [InlineKeyboardButton("Set Interval", callback_data=f"set_interval_{phone}")],
                [InlineKeyboardButton(get_text(user_id, 'select_target_groups'), callback_data=f"select_target_groups_{phone}")],
                [InlineKeyboardButton(f"{'Deactivate' if task_config['status'] == 'active' else 'Activate'}", callback_data=f"toggle_status_{phone}")],
                [InlineKeyboardButton("Save", callback_data=f"save_task_{phone}"), InlineKeyboardButton("Cancel", callback_data="cancel_task")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return TASK_SETUP

        elif data.startswith("save_task_"):
            phone = data.split("_")[2]
            task_config = context.user_data[f'task_config_{phone}']
            if not task_config.get('send_to_all_groups') and not task_config.get('folder_id'):
                query.edit_message_text("Please select a folder or choose to send to all groups.")
                return TASK_SETUP
            with db_lock:
                cursor.execute("INSERT OR REPLACE INTO userbot_settings (client_id, userbot_phone, message_link, fallback_message_link, start_time, repetition_interval, status, folder_id, send_to_all_groups) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (user_id, phone, task_config['message_link'], task_config['fallback_message_link'], task_config['start_time'], task_config['repetition_interval'], task_config['status'], task_config['folder_id'], task_config['send_to_all_groups']))
                db.commit()
            client, loop, lock = get_userbot_client(phone)
            if client and not task_config['send_to_all_groups'] and task_config['folder_id']:
                asyncio.run_coroutine_threadsafe(join_target_groups(client, lock, task_config['folder_id'], phone), loop)
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            query.edit_message_text(f"Task for userbot {display_name} has been set up and started. You’ll be notified when the first messages are sent.")
            del context.user_data[f'task_config_{phone}']
            context.user_data['setting_phone'] = None
            return ConversationHandler.END

        elif data.startswith("back_to_task_setup_"):
            phone = data.split("_")[3]
            context.user_data['setting_phone'] = phone
            task_config = context.user_data.get(f'task_config_{phone}', {})
            with db_lock:
                cursor.execute("SELECT name FROM folders WHERE id = ?", (task_config.get('folder_id'),))
                result = cursor.fetchone()
                folder_name = result[0] if result else "Not set"
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            message = (f"Task Settings for {display_name}:\n"
                       f"Primary Message: {task_config.get('message_link', 'Not set')}\n"
                       f"Fallback Message: {task_config.get('fallback_message_link', 'Not set')}\n"
                       f"Start Time: {format_lithuanian_time(task_config.get('start_time'))}\n"
                       f"Interval: {format_interval(task_config.get('repetition_interval'))}\n"
                       f"Target: {'All Groups' if task_config.get('send_to_all_groups') else folder_name}\n"
                       f"Status: {task_config.get('status', 'inactive')}")
            keyboard = [
                [InlineKeyboardButton("Set Message", callback_data=f"set_message_{phone}")],
                [InlineKeyboardButton("Set Time", callback_data=f"set_time_{phone}")],
                [InlineKeyboardButton("Set Interval", callback_data=f"set_interval_{phone}")],
                [InlineKeyboardButton(get_text(user_id, 'select_target_groups'), callback_data=f"select_target_groups_{phone}")],
                [InlineKeyboardButton(f"{'Deactivate' if task_config.get('status') == 'active' else 'Activate'}", callback_data=f"toggle_status_{phone}")],
                [InlineKeyboardButton("Save", callback_data=f"save_task_{phone}"), InlineKeyboardButton("Cancel", callback_data="cancel_task")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(message, reply_markup=markup)
            return TASK_SETUP

        elif data == "cancel_task":
            phone = context.user_data.get('setting_phone')
            if phone and f'task_config_{phone}' in context.user_data:
                del context.user_data[f'task_config_{phone}']
            context.user_data['setting_phone'] = None
            query.edit_message_text("Task setup cancelled.")
            return ConversationHandler.END

        elif data == "back_to_client_menu":
            return client_menu(update, context)

        elif data == "client_set_language":
            keyboard = [
                [InlineKeyboardButton("English", callback_data="lang_en")],
                [InlineKeyboardButton("Українська", callback_data="lang_uk")],
                [InlineKeyboardButton("Polski", callback_data="lang_pl")],
                [InlineKeyboardButton("Lietuvių", callback_data="lang_lt")],
                [InlineKeyboardButton("Русский", callback_data="lang_ru")],
                [InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")]
            ]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(get_text(user_id, 'select_language'), reply_markup=markup)
            return ConversationHandler.END

        elif data.startswith("lang_"):
            lang = data.split("_")[1]
            with db_lock:
                cursor.execute("UPDATE clients SET language = ? WHERE user_id = ?", (lang, user_id))
                db.commit()
            keyboard = [[InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data="back_to_client_menu")]]
            markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(get_text(user_id, 'language_set', lang=lang), reply_markup=markup)
            return ConversationHandler.END

    except Exception as e:
        log_event("Callback Error", f"User: {user_id}, Data: {data}, Error: {e}")
        query.edit_message_text(f"Error: {str(e)}. Please check your input and try again.")
        return ConversationHandler.END

def process_invitation_code(update: Update, context):
    try:
        code = update.message.text.strip()
        with db_lock:
            cursor.execute("SELECT subscription_end FROM clients WHERE invitation_code = ? AND user_id IS NULL", (code,))
            result = cursor.fetchone()
        if result and result[0] > int(datetime.now(utc_tz).timestamp()):
            user_id = update.message.from_user.id
            with db_lock:
                cursor.execute("UPDATE clients SET user_id = ? WHERE invitation_code = ?", (user_id, code))
                db.commit()
            log_event("Client Activated", f"User: {user_id}, Code: {code}")
            update.message.reply_text(get_text(user_id, 'account_activated'))
            return client_menu(update, context)
        update.message.reply_text(get_text(user_id, 'invalid_code'))
        return ConversationHandler.END
    except Exception as e:
        user_id = update.effective_user.id if update.effective_user else "Unknown"
        log_event("Invitation Code Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error: {str(e)}. Please check your invitation code and try again.")
        return ConversationHandler.END

def get_phone_number(update: Update, context):
    try:
        phone = update.message.text.strip()
        if not re.match(r'^\+\d{8,15}$', phone):
            update.message.reply_text("Invalid phone number. Use: +1234567890 (8-15 digits).")
            return WAITING_FOR_PHONE
        with db_lock:
            cursor.execute("SELECT phone_number FROM userbots WHERE phone_number = ?", (phone,))
            if cursor.fetchone():
                update.message.reply_text("Userbot with this phone number already exists.")
                return admin_panel(update, context)
        context.user_data['phone'] = phone
        update.message.reply_text("Enter API ID:")
        return WAITING_FOR_API_ID
    except Exception as e:
        log_event("Get Phone Number Error", f"Error: {e}")
        update.message.reply_text(f"Error: {str(e)}. Please try again.")
        return WAITING_FOR_PHONE

def get_api_id(update: Update, context):
    try:
        api_id = int(update.message.text.strip())
        if api_id <= 0:
            update.message.reply_text("API ID must be positive.")
            return WAITING_FOR_API_ID
        context.user_data['api_id'] = api_id
        update.message.reply_text("Enter API hash:")
        return WAITING_FOR_API_HASH
    except ValueError:
        update.message.reply_text("API ID must be a positive number.")
        return WAITING_FOR_API_ID
    except Exception as e:
        log_event("Get API ID Error", f"Error: {e}")
        update.message.reply_text(f"Error: {str(e)}. Please try again.")
        return WAITING_FOR_API_ID

def get_api_hash(update: Update, context):
    try:
        api_hash = update.message.text.strip()
        if not api_hash or len(api_hash) < 8:
            update.message.reply_text("API hash must be a non-empty string (min 8 characters). Please try again.")
            return WAITING_FOR_API_HASH
        
        phone = context.user_data.get('phone')
        api_id = context.user_data.get('api_id')
        session_file = f"./data/sessions/{phone}.session"
        
        context.user_data['api_hash'] = api_hash
        
        if os.path.exists(session_file):
            os.remove(session_file)
            logging.info(f"Removed existing session file for {phone}")
        
        future = asyncio.run_coroutine_threadsafe(create_client(session_file, api_id, api_hash), async_loop)
        client = future.result(timeout=10)
        context.user_data['client'] = client
        context.user_data['session_file'] = session_file
        
        future = asyncio.run_coroutine_threadsafe(async_connect_and_check(client, phone), async_loop)
        result = future.result(timeout=30)
        
        if result == "already_authorized":
            username = asyncio.run_coroutine_threadsafe(get_username_from_phone(client, phone), async_loop).result()
            with db_lock:
                cursor.execute("UPDATE userbots SET username = ? WHERE phone_number = ?", (username, phone))
                db.commit()
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            update.message.reply_text(f"Userbot {display_name} is already authorized.")
            asyncio.run_coroutine_threadsafe(client.disconnect(), async_loop).result()
            context.user_data.clear()
            return admin_panel(update, context)
        else:
            update.message.reply_text("Enter the code sent to your phone:")
            return WAITING_FOR_CODE_USERBOT
    except Exception as e:
        log_event("Get API Hash Error", f"Phone: {phone}, Error: {e}")
        update.message.reply_text(f"Error: {e}. Please try again.")
        return ConversationHandler.END

def get_code(update: Update, context):
    try:
        code = update.message.text.strip()
        required_keys = ['client', 'phone', 'api_id', 'api_hash', 'session_file']
        missing = [key for key in required_keys if key not in context.user_data]
        if missing:
            update.message.reply_text(f"Error: Missing data ({', '.join(missing)}). Please start over with /admin.")
            return admin_panel(update, context)
        
        client = context.user_data['client']
        phone = context.user_data['phone']

        future = asyncio.run_coroutine_threadsafe(async_sign_in(client, phone, code), async_loop)
        future.result(timeout=60)
        
        if asyncio.run_coroutine_threadsafe(client.is_user_authorized(), async_loop).result():
            username = asyncio.run_coroutine_threadsafe(get_username_from_phone(client, phone), async_loop).result()
            with db_lock:
                cursor.execute("INSERT INTO userbots (phone_number, session_file, status, api_id, api_hash, username) VALUES (?, ?, 'active', ?, ?, ?)",
                               (phone, context.user_data['session_file'], context.user_data['api_id'], context.user_data['api_hash'], username))
                db.commit()
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            log_event("Userbot Added", f"Phone: {phone}")
            notify_admins(context.bot, f"Userbot {display_name} added.")
            update.message.reply_text(f"Userbot {display_name} added successfully!")
        else:
            update.message.reply_text("Sign-in failed. Please check your code and try again.")
    except SessionPasswordNeededError:
        update.message.reply_text("Two-factor authentication is enabled. Please enter your password.")
        return WAITING_FOR_PASSWORD
    except Exception as e:
        log_event("Get Code Error", f"Phone: {phone}, Error: {e}")
        update.message.reply_text(f"Error: {e}. Please try again.")
    finally:
        if 'client' in context.user_data:
            asyncio.run_coroutine_threadsafe(async_disconnect(context.user_data['client']), async_loop).result()
        context.user_data.clear()
        return admin_panel(update, context)

def get_password(update: Update, context):
    try:
        password = update.message.text.strip()
        client = context.user_data['client']
        phone = context.user_data['phone']
        api_id = context.user_data['api_id']
        api_hash = context.user_data['api_hash']
        session_file = context.user_data['session_file']
        future = asyncio.run_coroutine_threadsafe(async_sign_in_with_password(client, password), async_loop)
        future.result(timeout=60)
        if asyncio.run_coroutine_threadsafe(client.is_user_authorized(), async_loop).result():
            username = asyncio.run_coroutine_threadsafe(get_username_from_phone(client, phone), async_loop).result()
            with db_lock:
                cursor.execute("INSERT INTO userbots (phone_number, session_file, status, api_id, api_hash, username) VALUES (?, ?, 'active', ?, ?, ?)",
                               (phone, session_file, api_id, api_hash, username))
                db.commit()
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            log_event("Userbot Added", f"Phone: {phone}")
            notify_admins(context.bot, f"Userbot {display_name} added.")
            update.message.reply_text(f"Userbot {display_name} added!")
        else:
            update.message.reply_text("Authentication failed.")
    except Exception as e:
        log_event("Get Password Error", f"Phone: {phone}, Error: {e}")
        update.message.reply_text(f"Error: {e}. Retry with /admin.")
    finally:
        if 'client' in context.user_data:
            asyncio.run_coroutine_threadsafe(async_disconnect(context.user_data['client']), async_loop).result()
        context.user_data.clear()
        return admin_panel(update, context)

def process_generate_invite(update: Update, context):
    try:
        match = re.match(r'(\d+)day (\d+)acc ?(\w+)?', update.message.text.strip())
        if not match:
            update.message.reply_text("Invalid format. Use: 30day 4acc [folder_name]")
            return WAITING_FOR_SUB_DETAILS
        days, num_userbots, folder_name = int(match.group(1)), int(match.group(2)), match.group(3)
        if days <= 0 or num_userbots <= 0:
            update.message.reply_text("Days and userbots must be positive.")
            return WAITING_FOR_SUB_DETAILS
        if folder_name:
            with db_lock:
                cursor.execute("SELECT id FROM folders WHERE name = ? AND created_by = ?", (folder_name, str(update.effective_user.id)))
                if not cursor.fetchone():
                    update.message.reply_text(f"Folder '{folder_name}' not found.")
                    return WAITING_FOR_SUB_DETAILS
        code = str(uuid.uuid4())[:8]
        sub_end = int((datetime.now(utc_tz) + timedelta(days=days)).timestamp())
        logging.info(f"Generated sub_end: {sub_end}, which is {datetime.fromtimestamp(sub_end, utc_tz)}")
        with db_lock:
            cursor.execute("SELECT phone_number FROM userbots WHERE assigned_client IS NULL LIMIT ?", (num_userbots,))
            available = [row[0] for row in cursor.fetchall()]
        if len(available) < num_userbots:
            update.message.reply_text("Not enough userbots available.")
            return admin_panel(update, context)
        userbot_phones = ",".join(available)
        with db_lock:
            cursor.execute("INSERT INTO clients (invitation_code, subscription_end, dedicated_userbots, folder_name) VALUES (?, ?, ?, ?)",
                           (code, sub_end, userbot_phones, folder_name))
            for phone in available:
                cursor.execute("UPDATE userbots SET assigned_client = ? WHERE phone_number = ?", (code, phone))
            db.commit()
        log_event("Invitation Generated", f"Code: {code}, Days: {days}, Userbots: {num_userbots}, Folder: {folder_name or 'None'}")
        update.message.reply_text(f"Invitation code: {code}")
        return admin_panel(update, context)
    except Exception as e:
        log_event("Generate Invite Error", f"Error: {e}")
        update.message.reply_text(f"Error: {e}. Use format: 30day 4acc [folder_name]")
        return WAITING_FOR_SUB_DETAILS

def process_folder_name(update: Update, context):
    try:
        folder_name = update.message.text.strip()
        user_id = update.effective_user.id
        with db_lock:
            cursor.execute("SELECT id FROM folders WHERE name = ? AND created_by = ?", (folder_name, str(user_id)))
            if cursor.fetchone():
                update.message.reply_text(f"Folder '{folder_name}' already exists. Please choose a different name.")
                return WAITING_FOR_FOLDER_NAME
            cursor.execute("INSERT INTO folders (name, created_by) VALUES (?, ?)", (folder_name, str(user_id)))
            folder_id = cursor.lastrowid
            db.commit()
        context.user_data['folder_name'] = folder_name
        context.user_data['folder_id'] = folder_id
        update.message.reply_text(f"Folder '{folder_name}' created. Now, send target group link(s) (one per line):")
        return WAITING_FOR_GROUP_URLS
    except Exception as e:
        log_event("Folder Name Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error creating folder: {e}")
        return ConversationHandler.END

def process_add_group(update: Update, context):
    try:
        text = update.message.text.strip()
        user_id = update.effective_user.id
        if text.lower() == 'done':
            update.message.reply_text("Finished adding groups.")
            return client_menu(update, context)
        urls = [url.strip() for url in text.split('\n') if url.strip()]
        folder_id = context.user_data.get('folder_id')
        if not folder_id:
            update.message.reply_text("No folder selected. Please start over.")
            return JoyceHandler.END
        
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            userbots_str = result[0] if result else ""
        if not userbots_str:
            update.message.reply_text("No userbots assigned.")
            return ConversationHandler.END
        phone = userbots_str.split(",")[0]
        client, loop, lock = get_userbot_client(phone)
        if not client:
            update.message.reply_text("Failed to initialize userbot client.")
            return ConversationHandler.END

        with db_lock:
            cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
            result = cursor.fetchone()
            username = result[0] if result and result[0] else None
        display_name = f"@{username}" if username else f"{phone} (no username set)"
        
        async def run_add_groups_task():
            async with lock:
                await client.start()
                try:
                    results = await add_groups_to_folder(client, urls, folder_id, str(user_id))
                finally:
                    await client.disconnect()
            return results

        results = asyncio.run_coroutine_threadsafe(run_add_groups_task(), loop).result(timeout=180)
        success_count = 0
        feedback = []
        addlist_detected = False
        failed_urls = []
        for url, (success, msg) in zip(urls, results):
            if success:
                success_count += 1
            else:
                failed_urls.append(url)
            if "Addlist links not supported" in msg:
                addlist_detected = True
            feedback.append(f"{url}: {msg}")
        update.message.reply_text(f"Added {success_count} out of {len(urls)} group(s) to folder.")
        if feedback:
            update.message.reply_text("Details:\n" + "\n".join(feedback))
        if addlist_detected:
            update.message.reply_text("Note: Addlist links (e.g., https://t.me/addlist/...) are not supported. Please provide individual group links instead.")
        return WAITING_FOR_GROUP_URLS
    except Exception as e:
        log_event("Add Group Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error adding groups: {e}")
        return ConversationHandler.END

def process_group_links(update: Update, context):
    """Process group links for joining or folder management."""
    try:
        user_id = update.effective_user.id
        links = [url.strip() for url in update.message.text.split('\n') if url.strip()]
        selected_userbot = context.user_data.get('selected_userbot')
        folder_id = context.user_data.get('selected_folder_id')
        folder_action = context.user_data.get('folder_action')

        if selected_userbot:  # Handling "Join Target Groups"
            with db_lock:
                cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
                userbots_str = result[0] if result else ""
            if not userbots_str:
                update.message.reply_text("No userbots assigned.")
                return ConversationHandler.END
            phones = userbots_str.split(",") if selected_userbot == "all" else [selected_userbot]
            folder_id = None  # Explicitly set to None for "Join Target Groups"
            for phone in phones:
                client, loop, lock = get_userbot_client(phone)
                if not client:
                    update.message.reply_text(f"Failed to initialize userbot {phone}.")
                    continue
                async def join_groups_task():
                    async with lock:
                        await client.start()
                        try:
                            results = await join_groups(client, links, folder_id, phone, str(user_id))
                        finally:
                            await client.disconnect()
                        return results
                results = asyncio.run_coroutine_threadsafe(join_groups_task(), loop).result()
                success_count = 0
                feedback = []
                for url, result in zip(links, results):
                    if isinstance(result, Exception):
                        feedback.append(f"{url}: Failed - {str(result)}")
                    else:
                        success, msg = result
                        if success:
                            success_count += 1
                        feedback.append(f"{url}: {msg}")
                with db_lock:
                    cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                    result = cursor.fetchone()
                    username = result[0] if result and result[0] else None
                display_name = f"@{username}" if username else f"{phone} (no username set)"
                update.message.reply_text(f"Userbot {display_name} added {success_count} out of {len(links)} groups.")
                if feedback:
                    update.message.reply_text("Details:\n" + "\n".join(feedback))
            context.user_data.clear()
            return client_menu(update, context)

        elif folder_action:  # Handling "Manage Folders"
            if not folder_id:
                update.message.reply_text("No folder selected. Please start over.")
                return ConversationHandler.END
            with db_lock:
                cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
                userbots_str = result[0] if result else ""
            if not userbots_str:
                update.message.reply_text("No userbots assigned.")
                return ConversationHandler.END
            phone = userbots_str.split(",")[0]
            client, loop, lock = get_userbot_client(phone)
            if not client:
                update.message.reply_text("Failed to initialize userbot client.")
                return ConversationHandler.END

            async def manage_folder_task():
                async with lock:
                    await client.start()
                    try:
                        if folder_action == "update_folder_list":
                            with db_lock:
                                cursor.execute("DELETE FROM target_groups WHERE folder_id = ? AND added_by = ?", (folder_id, str(user_id)))
                                db.commit()
                        results = await add_groups_to_folder(client, links, folder_id, str(user_id))
                    finally:
                        await client.disconnect()
                    return results

            results = asyncio.run_coroutine_threadsafe(manage_folder_task(), loop).result(timeout=180)
            success_count = 0
            feedback = []
            for url, (success, msg) in zip(links, results):
                if success:
                    success_count += 1
                feedback.append(f"{url}: {msg}")
            action_text = "updated" if folder_action == "update_folder_list" else "added to"
            update.message.reply_text(f"Folder {action_text} with {success_count} out of {len(links)} groups.")
            if feedback:
                update.message.reply_text("Details:\n" + "\n".join(feedback))
            context.user_data.clear()
            return client_menu(update, context)

    except Exception as e:
        log_event("Process Group Links Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error processing group links: {e}")
        return ConversationHandler.END

def process_primary_message_link(update: Update, context):
    try:
        user_id = update.effective_user.id
        link = update.message.text.strip()
        phone = context.user_data.get('setting_phone')
        if not phone:
            update.message.reply_text("No userbot selected. Please start over.")
            return ConversationHandler.END
        task_config = context.user_data[f'task_config_{phone}']
        client, loop, lock = get_userbot_client(phone)
        if not client:
            update.message.reply_text(f"Failed to initialize userbot {phone}.")
            return ConversationHandler.END

        async def verify_link():
            async with lock:
                await client.start()
                try:
                    await get_message_from_link(client, link)
                    return True, None
                except Exception as e:
                    return False, str(e)
                finally:
                    await client.disconnect()

        success, error = asyncio.run_coroutine_threadsafe(verify_link(), loop).result()
        if not success:
            update.message.reply_text(f"Invalid message link: {error}")
            return WAITING_FOR_PRIMARY_MESSAGE_LINK

        task_config['message_link'] = link
        keyboard = [[InlineKeyboardButton("Add Fallback Message", callback_data=f"add_fallback_{phone}"), InlineKeyboardButton("Back to Task Setup", callback_data=f"back_to_task_setup_{phone}")]]
        markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text("Primary message link set. Add a fallback message or return to task setup:", reply_markup=markup)
        return WAITING_FOR_FALLBACK_MESSAGE_LINK

    except Exception as e:
        log_event("Primary Message Link Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error setting message link: {e}")
        return ConversationHandler.END

def process_fallback_message_link(update: Update, context):
    try:
        user_id = update.effective_user.id
        link = update.message.text.strip()
        phone = context.user_data.get('setting_phone')
        if not phone:
            update.message.reply_text("No userbot selected. Please start over.")
            return ConversationHandler.END
        task_config = context.user_data[f'task_config_{phone}']
        client, loop, lock = get_userbot_client(phone)
        if not client:
            update.message.reply_text(f"Failed to initialize userbot {phone}.")
            return ConversationHandler.END

        async def verify_link():
            async with lock:
                await client.start()
                try:
                    await get_message_from_link(client, link)
                    return True, None
                except Exception as e:
                    return False, str(e)
                finally:
                    await client.disconnect()

        success, error = asyncio.run_coroutine_threadsafe(verify_link(), loop).result()
        if not success:
            update.message.reply_text(f"Invalid fallback message link: {error}")
            return WAITING_FOR_FALLBACK_MESSAGE_LINK

        task_config['fallback_message_link'] = link
        return back_to_task_setup(update, context, phone)

    except Exception as e:
        log_event("Fallback Message Link Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error setting fallback message link: {e}")
        return ConversationHandler.END

def process_start_time(update: Update, context):
    try:
        user_id = update.effective_user.id
        time_str = update.message.text.strip()
        phone = context.user_data.get('setting_phone')
        if not phone:
            update.message.reply_text("No userbot selected. Please start over.")
            return ConversationHandler.END
        task_config = context.user_data[f'task_config_{phone}']
        start_time = parse_lithuanian_time(time_str)
        if not start_time:
            update.message.reply_text("Invalid time format. Use HH:MM (e.g., 17:30).")
            return WAITING_FOR_START_TIME
        task_config['start_time'] = start_time
        return back_to_task_setup(update, context, phone)
    except Exception as e:
        log_event("Start Time Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error setting start time: {e}")
        return ConversationHandler.END

def back_to_task_setup(update: Update, context, phone=None):
    if not phone:
        phone = context.user_data.get('setting_phone')
    if not phone:
        update.message.reply_text("No userbot selected. Please start over.")
        return ConversationHandler.END
    user_id = update.effective_user.id
    task_config = context.user_data.get(f'task_config_{phone}', {})
    with db_lock:
        cursor.execute("SELECT name FROM folders WHERE id = ?", (task_config.get('folder_id'),))
        result = cursor.fetchone()
        folder_name = result[0] if result else "Not set"
    with db_lock:
        cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
        result = cursor.fetchone()
        username = result[0] if result and result[0] else None
    display_name = f"@{username}" if username else f"{phone} (no username set)"
    message = (f"Task Settings for {display_name}:\n"
               f"Primary Message: {task_config.get('message_link', 'Not set')}\n"
               f"Fallback Message: {task_config.get('fallback_message_link', 'Not set')}\n"
               f"Start Time: {format_lithuanian_time(task_config.get('start_time'))}\n"
               f"Interval: {format_interval(task_config.get('repetition_interval'))}\n"
               f"Target: {'All Groups' if task_config.get('send_to_all_groups') else folder_name}\n"
               f"Status: {task_config.get('status', 'inactive')}")
    keyboard = [
        [InlineKeyboardButton("Set Message", callback_data=f"set_message_{phone}")],
        [InlineKeyboardButton("Set Time", callback_data=f"set_time_{phone}")],
        [InlineKeyboardButton("Set Interval", callback_data=f"set_interval_{phone}")],
        [InlineKeyboardButton(get_text(user_id, 'select_target_groups'), callback_data=f"select_target_groups_{phone}")],
        [InlineKeyboardButton(f"{'Deactivate' if task_config.get('status') == 'active' else 'Activate'}", callback_data=f"toggle_status_{phone}")],
        [InlineKeyboardButton("Save", callback_data=f"save_task_{phone}"), InlineKeyboardButton("Cancel", callback_data="cancel_task")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(message, reply_markup=markup)
    return TASK_SETUP

def process_extend_code(update: Update, context):
    try:
        code = update.message.text.strip()
        user_id = update.effective_user.id
        with db_lock:
            cursor.execute("SELECT subscription_end FROM clients WHERE invitation_code = ?", (code,))
            result = cursor.fetchone()
        if not result:
            update.message.reply_text("Invalid code.")
            return admin_panel(update, context)
        context.user_data['extend_code'] = code
        update.message.reply_text("Enter the number of days to extend the subscription:")
        return WAITING_FOR_EXTEND_DAYS
    except Exception as e:
        log_event("Extend Code Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error: {e}")
        return ConversationHandler.END

def process_extend_days(update: Update, context):
    try:
        days = int(update.message.text.strip())
        if days <= 0:
            update.message.reply_text("Number of days must be positive.")
            return WAITING_FOR_EXTEND_DAYS
        code = context.user_data.get('extend_code')
        if not code:
            update.message.reply_text("No code provided. Please start over.")
            return admin_panel(update, context)
        with db_lock:
            cursor.execute("SELECT subscription_end FROM clients WHERE invitation_code = ?", (code,))
            result = cursor.fetchone()
            if not result:
                update.message.reply_text("Invalid code.")
                return admin_panel(update, context)
            current_end = result[0]
            new_end = current_end + (days * 86400)
            cursor.execute("UPDATE clients SET subscription_end = ? WHERE invitation_code = ?", (new_end, code))
            db.commit()
        log_event("Subscription Extended", f"Code: {code}, Days: {days}, New End: {datetime.fromtimestamp(new_end)}")
        update.message.reply_text(f"Subscription extended by {days} days. New end date: {datetime.fromtimestamp(new_end).strftime('%Y-%m-%d')}")
        context.user_data.clear()
        return admin_panel(update, context)
    except ValueError:
        update.message.reply_text("Please enter a valid number of days.")
        return WAITING_FOR_EXTEND_DAYS
    except Exception as e:
        log_event("Extend Days Error", f"User: {update.effective_user.id}, Error: {e}")
        update.message.reply_text(f"Error: {e}")
        return ConversationHandler.END

def process_add_userbots_code(update: Update, context):
    try:
        code = update.message.text.strip()
        user_id = update.effective_user.id
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE invitation_code = ?", (code,))
            result = cursor.fetchone()
        if not result:
            update.message.reply_text("Invalid code.")
            return admin_panel(update, context)
        context.user_data['add_userbots_code'] = code
        update.message.reply_text("Enter the number of additional userbots to assign:")
        return WAITING_FOR_ADD_USERBOTS_COUNT
    except Exception as e:
        log_event("Add Userbots Code Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text(f"Error: {e}")
        return ConversationHandler.END

def process_add_userbots_count(update: Update, context):
    try:
        count = int(update.message.text.strip())
        if count <= 0:
            update.message.reply_text("Number of userbots must be positive.")
            return WAITING_FOR_ADD_USERBOTS_COUNT
        code = context.user_data.get('add_userbots_code')
        if not code:
            update.message.reply_text("No code provided. Please start over.")
            return admin_panel(update, context)
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE invitation_code = ?", (code,))
            result = cursor.fetchone()
            if not result:
                update.message.reply_text("Invalid code.")
                return admin_panel(update, context)
            current_userbots = result[0].split(",") if result[0] else []
            cursor.execute("SELECT phone_number FROM userbots WHERE assigned_client IS NULL LIMIT ?", (count,))
            available = [row[0] for row in cursor.fetchall()]
        if len(available) < count:
            update.message.reply_text(f"Not enough available userbots. Only {len(available)} available.")
            return admin_panel(update, context)
        new_userbots = current_userbots + available
        with db_lock:
            cursor.execute("UPDATE clients SET dedicated_userbots = ? WHERE invitation_code = ?", (",".join(new_userbots), code))
            for phone in available:
                cursor.execute("UPDATE userbots SET assigned_client = ? WHERE phone_number = ?", (code, phone))
            db.commit()
        log_event("Userbots Added", f"Code: {code}, Added: {count}, Total: {len(new_userbots)}")
        update.message.reply_text(f"Added {count} userbots to client with code {code}. Total userbots: {len(new_userbots)}")
        context.user_data.clear()
        return admin_panel(update, context)
    except ValueError:
        update.message.reply_text("Please enter a valid number of userbots.")
        return WAITING_FOR_ADD_USERBOTS_COUNT
    except Exception as e:
        log_event("Add Userbots Count Error", f"User: {update.effective_user.id}, Error: {e}")
        update.message.reply_text(f"Error: {e}")
        return ConversationHandler.END

async def forward_message(client, chat_id, from_chat, message_id, phone, user_id):
    try:
        await client.forward_messages(chat_id, message_id, from_chat)
        with db_lock:
            cursor.execute("UPDATE clients SET total_messages_sent = total_messages_sent + 1 WHERE user_id = ?", (user_id,))
            db.commit()
        log_event("Message Forwarded", f"Phone: {phone}, Chat ID: {chat_id}, Message ID: {message_id}")
        return True, None
    except ChatSendMediaForbiddenError:
        return False, "Cannot send media to this chat."
    except FloodWaitError as e:
        return False, f"Flood wait error: {e.seconds} seconds"
    except Exception as e:
        return False, str(e)

async def execute_task(client, lock, settings, user_id, bot):
    async with lock:
        try:
            await client.start()
            current_time = int(datetime.now(utc_tz).timestamp())
            if settings['start_time'] > current_time:
                return
            if settings['repetition_interval']:
                last_run = settings.get('last_run', settings['start_time'])
                if current_time < last_run + settings['repetition_interval'] * 60:
                    return
            with db_lock:
                if settings['send_to_all_groups']:
                    dialogs = await client.get_dialogs()
                    target_groups = [(dialog.entity.id, dialog.entity.title) for dialog in dialogs if dialog.is_group]
                else:
                    cursor.execute("SELECT group_id, group_name FROM target_groups WHERE folder_id = ?", (settings['folder_id'],))
                    target_groups = cursor.fetchall()
            if not target_groups:
                log_event("No Target Groups", f"Phone: {settings['userbot_phone']}, User: {user_id}")
                return

            from_chat, message_id = await get_message_from_link(client, settings['message_link'])
            fallback_chat, fallback_id = (await get_message_from_link(client, settings['fallback_message_link']) 
                                        if settings['fallback_message_link'] else (None, None))

            success_count = 0
            errors = []
            for group_id, group_name in target_groups:
                logging.info(f"Attempting to forward message to group {group_name} (ID: {group_id})")
                success, error = await forward_message(client, PeerChannel(group_id), from_chat, message_id, settings['userbot_phone'], user_id)
                if not success and fallback_chat and error == "Cannot send media to this chat.":
                    logging.info(f"Using fallback message for group {group_name}")
                    success, error = await forward_message(client, PeerChannel(group_id), fallback_chat, fallback_id, settings['userbot_phone'], user_id)
                if success:
                    success_count += 1
                    logging.info(f"Successfully forwarded message to group {group_name}")
                else:
                    errors.append(f"{group_name}: {error}")
                    logging.error(f"Failed to forward message to group {group_name}: {error}")
            with db_lock:
                cursor.execute("UPDATE clients SET groups_reached = groups_reached + ? WHERE user_id = ?", (success_count, user_id))
                cursor.execute("UPDATE userbot_settings SET last_run = ? WHERE client_id = ? AND userbot_phone = ?", 
                               (current_time, user_id, settings['userbot_phone']))
                db.commit()
            log_event("Task Executed", f"Phone: {settings['userbot_phone']}, User: {user_id}, Success: {success_count}, Errors: {len(errors)}")
            if success_count > 0:
                bot.send_message(user_id, f"Userbot {settings['userbot_phone']} forwarded messages to {success_count} groups.")
            if errors:
                bot.send_message(user_id, "Errors:\n" + "\n".join(errors[:5]))
        except Exception as e:
            log_event("Task Execution Error", f"Phone: {settings['userbot_phone']}, User: {user_id}, Error: {e}")
        finally:
            await client.disconnect()

async def check_tasks(bot):
    while True:
        try:
            with db_lock:
                cursor.execute("SELECT client_id, userbot_phone, message_link, fallback_message_link, start_time, repetition_interval, status, folder_id, send_to_all_groups FROM userbot_settings WHERE status = 'active'")
                tasks = cursor.fetchall()
            for task in tasks:
                settings = dict(zip(['client_id', 'userbot_phone', 'message_link', 'fallback_message_link', 'start_time', 'repetition_interval', 'status', 'folder_id', 'send_to_all_groups'], task))
                client, loop, lock = get_userbot_client(settings['userbot_phone'])
                if client:
                    asyncio.run_coroutine_threadsafe(execute_task(client, lock, settings, settings['client_id'], bot), loop)
        except Exception as e:
            log_event("Check Tasks Error", f"Error: {e}")
        await asyncio.sleep(CHECK_TASKS_INTERVAL)

# Main bot setup
async_loop = asyncio.new_event_loop()
def run_async_loop():
    asyncio.set_event_loop(async_loop)
    async_loop.run_forever()

threading.Thread(target=run_async_loop, daemon=True).start()
asyncio.run_coroutine_threadsafe(check_tasks(updater.bot), async_loop)

conv_handler = ConversationHandler(
    entry_points=[
        CommandHandler('start', start),
        CommandHandler('admin', admin_panel),
        CallbackQueryHandler(handle_callback),
    ],
    states={
        WAITING_FOR_CODE: [MessageHandler(Filters.text & ~Filters.command, process_invitation_code)],
        WAITING_FOR_PHONE: [MessageHandler(Filters.text & ~Filters.command, get_phone_number)],
        WAITING_FOR_API_ID: [MessageHandler(Filters.text & ~Filters.command, get_api_id)],
        WAITING_FOR_API_HASH: [MessageHandler(Filters.text & ~Filters.command, get_api_hash)],
        WAITING_FOR_CODE_USERBOT: [MessageHandler(Filters.text & ~Filters.command, get_code)],
        WAITING_FOR_PASSWORD: [MessageHandler(Filters.text & ~Filters.command, get_password)],
        WAITING_FOR_SUB_DETAILS: [MessageHandler(Filters.text & ~Filters.command, process_generate_invite)],
        WAITING_FOR_GROUP_URLS: [MessageHandler(Filters.text & ~Filters.command, process_add_group)],
        WAITING_FOR_FOLDER_CHOICE: [CallbackQueryHandler(handle_callback)],
        WAITING_FOR_FOLDER_NAME: [MessageHandler(Filters.text & ~Filters.command, process_folder_name)],
        WAITING_FOR_FOLDER_SELECTION: [CallbackQueryHandler(handle_callback)],
        TASK_SETUP: [CallbackQueryHandler(handle_callback)],
        WAITING_FOR_LANGUAGE: [CallbackQueryHandler(handle_callback)],
        WAITING_FOR_EXTEND_CODE: [MessageHandler(Filters.text & ~Filters.command, process_extend_code)],
        WAITING_FOR_EXTEND_DAYS: [MessageHandler(Filters.text & ~Filters.command, process_extend_days)],
        WAITING_FOR_ADD_USERBOTS_CODE: [MessageHandler(Filters.text & ~Filters.command, process_add_userbots_code)],
        WAITING_FOR_ADD_USERBOTS_COUNT: [MessageHandler(Filters.text & ~Filters.command, process_add_userbots_count)],
        SELECT_TARGET_GROUPS: [CallbackQueryHandler(handle_callback)],
        WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(handle_callback)],
        WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command, process_group_links)],
        WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(handle_callback)],
        WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, process_primary_message_link)],
        WAITING_FOR_FALLBACK_MESSAGE_LINK: [
            MessageHandler(Filters.text & ~Filters.command, process_fallback_message_link),
            CallbackQueryHandler(handle_callback, pattern=r'^add_fallback_.*|^back_to_task_setup_.*')
        ],
        WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command, process_start_time)],
    },
    fallbacks=[CommandHandler('start', start)]
)

dp.add_handler(conv_handler)

# Start the bot
updater.start_polling()
updater.idle()
