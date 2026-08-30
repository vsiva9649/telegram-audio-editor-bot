# ============================================================
# Telegram Audio Editor Bot
# Author : Siva
# GitHub : https://github.com/vsiva9649
# Repository: telegram-audio-editor-bot
# ============================================================

import sys
sys.stdout = open("bot.log", "a", buffering=1, encoding="utf-8")
sys.stderr = sys.stdout

import os
import re
import asyncio
import math
import time
import random
import subprocess
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode

# Monkeypatch InlineKeyboardButton to support 'style' parameter (Bot API 7.10+)
_old_ikb_init = InlineKeyboardButton.__init__
def _new_ikb_init(self, *args, **kwargs):
    kwargs.pop("style", None)
    _old_ikb_init(self, *args, **kwargs)
InlineKeyboardButton.__init__ = _new_ikb_init
from pyrogram.errors import FloodWait
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, APIC, TIT2, TPE1, error as ID3Error
from mutagen.mp4 import MP4, MP4Cover
from mutagen.flac import FLAC, Picture
from mutagen import MutagenError
import mutagen
import aiosqlite
from pydub import AudioSegment
import aiohttp
import aiofiles

http_session = None

def get_http_session():
    global http_session
    if http_session is None or http_session.closed:
        connector = aiohttp.TCPConnector(
            limit=100,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            keepalive_timeout=30
        )
        http_session = aiohttp.ClientSession(connector=connector)
    return http_session

# --- CONFIGURATION ---
API_ID = 
API_HASH = ""
BOT_TOKEN = ""
SUPER_ADMIN_ID = 
DB_FILE = "bot_data.db"
DOWNLOAD_DIR = "downloads"

executor = ThreadPoolExecutor(max_workers=32)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

app = Client("audio_bulk_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, max_concurrent_transmissions=15)

user_data = {}
admin_state = {}
user_queues = {}
user_tasks = {}
queue_status_msgs = {}      # chat_id -> {"msg": message, "count": int}
queue_status_locks = {}     # chat_id -> asyncio.Lock
last_progress_texts = {}    # (chat_id, msg_id) -> last text to avoid MessageNotModified
IN_FLIGHT_USAGE = {}        # user_id -> count of in-flight message processing tasks

SPINNER = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']

# --- Global limits to prevent Telegram API FloodWaits ---
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(100)  # Support high traffic
UPLOAD_SEMAPHORE = asyncio.Semaphore(100)    # Support high traffic

def get_user_data(chat_id):
    if chat_id not in user_data:
        user_data[chat_id] = {
            'artist': None,
            'remove_artist': False,
            'image_path': None,
            'remove_cover': False,  # <--- ADD THIS LINE
            'title_prefix': None,
            'counter': 1,
            'voice_tag_path': None,
            'voice_tag_position': None,
            'voice_tag_type': None,
            'temp_voice_tag_path': None,
            'temp_voice_tag_position': None,
            'target_channel': None,
            'batch_mode': False,
            'batch_first_link': None,
            'batch_last_link': None,
            'batch_chat_id': None,
            'merge_mode': False,
            'merge_audios': [],
            'merge_timer_task': None,
            'caption_enabled': True,
            'awaiting': None,
            'trim_start': None,
            'trim_end': None,
        }
    return user_data[chat_id]

def get_admin_state(chat_id):
    return admin_state.get(chat_id, {})

def set_admin_state(chat_id, **kwargs):
    admin_state[chat_id] = kwargs

def clear_admin_state(chat_id):
    admin_state.pop(chat_id, None)

# --- DATABASE HANDLER ---
class Database:
    def __init__(self, db_path):
        self.db_path = db_path

    async def _execute(self, query, params=(), fetch_one=False, fetch_all=False):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(query, params)
            await db.commit()
            if fetch_one:
                return await cursor.fetchone()
            if fetch_all:
                return await cursor.fetchall()
            return None

    async def init_db(self):
        await self._execute('''CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY,
            added_by INTEGER,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        await self._execute('''CREATE TABLE IF NOT EXISTS subscriptions (
            user_id INTEGER PRIMARY KEY,
            expiry_date TEXT NOT NULL,
            added_by INTEGER,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        await self._execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        await self._execute('''CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )''')
        await self._execute('''CREATE TABLE IF NOT EXISTS usage_tracker (
            user_id INTEGER,
            count INTEGER DEFAULT 0,
            last_date TEXT,
            PRIMARY KEY (user_id, last_date)
        )''')
        
        # Insert default settings if not exists
        await self._execute('''INSERT OR IGNORE INTO settings (key, value) VALUES ('mode', 'subscription')''')
        await self._execute('''INSERT OR IGNORE INTO settings (key, value) VALUES ('daily_free_limit', '10')''')

        # Auto-extract users from admins and subscriptions on start
        await self._execute('''INSERT OR IGNORE INTO users (user_id) 
                               SELECT user_id FROM admins 
                               UNION 
                               SELECT user_id FROM subscriptions''')
        await self._execute('INSERT OR IGNORE INTO admins (user_id, added_by) VALUES (?, ?)',
                            (SUPER_ADMIN_ID, SUPER_ADMIN_ID))

    async def get_setting(self, key, default=None):
        row = await self._execute('SELECT value FROM settings WHERE key = ?', (key,), fetch_one=True)
        return row['value'] if row else default

    async def set_setting(self, key, value):
        await self._execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, str(value)))

    async def get_user_usage(self, user_id, current_date):
        row = await self._execute('SELECT count FROM usage_tracker WHERE user_id = ? AND last_date = ?',
                                   (user_id, current_date), fetch_one=True)
        return row['count'] if row else 0

    async def increment_user_usage(self, user_id, current_date):
        current_count = await self.get_user_usage(user_id, current_date)
        await self._execute('INSERT OR REPLACE INTO usage_tracker (user_id, count, last_date) VALUES (?, ?, ?)',
                             (user_id, current_count + 1, current_date))

    async def add_user(self, user_id):
        await self._execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (user_id,))

    async def get_all_users(self):
        rows = await self._execute('SELECT user_id FROM users', fetch_all=True)
        return [row['user_id'] for row in rows]

    async def get_users_count(self):
        row = await self._execute('SELECT COUNT(*) as count FROM users', fetch_one=True)
        return row['count'] if row else 0

    async def add_admin(self, user_id, added_by):
        await self._execute('INSERT OR IGNORE INTO admins (user_id, added_by) VALUES (?, ?)',
                            (user_id, added_by))

    async def remove_admin(self, user_id):
        await self._execute('DELETE FROM admins WHERE user_id = ?', (user_id,))

    async def is_admin(self, user_id):
        if user_id == SUPER_ADMIN_ID:
            return True
        row = await self._execute('SELECT 1 FROM admins WHERE user_id = ?', (user_id,), fetch_one=True)
        return row is not None

    async def list_admins(self):
        rows = await self._execute('SELECT user_id, added_by, added_at FROM admins ORDER BY added_at', fetch_all=True)
        return rows

    async def add_subscription(self, user_id, days, added_by):
        expiry = (datetime.now() + timedelta(days=days)).isoformat()
        await self._execute('''INSERT OR REPLACE INTO subscriptions (user_id, expiry_date, added_by)
                               VALUES (?, ?, ?)''', (user_id, expiry, added_by))

    async def remove_subscription(self, user_id):
        await self._execute('DELETE FROM subscriptions WHERE user_id = ?', (user_id,))

    async def get_subscription(self, user_id):
        row = await self._execute('SELECT expiry_date FROM subscriptions WHERE user_id = ?',
                                   (user_id,), fetch_one=True)
        if row:
            return datetime.fromisoformat(row['expiry_date'])
        return None

    async def is_subscription_valid(self, user_id):
        if await self.is_admin(user_id):
            return True
        expiry = await self.get_subscription(user_id)
        return expiry and expiry > datetime.now()

    async def list_subscriptions(self):
        rows = await self._execute('SELECT user_id, expiry_date, added_by, added_at FROM subscriptions ORDER BY added_at', fetch_all=True)
        return rows

    async def export_backup(self):
        users_rows = await self._execute('SELECT * FROM users', fetch_all=True)
        admins_rows = await self._execute('SELECT * FROM admins', fetch_all=True)
        subs_rows = await self._execute('SELECT * FROM subscriptions', fetch_all=True)
        
        return {
            'users': [dict(row) for row in users_rows] if users_rows else [],
            'admins': [dict(row) for row in admins_rows] if admins_rows else [],
            'subscriptions': [dict(row) for row in subs_rows] if subs_rows else []
        }

    async def import_backup(self, backup_data):
        await self._execute('DELETE FROM users')
        await self._execute('DELETE FROM admins')
        await self._execute('DELETE FROM subscriptions')
        
        users = backup_data.get('users', [])
        for u in users:
            await self._execute('INSERT OR REPLACE INTO users (user_id, added_at) VALUES (?, ?)',
                                (u['user_id'], u['added_at']))
                                
        admins = backup_data.get('admins', [])
        for a in admins:
            await self._execute('INSERT OR REPLACE INTO admins (user_id, added_by, added_at) VALUES (?, ?, ?)',
                                (a['user_id'], a['added_by'], a['added_at']))
                                
        subs = backup_data.get('subscriptions', [])
        for s in subs:
            await self._execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry_date, added_by, added_at) VALUES (?, ?, ?, ?)',
                                (s['user_id'], s['expiry_date'], s['added_by'], s['added_at']))
                                
        # Make sure SUPER_ADMIN is always admin
        await self._execute('INSERT OR IGNORE INTO admins (user_id, added_by) VALUES (?, ?)',
                            (SUPER_ADMIN_ID, SUPER_ADMIN_ID))

db = Database(DB_FILE)

@app.on_message(group=-1)
async def auto_add_user(client, message):
    if message.from_user:
        await db.add_user(message.from_user.id)

def extract_episode_number(text):
    if not text:
        return None
    # Clean the text: remove temp_audio prefix and extensions
    clean = re.sub(r'temp_audio_\d+_\d+(?:\.\d+)?', '', text, flags=re.IGNORECASE)
    clean = re.sub(r'\.(mp3|m4a|mp4|flac|ogg|wav)$', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b(128|192|256|320|64)k(?:bps)?\b', '', clean, flags=re.IGNORECASE)
    
    # Try explicit patterns like "Ep 3009", "Episode - 3009"
    explicit_match = re.search(r'\b(?:ep|episode|part|ch|chapter|track)\s*(?:-|_)?\s*(\d+)\b', clean, flags=re.IGNORECASE)
    if explicit_match:
        result = int(explicit_match.group(1))
        print(f"DEBUG: Number Extraction | Input: '{text}' | Explicit Match: {result}")
        return result
    
    # Extract all numbers
    numbers = re.findall(r'\d+', clean)
    if not numbers:
        return None
    
    # Find the best candidate
    result = int(numbers[-1])
    print(f"DEBUG: Number Extraction | Input: '{text}' | Selected Last: {result}")
    return result

def get_sorting_name(message):
    """Combines fields to ensure the episode number can be found, avoiding bot-generated filenames."""
    parts = []
    if message.audio:
        if message.audio.title:
            parts.append(message.audio.title)
        if message.audio.file_name:
            parts.append(message.audio.file_name)
    if message.document and message.document.file_name:
        parts.append(message.document.file_name)
    if message.caption:
        parts.append(message.caption.split('\n')[0].strip())
    
    return " ".join(parts)

def get_clean_title(message):
    # Try to find the single most representative title source to avoid duplication
    title = ""
    if message.audio:
        if message.audio.title:
            title = message.audio.title
        elif message.audio.file_name:
            title = message.audio.file_name
    elif message.document and message.document.file_name:
        title = message.document.file_name
    
    if not title and message.caption:
        title = message.caption.split('\n')[0].strip()
        
    if not title:
        return ""

    # Clean the text: remove temp_audio prefix and extensions
    clean = re.sub(r'temp_audio_\d+_\d+(?:\.\d+)?', '', title, flags=re.IGNORECASE)
    clean = re.sub(r'\.(mp3|m4a|mp4|flac|ogg|wav)$', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b(128|192|256|320|64)k(?:bps)?\b', '', clean, flags=re.IGNORECASE)
    return clean.strip()

def get_message_title(message):
    """Extracts a usable title string from a Telegram message for numbering/sorting."""
    if message.audio and message.audio.title:
        return message.audio.title
    if message.caption:
        # Use first line of caption, removing potential markdown/tags
        return message.caption.split('\n')[0].strip()
    if message.document and message.document.file_name:
        return message.document.file_name
    return ""

def get_mumbai_date_str():
    # Indian Standard Time (IST) is UTC + 5:30
    utc_now = datetime.utcnow()
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now.strftime("%Y-%m-%d")

async def is_authorized(user_id):
    if await is_admin_user(user_id):
        return True
    mode = await db.get_setting('mode', 'subscription')
    if mode == 'public':
        return True
    return await db.is_subscription_valid(user_id)

async def is_admin_user(user_id):
    return await db.is_admin(user_id)

async def check_user_limit(user_id, chat_id, additional_count=1):
    # Returns (is_ok, current_usage, limit, message_to_user)
    if await db.is_subscription_valid(user_id):
        return True, 0, 0, ""
    
    mode = await db.get_setting('mode', 'subscription')
    if mode == 'subscription':
        return False, 0, 0, "❌ This bot is in Subscription Mode. Only subscribed users can use it."
    
    limit_str = await db.get_setting('daily_free_limit', '10')
    try:
        limit = int(limit_str)
    except ValueError:
        limit = 10
    
    today_str = get_mumbai_date_str()
    usage = await db.get_user_usage(user_id, today_str)
    
    # queue size
    queue = user_queues.get(chat_id)
    queue_size = queue.qsize() if queue else 0
    
    # active tasks size
    active_tasks = 1 if (chat_id in user_tasks and not user_tasks[chat_id].done()) else 0
    
    # merge size
    data = get_user_data(chat_id)
    merge_size = len(data.get('merge_audios', [])) if data.get('merge_mode') else 0
    
    # in-flight count
    in_flight = IN_FLIGHT_USAGE.get(user_id, 0)
    
    total_requested = usage + queue_size + active_tasks + merge_size + in_flight + additional_count
    if total_requested > limit:
        remaining = max(0, limit - usage - queue_size - active_tasks)
        return False, usage, limit, (
            f"❌ **Daily Limit Exceeded!**\n\n"
            f"As a free user, your daily limit is **{limit}** files (resets at 12:00 AM IST).\n"
            f"You have used **{usage}** files today, and have **{queue_size + active_tasks}** files currently processing/queued.\n"
            f"Remaining allowance: **{remaining}** files.\n\n"
            f"Please subscribe using /mysub or contact an admin to get unlimited access!"
        )
    return True, usage, limit, ""

# --- Utility: Async retry with exponential backoff ---
async def call_with_idle_timeout(func, *args, idle_timeout=60, last_update_ref=None, **kwargs):
    task = asyncio.create_task(func(*args, **kwargs))
    while not task.done():
        done, pending = await asyncio.wait([task], timeout=2.0)
        if task in done:
            break
        if last_update_ref and (time.time() - last_update_ref[0] > idle_timeout):
            task.cancel()
            raise TimeoutError(f"Task idle for more than {idle_timeout} seconds")
    return await task

async def retry_async(func, *args, max_retries=3, base_delay=1, backoff=2, **kwargs):
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            # If Telegram asks to wait more than 60s, abort to save the bot from a ban
            if e.value > 60: 
                raise e
            await asyncio.sleep(e.value + random.uniform(0.5, 1.5))
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (backoff ** attempt) + random.uniform(0, 1)
            await asyncio.sleep(delay)

# --- Progress throttling helpers ---
def should_update_progress(chat_id, msg_id, text):
    key = (chat_id, msg_id)
    last = last_progress_texts.get(key)
    if last == text:
        return False
    last_progress_texts[key] = text
    return True

async def throttle_progress_edit(message, text):
    if not should_update_progress(message.chat.id, message.id, text):
        return
    try:
        await message.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        await message.edit_text(text)
    except Exception:
        pass

# --- Throttled download/upload progress callbacks ---
async def download_progress(current, total, message, start_time, last_update):
    if total == 0:
        return
    now = time.time()
    if current != total and (now - last_update[0]) < 2.0:
        return
    last_update[0] = now
    percent = current * 100 / total
    elapsed = datetime.now() - start_time
    speed = current / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
    speed_mb = speed / 1024 / 1024
    downloaded_mb = current / 1024 / 1024
    total_mb = total / 1024 / 1024
    bar = '█' * int(percent // 10) + '░' * (10 - int(percent // 10))
    text = (
        f"⏳ **Downloading...** `{bar}` {percent:.1f}%\n"
        f"📥 `{downloaded_mb:.2f} MB / {total_mb:.2f} MB`\n"
        f"⚡ `{speed_mb:.2f} MB/s`"
    )
    await throttle_progress_edit(message, text)

async def upload_progress(current, total, message, start_time, last_update):
    if total == 0:
        return
    now = time.time()
    if current != total and (now - last_update[0]) < 2.0:
        return
    last_update[0] = now
    percent = current * 100 / total
    elapsed = datetime.now() - start_time
    speed = current / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
    speed_mb = speed / 1024 / 1024
    uploaded_mb = current / 1024 / 1024
    total_mb = total / 1024 / 1024
    bar = '█' * int(percent // 10) + '░' * (10 - int(percent // 10))
    text = (
        f"⬆️ **Uploading...** `{bar}` {percent:.1f}%\n"
        f"📤 `{uploaded_mb:.2f} MB / {total_mb:.2f} MB`\n"
        f"⚡ `{speed_mb:.2f} MB/s`"
    )
    await throttle_progress_edit(message, text)

# --- Download/Upload Progress Simulation Helpers ---
async def simulate_download_progress(progress_msg, expected_size, start_time, stop_event):
    if not progress_msg or expected_size <= 0:
        return
    total_mb = expected_size / 1024 / 1024
    steps = [3, 8, 15, 23, 31, 39, 47, 54, 61, 67, 73, 78, 83, 87, 90, 93, 95, 97, 98, 99]
    step_idx = 0
    while not stop_event.is_set():
        if step_idx < len(steps):
            percent = steps[step_idx]
            step_idx += 1
        else:
            percent = 99
        elapsed = datetime.now() - start_time
        elapsed_sec = elapsed.total_seconds()
        simulated_current = expected_size * (percent / 100.0)
        speed = simulated_current / elapsed_sec if elapsed_sec > 0 else 0
        speed_mb = speed / 1024 / 1024
        downloaded_mb = simulated_current / 1024 / 1024
        bar = '█' * int(percent // 10) + '░' * (10 - int(percent // 10))
        text = (
            f"⏳ **Downloading...** `{bar}` {percent:.1f}%\n"
            f"📥 `{downloaded_mb:.2f} MB / {total_mb:.2f} MB`\n"
            f"⚡ `{speed_mb:.2f} MB/s`"
        )
        await throttle_progress_edit(progress_msg, text)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.5)
        except asyncio.TimeoutError:
            pass

async def simulate_upload_progress(progress_msg, expected_size, start_time, stop_event):
    if not progress_msg or expected_size <= 0:
        return
    total_mb = expected_size / 1024 / 1024
    steps = [2, 7, 13, 20, 28, 36, 44, 52, 59, 66, 72, 77, 82, 86, 89, 92, 94, 96, 97, 98, 99]
    step_idx = 0
    while not stop_event.is_set():
        if step_idx < len(steps):
            percent = steps[step_idx]
            step_idx += 1
        else:
            percent = 99
        elapsed = datetime.now() - start_time
        elapsed_sec = elapsed.total_seconds()
        simulated_current = expected_size * (percent / 100.0)
        speed = simulated_current / elapsed_sec if elapsed_sec > 0 else 0
        speed_mb = speed / 1024 / 1024
        uploaded_mb = simulated_current / 1024 / 1024
        bar = '█' * int(percent // 10) + '░' * (10 - int(percent // 10))
        text = (
            f"⬆️ **Uploading...** `{bar}` {percent:.1f}%\n"
            f"📤 `{uploaded_mb:.2f} MB / {total_mb:.2f} MB`\n"
            f"⚡ `{speed_mb:.2f} MB/s`"
        )
        await throttle_progress_edit(progress_msg, text)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.5)
        except asyncio.TimeoutError:
            pass

import io

class ProgressFileReader(io.IOBase):
    def __init__(self, filepath, progress_cb, progress_msg, start_time, last_update):
        super().__init__()
        self.filepath = filepath
        self.file = open(filepath, 'rb')
        self.progress_cb = progress_cb
        self.progress_msg = progress_msg
        self.start_time = start_time
        self.last_update = last_update
        self.total = os.path.getsize(filepath)
        self.current = 0
        self._loop = asyncio.get_event_loop()

    def read(self, size=-1):
        chunk = self.file.read(size)
        if chunk:
            self.current += len(chunk)
            if self.progress_cb and self.progress_msg:
                asyncio.run_coroutine_threadsafe(
                    self.progress_cb(self.current, self.total, self.progress_msg, self.start_time, self.last_update),
                    self._loop
                )
        return chunk

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        return self.file.seek(offset, whence)

    def tell(self):
        return self.file.tell()

    def __len__(self):
        return self.total

    def close(self):
        self.file.close()
        super().close()

# --- Download via Bot API with progress reporting ---
async def download_via_bot_api(file_id, file_path, progress_msg, start_time, expected_size):
    token = BOT_TOKEN
    
    # Download 100% locally via Local Bot API
    get_file_url = f"http://127.0.0.1:8081/bot{token}/getFile?file_id={file_id}"
    
    stop_event = asyncio.Event()
    sim_task = None
    if progress_msg and expected_size > 0:
        sim_task = asyncio.create_task(
            simulate_download_progress(progress_msg, expected_size, start_time, stop_event)
        )
    elif progress_msg:
        await throttle_progress_edit(progress_msg, "📥 **Downloading via local server...**")
        
    try:
        result = None
        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            try:
                session = get_http_session()
                async with session.get(get_file_url, timeout=aiohttp.ClientTimeout(total=1800, connect=15)) as resp:
                    if resp.status == 200:
                        res_json = await resp.json()
                        if res_json.get("ok"):
                            result = res_json
                            break
                        else:
                            desc = res_json.get("description", "")
                            if "too many requests" in desc.lower() or "retry after" in desc.lower():
                                match = re.search(r"retry after\s+(\d+)", desc, re.IGNORECASE)
                                retry_after = int(match.group(1)) if match else 5
                                await asyncio.sleep(retry_after)
                                continue
                            raise Exception(f"Local Bot API getFile error: {desc}")
                    elif resp.status == 429:
                        retry_after = int(resp.headers.get('Retry-After', 5))
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        error_text = await resp.text()
                        raise Exception(f"Failed getFile from local Bot API (HTTP {resp.status}): {error_text}")
            except (aiohttp.ClientError, ConnectionResetError, asyncio.TimeoutError):
                attempt += 1
                if attempt >= max_attempts:
                    raise
                await asyncio.sleep(2 ** (attempt - 1))
            except Exception:
                attempt += 1
                if attempt >= max_attempts:
                    raise
                await asyncio.sleep(2 ** (attempt - 1))
                
        if not result:
            raise Exception("Failed to get file info from local Bot API after retries")
            
        file_path_tg = result["result"]["file_path"]
        file_size_tg = result["result"].get("file_size", expected_size)
                
        # Local Bot API returns the real filesystem path.
        # Use it directly; do not remap /var/lib/telegram-bot-api/.
        local_path = file_path_tg
            
        if os.path.exists(local_path):
            def perform_copy():
                shutil.copy(local_path, file_path)
                try:
                    os.remove(local_path)
                except Exception as e:
                    print(f"Failed to remove bot api local file: {e}")
            await asyncio.get_event_loop().run_in_executor(executor, perform_copy)
        else:
            raise FileNotFoundError(f"Local file not found at {local_path}")
            
        # Verify file size
        actual_size = os.path.getsize(file_path)
        if file_size_tg > 0 and actual_size != file_size_tg:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise TimeoutError(f"Incomplete download: {actual_size}/{file_size_tg} bytes.")
        elif actual_size == 0:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise TimeoutError("Downloaded file is empty (0 bytes).")
            
        stop_event.set()
        if sim_task:
            try:
                await sim_task
            except:
                pass
            sim_task = None
            
        if progress_msg:
            elapsed = datetime.now() - start_time
            duration = elapsed.total_seconds()
            speed = actual_size / duration if duration > 0 else actual_size
            speed_mb = speed / 1024 / 1024
            size_mb = actual_size / 1024 / 1024
            bar = '█' * 10
            text = (
                f"⏳ **Downloading...** `{bar}` 100.0%\n"
                f"📥 `{size_mb:.2f} MB / {size_mb:.2f} MB`\n"
                f"⚡ `{speed_mb:.2f} MB/s`"
            )
            await throttle_progress_edit(progress_msg, text)
            await asyncio.sleep(0.5)
            
        return file_path
    finally:
        stop_event.set()
        if sim_task:
            try:
                await sim_task
            except:
                pass

# --- Unified Download Selector ---
async def download_file_with_selector(message, target_path, progress_msg=None, start_time=None):
    media = message.audio or message.document or message.voice or message.video
    if not media:
        raise FileNotFoundError("No downloadable media found in message")
    
    expected_size = getattr(media, 'file_size', 0)
    if not start_time:
        start_time = datetime.now()
        
    return await download_via_bot_api(media.file_id, target_path, progress_msg, start_time, expected_size)

UPLOAD_COUNTER = 0

# --- Upload via Bot API / Pyrogram with retry, flood handling and hybrid balance ---
async def upload_via_bot_api(chat_id, file_path, performer, title, thumb_path, progress_msg, start_time, duration=0, caption=None):
    global UPLOAD_COUNTER
    if not start_time:
        start_time = datetime.now()
        
    token = BOT_TOKEN
    file_size = os.path.getsize(file_path)
    
    # Rule: If file size > 50 MB, use local Bot API to upload.
    is_local_upload = file_size > 50 * 1024 * 1024
    
    if progress_msg:
        await throttle_progress_edit(progress_msg, "⬆️ **Starting upload...**")
        
    if is_local_upload:
        max_retries = 5
        for attempt in range(max_retries):
            try:
                url = f"http://127.0.0.1:8081/bot{token}/sendAudio"
                import shutil
                import uuid
                temp_filename = f"upload_{uuid.uuid4().hex}.wav"
                shared_path_host = os.path.join("/var/lib/telegram-bot-api", temp_filename)
                shutil.copy(file_path, shared_path_host)
                os.chmod(shared_path_host, 0o666)
                
                shared_thumb_host = None
                if thumb_path:
                    temp_thumb = f"thumb_{uuid.uuid4().hex}.jpg"
                    shared_thumb_host = os.path.join("/var/lib/telegram-bot-api", temp_thumb)
                    shutil.copy(thumb_path, shared_thumb_host)
                    os.chmod(shared_thumb_host, 0o666)
                    
                data = {
                    'chat_id': str(chat_id),
                    'audio': f"file://{shared_path_host}"
                }
                if performer: data['performer'] = performer
                if title: data['title'] = title
                if duration: data['duration'] = str(duration)
                if caption: data['caption'] = caption
                if shared_thumb_host: data['thumbnail'] = f"file://{shared_thumb_host}"
                
                async with UPLOAD_SEMAPHORE:
                    session = get_http_session()
                    async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=1800, connect=30)) as resp:
                        res = await resp.json()
                        try:
                            os.remove(shared_path_host)
                            if shared_thumb_host:
                                os.remove(shared_thumb_host)
                        except Exception:
                            pass
                        if not res.get("ok"):
                            desc = res.get('description', '')
                            if "too many requests" in desc.lower() or "retry after" in desc.lower():
                                match = re.search(r"retry after\s+(\d+)", desc, re.IGNORECASE)
                                retry_after = int(match.group(1)) if match else 5
                                await asyncio.sleep(retry_after)
                                continue
                            raise Exception(f"sendAudio error: {desc}")
                        if progress_msg:
                            await throttle_progress_edit(progress_msg, "⬆️ **Upload complete!**")
                        return res["result"]["message_id"]
            except (aiohttp.ClientError, ConnectionResetError, asyncio.TimeoutError) as e:
                print(f"DEBUG: Connection error during local upload (attempt {attempt+1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2)

    # For files <= 50 MB: Alternating & Fallback between Bot API & Pyrogram (MTProto)
    UPLOAD_COUNTER += 1
    prefer_pyrogram = (UPLOAD_COUNTER % 2 == 0)

    async def try_bot_api_upload():
        url = f"https://api.telegram.org/bot{token}/sendAudio"
        data = aiohttp.FormData()
        data.add_field('chat_id', str(chat_id))
        data.add_field('audio', open(file_path, 'rb'))
        if performer: data.add_field('performer', performer)
        if title: data.add_field('title', title)
        if duration: data.add_field('duration', str(duration))
        if caption: data.add_field('caption', caption)
        if thumb_path and os.path.exists(thumb_path): data.add_field('thumbnail', open(thumb_path, 'rb'))
        
        async with UPLOAD_SEMAPHORE:
            session = get_http_session()
            async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=1800, connect=30)) as resp:
                res = await resp.json()
                if not res.get("ok"):
                    desc = res.get('description', '')
                    raise Exception(f"Bot API sendAudio error: {desc}")
                if progress_msg:
                    await throttle_progress_edit(progress_msg, "⬆️ **Upload complete!**")
                return res["result"]["message_id"]

    async def try_pyrogram_upload():
        last_update = [time.time()]
        async def pyro_cb(current, total):
            if progress_msg:
                await upload_progress(current, total, progress_msg, start_time, last_update)
                
        valid_thumb = thumb_path if (thumb_path and os.path.exists(thumb_path)) else None
        async with UPLOAD_SEMAPHORE:
            sent_msg = await app.send_audio(
                chat_id=chat_id,
                audio=file_path,
                caption=caption if caption else None,
                performer=performer if performer else None,
                title=title if title else None,
                duration=int(duration) if duration and int(duration) > 0 else None,
                thumb=valid_thumb,
                progress=pyro_cb if progress_msg else None
            )
            if progress_msg:
                await throttle_progress_edit(progress_msg, "⬆️ **Upload complete!**")
            return sent_msg.id

    methods = [try_pyrogram_upload, try_bot_api_upload] if prefer_pyrogram else [try_bot_api_upload, try_pyrogram_upload]

    for primary_method, fallback_method in [(methods[0], methods[1]), (methods[1], methods[0])]:
        try:
            return await primary_method()
        except FloodWait as e:
            print(f"DEBUG: Primary upload method hit FloodWait ({e.value}s). Trying fallback method...")
            try:
                return await fallback_method()
            except Exception as fe:
                print(f"DEBUG: Fallback upload also failed: {fe}. Sleeping for FloodWait ({e.value}s)...")
                await asyncio.sleep(e.value)
        except Exception as e:
            err_str = str(e).lower()
            if "flood" in err_str or "too many requests" in err_str or "429" in err_str:
                print(f"DEBUG: Rate limit on upload: {e}. Trying fallback method...")
                try:
                    return await fallback_method()
                except Exception as fe:
                    print(f"DEBUG: Fallback upload failed: {fe}")
                    await asyncio.sleep(5)
            else:
                try:
                    return await fallback_method()
                except Exception:
                    raise e

    return await methods[0]()

# --- Audio processing queue logic ---
async def audio_processor(chat_id):
    queue = user_queues.get(chat_id)
    if not queue:
        return
    while True:
        try:
            # Wait for the first item
            item = await asyncio.wait_for(queue.get(), timeout=300)
            
            # Settle period: wait a bit to group multiple uploads for sorting
            items_to_process = [item]
            try:
                # Give a 5-second window to catch more files sent in a burst
                while True:
                    next_item = await asyncio.wait_for(queue.get(), timeout=5)
                    items_to_process.append(next_item)
            except asyncio.TimeoutError:
                pass
            
            # Sort grouped items by (found_number, message_id) to ensure correct order
            def sort_key(x):
                # Use filename-based extraction for sorting
                num = extract_episode_number(get_sorting_name(x['message']))
                # If no number is found, use a very large value to put unnumbered files at the end
                # but keep them sorted by message ID relative to each other.
                return (num if num is not None else float('inf'), x['message'].id)

            items_to_process.sort(key=sort_key)

            for it in items_to_process:
                try:
                    await process_single_audio(it['client'], it['message'], it['data'], chat_id)
                except Exception as e:
                    print(f"Error processing audio for {chat_id}: {e}")
                finally:
                    queue.task_done()

            if queue.empty() and chat_id in queue_status_msgs:
                entry = queue_status_msgs[chat_id]
                msg_obj = entry.get("msg")
                if msg_obj:
                    try:
                        await msg_obj.delete()
                    except Exception as e:
                        print(f"DEBUG: failed to delete queue status msg: {e}")
                del queue_status_msgs[chat_id]
        except asyncio.TimeoutError:
            if chat_id in user_tasks:
                del user_tasks[chat_id]
            if chat_id in user_queues:
                del user_queues[chat_id]
            if chat_id in queue_status_msgs:
                del queue_status_msgs[chat_id]
            break

async def process_single_audio(client, message, data, chat_id):
    file_name = None
    mime_type = None

    if message.audio:
        file_name = message.audio.file_name
        mime_type = message.audio.mime_type
    elif message.document:
        file_name = message.document.file_name
        mime_type = message.document.mime_type
    elif message.voice:
        file_name = "voice.ogg"
        mime_type = message.voice.mime_type
    elif message.video:
        file_name = message.video.file_name
        mime_type = message.video.mime_type

    if not file_name:
        if message.audio: file_name = "audio.mp3"
        elif message.voice: file_name = "voice.ogg"
        elif message.video: file_name = "video.mp4"
        else: file_name = "file.mp3"

    ext = os.path.splitext(file_name)[1].lower()
    supported_formats = ['.mp3', '.m4a', '.mp4', '.flac', '.ogg', '.wav']

    # If extension is empty or not in supported formats, try to guess from mime_type or file type
    if ext not in supported_formats:
        guess = None
        if mime_type:
            if "mpeg" in mime_type or "mp3" in mime_type: guess = ".mp3"
            elif "mp4" in mime_type: guess = ".m4a"
            elif "flac" in mime_type: guess = ".flac"
            elif "ogg" in mime_type: guess = ".ogg"
            elif "wav" in mime_type or "wave" in mime_type: guess = ".wav"
        
        if guess:
            ext = guess
        elif message.audio:
            ext = ".mp3"
        elif message.voice:
            ext = ".ogg"
        elif message.video:
            ext = ".mp4"

    if ext not in supported_formats:
        err = await client.send_message(chat_id, f"❌ Unsupported format ({ext}).")
        return

    msg = await client.send_message(chat_id, "⏳ Starting...")
    start_time = datetime.now()

    import random; import string; salt = "".join(random.choices(string.ascii_letters, k=5)); temp_filename = f"temp_audio_{chat_id}_{datetime.now().timestamp()}_{salt}{ext}"
    audio_path = os.path.join(DOWNLOAD_DIR, temp_filename)

    try:
        downloaded_path = await download_file_with_selector(message, audio_path, progress_msg=msg, start_time=start_time)
        if not downloaded_path or not os.path.exists(downloaded_path):
            raise FileNotFoundError(f"Downloaded file not found at {downloaded_path}")

        # Voice tag insertion
        voice_tag_path = data.get('voice_tag_path')
        voice_tag_position = data.get('voice_tag_position')
        voice_tag_type = data.get('voice_tag_type', 'insert')
        if voice_tag_path and os.path.exists(voice_tag_path) and voice_tag_position:
            if not await db.is_subscription_valid(chat_id):
                data['voice_tag_path'] = None
                data['voice_tag_position'] = None
                data['voice_tag_type'] = None
                try:
                    os.remove(voice_tag_path)
                except:
                    pass
                await msg.edit_text("⚠️ **Voice tag skipped (Feature requires active subscription)**")
                await asyncio.sleep(1)
            else:
                await msg.edit_text("🎤 **Inserting voice tag...**")
            try:
                def insert_tag():
                    main_audio = AudioSegment.from_file(downloaded_path)
                    tag_audio = AudioSegment.from_file(voice_tag_path)
                    modified = insert_voice_tag(main_audio, tag_audio, voice_tag_position, voice_tag_type)
                    format_map = {'.mp3': 'mp3', '.m4a': 'mp4', '.mp4': 'mp4', '.flac': 'flac', '.ogg': 'ogg'}
                    export_format = format_map.get(ext, ext[1:])
                    output_path = downloaded_path + ".combined" + ext
                    modified.export(output_path, format=export_format)
                    return output_path

                output_path = await asyncio.get_event_loop().run_in_executor(executor, insert_tag)
                os.remove(downloaded_path)
                os.rename(output_path, downloaded_path)
                await msg.edit_text("✅ **Voice tag inserted**")
                await asyncio.sleep(0.5)
            except Exception as e:
                await msg.edit_text(f"⚠️ Voice tag insertion failed: {e}")
                await asyncio.sleep(1)

        # Determine the title to use
        current_title = None

        if data['title_prefix'] is not None:
            # Use manual counter strictly for naming to ensure 1, 2, 3... sequence
            current_title = f"{data['title_prefix']} {data['counter']}".strip()
            data['counter'] += 1
        else:
            # "Keep Original Title" is active - Try to extract real metadata first
            extracted_title = None
            try:
                # Let Mutagen auto-detect the file type instead of trusting the extension
                meta = mutagen.File(downloaded_path)
                if meta is not None:
                    if isinstance(meta, MP3) and meta.tags and meta.tags.getall("TIT2"):
                        extracted_title = str(meta.tags.getall("TIT2")[0].text[0])
                    elif isinstance(meta, MP4) and meta.tags and '\xa9nam' in meta.tags:
                        extracted_title = str(meta.tags['\xa9nam'][0])
                    elif isinstance(meta, FLAC) and 'title' in meta and meta['title']:
                        extracted_title = str(meta['title'][0])
            except Exception as e:
                print(f"Could not extract metadata: {e}")

            # Priority Fallback Chain: 
            # 1. Caption -> 2. Real File Metadata -> 3. Telegram Audio Title -> 4. Filename
            caption = message.caption.strip() if message.caption else ""
            
            if caption:
                current_title = caption
            else:
                current_title = get_clean_title(message)
                if not current_title:
                    if extracted_title:
                        current_title = extracted_title
                    elif message.audio and message.audio.title:
                        current_title = message.audio.title
                    else:
                        current_title = file_name or "Unknown Title"

            # Apply trims if the user configured them
            if data.get('trim_start') and isinstance(data['trim_start'], int) and data['trim_start'] > 0:
                trim_len = data['trim_start']
                current_title = current_title[trim_len:] if len(current_title) > trim_len else ""
            if data.get('trim_end') and isinstance(data['trim_end'], int) and data['trim_end'] > 0:
                trim_len = data['trim_end']
                current_title = current_title[:-trim_len] if len(current_title) > trim_len else ""

        # --- NEW CODE START ---
        # Deep clean hidden video/image streams if removing cover or setting a new one
        if data.get('remove_cover') or (data.get('image_path') and os.path.exists(data['image_path'])):
            await throttle_progress_edit(msg, "🧹 **Stripping hidden art streams...**")
            def deep_clean_audio():
                clean_path = downloaded_path + ".clean" + ext
                # -vn -sn -dn explicitly drops video, subtitles, and data streams
                # -map_metadata -1 wipes all global metadata
                cmd = ["ffmpeg", "-y", "-i", downloaded_path, "-map", "0:a:0", "-c:a", "copy", "-map_metadata", "-1", "-vn", "-sn", "-dn", clean_path]
                try:
                    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, timeout=30)
                except Exception as e:
                    print(f"DEBUG: deep_clean_audio subprocess error: {e}")
                if os.path.exists(clean_path) and os.path.getsize(clean_path) > 0:
                    shutil.move(clean_path, downloaded_path)
                    return True
                return False

            try:
                cleaned = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(executor, deep_clean_audio),
                    timeout=35.0
                )
                if not cleaned:
                    print(f"DEBUG: Deep clean did not produce a valid file for {chat_id}")
            except Exception as e:
                print(f"Deep clean failed: {e}")
        # --- NEW CODE END ---

        await throttle_progress_edit(msg, "⚙️ **Processing metadata...**")

        def edit_metadata():
            # If removing cover, we start by completely deleting all tags for a fresh start
            if data.get('remove_cover'):
                try:
                    m = mutagen.File(downloaded_path)
                    if m:
                        m.delete()
                        m.save()
                except:
                    pass

            audio_file = mutagen.File(downloaded_path)
            
            if audio_file is None:
                print("Unrecognized audio format. Skipping metadata edit.")
                return

            # Determine Artist to use
            artist_to_set = data.get('artist')
            if data.get('remove_artist'):
                artist_to_set = "Unknown Artist"

            import random
            import string
            salt_str = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

            if isinstance(audio_file, MP3):
                if audio_file.tags is None:
                    try:
                        audio_file.add_tags()
                    except ID3Error:
                        pass
                
                if audio_file.tags is not None:
                    # Double check removal of all possible image tags
                    audio_file.tags.delall("APIC")
                    audio_file.tags.delall("PIC")
                    
                    if artist_to_set:
                        audio_file.tags.delall("TPE1")
                        audio_file.tags.add(TPE1(encoding=3, text=artist_to_set))
                    if current_title:
                        audio_file.tags.delall("TIT2")
                        audio_file.tags.add(TIT2(encoding=3, text=current_title))
                    
                    # Image Handling
                    if data.get('image_path') and os.path.exists(data['image_path']):
                        with open(data['image_path'], 'rb') as img:
                            audio_file.tags.add(APIC(encoding=3, mime='image/jpeg', type=3, desc='Cover', data=img.read()))
                        
                    from mutagen.id3 import COMM
                    audio_file.tags.delall("COMM")
                    audio_file.tags.add(COMM(encoding=3, lang='eng', desc='salt', text=[salt_str]))
                    audio_file.save(v2_version=3)
                
            elif isinstance(audio_file, MP4):
                if audio_file.tags is None:
                    try:
                        audio_file.add_tags()
                    except:
                        pass
                
                if audio_file.tags is not None:
                    # Force remove cover atom
                    if 'covr' in audio_file.tags:
                        del audio_file.tags['covr']
                    
                    if artist_to_set:
                        audio_file.tags['\xa9ART'] = [artist_to_set]
                    if current_title:
                        audio_file.tags['\xa9nam'] = [current_title]
                    
                    # Image Handling
                    if data.get('image_path') and os.path.exists(data['image_path']):
                        with open(data['image_path'], 'rb') as img:
                            audio_file.tags['covr'] = [MP4Cover(img.read(), imageformat=MP4Cover.FORMAT_JPEG)]
                            
                    audio_file.tags['\xa9cmt'] = [salt_str]
                    audio_file.save()
                
            elif isinstance(audio_file, FLAC):
                # Force remove all pictures
                audio_file.clear_pictures()
                
                if artist_to_set:
                    audio_file['artist'] = [artist_to_set]
                if current_title:
                    audio_file['title'] = [current_title]
                
                # Image Handling
                if data.get('image_path') and os.path.exists(data['image_path']):
                    pic = Picture()
                    pic.type = 3
                    pic.mime = "image/jpeg"
                    with open(data['image_path'], 'rb') as img:
                        pic.data = img.read()
                    audio_file.add_picture(pic)
                    
                audio_file['comment'] = [salt_str]
                audio_file.save()

        try:
            await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(executor, edit_metadata),
                timeout=30.0
            )
        except asyncio.TimeoutError:
            print(f"DEBUG: edit_metadata timed out for {chat_id}")
        except Exception as e:
            print(f"DEBUG: edit_metadata failed: {e}")

        await throttle_progress_edit(msg, "✅ **Metadata updated**")

        # --- Get Audio Duration ---
        audio_duration = 0
        def get_duration():
            duration = 0
            try:
                # Primary: ffprobe (most reliable)
                cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", downloaded_path]
                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, stdin=subprocess.DEVNULL, timeout=10)
                out = res.stdout.strip()
                if out and out != "N/A":
                    duration = int(float(out))
                    print(f"DEBUG: ffprobe duration: {duration}s")
            except Exception as e:
                print(f"DEBUG: ffprobe failed: {e}")
            
            if duration <= 0:
                try:
                    # Secondary: mutagen
                    meta = mutagen.File(downloaded_path)
                    if meta and hasattr(meta, 'info') and hasattr(meta.info, 'length'):
                        duration = int(meta.info.length)
                        print(f"DEBUG: mutagen duration: {duration}s")
                except Exception as e:
                    print(f"DEBUG: mutagen failed: {e}")
            
            if duration <= 0:
                try:
                    # Tertiary: pydub
                    duration = int(len(AudioSegment.from_file(downloaded_path)) / 1000)
                    print(f"DEBUG: pydub duration: {duration}s")
                except Exception as e:
                    print(f"DEBUG: pydub failed: {e}")
            return duration

        try:
            audio_duration = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(executor, get_duration),
                timeout=15.0
            )
        except Exception as e:
            print(f"DEBUG: get_duration error/timeout: {e}")
            audio_duration = 0

        # Upload
        target = data.get('target_channel')
        send_to_chat = target if target else chat_id
        thumb_path = data['image_path'] if (data['image_path'] and os.path.exists(data['image_path'])) else None
        
        # Prepare thumbnail for Telegram (must be <= 320x320 and <= 200KB)
        final_thumb_path = None
        if thumb_path and os.path.exists(thumb_path):
            resized_thumb_path = os.path.join(DOWNLOAD_DIR, f"thumb_{chat_id}_{datetime.now().timestamp()}.jpg")
            def resize_thumb():
                cmd = [
                    "ffmpeg", "-y", "-i", thumb_path,
                    "-vf", "scale=w=320:h=320:force_original_aspect_ratio=decrease",
                    "-q:v", "2",
                    resized_thumb_path
                ]
                try:
                    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, timeout=10)
                except Exception as e:
                    print(f"DEBUG: resize_thumb subprocess error: {e}")
                if os.path.exists(resized_thumb_path) and os.path.getsize(resized_thumb_path) > 0:
                    return resized_thumb_path
                return None
            
            try:
                final_thumb_path = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(executor, resize_thumb),
                    timeout=15.0
                )
            except Exception as e:
                print(f"DEBUG: resize_thumb timeout/error: {e}")
                final_thumb_path = None

        if data.get('remove_cover'):
            thumb_path = None
            final_thumb_path = None
            data['image_path'] = None

        file_size = os.path.getsize(downloaded_path)
        upload_start = datetime.now()

        if msg:
            await throttle_progress_edit(msg, "⏳ **Waiting for an upload slot...**")

        async with UPLOAD_SEMAPHORE:
            if msg:
                await throttle_progress_edit(msg, "⬆️ **Starting upload...**")
            sent_message_id = await upload_via_bot_api(
                chat_id=send_to_chat,
                file_path=downloaded_path,
                performer=data['artist'] if data['artist'] else None,
                title=current_title,
                thumb_path=final_thumb_path if final_thumb_path else thumb_path,
                progress_msg=msg,
                start_time=upload_start,
                duration=audio_duration,
                caption=current_title if data.get('caption_enabled', True) else None
            )

        if not await db.is_subscription_valid(chat_id):
            await db.increment_user_usage(chat_id, get_mumbai_date_str())

        # Only delete the progress message, keep everything else
        await msg.delete()

    except FloodWait as e:
        await msg.edit_text(f"⏳ **Telegram Rate Limit Hit!**\nTelegram requires a strict cooldown of {e.value} seconds before processing more files. The bot is overloaded. Please wait and try again later.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)}")
        if data['title_prefix'] is not None and not (message.caption and message.caption.strip()):
            data['counter'] -= 1
    finally:
        if os.path.exists(audio_path):
            try:
                os.remove(audio_path)
            except:
                pass
        for suffix in [".clean" + ext, ".combined" + ext]:
            p = audio_path + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except:
                    pass
        if 'final_thumb_path' in locals() and final_thumb_path and os.path.exists(final_thumb_path):
            try:
                os.remove(final_thumb_path)
            except:
                pass

def insert_voice_tag(audio, tag, position, tag_type='insert'):
    audio_len = len(audio)
    if position == 'start':
        insert_ms = min(2 * 60 * 1000, audio_len // 2) if audio_len > 2 * 60 * 1000 else 0
        if tag_type == 'ongoing':
            return audio.overlay(tag, position=insert_ms)
        else:
            return audio[:insert_ms] + tag + audio[insert_ms:]
    elif position == 'middle':
        insert_ms = audio_len // 2
        if tag_type == 'ongoing':
            return audio.overlay(tag, position=insert_ms)
        else:
            return audio[:insert_ms] + tag + audio[insert_ms:]
    elif position == 'end':
        if tag_type == 'ongoing':
            if audio_len <= 2 * 60 * 1000:
                insert_ms = max(0, audio_len - len(tag))
            else:
                insert_ms = max(0, audio_len - 2 * 60 * 1000)
            return audio.overlay(tag, position=insert_ms)
        else:
            if audio_len <= 2 * 60 * 1000:
                insert_ms = audio_len
            else:
                insert_ms = audio_len - 2 * 60 * 1000
            return audio[:insert_ms] + tag + audio[insert_ms:]
    elif position == 'everywhere':
        audio = insert_voice_tag(audio, tag, 'start', tag_type=tag_type)
        audio = insert_voice_tag(audio, tag, 'middle', tag_type=tag_type)
        audio = insert_voice_tag(audio, tag, 'end', tag_type=tag_type)
        return audio
    return audio

def get_audio_duration_seconds(filepath):
    duration = 0
    # 1. Try ffprobe
    try:
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", filepath]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, stdin=subprocess.DEVNULL)
        out = res.stdout.strip()
        if out and out != "N/A":
            return int(float(out))
    except Exception as e:
        print(f"DEBUG: ffprobe duration failed for {filepath}: {e}")
    
    # 2. Try mutagen
    try:
        meta = mutagen.File(filepath)
        if meta and hasattr(meta, 'info') and hasattr(meta.info, 'length'):
            return int(meta.info.length)
    except Exception as e:
        print(f"DEBUG: mutagen duration failed for {filepath}: {e}")
        
    # 3. Try pydub
    try:
        return int(len(AudioSegment.from_file(filepath)) / 1000.0)
    except Exception as e:
        print(f"DEBUG: pydub duration failed for {filepath}: {e}")
        
    return 0

def process_merge_task_ffmpeg(temp_files, out_path):
    # Determine if all files are MP3
    all_mp3 = all(f.lower().endswith('.mp3') for f in temp_files)
    
    success = False
    
    if all_mp3 and len(temp_files) > 1:
        # Fast path: stream copy using concat demuxer
        inputs_txt_path = out_path + ".inputs.txt"
        try:
            with open(inputs_txt_path, "w", encoding="utf-8") as inf:
                for tf in temp_files:
                    escaped_path = tf.replace("'", "'\\''")
                    inf.write(f"file '{escaped_path}'\n")
            
            # Run ffmpeg concat stream copy
            cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", inputs_txt_path, "-c", "copy", out_path]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, stdin=subprocess.DEVNULL)
            if res.returncode == 0:
                success = True
                print("DEBUG: Fast stream-copy merge succeeded.")
            else:
                print(f"DEBUG: Fast stream-copy merge failed with returncode {res.returncode}. Stderr: {res.stderr}")
        except Exception as e:
            print(f"DEBUG: Fast stream-copy merge encountered error: {e}")
        finally:
            if os.path.exists(inputs_txt_path):
                try:
                    os.remove(inputs_txt_path)
                except:
                    pass
                    
    if not success:
        # Fallback/Transcode path: ffmpeg concat filter re-encode to MP3
        print("DEBUG: Running FFmpeg concat re-encode merge path.")
        try:
            cmd = ["ffmpeg", "-y"]
            for tf in temp_files:
                cmd.extend(["-i", tf])
            
            filter_str = "".join(f"[{i}:a]" for i in range(len(temp_files))) + f"concat=n={len(temp_files)}:v=0:a=1[a]"
            cmd.extend([
                "-filter_complex", filter_str,
                "-map", "[a]",
                "-acodec", "libmp3lame",
                "-b:a", "128k",
                out_path
            ])
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, stdin=subprocess.DEVNULL)
            if res.returncode == 0:
                success = True
                print("DEBUG: FFmpeg re-encode merge succeeded.")
            else:
                print(f"DEBUG: FFmpeg re-encode merge failed with returncode {res.returncode}. Stderr: {res.stderr}")
        except Exception as e:
            print(f"DEBUG: FFmpeg re-encode merge encountered error: {e}")
            
    if not success:
        # Ultimate fallback to pydub if everything else fails
        print("DEBUG: Falling back to pydub for merge task.")
        segments = []
        for f in temp_files:
            segments.append(AudioSegment.from_file(f))
            
        combined = segments[0]
        for s in segments[1:]:
            combined += s
            
        combined.export(out_path, format="mp3")

async def perform_audio_merge(client, status_msg, user, chat_id):
    user['merge_mode'] = False
    user['awaiting'] = None
    if user.get('merge_timer_task'):
        user['merge_timer_task'].cancel()
        user['merge_timer_task'] = None
    
    if not await db.is_subscription_valid(chat_id):
        await status_msg.edit_text("❌ Merge feature is only available for subscribed users.")
        user['merge_audios'] = []
        return

    is_ok, usage, limit, err_msg = await check_user_limit(chat_id, chat_id, additional_count=1)
    if not is_ok:
        await status_msg.edit_text(err_msg, parse_mode=ParseMode.MARKDOWN)
        user['merge_audios'] = []
        return
    
    msgs = user['merge_audios']
    total = len(msgs)
    
    await status_msg.edit_text(f"⏳ **Sorting {total} audios by episode number...**")
    
    def merge_sort_key(m):
        num = extract_episode_number(get_sorting_name(m))
        return (num if num is not None else float('inf'), m.id)
    
    sorted_msgs = sorted(msgs, key=merge_sort_key)
    
    ep_numbers = [extract_episode_number(get_sorting_name(m)) for m in sorted_msgs]
    valid_eps = [num for num in ep_numbers if num is not None]
    if valid_eps:
        min_ep = min(valid_eps)
        max_ep = max(valid_eps)
        if min_ep == max_ep:
            merged_title = f"Ep {min_ep}"
        else:
            merged_title = f"Ep {min_ep}-{max_ep}"
    else:
        merged_title = "Merged Audio"
        
    temp_files = []
    
    try:
        for idx, m in enumerate(sorted_msgs, start=1):
            await status_msg.edit_text(f"📥 **Downloading audio {idx}/{total}...**")
            
            ext = ".mp3"
            file_name = None
            if m.audio:
                file_name = m.audio.file_name
            elif m.document:
                file_name = m.document.file_name
            if file_name:
                ext_guess = os.path.splitext(file_name)[1].lower()
                if ext_guess in ['.mp3', '.m4a', '.mp4', '.flac', '.ogg', '.wav']:
                    ext = ext_guess
                    
            temp_name = f"merge_tmp_{chat_id}_{idx}_{int(time.time())}{ext}"
            temp_path = os.path.join(DOWNLOAD_DIR, temp_name)
            
            downloaded = await download_file_with_selector(m, temp_path, progress_msg=status_msg)
                
            temp_files.append(downloaded)
            
        await status_msg.edit_text("🔗 **Merging and exporting audio...**")
        
        out_filename = f"{merged_title}.mp3"
        out_path = os.path.join(DOWNLOAD_DIR, f"merged_out_{chat_id}_{int(time.time())}.mp3")
        
        await asyncio.get_event_loop().run_in_executor(
            executor, 
            process_merge_task_ffmpeg, 
            temp_files, 
            out_path
        )
        
        await status_msg.edit_text("✍️ **Writing metadata...**")
        def write_metadata(path, title, artist):
            try:
                audio_file = MP3(path, ID3=ID3)
                try:
                    audio_file.add_tags()
                except:
                    pass
                audio_file.tags.delall("TIT2")
                audio_file.tags.add(TIT2(encoding=3, text=title))
                if artist:
                    audio_file.tags.delall("TPE1")
                    audio_file.tags.add(TPE1(encoding=3, text=artist))
                audio_file.save()
            except Exception as e:
                print(f"Error setting metadata: {e}")
                
        await asyncio.get_event_loop().run_in_executor(
            executor, 
            write_metadata, 
            out_path, 
            merged_title, 
            user.get('artist')
        )
        
        await status_msg.edit_text("Upload status: ⏳ **Uploading merged audio...**")
        
        send_to_chat = chat_id
        if user.get('target_channel'):
            send_to_chat = user['target_channel']
            
        audio_duration = await asyncio.get_event_loop().run_in_executor(
            executor,
            get_audio_duration_seconds,
            out_path
        )
        upload_start = datetime.now()
        
        if status_msg:
            await throttle_progress_edit(status_msg, "Upload status: ⏳ **Waiting for an upload slot...**")

        # Route directly through upload_via_bot_api which dynamically routes based on file size
        async with UPLOAD_SEMAPHORE:
            if status_msg:
                await throttle_progress_edit(status_msg, "Upload status: ⬆️ **Starting upload...**")
            await upload_via_bot_api(
                chat_id=send_to_chat,
                file_path=out_path,
                performer=user['artist'] if user['artist'] else None,
                title=merged_title,
                thumb_path=None,
                progress_msg=status_msg,
                start_time=upload_start,
                duration=audio_duration,
                caption=merged_title if user.get('caption_enabled', True) else None
            )

        if not await db.is_subscription_valid(chat_id):
            await db.increment_user_usage(chat_id, get_mumbai_date_str())
                
        await status_msg.delete()
        await client.send_message(chat_id, f"✅ **Merge complete!**\nTitle: `{merged_title}`")
        
    except Exception as e:
        await status_msg.edit_text(f"❌ **Error during merge:** {e}")
    finally:
        for f in temp_files:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except:
                    pass
        if 'out_path' in locals() and os.path.exists(out_path):
            try:
                os.remove(out_path)
            except:
                pass
        user['merge_audios'] = []

# --- Handlers ---

def get_welcome_markup(data):
    caption_text = "📝 Caption: ON" if data.get('caption_enabled', True) else "📝 Caption: OFF"
    caption_style = "success" if data.get('caption_enabled', True) else "danger"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Artist", callback_data="edit_artist", style="primary"),
         InlineKeyboardButton("🗑️ Remove Artist", callback_data="rm_artist", style="danger")],
        [InlineKeyboardButton("🖼️ Set Cover", callback_data="edit_image", style="primary"),
         InlineKeyboardButton("🗑️ Remove Cover", callback_data="rm_image", style="danger")],
        [InlineKeyboardButton("🔄 Keep Original Cover", callback_data="keep_cover", style="success")],
        [InlineKeyboardButton("🔢 Set Title", callback_data="edit_title", style="primary"),
         InlineKeyboardButton("🔄 Keep Original Title", callback_data="rm_title", style="success")],
        [InlineKeyboardButton("➕ Add Voice Tag", callback_data="edit_voicetag", style="primary"),
         InlineKeyboardButton("➖ Remove Voice Tag", callback_data="rm_voicetag", style="danger")],
        [InlineKeyboardButton("📢 Set Target Channel", callback_data="set_target", style="primary"),
         InlineKeyboardButton("❌ Remove Target Channel", callback_data="rm_target", style="danger")],
        [InlineKeyboardButton("📦 Batch Process", callback_data="batch_start", style="primary"),
         InlineKeyboardButton("🔗 Merge Audio", callback_data="merge_start", style="primary")],
        [InlineKeyboardButton(caption_text, callback_data="toggle_caption", style=caption_style)],
        [InlineKeyboardButton("🗑️ Start Trim", callback_data="start_trim_set"),
         InlineKeyboardButton("🔄 Normal Start", callback_data="start_trim_off", style="success")],
        [InlineKeyboardButton("🗑️ End Trim", callback_data="end_trim_set"),
         InlineKeyboardButton("🔄 Normal End", callback_data="end_trim_off", style="success")]
    ])

@app.on_message(filters.command("start") & filters.private)
async def send_welcome(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_authorized(user_id):
        await message.reply_text("❌ You are not authorized. Please contact an admin.")
        return
    data = get_user_data(chat_id)
    markup = get_welcome_markup(data)
    welcome = await message.reply_text(
        "🎵 **Welcome to the Audio Bulk Editor Bot!**\n\nConfigure your settings below. Then send audio files (MP3, M4A, FLAC) or use **Batch** to process a range of channel posts.\n\n"
        "Check /status to see your current config.\n"
        "Use /cancel to cancel any ongoing input.\n"
        "Use /stop to clear your queue and stop processing.",
        reply_markup=markup, parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command("status") & filters.private)
async def check_status(client, message):
    user_id = message.from_user.id
    if not await is_authorized(user_id):
        err = await message.reply_text("❌ Not authorized.")
        return
    data = get_user_data(message.chat.id)
    seq = f"{data['title_prefix']} {data['counter']}".strip() if data['title_prefix'] else "Keeping Original"
    voice = "Not set"
    if data.get('voice_tag_path') and os.path.exists(data['voice_tag_path']):
        if not await db.is_subscription_valid(user_id):
            old_path = data.get('voice_tag_path')
            data['voice_tag_path'] = None
            data['voice_tag_position'] = None
            data['voice_tag_type'] = None
            if old_path and os.path.exists(old_path):
                try:
                    os.remove(old_path)
                except:
                    pass
        else:
            pos_map = {'start': 'Start (after 2 min)', 'middle': 'Middle', 'end': 'End (before last 2 min)', 'everywhere': 'Everywhere'}
            type_map = {'ongoing': 'Ongoing (Play together)', 'insert': 'Stop & Add'}
            voice_type = type_map.get(data.get('voice_tag_type'), 'Stop & Add')
            voice = f"Set ✅ ({pos_map.get(data['voice_tag_position'], 'Set')} - {voice_type})"
    target = f"Set to `{data['target_channel']}`" if data.get('target_channel') else "Not set"
    batch = "Active" if data.get('batch_mode') else "Inactive"
    start_trim = f"{data['trim_start']} chars" if data.get('trim_start') is not None else "Off"
    end_trim = f"{data['trim_end']} chars" if data.get('trim_end') is not None else "Off"
    
    if data.get('image_path'):
        cover_status = 'Set ✅'
    elif data.get('remove_cover'):
        cover_status = 'Stripping Cover 🗑️'
    else:
        cover_status = 'Keeping Original'
        
    caption_status = "Enabled ✅" if data.get('caption_enabled', True) else "Disabled ❌"
    text = (
        "📊 **Your Configuration:**\n\n"
        f"🎤 Artist: {data['artist'] or 'Keeping Original'}\n"
        f"🖼️ Cover: {cover_status}\n"
        f"🔊 Voice Tag: {voice}\n"
        f"📢 Target Channel: {target}\n"
        f"📦 Batch Mode: {batch}\n"
        f"🔢 Next Title: {seq}\n"
        f"📝 Caption: {caption_status}\n"
        f"🗑️ Start Trim: {start_trim}\n"
        f"✂️ End Trim: {end_trim}"
    )
    resp = await message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command("mysub") & filters.private)
async def my_subscription(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    is_admin = await db.is_admin(user_id)
    expiry = await db.get_subscription(user_id)
    mode = await db.get_setting('mode', 'subscription')
    
    if is_admin:
        text = "👑 **You are an Admin.**\nUnlimited access."
    elif expiry and expiry > datetime.now():
        remaining = (expiry - datetime.now()).days
        text = f"✅ **Subscription Active**\nExpiry date: **{expiry.date()}** ({remaining} days left).\nUnlimited access."
    else:
        if mode == 'public':
            limit_str = await db.get_setting('daily_free_limit', '10')
            try:
                limit = int(limit_str)
            except ValueError:
                limit = 10
            
            today_str = get_mumbai_date_str()
            usage = await db.get_user_usage(user_id, today_str)
            
            queue = user_queues.get(chat_id)
            queue_size = queue.qsize() if queue else 0
            active_tasks = 1 if (chat_id in user_tasks and not user_tasks[chat_id].done()) else 0
            
            remaining_allowance = max(0, limit - usage - queue_size - active_tasks)
            
            text = (
                "❌ **No Active Subscription**\n\n"
                f"⚙️ Bot Mode: **PUBLIC**\n"
                f"🔢 Daily Limit: **{limit}** files\n"
                f"📊 Used Today: **{usage}** files\n"
                f"⏳ Processing/Queued: **{queue_size + active_tasks}** files\n"
                f"✅ Remaining Allowance: **{remaining_allowance}** files\n\n"
                "Resets daily at 12:00 AM Indian Standard Time (IST)."
            )
        else:
            text = (
                "❌ **No Active Subscription**\n\n"
                f"⚙️ Bot Mode: **SUBSCRIPTION**\n"
                "You cannot use the bot without an active subscription."
            )
    
    resp = await message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command("admin") & filters.private)
async def admin_panel(client, message):
    user_id = message.from_user.id
    if not await is_admin_user(user_id):
        err = await message.reply_text("⛔ Admin only.")
        return
    
    mode = await db.get_setting('mode', 'subscription')
    limit = await db.get_setting('daily_free_limit', '10')
    
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⚙️ Mode: {mode.upper()}", callback_data="admin_setmode"),
         InlineKeyboardButton(f"🔢 Limit: {limit}", callback_data="admin_setlimit")],
        [InlineKeyboardButton("➕ Add Admin", callback_data="admin_addadmin", style="primary"),
         InlineKeyboardButton("➖ Remove Admin", callback_data="admin_removeadmin", style="danger")],
        [InlineKeyboardButton("📋 List Admins", callback_data="admin_listadmins", style="success")],
        [InlineKeyboardButton("💳 Add Subscription", callback_data="admin_addsub", style="primary"),
         InlineKeyboardButton("❌ Remove Subscription", callback_data="admin_removesub", style="danger")],
        [InlineKeyboardButton("📜 List Subscriptions", callback_data="admin_listsubs", style="success")],
        [InlineKeyboardButton("📤 Backup", callback_data="admin_backup", style="success"),
         InlineKeyboardButton("📥 Restore", callback_data="admin_restore", style="primary")]
    ])
    resp = await message.reply_text("**Admin Panel**", reply_markup=markup, parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command(["broadcast", "boardcast"]) & filters.private)
async def broadcast_command(client, message):
    user_id = message.from_user.id
    if not await is_admin_user(user_id):
        return

    if not message.reply_to_message:
        await message.reply_text("❌ **Reply to a message to broadcast it.**")
        return

    reply_msg = message.reply_to_message
    users = await db.get_all_users()
    total = len(users)
    
    status_msg = await message.reply_text(f"🚀 **Starting broadcast to {total} users...**")
    
    success = 0
    blocked = 0
    failed = 0
    
    for uid in users:
        try:
            await reply_msg.copy(uid)
            success += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await reply_msg.copy(uid)
                success += 1
            except:
                failed += 1
        except Exception as e:
            err_str = str(e).lower()
            if "blocked" in err_str or "deactivated" in err_str:
                blocked += 1
            else:
                failed += 1
        
        if (success + blocked + failed) % 20 == 0:
            try:
                await status_msg.edit_text(
                    f"🚀 **Broadcasting...**\n\n"
                    f"✅ Success: `{success}`\n"
                    f"🚫 Blocked: `{blocked}`\n"
                    f"❌ Failed: `{failed}`\n"
                    f"📊 Progress: `{success + blocked + failed}/{total}`"
                )
            except:
                pass

    await status_msg.edit_text(
        f"🏁 **Broadcast Completed!**\n\n"
        f"✅ Success: `{success}`\n"
        f"🚫 Blocked: `{blocked}`\n"
        f"❌ Failed: `{failed}`\n"
        f"📊 Total processed: `{total}`"
    )

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_command(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    if not await is_authorized(user_id):
        return
    data = get_user_data(chat_id)
    cancelled = False
    if data['awaiting']:
        data['awaiting'] = None
        if data.get('temp_voice_tag_path') and os.path.exists(data['temp_voice_tag_path']):
            os.remove(data['temp_voice_tag_path'])
            data['temp_voice_tag_path'] = None
        data['temp_voice_tag_position'] = None
        cancelled = True
    if data.get('batch_mode'):
        data['batch_mode'] = False
        data['batch_first_link'] = None
        data['batch_last_link'] = None
        data['batch_chat_id'] = None
        cancelled = True
    if data.get('merge_mode'):
        data['merge_mode'] = False
        data['merge_audios'] = []
        if data.get('merge_timer_task'):
            data['merge_timer_task'].cancel()
            data['merge_timer_task'] = None
        cancelled = True
    if get_admin_state(chat_id):
        clear_admin_state(chat_id)
        cancelled = True
    if cancelled:
        resp = await message.reply_text("✅ Current input cancelled.")
    else:
        resp = await message.reply_text("Nothing to cancel.")

@app.on_message(filters.command("stop") & filters.private)
async def stop_command(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    if not await is_authorized(user_id):
        return
    if chat_id in user_queues:
        queue = user_queues.pop(chat_id, None)
        if queue:
            while not queue.empty():
                try:
                    queue.get_nowait()
                except:
                    break
    if chat_id in user_tasks:
        task = user_tasks.pop(chat_id)
        task.cancel()
    if chat_id in queue_status_msgs:
        entry = queue_status_msgs[chat_id]
        msg_obj = entry.get("msg")
        if msg_obj:
            try:
                await msg_obj.delete()
            except Exception as e:
                print(f"DEBUG: failed to delete queue status msg: {e}")
        del queue_status_msgs[chat_id]
    data = get_user_data(chat_id)
    data['awaiting'] = None
    data['batch_mode'] = False
    data['batch_first_link'] = None
    data['batch_last_link'] = None
    data['batch_chat_id'] = None
    if data.get('temp_voice_tag_path') and os.path.exists(data['temp_voice_tag_path']):
        os.remove(data['temp_voice_tag_path'])
        data['temp_voice_tag_path'] = None
    data['temp_voice_tag_position'] = None
    data['merge_mode'] = False
    data['merge_audios'] = []
    if data.get('merge_timer_task'):
        data['merge_timer_task'].cancel()
        data['merge_timer_task'] = None
    resp = await message.reply_text("🛑 Processing stopped. Your queue has been cleared.")

@app.on_message(filters.command("done") & filters.private)
async def done_command(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    if not await is_authorized(user_id):
        return
    data = get_user_data(chat_id)
    if data.get('batch_mode'):
        data['batch_mode'] = False
        data['awaiting'] = None
        data['batch_first_link'] = None
        data['batch_last_link'] = None
        data['batch_chat_id'] = None
        resp = await message.reply_text("✅ Batch mode finished.")
    elif data.get('merge_mode'):
        if not await db.is_subscription_valid(user_id):
            await message.reply_text("❌ Merge feature is only available for subscribed users.")
            data['merge_mode'] = False
            data['awaiting'] = None
            data['merge_audios'] = []
            if data.get('merge_timer_task'):
                data['merge_timer_task'].cancel()
                data['merge_timer_task'] = None
            return
        if not data.get('merge_audios'):
            resp = await message.reply_text("❌ No audios to merge.")
            return
        status_msg = await message.reply_text("⏳ Initializing merge...")
        asyncio.create_task(perform_audio_merge(client, status_msg, data, chat_id))
    else:
        resp = await message.reply_text("Not in batch or merge mode.")

@app.on_callback_query()
async def handle_callbacks(client, call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    data = call.data

    if data.startswith("admin_"):
        if not await is_admin_user(user_id):
            await call.answer("⛔ Admin only.", show_alert=True)
            return

        if data == "admin_listadmins":
            rows = await db.list_admins()
            if not rows:
                text = "No admins found (besides super admin?)."
            else:
                text = "**Admin List:**\n"
                for row in rows:
                    text += f"• `{row['user_id']}` (added by {row['added_by']} at {row['added_at']})\n"
            await call.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
            await call.answer()
            return

        if data == "admin_listsubs":
            rows = await db.list_subscriptions()
            if not rows:
                text = "No subscriptions found."
            else:
                text = "**Active Subscriptions:**\n"
                for row in rows:
                    expiry = datetime.fromisoformat(row['expiry_date'])
                    remaining = (expiry - datetime.now()).days
                    text += f"• `{row['user_id']}` – expires {expiry.date()} ({remaining} days left) – added by {row['added_by']}\n"
            await call.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
            await call.answer()
            return

        if data == "admin_addadmin":
            if user_id != SUPER_ADMIN_ID:
                await call.answer("⛔ Only the super admin can add admins.", show_alert=True)
                return
            set_admin_state(chat_id, action='addadmin', step='waiting_for_userid')
            await call.message.edit_text("Send me the user ID of the new admin:")
            await call.answer()
            return

        if data == "admin_removeadmin":
            if user_id != SUPER_ADMIN_ID:
                await call.answer("⛔ Only the super admin can remove admins.", show_alert=True)
                return
            set_admin_state(chat_id, action='removeadmin', step='waiting_for_userid')
            await call.message.edit_text("Send me the user ID of the admin to remove:")
            await call.answer()
            return

        if data == "admin_addsub":
            set_admin_state(chat_id, action='addsub', step='waiting_for_userid')
            await call.message.edit_text("Send me the user ID to add subscription:")
            await call.answer()
            return

        if data == "admin_removesub":
            set_admin_state(chat_id, action='removesub', step='waiting_for_userid')
            await call.message.edit_text("Send me the user ID to remove subscription:")
            await call.answer()
            return

        if data == "admin_backup":
            await call.answer("Creating backup...")
            backup_data = await db.export_backup()
            import json
            backup_filename = f"trex_backup_{chat_id}_{int(time.time())}.json"
            backup_path = os.path.join(DOWNLOAD_DIR, backup_filename)
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=4)
            
            await client.send_document(
                chat_id=chat_id,
                document=backup_path,
                caption="📤 **Database Backup**\n\nSave this file. You can restore it in the future using the Restore button."
            )
            if os.path.exists(backup_path):
                os.remove(backup_path)
            return

        if data == "admin_setmode":
            current_mode = await db.get_setting('mode', 'subscription')
            new_mode = 'public' if current_mode == 'subscription' else 'subscription'
            await db.set_setting('mode', new_mode)
            await call.answer(f"Mode changed to {new_mode.upper()}")
            
            limit = await db.get_setting('daily_free_limit', '10')
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"⚙️ Mode: {new_mode.upper()}", callback_data="admin_setmode"),
                 InlineKeyboardButton(f"🔢 Limit: {limit}", callback_data="admin_setlimit")],
                [InlineKeyboardButton("➕ Add Admin", callback_data="admin_addadmin", style="primary"),
                 InlineKeyboardButton("➖ Remove Admin", callback_data="admin_removeadmin", style="danger")],
                [InlineKeyboardButton("📋 List Admins", callback_data="admin_listadmins", style="success")],
                [InlineKeyboardButton("💳 Add Subscription", callback_data="admin_addsub", style="primary"),
                 InlineKeyboardButton("❌ Remove Subscription", callback_data="admin_removesub", style="danger")],
                [InlineKeyboardButton("📜 List Subscriptions", callback_data="admin_listsubs", style="success")],
                [InlineKeyboardButton("📤 Backup", callback_data="admin_backup", style="success"),
                 InlineKeyboardButton("📥 Restore", callback_data="admin_restore", style="primary")]
            ])
            try:
                await call.message.edit_reply_markup(reply_markup=markup)
            except Exception:
                pass
            return

        if data == "admin_setlimit":
            set_admin_state(chat_id, action='setlimit', step='waiting_for_limit')
            await call.message.edit_text("🔢 **Set Daily Limit**\n\nPlease send me the new daily limit (positive integer) for free users in public mode:")
            await call.answer()
            return

        if data == "admin_restore":
            set_admin_state(chat_id, action='restore', step='waiting_for_file')
            await call.message.edit_text("📥 **Restore Database**\n\nPlease send me the backup `.json` file:")
            await call.answer()
            return

        await call.answer("Unknown action.")
        return

    if not await is_authorized(user_id):
        await call.answer("⛔ Not authorized.", show_alert=True)
        return

    user = get_user_data(chat_id)

    if data == "toggle_caption":
        user['caption_enabled'] = not user.get('caption_enabled', True)
        status_str = "Enabled ✅" if user['caption_enabled'] else "Disabled ❌"
        await call.answer(f"Captions are now {status_str}")
        try:
            await call.message.edit_reply_markup(reply_markup=get_welcome_markup(user))
        except Exception:
            pass
        return

    if data == "edit_artist":
        user['awaiting'] = 'artist'
        prompt = await call.message.reply_text("Send me the Artist Name:")
        await call.answer()
    elif data == "rm_artist":
        user['artist'] = None
        user['remove_artist'] = True
        await call.answer("Artist removed.")
        confirm = await call.message.reply_text("✅ All artist names will be set to: **Unknown Artist**")
    elif data == "edit_image":
        user['awaiting'] = 'image'
        user['remove_cover'] = False
        prompt = await call.message.reply_text("Send me the Photo (as image, not file):")
        await call.answer()
    elif data == "rm_image":
        if user.get('image_path') and os.path.exists(user['image_path']):
            os.remove(user['image_path'])
        user['image_path'] = None
        user['remove_cover'] = True
        await call.answer("Cover will be stripped.")
        confirm = await call.message.reply_text("✅ All cover images will be strictly removed from files.")
    elif data == "keep_cover":
        if user.get('image_path') and os.path.exists(user['image_path']):
            os.remove(user['image_path'])
        user['image_path'] = None
        user['remove_cover'] = False
        await call.answer("Keeping original covers.")
        confirm = await call.message.reply_text("✅ The original cover art inside the files will be kept.")
    elif data == "edit_title":
        user['awaiting'] = 'title'
        prompt = await call.message.reply_text("Send base title with starting number (e.g., `Track 5`):", parse_mode=ParseMode.MARKDOWN)
        await call.answer()
    elif data == "rm_title":
        user['title_prefix'] = None
        user['counter'] = 1
        await call.answer("Title sequence disabled.")
        confirm = await call.message.reply_text("✅ Title cleared.")
    elif data == "edit_voicetag":
        if not await db.is_subscription_valid(user_id):
            await call.answer("❌ This feature is only available for subscribed users.", show_alert=True)
            return
        user['awaiting'] = 'voice_tag'
        prompt = await call.message.reply_text("🎤 Send voice tag (≤5 min, MP3/M4A/FLAC/OGG):")
        await call.answer()
    elif data == "rm_voicetag":
        if user.get('voice_tag_path') and os.path.exists(user['voice_tag_path']):
            os.remove(user['voice_tag_path'])
        user['voice_tag_path'] = None
        user['voice_tag_position'] = None
        user['voice_tag_type'] = None
        await call.answer("Voice tag removed.")
        confirm = await call.message.reply_text("✅ Voice tag cleared.")
    elif data == "set_target":
        user['awaiting'] = 'target_channel'
        prompt = await call.message.reply_text("📢 Send channel username (@channel) or ID (-100...):")
        await call.answer()
    elif data == "rm_target":
        user['target_channel'] = None
        await call.answer("Target channel removed.")
        confirm = await call.message.reply_text("✅ Target channel cleared.")
    elif data == "batch_start":
        user['batch_mode'] = True
        user['awaiting'] = 'batch_first_link'
        user['batch_first_link'] = None
        user['batch_last_link'] = None
        user['batch_chat_id'] = None
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel Batch", callback_data="batch_cancel", style="danger")]
        ])
        prompt = await call.message.reply_text(
            "📦 **Batch Mode**\n\n"
            "Send me the **first** audio post link (e.g., `https://t.me/channel/123`).",
            reply_markup=markup, parse_mode=ParseMode.MARKDOWN
        )
        await call.answer()
    elif data == "batch_cancel":
        if user.get('batch_mode'):
            user['batch_mode'] = False
            user['awaiting'] = None
            user['batch_first_link'] = None
            user['batch_last_link'] = None
            user['batch_chat_id'] = None
            await call.message.edit_text("❌ Batch cancelled.")
        await call.answer()
    elif data == "merge_start":
        if not await db.is_subscription_valid(user_id):
            await call.answer("❌ This feature is only available for subscribed users.", show_alert=True)
            return
        user['merge_mode'] = True
        user['awaiting'] = 'merge_audios'
        user['merge_audios'] = []
        if user.get('merge_timer_task'):
            user['merge_timer_task'].cancel()
            user['merge_timer_task'] = None
        user['merge_status_msg_id'] = None
        await call.answer("Merge Mode Activated.")
        await call.message.reply_text(
            "🔗 **Merge Mode Activated**\n\n"
            "Send me the audio files you want to merge (one by one or all at once).\n"
            "I will wait until you stop sending (5 seconds of inactivity), then show a **Done** button.\n"
            "They will be sorted by episode number and merged.",
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "merge_cancel":
        user['merge_mode'] = False
        user['awaiting'] = None
        user['merge_audios'] = []
        if user.get('merge_timer_task'):
            user['merge_timer_task'].cancel()
            user['merge_timer_task'] = None
        user['merge_status_msg_id'] = None
        await call.message.edit_text("❌ Merge process cancelled.")
        await call.answer()
    elif data == "merge_done":
        if not await db.is_subscription_valid(user_id):
            await call.answer("❌ This feature is only available for subscribed users.", show_alert=True)
            return
        if not user.get('merge_mode') or not user.get('merge_audios'):
            await call.answer("No audios to merge.", show_alert=True)
            return
        await call.answer("Merging started...")
        asyncio.create_task(perform_audio_merge(client, call.message, user, chat_id))
    elif data == "start_trim_set":
        user['awaiting'] = 'start_trim'
        prompt = await call.message.reply_text("Send the number of characters to remove from the **start** of the caption:")
        await call.answer()
    elif data == "start_trim_off":
        user['trim_start'] = None
        await call.answer("Start trim turned off (normal).")
        confirm = await call.message.reply_text("✅ Start trim cleared.")
    elif data == "end_trim_set":
        user['awaiting'] = 'end_trim'
        prompt = await call.message.reply_text("Send the number of characters to remove from the **end** of the caption:")
        await call.answer()
    elif data == "end_trim_off":
        user['trim_end'] = None
        await call.answer("End trim turned off (normal).")
        confirm = await call.message.reply_text("✅ End trim cleared.")
    elif data.startswith("voicetag_pos_"):
        if not await db.is_subscription_valid(user_id):
            await call.answer("❌ This feature is only available for subscribed users.", show_alert=True)
            return
        if not user.get('temp_voice_tag_path') or not os.path.exists(user['temp_voice_tag_path']):
            await call.answer("No pending voice tag.", show_alert=True)
            return
        if data == "voicetag_pos_cancel":
            if user.get('temp_voice_tag_path') and os.path.exists(user['temp_voice_tag_path']):
                os.remove(user['temp_voice_tag_path'])
            user['temp_voice_tag_path'] = None
            user['temp_voice_tag_position'] = None
            user['awaiting'] = None
            await call.message.edit_text("❌ Voice tag cancelled.")
            await call.answer()
            return
        user['temp_voice_tag_position'] = data.replace("voicetag_pos_", "")
        user['awaiting'] = 'voice_tag_type'
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔊 Ongoing (Play together)", callback_data="voicetag_type_ongoing", style="primary")],
            [InlineKeyboardButton("⏸️ Stop & Add (Insert)", callback_data="voicetag_type_insert", style="primary")],
            [InlineKeyboardButton("❌ Cancel", callback_data="voicetag_pos_cancel", style="danger")]
        ])
        await call.message.edit_text("Choose mixing option for voice tag:", reply_markup=markup)
        await call.answer()
    elif data.startswith("voicetag_type_"):
        if not await db.is_subscription_valid(user_id):
            await call.answer("❌ This feature is only available for subscribed users.", show_alert=True)
            return
        if not user.get('temp_voice_tag_path') or not os.path.exists(user['temp_voice_tag_path']) or not user.get('temp_voice_tag_position'):
            await call.answer("No pending voice tag configuration.", show_alert=True)
            return
        tag_type = data.replace("voicetag_type_", "")
        user['voice_tag_path'] = user.pop('temp_voice_tag_path')
        user['voice_tag_position'] = user.pop('temp_voice_tag_position')
        user['voice_tag_type'] = tag_type
        user['awaiting'] = None
        
        pos_names = {'start': 'Start (after 2 min)', 'middle': 'Middle', 'end': 'End (before last 2 min)', 'everywhere': 'Everywhere'}
        type_names = {'ongoing': 'Ongoing (Play together)', 'insert': 'Stop & Add'}
        await call.message.edit_text(f"✅ Voice tag saved!\nPosition: **{pos_names[user['voice_tag_position']]}**\nType: **{type_names[tag_type]}**")
        await call.answer()
    else:
        await call.answer("Unknown action.")

@app.on_message(filters.text & filters.private & ~filters.command(["start", "status", "admin", "mysub", "cancel", "stop", "done"]))
async def handle_admin_text(client, message):
    if message.outgoing:
        return
    user_id = message.from_user.id
    chat_id = message.chat.id

    if not await is_admin_user(user_id):
        await handle_user_text(client, message)
        return

    state = get_admin_state(chat_id)
    if not state:
        await handle_user_text(client, message)
        return

    action = state['action']
    step = state['step']
    text = message.text.strip()

    if action == 'restore' and step == 'waiting_for_file':
        await message.reply_text("❌ Please send the backup `.json` file (as a file/document), or use /cancel to stop.")
        return

    if action == 'setlimit' and step == 'waiting_for_limit':
        try:
            limit = int(text)
            if limit <= 0:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Invalid limit. Please send a positive integer.")
            return
        await db.set_setting('daily_free_limit', str(limit))
        await message.reply_text(f"✅ Daily limit for free users in public mode set to **{limit}**.")
        clear_admin_state(chat_id)
        return

    if action == 'addadmin' and step == 'waiting_for_userid':
        try:
            target = int(text)
        except ValueError:
            err = await message.reply_text("❌ Invalid user ID. Please send a number.")
            return
        await db.add_admin(target, user_id)
        confirm = await message.reply_text(f"✅ User {target} is now an admin.")
        clear_admin_state(chat_id)
        return

    if action == 'removeadmin' and step == 'waiting_for_userid':
        try:
            target = int(text)
        except ValueError:
            err = await message.reply_text("❌ Invalid user ID. Please send a number.")
            return
        if target == SUPER_ADMIN_ID:
            err = await message.reply_text("❌ Cannot remove the super admin.")
            return
        await db.remove_admin(target)
        confirm = await message.reply_text(f"✅ User {target} is no longer an admin.")
        clear_admin_state(chat_id)
        return

    if action == 'addsub':
        if step == 'waiting_for_userid':
            try:
                target = int(text)
            except ValueError:
                err = await message.reply_text("❌ Invalid user ID. Please send a number.")
                return
            set_admin_state(chat_id, action='addsub', step='waiting_for_days', target_user=target)
            prompt = await message.reply_text("Now send the number of days for the subscription:")
            return
        elif step == 'waiting_for_days':
            try:
                days = int(text)
                if days <= 0:
                    raise ValueError
            except ValueError:
                err = await message.reply_text("❌ Invalid number. Please send a positive integer.")
                return
            target = state['target_user']
            await db.add_subscription(target, days, user_id)
            confirm = await message.reply_text(f"✅ Added {days} days subscription for user {target}.")
            clear_admin_state(chat_id)
            return

    if action == 'removesub' and step == 'waiting_for_userid':
        try:
            target = int(text)
        except ValueError:
            err = await message.reply_text("❌ Invalid user ID. Please send a number.")
            return
        await db.remove_subscription(target)
        confirm = await message.reply_text(f"✅ Removed subscription for user {target}.")
        clear_admin_state(chat_id)
        return

    clear_admin_state(chat_id)
    err = await message.reply_text("Admin action cancelled (unexpected input). Use /cancel to cancel.")

async def handle_user_text(client, message):
    user_id = message.from_user.id
    if not await is_authorized(user_id):
        err = await message.reply_text("❌ Not authorized.")
        return

    chat_id = message.chat.id
    data = get_user_data(chat_id)

    # Batch mode handling
    if data.get('batch_mode'):
        if data['awaiting'] == 'batch_first_link':
            link = message.text.strip()
            chat_id_from_link, msg_id = parse_telegram_link(link)
            if not chat_id_from_link or not msg_id:
                err = await message.reply_text("❌ Invalid Telegram message link.")
                return
            try:
                fetched = await client.get_messages(chat_id_from_link, msg_id)
            except Exception as e:
                err = await message.reply_text(f"❌ Could not fetch message: {e}")
                return
            if not (fetched.audio or fetched.document):
                err = await message.reply_text("❌ The linked message does not contain an audio file.")
                return
            data['batch_first_link'] = {'chat_id': chat_id_from_link, 'msg_id': msg_id}
            data['batch_chat_id'] = chat_id_from_link
            data['awaiting'] = 'batch_last_link'
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel Batch", callback_data="batch_cancel", style="danger")]
            ])
            prompt = await message.reply_text(
                "✅ First link accepted. Now send me the **last** audio post link from the same channel.",
                reply_markup=markup
            )
            return

        elif data['awaiting'] == 'batch_last_link':
            link = message.text.strip()
            chat_id_from_link, msg_id = parse_telegram_link(link)
            if not chat_id_from_link or not msg_id:
                err = await message.reply_text("❌ Invalid Telegram message link.")
                return
            if chat_id_from_link != data['batch_chat_id']:
                err = await message.reply_text("❌ The link is from a different channel. Please send a link from the same channel as the first.")
                return
            try:
                fetched = await client.get_messages(chat_id_from_link, msg_id)
            except Exception as e:
                err = await message.reply_text(f"❌ Could not fetch message: {e}")
                return
            if not (fetched.audio or fetched.document):
                err = await message.reply_text("❌ The linked message does not contain an audio file.")
                return

            first_id = data['batch_first_link']['msg_id']
            last_id = msg_id
            if last_id < first_id:
                first_id, last_id = last_id, first_id

            status = await message.reply_text(f"⏳ Fetching messages from {first_id} to {last_id}...")

            if chat_id not in user_queues:
                user_queues[chat_id] = asyncio.Queue()
            queue = user_queues[chat_id]

            total_messages = last_id - first_id + 1
            fetched_count = 0
            audio_count = 0
            all_audio_messages = []

            spinner_index = 0
            last_status_update = time.time()
            for start_id in range(first_id, last_id + 1, 100):
                batch_ids = list(range(start_id, min(start_id + 100, last_id + 1)))
                try:
                    messages = await client.get_messages(chat_id_from_link, batch_ids)
                except Exception as e:
                    await status.edit_text(f"❌ Error fetching batch: {e}")
                    data['batch_mode'] = False
                    data['awaiting'] = None
                    return
                for msg_obj in messages:
                    if msg_obj and (msg_obj.audio or msg_obj.document):
                        if msg_obj.media_group_id:
                            if 'media_group_captions' not in data:
                                data['media_group_captions'] = {}
                            if msg_obj.caption:
                                data['media_group_captions'][msg_obj.media_group_id] = msg_obj.caption
                            elif msg_obj.media_group_id in data['media_group_captions']:
                                msg_obj.caption = data['media_group_captions'][msg_obj.media_group_id]
                        all_audio_messages.append(msg_obj)
                        audio_count += 1
                    fetched_count += 1
                percent = (fetched_count * 100) // total_messages
                bar = '█' * int(percent // 10) + '░' * (10 - int(percent // 10))
                spinner = SPINNER[spinner_index % len(SPINNER)]
                spinner_index += 1
                if time.time() - last_status_update > 1.0:
                    new_text = (
                        f"{spinner} **Fetching messages...** `{bar}` {percent}%\n"
                        f"📊 Scanned: {fetched_count}/{total_messages} | 🎵 Audio found: {audio_count}"
                    )
                    if should_update_progress(chat_id, status.id, new_text):
                        try:
                            await status.edit_text(new_text)
                        except FloodWait as e:
                            await asyncio.sleep(e.value)
                        last_status_update = time.time()
                await asyncio.sleep(0.1)

            # Deduplicate by episode number or clean title to avoid processing duplicate posts
            seen_keys = set()
            unique_audio_messages = []
            for msg_obj in all_audio_messages:
                title = get_sorting_name(msg_obj)
                ep_num = extract_episode_number(title)
                if ep_num is not None:
                    key = f"ep_{ep_num}"
                else:
                    key = title.strip().lower()
                
                if not key:
                    key = f"msg_{msg_obj.id}"
                
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_audio_messages.append(msg_obj)
            
            all_audio_messages = unique_audio_messages
            audio_count = len(all_audio_messages)

            def batch_sort_key(m):
                num = extract_episode_number(get_sorting_name(m))
                return (num if num is not None else float('inf'), m.id)
            
            all_audio_messages.sort(key=batch_sort_key)

            is_ok, usage, limit, err_msg = await check_user_limit(user_id, chat_id, additional_count=len(all_audio_messages))
            if not is_ok:
                await status.edit_text(err_msg, parse_mode=ParseMode.MARKDOWN)
                data['batch_mode'] = False
                data['awaiting'] = None
                data['batch_first_link'] = None
                data['batch_last_link'] = None
                data['batch_chat_id'] = None
                return

            for msg_obj in all_audio_messages:
                await queue.put({
                    'client': client,
                    'message': msg_obj,
                    'data': data,
                })

            await status.edit_text(
                f"✅ **Batch complete!** {audio_count} audio files added to queue.\n"
                f"They will be processed in order."
            )

            if chat_id not in user_tasks or user_tasks[chat_id].done():
                task = asyncio.create_task(audio_processor(chat_id))
                user_tasks[chat_id] = task

            data['batch_mode'] = False
            data['awaiting'] = None
            data['batch_first_link'] = None
            data['batch_last_link'] = None
            data['batch_chat_id'] = None
            return

    # Regular input handling
    if data['awaiting'] == 'artist':
        data['artist'] = message.text
        data['remove_artist'] = False
        data['awaiting'] = None
        confirm = await message.reply_text(f"✅ Artist set to: **{message.text}**", parse_mode=ParseMode.MARKDOWN)
    elif data['awaiting'] == 'title':
        text = message.text.strip()
        match = re.search(r'^(.*?)\s*(\d+)$', text)
        if match:
            data['title_prefix'] = match.group(1).strip()
            data['counter'] = int(match.group(2))
        else:
            data['title_prefix'] = text
            data['counter'] = 1
        data['awaiting'] = None
        preview = f"{data['title_prefix']} {data['counter']}".strip()
        confirm = await message.reply_text(f"✅ Title sequence set! First: **{preview}**", parse_mode=ParseMode.MARKDOWN)
    elif data['awaiting'] == 'target_channel':
        target = message.text.strip()
        if target.startswith('@') or target.lstrip('-').isdigit():
            data['target_channel'] = target
            data['awaiting'] = None
            confirm = await message.reply_text(f"✅ Target channel set to `{target}`.", parse_mode=ParseMode.MARKDOWN)
        else:
            err = await message.reply_text("❌ Invalid format. Use @username or numeric ID.")
    elif data['awaiting'] == 'start_trim':
        try:
            num = int(message.text.strip())
            if num <= 0:
                raise ValueError
        except ValueError:
            err = await message.reply_text("❌ Please send a positive integer (number of characters).")
            return
        data['trim_start'] = num
        data['awaiting'] = None
        confirm = await message.reply_text(f"✅ Start trim set to {num} characters.")
    elif data['awaiting'] == 'end_trim':
        try:
            num = int(message.text.strip())
            if num <= 0:
                raise ValueError
        except ValueError:
            err = await message.reply_text("❌ Please send a positive integer (number of characters).")
            return
        data['trim_end'] = num
        data['awaiting'] = None
        confirm = await message.reply_text(f"✅ End trim set to {num} characters.")
    else:
        pass

@app.on_message(filters.photo & filters.private)
async def handle_photo(client, message):
    if message.outgoing:
        return
    user_id = message.from_user.id
    if not await is_authorized(user_id):
        return
    chat_id = message.chat.id
    data = get_user_data(chat_id)
    if data['awaiting'] == 'image':
        status = await message.reply_text("⏳ Downloading cover...")
        path = await message.download(file_name=os.path.join(DOWNLOAD_DIR, f"cover_{chat_id}.jpg"))
        data['image_path'] = path
        data['awaiting'] = None
        await status.edit_text("✅ Cover saved!")

@app.on_message((filters.audio | filters.voice | filters.document | filters.video) & filters.private)
async def handle_audio_or_voice(client, message):
    if message.outgoing:
        return
    user_id = message.from_user.id
    chat_id = message.chat.id
    data = get_user_data(chat_id)

    if user_id not in IN_FLIGHT_USAGE:
        IN_FLIGHT_USAGE[user_id] = 0
    IN_FLIGHT_USAGE[user_id] += 1

    try:
        if not await is_authorized(user_id):
            return
        
        is_voice_tag = (data.get('awaiting') == 'voice_tag')
        is_restore = (await is_admin_user(user_id) and get_admin_state(chat_id).get('action') == 'restore')
        
        if not is_voice_tag and not is_restore:
            is_ok, usage, limit, err_msg = await check_user_limit(user_id, chat_id, additional_count=0)
            if not is_ok:
                now = time.time()
                last_warning = data.get('last_limit_warning_time', 0)
                if now - last_warning >= 5:
                    data['last_limit_warning_time'] = now
                    await message.reply_text(err_msg, parse_mode=ParseMode.MARKDOWN)
                return

        if await is_admin_user(user_id):
            state = get_admin_state(chat_id)
            if state and state.get('action') == 'restore' and state.get('step') == 'waiting_for_file':
                if not message.document:
                    await message.reply_text("❌ Please send a valid backup `.json` file.")
                    return
                file_name = message.document.file_name or ""
                if not file_name.endswith('.json'):
                    await message.reply_text("❌ Please send a `.json` file.")
                    return
                
                status = await message.reply_text("⏳ Downloading backup file...")
                temp_path = os.path.join(DOWNLOAD_DIR, f"restore_{chat_id}_{int(time.time())}.json")
                try:
                    downloaded = await message.download(file_name=temp_path)
                    if not downloaded or not os.path.exists(downloaded):
                        raise FileNotFoundError("Failed to download file.")
                    await status.edit_text("⚙️ Restoring database backup...")
                    import json
                    with open(downloaded, 'r', encoding='utf-8') as f:
                        backup_data = json.load(f)
                    if not isinstance(backup_data, dict) or ('users' not in backup_data and 'admins' not in backup_data and 'subscriptions' not in backup_data):
                        raise ValueError("Invalid backup file structure.")
                    await db.import_backup(backup_data)
                    await status.edit_text("✅ **Database restored successfully!**")
                    clear_admin_state(chat_id)
                except Exception as e:
                    await status.edit_text(f"❌ **Failed to restore backup:** {e}")
                finally:
                    if os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except:
                            pass
                return

        if data.get('merge_mode'):
            if not await db.is_subscription_valid(user_id):
                data['merge_mode'] = False
                data['awaiting'] = None
                data['merge_audios'] = []
                if data.get('merge_timer_task'):
                    data['merge_timer_task'].cancel()
                    data['merge_timer_task'] = None
                await message.reply_text("❌ Merge feature is only available for subscribed users.")
                return
            if not (message.audio or message.document or message.voice):
                await message.reply_text("❌ Please send only audio files.")
                return
            data['merge_audios'].append(message)
            if data.get('merge_timer_task'):
                data['merge_timer_task'].cancel()
            async def wait_and_show_done(cid, count):
                await asyncio.sleep(5)
                markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Merge Now", callback_data="merge_done", style="success")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="merge_cancel", style="danger")]
                ])
                await client.send_message(
                    cid, 
                    f"✅ **Received {count} audios.**\n\n"
                    "If you have sent all files, click **Merge Now** below to combine them.",
                    reply_markup=markup,
                    parse_mode=ParseMode.MARKDOWN
                )
            data['merge_timer_task'] = asyncio.create_task(wait_and_show_done(chat_id, len(data['merge_audios'])))
            return

        # Share caption across media group (album) items
        if message.media_group_id:
            if 'media_group_captions' not in data:
                data['media_group_captions'] = {}
            if message.caption:
                data['media_group_captions'][message.media_group_id] = message.caption
            elif message.media_group_id in data['media_group_captions']:
                message.caption = data['media_group_captions'][message.media_group_id]

        if data['awaiting'] == 'voice_tag':
            await handle_voice_tag_upload(client, message, data, chat_id)
            return

        if chat_id not in user_queues:
            user_queues[chat_id] = asyncio.Queue()
        queue = user_queues[chat_id]

        # Add file to queue FIRST and ensure processor is running
        await queue.put({'client': client, 'message': message, 'data': data})

        if chat_id not in user_tasks or user_tasks[chat_id].done():
            task = asyncio.create_task(audio_processor(chat_id))
            user_tasks[chat_id] = task

        # --- Thread-safe queue status message creation/edit ---
        if chat_id not in queue_status_locks:
            queue_status_locks[chat_id] = asyncio.Lock()

        async with queue_status_locks[chat_id]:
            if chat_id not in queue_status_msgs:
                msg = await message.reply_text("📥 1 file added to queue...")
                queue_status_msgs[chat_id] = {"msg": msg, "count": 1, "pending_update": False}
            else:
                entry = queue_status_msgs[chat_id]
                entry["count"] += 1
                
                if not entry.get("pending_update"):
                    entry["pending_update"] = True
                    
                    async def delayed_update(msg_obj, current_entry):
                        await asyncio.sleep(1.0)
                        new_text = f"📥 {current_entry['count']} file(s) added to queue..."
                        await throttle_progress_edit(msg_obj, new_text)
                        current_entry["pending_update"] = False
                        
                    asyncio.create_task(delayed_update(entry["msg"], entry))
    finally:
        if user_id in IN_FLIGHT_USAGE:
            IN_FLIGHT_USAGE[user_id] -= 1
            if IN_FLIGHT_USAGE[user_id] <= 0:
                del IN_FLIGHT_USAGE[user_id]

async def handle_voice_tag_upload(client, message, data, chat_id):
    if not await db.is_subscription_valid(chat_id):
        await message.reply_text("❌ This feature is only available for subscribed users.")
        data['awaiting'] = None
        return
    file_name = None
    mime_type = None
    if message.audio:
        file_name = message.audio.file_name
        mime_type = message.audio.mime_type
    elif message.document:
        file_name = message.document.file_name
        mime_type = message.document.mime_type
    elif message.voice:
        file_name = f"voice_{chat_id}.ogg"
        mime_type = message.voice.mime_type

    ext = os.path.splitext(file_name)[1].lower() if file_name else ""
    supported = ['.mp3', '.m4a', '.mp4', '.flac', '.ogg', '.wav']

    if ext not in supported:
        guess = None
        if mime_type:
            if "mpeg" in mime_type or "mp3" in mime_type: guess = ".mp3"
            elif "mp4" in mime_type: guess = ".m4a"
            elif "flac" in mime_type: guess = ".flac"
            elif "ogg" in mime_type: guess = ".ogg"
            elif "wav" in mime_type or "wave" in mime_type: guess = ".wav"
        
        if guess:
            ext = guess
        elif message.audio:
            ext = ".mp3"
        elif message.voice:
            ext = ".ogg"

    if ext not in supported:
        err = await message.reply_text("❌ Unsupported format for voice tag.")
        return

    status = await message.reply_text("⏳ Downloading voice tag...")
    temp_path = os.path.join(DOWNLOAD_DIR, f"voicetag_{chat_id}_{datetime.now().timestamp()}{ext}")

    try:
        downloaded = await message.download(file_name=temp_path)

        def get_duration():
            seg = AudioSegment.from_file(downloaded)
            return len(seg) / 1000.0

        duration = await asyncio.get_event_loop().run_in_executor(executor, get_duration)
        if duration > 300:
            os.remove(downloaded)
            await status.edit_text("❌ Voice tag >5 minutes.")
            return
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        await status.edit_text(f"❌ Could not read audio: {e}")
        return

    data['temp_voice_tag_path'] = downloaded
    data['awaiting'] = 'voice_tag_position'

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Start (after 2 min)", callback_data="voicetag_pos_start", style="primary"),
         InlineKeyboardButton("🎯 Middle", callback_data="voicetag_pos_middle", style="primary")],
        [InlineKeyboardButton("🏁 End (before last 2 min)", callback_data="voicetag_pos_end", style="primary"),
         InlineKeyboardButton("🔁 Everywhere", callback_data="voicetag_pos_everywhere", style="primary")],
        [InlineKeyboardButton("❌ Cancel", callback_data="voicetag_pos_cancel", style="danger")]
    ])
    await status.edit_text("✅ Voice tag downloaded! Choose position:", reply_markup=markup)

def parse_telegram_link(link):
    pattern = r'https?://t\.me/(?:c/)?([^/]+)/(\d+)'
    match = re.search(pattern, link)
    if not match:
        return None, None
    chat_part, msg_id = match.groups()
    try:
        msg_id = int(msg_id)
    except:
        return None, None
    if chat_part.startswith('-100') or chat_part.isdigit():
        chat_id = int(chat_part) if chat_part.startswith('-100') else int('-100' + chat_part)
    else:
        chat_id = '@' + chat_part
    return chat_id, msg_id

async def main():
    print("Initializing database...")
    await db.init_db()
    
    await app.start()
    
    # One-time cleanup and re-extraction of ONLY real users from history
    print("Extracting real users from history...")
    try:
        # We can't use get_dialogs, so we check people from the session peers 
        # but verify if they have a message history.
        import sqlite3
        session_file = "audio_bulk_bot.session"
        if os.path.exists(session_file):
            conn = sqlite3.connect(session_file)
            cursor = conn.execute("SELECT id FROM peers WHERE type = 'user' AND id > 0")
            potential_ids = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            # Check only a subset or all, but with a timeout/limit to avoid hanging
            # To be efficient, we check if we can get at least one message from them
            count = 0
            for uid in potential_ids:
                try:
                    # If we can get history, they've messaged us or we messaged them
                    async for _ in app.get_chat_history(uid, limit=1):
                        await db.add_user(uid)
                        count += 1
                        break 
                except:
                    continue
            print(f"Extraction finished. Added {count} verified users.")
    except Exception as e:
        print(f"Extraction failed: {e}")

    print("Bot started and running...")
    await idle()
    if http_session and not http_session.closed:
        await http_session.close()
    await app.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())