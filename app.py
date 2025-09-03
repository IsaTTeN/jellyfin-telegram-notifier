import logging
import os
import json
import requests
import tempfile
import re
import base64
import threading
import time
import markdown
import smtplib
from requests.exceptions import HTTPError
from flask import Flask, request
from dotenv import load_dotenv
from apprise import Apprise
from urllib.parse import quote
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta, timezone
from collections import Counter, OrderedDict
import sqlite3
import hashlib

load_dotenv()
app = Flask(__name__)

# Set up logging
#log_directory = '/app/log'
log_directory = 'A:/git'
log_filename = os.path.join(log_directory, 'jellyfin_telegram-notifier.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure the log directory exists
os.makedirs(log_directory, exist_ok=True)

# Create a handler for rotating log files daily
rotating_handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=7)
rotating_handler.setLevel(logging.INFO)
rotating_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add the rotating handler to the logger
logging.getLogger().addHandler(rotating_handler)

# Constants
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GOTIFY_URL = os.environ.get("GOTIFY_URL", "").rstrip("/")
GOTIFY_TOKEN = os.environ.get("GOTIFY_TOKEN", "")
JELLYFIN_BASE_URL = os.environ["JELLYFIN_BASE_URL"].rstrip("/")
JELLYFIN_API_KEY = os.environ["JELLYFIN_API_KEY"]
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
MDBLIST_API_KEY = os.environ.get("MDBLIST_API_KEY", "")
IMGBB_API_KEY = os.environ.get("IMGBB_API_KEY", "")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
TMDB_LANGUAGE = os.getenv("TMDB_LANGUAGE", "en-US")  # напр. "ru-RU"
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/tv"
TMDB_BASE = "https://api.themoviedb.org/3"
LANGUAGE = os.environ["LANGUAGE"]
EPISODE_PREMIERED_WITHIN_X_DAYS = int(os.environ["EPISODE_PREMIERED_WITHIN_X_DAYS"])
SEASON_ADDED_WITHIN_X_DAYS = int(os.environ["SEASON_ADDED_WITHIN_X_DAYS"])
SIGNAL_URL = os.environ.get("SIGNAL_URL", "").rstrip("/")
SIGNAL_NUMBER = os.environ.get("SIGNAL_NUMBER", "")
SIGNAL_RECIPIENTS = os.environ.get("SIGNAL_RECIPIENTS", "")
WHATSAPP_API_URL = os.environ.get("WHATSAPP_API_URL", "").rstrip("/")
WHATSAPP_NUMBER = os.environ.get("WHATSAPP_NUMBER", "")
WHATSAPP_JID = os.environ.get("WHATSAPP_JID", "")
WHATSAPP_GROUP_JID = os.environ.get("WHATSAPP_GROUP_JID", "")
WHATSAPP_API_USERNAME = os.environ.get("WHATSAPP_API_USERNAME", "")
WHATSAPP_API_PWD = os.environ.get("WHATSAPP_API_PWD", "")
MATRIX_URL = os.environ.get("MATRIX_URL", "").rstrip("/")
MATRIX_ACCESS_TOKEN = os.environ.get("MATRIX_ACCESS_TOKEN", "")
MATRIX_ROOM_ID = os.environ.get("MATRIX_ROOM_ID", "")
SMTP_SUBJECT = "Новый релиз в Jellyfin"
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
SMTP_FROM = os.environ.get("SMTP_FROM", "")
SMTP_TO   = os.environ.get("SMTP_TO", "")  # список через запятую/пробел
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "1") not in ("0", "", "false", "False")   # для STARTTLS (587)
SMTP_USE_SSL = os.environ.get("SMTP_USE_SSL", "0") in ("1", "true", "True")   # для SMTPS (465); если 1, то TLS не используем
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "")   # ID канала, например C0123456789
#выключение контроля добавленного контента
DISABLE_DEDUP = os.getenv("NOTIFIER_DISABLE_DEDUP", "1").lower() in ("1", "true", "yes")
#настройки для фильмов
MOVIE_POLL_ENABLED = os.getenv("MOVIE_POLL_ENABLED", "1").lower() in ("1", "true", "yes")
MOVIE_POLL_INTERVAL_SEC = int(os.getenv("MOVIE_POLL_INTERVAL_SEC", "80"))   # каждые 5 минут
MOVIE_POLL_GRACE_MIN = int(os.getenv("MOVIE_POLL_GRACE_MIN", "45"))  # не трогать фильмы, созданные за последние N минут
MOVIE_POLL_PAGE_SIZE = int(os.getenv("MOVIE_POLL_PAGE_SIZE", "500"))  # сколько брать за 1 запрос
MOVIE_POLL_MAX_TOTAL = int(os.getenv("MOVIE_POLL_MAX_TOTAL", "0"))    # 0 = не ограничивать общее число
# GC БД качества
QUALITY_GC_ENABLED = os.getenv("QUALITY_GC_ENABLED", "1").lower() in ("1","true","yes","on")
QUALITY_GC_INTERVAL_HOURS = int(os.getenv("QUALITY_GC_INTERVAL_HOURS", "24"))   # как часто чистить
QUALITY_GC_GRACE_DAYS = int(os.getenv("QUALITY_GC_GRACE_DAYS", "1"))            # не трогать записи моложе N дней
QUALITY_GC_PAGE_SIZE = int(os.getenv("QUALITY_GC_PAGE_SIZE", "500"))            # сколько фильмов за раз тянуть из Jellyfin
# Форсированная одноразовая очистка БД качества при старте
FORCE_QUALITY_GC_ON_START = os.getenv("FORCE_QUALITY_GC_ON_START", "0").lower() in ("1","true","yes","on")
# Необязательное переопределение grace-срока именно для форс-запуска (по умолчанию 0 = удалять сразу)
FORCE_QUALITY_GC_GRACE_DAYS = os.getenv("FORCE_QUALITY_GC_GRACE_DAYS")
# Сжать БД после очистки
FORCE_QUALITY_GC_VACUUM = os.getenv("FORCE_QUALITY_GC_VACUUM", "0").lower() in ("1","true","yes","on")
#выключить отправку информации о звуковых дорожках
INCLUDE_AUDIO_TRACKS = os.getenv("INCLUDE_AUDIO_TRACKS", "1").lower() in ("1", "true", "yes", "on")
#подавление дублирующих сообщение webhook если это былдо обновление контента
SUPPRESS_WEBHOOK_AFTER_QUALITY_UPDATE_MIN = int(os.getenv("SUPPRESS_WEBHOOK_AFTER_QUALITY_UPDATE_MIN", "60"))  # по умолчанию 60 минут
# Опрос сериалов (по новым/изменённым эпизодам)
SERIES_POLL_ENABLED = os.getenv("SERIES_POLL_ENABLED", "1").lower() in ("1","true","yes","on")
SERIES_POLL_INTERVAL_SEC = int(os.getenv("SERIES_POLL_INTERVAL_SEC", "80"))  # период, сек
SERIES_POLL_PAGE_SIZE = int(os.getenv("SERIES_POLL_PAGE_SIZE", "500"))
SERIES_POLL_MAX_TOTAL = int(os.getenv("SERIES_POLL_MAX_TOTAL", "0"))  # 0 = без ограничения
SERIES_POLL_GRACE_MIN = int(os.getenv("SERIES_POLL_GRACE_MIN", "0"))  # свежие эпизоды отдаём на откуп вебхуку
# Посылать ли уведомление при ПЕРВОМ обнаружении сезона (по умолчанию нет)
SERIES_POLL_INITIAL_ANNOUNCE = os.getenv("SERIES_POLL_INITIAL_ANNOUNCE", "0").lower() in ("1","true","yes","on")
# Блокировать таймеры отправки на время сканирования библиотеки Jellyfin
NOTIFY_BLOCK_DURING_SCAN = os.getenv("NOTIFY_BLOCK_DURING_SCAN", "1").lower() in ("1","true","yes","on")
SCAN_RECHECK_DELAY_SEC = int(os.getenv("SCAN_RECHECK_DELAY_SEC", "5"))   # пауза между проверками
MAX_SCAN_WAIT_MIN = int(os.getenv("MAX_SCAN_WAIT_MIN", "0"))             # 0 = ждать бесконечно
# Какие имена задач считать «сканом» (нижний регистр, через запятую)
SCAN_TASK_NAME_MATCH = [s.strip() for s in os.getenv(
    "SCAN_TASK_NAME_MATCH",
    "scan,library,metadata,refresh"
).lower().split(",") if s.strip()]
EXTERNAL_CACHE_ENABLED = os.getenv("EXTERNAL_CACHE_ENABLED", "1").lower() in ("1","true","yes","on")
TRAILER_CACHE_TTL_DAYS = int(os.getenv("TRAILER_CACHE_TTL_DAYS", "30"))
RATINGS_CACHE_TTL_DAYS = int(os.getenv("RATINGS_CACHE_TTL_DAYS", "14"))
# Пределы для блока аудио-дорожек у сезонов
SEASON_AUDIO_TRACKS_MAX = int(os.getenv("SEASON_AUDIO_TRACKS_MAX", "12"))   # максимум уникальных дорожек
SEASON_AUDIO_SCAN_LIMIT = int(os.getenv("SEASON_AUDIO_SCAN_LIMIT", "50"))   # максимум серий для сканирования (present)
#Для whatsapp повторные отправки
WHATSAPP_IMAGE_RETRY_ATTEMPTS = int(os.getenv("WHATSAPP_IMAGE_RETRY_ATTEMPTS", "3"))
WHATSAPP_IMAGE_RETRY_DELAY_SEC = int(os.getenv("WHATSAPP_IMAGE_RETRY_DELAY_SEC", "2"))
# --- Episode/Season quality polling (по сериям -> уведомление на сезон) ---
EP_QUALITY_POLL_ENABLED = (os.getenv("EP_QUALITY_POLL_ENABLED", "1").lower() in ("1","true","yes","on"))
EP_QUALITY_POLL_INTERVAL_SEC = int(os.getenv("EP_QUALITY_POLL_INTERVAL_SEC", "90"))
EP_QUALITY_POLL_PAGE_SIZE = int(os.getenv("EP_QUALITY_POLL_PAGE_SIZE", "500"))
EP_QUALITY_POLL_MAX_TOTAL = int(os.getenv("EP_QUALITY_POLL_MAX_TOTAL", "0"))  # 0 = без ограничения
# Для "свежих" эпизодов можно переиспользовать SERIES_POLL_GRACE_MIN


# Глобальные переменные
imgbb_upload_done = threading.Event()   # Сигнал о завершении загрузки
uploaded_image_url = None               # Здесь хранится ссылка после удачной загрузки
# Gotify больше не добавляем в APPRISE_URLS вообще!
APPRISE_OTHER_URLS = os.environ.get("APPRISE_OTHER_URLS", "")
APPRISE_URLS = APPRISE_OTHER_URLS.strip()

apobj = Apprise()
for url in APPRISE_URLS.split():
    apobj.add(url)

# Path for the JSON file to store notified items
#notified_items_file = '/app/data/notified_items.json'
notified_items_file = 'A:/git/notified_items.json'

# === SQLite для качества (только для Movie на первом этапе) ===
QUALITY_DB_FILE = os.path.join(os.path.dirname(notified_items_file), "media_quality.db")
os.makedirs(os.path.dirname(QUALITY_DB_FILE), exist_ok=True)

def _utcnow_iso() -> str:
    """
    Возвращает текущий момент в UTC в формате ISO 8601 c 'Z' на конце,
    без микросекунд (например: 2025-08-31T19:45:00Z).
    """
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')

def _init_quality_db():
    conn = sqlite3.connect(QUALITY_DB_FILE)
    try:
        cur = conn.cursor()
        # снимок по конкретному ItemId (история)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS media_quality (
            item_id TEXT PRIMARY KEY,
            movie_name TEXT,
            year INTEGER,
            video_codec TEXT,
            video_bitrate INTEGER,
            width INTEGER,
            height INTEGER,
            fps REAL,
            bit_depth INTEGER,
            dynamic_range TEXT,
            audio_codec TEXT,
            audio_bitrate INTEGER,
            audio_channels INTEGER,
            container TEXT,
            size_bytes INTEGER,
            duration_sec REAL,
            signature TEXT,
            date_seen TEXT
        )""")
        # "последняя версия" по логическому ключу (tmdb/imdb или name+year)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS content_quality (
            logical_key TEXT PRIMARY KEY,
            last_item_id TEXT,
            movie_name TEXT,
            year INTEGER,
            video_codec TEXT,
            video_bitrate INTEGER,
            width INTEGER,
            height INTEGER,
            fps REAL,
            bit_depth INTEGER,
            dynamic_range TEXT,
            audio_codec TEXT,
            audio_bitrate INTEGER,
            audio_channels INTEGER,
            container TEXT,
            size_bytes INTEGER,
            duration_sec REAL,
            signature TEXT,
            date_seen TEXT
        )""")
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS recent_quality_updates
                    (
                        logical_key
                        TEXT
                        PRIMARY
                        KEY,
                        notified_at
                        TEXT,
                        item_id
                        TEXT
                    )
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS season_progress
                    (
                        season_id
                        TEXT
                        PRIMARY
                        KEY,
                        series_id
                        TEXT,
                        series_name
                        TEXT,
                        season_number
                        INTEGER,
                        release_year
                        INTEGER,
                        present
                        INTEGER
                        DEFAULT
                        0, -- сколько фактически есть серий на диске
                        total
                        INTEGER
                        DEFAULT
                        0, -- сколько всего серий (present + missing)
                        last_notified_present
                        INTEGER
                        DEFAULT
                        0, -- до какого значения уже сообщали
                        updated_at
                        TEXT
                    )
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS external_cache
                    (
                        cache_key
                        TEXT
                        PRIMARY
                        KEY,  -- уникальный ключ (см. ниже)
                        kind
                        TEXT
                        NOT
                        NULL, -- 'trailer' | 'ratings'
                        subkind
                        TEXT, -- 'movie' | 'show' (для рейтингов/трейлеров)
                        value
                        TEXT, -- для трейлера: URL; для рейтингов: готовый текст
                        updated_at
                        TEXT  -- ISO8601 UTC, когда обновляли
                    )
                    """)
        # метаданные приложения/БД
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS app_meta
                    (
                        key
                        TEXT
                        PRIMARY
                        KEY,
                        value
                        TEXT
                    )
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS season_quality
                    (
                        season_id
                        TEXT
                        PRIMARY
                        KEY,
                        series_id
                        TEXT,
                        series_name
                        TEXT,
                        season_number
                        INTEGER,
                        release_year
                        INTEGER,
                        signature
                        TEXT, -- агрегированный снимок качества по доступным эпизодам
                        updated_at
                        TEXT  -- ISO
                    )""")
        # Миграция: добавим episode_count, если столбца нет
        cur.execute("PRAGMA table_info(season_quality)")
        cols = {r[1] for r in cur.fetchall()}
        if "episode_count" not in cols:
            cur.execute("ALTER TABLE season_quality ADD COLUMN episode_count INTEGER")
        # если нет штампа создания БД — проставим сейчас
        cur.execute("SELECT value FROM app_meta WHERE key='db_created_at'")
        row = cur.fetchone()
        if not row:
            cur.execute("INSERT INTO app_meta(key,value) VALUES('db_created_at', ?)", (_utcnow_iso(),))
        # --- Мягкая миграция: добавляем колонку image_profiles, если её ещё нет
        try:
            cur.execute("ALTER TABLE media_quality ADD COLUMN image_profiles TEXT")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE content_quality ADD COLUMN image_profiles TEXT")
        except Exception:
            pass
        conn.commit()
    finally:
        conn.close()

_init_quality_db()

# Убедимся, что папка /app/data существует
os.makedirs(os.path.dirname(notified_items_file), exist_ok=True)

# Function to load notified items from the JSON file
def load_notified_items():
    # Если файл есть — читаем
    if os.path.exists(notified_items_file):
        with open(notified_items_file, 'r', encoding='utf-8') as file:
            return json.load(file)
    # Иначе — создаём пустой JSON и возвращаем пустой словарь
    with open(notified_items_file, 'w', encoding='utf-8') as file:
        json.dump({}, file, ensure_ascii=False, indent=2)
    return {}

# Function to save notified items to the JSON file
def save_notified_items(notified_items_to_save):
    with open(notified_items_file, 'w', encoding='utf-8') as file:
        json.dump(notified_items_to_save, file, ensure_ascii=False, indent=2)


notified_items = load_notified_items()

# 2. Словарь переводов
MESSAGES = {
    "en": {
        "new_movie_title": "🍿New Movie Added🍿",
        "new_season_title": "📺New Season Added📺",
        "new_episode_title": "📺New Episode Added📺",
        "new_album_title": "🎵New Album Added🎵",
        "new_runtime": "🕒Runtime🕒",
        "new_ratings_movie": "⭐Ratings movie⭐",
        "new_ratings_show": "⭐Ratings show⭐",
        "new_trailer": "Trailer",
        "new_release_date": "Release Date",
        "new_series": "Series",
        "new_episode_t": "Episode Title",
        "audio_tracks": "Audio tracks",
        "image_profiles": "Image profiles",
        "quality_updated": "🔼Quality update🔼",
        "season_added_progress": "Added {added} of {total} episodes",
        "season_added_count_only": "Added {added} episodes",
    },
    "ru": {
        "new_movie_title": "🍿Новый фильм добавлен🍿",
        "new_season_title": "📺Новый сезон добавлен📺",
        "new_episode_title": "📺Новый эпизод добавлен📺",
        "new_album_title": "🎵Новый альбом добавлен🎵",
        "new_runtime": "🕒Продолжительность🕒",
        "new_ratings_movie": "⭐Рейтинги фильма⭐",
        "new_ratings_show": "⭐Рейтинги сериала⭐",
        "new_trailer": "Трейлер",
        "new_release_date": "Дата выхода",
        "new_series": "Сериал",
        "new_episode_t": "Название эпизода",
        "audio_tracks": "Аудио-дорожки",
        "image_profiles": "Профили изображения",
        "quality_updated": "🔼Обновление качества🔼",
        "season_added_progress": "Добавлено {added} из {total} серий",
        "season_added_count_only": "Добавлено {added} серий",
    }
}
#Выбираем рабочий язык: если заданный отсутствует в MESSAGES — ставим en
LANG = LANGUAGE if LANGUAGE in MESSAGES else "en"

def t(key: str) -> str:
    """
    Возвращает перевод по ключу для текущего языка LANG.
    Если ключ отсутствует — падает KeyError, чтобы вы не пропустили необходимость перевода.
    """
    return MESSAGES[LANG][key]

#Обнаружение сканирования
def _task_name_matches(name: str | None) -> bool:
    if not name:
        return False
    n = name.lower()
    return any(seg in n for seg in SCAN_TASK_NAME_MATCH)

def is_jellyfin_scanning() -> tuple[bool, str | None]:
    """
    Пытаемся понять, выполняется ли сейчас скан/рефреш медиатеки/метаданных в Jellyfin.
    1) /emby/ScheduledTasks/Running (если поддерживается)
    2) /emby/ScheduledTasks (ищем состояния Running/Executing/IsRunning)
    Возврат: (True/False, краткое описание)
    """
    headers = {'accept': 'application/json'}
    params = {'api_key': JELLYFIN_API_KEY}

    # 1) текущие выполняемые задачи
    try:
        url = f"{JELLYFIN_BASE_URL}/emby/ScheduledTasks/Running"
        r = requests.get(url, headers=headers, params=params, timeout=6)
        if r.status_code == 200:
            data = r.json() or []
            for t in data:
                name = t.get("Name") or t.get("Key") or ""
                state = t.get("State") or ""
                prog = t.get("CurrentProgressPercentage") or t.get("Progress") or t.get("PercentComplete")
                if _task_name_matches(name):
                    desc = f"{name} {prog}%" if prog is not None else name
                    return True, desc
    except Exception:
        pass

    # 2) общий список задач
    try:
        url = f"{JELLYFIN_BASE_URL}/emby/ScheduledTasks"
        r = requests.get(url, headers=headers, params=params, timeout=8)
        if r.status_code == 200:
            data = r.json() or []
            for t in data:
                name = t.get("Name") or t.get("Key") or ""
                state = (t.get("State") or "").lower()
                is_running = bool(t.get("IsRunning")) or state in ("running", "executing", "inprogress")
                if is_running and _task_name_matches(name):
                    prog = t.get("CurrentProgressPercentage") or t.get("Progress") or t.get("PercentComplete")
                    desc = f"{name} {prog}%" if prog is not None else name
                    return True, desc
    except Exception:
        pass

    return False, None

def wait_until_scan_idle(reason: str = ""):
    """
    Если включён NOTIFY_BLOCK_DURING_SCAN — ждём завершения скана Jellyfin.
    MAX_SCAN_WAIT_MIN=0 => ждём бесконечно.
    """
    if not NOTIFY_BLOCK_DURING_SCAN:
        return
    start = time.time()
    first_log = True
    while True:
        running, detail = is_jellyfin_scanning()
        if not running:
            if not first_log:
                logging.info("Jellyfin scan finished, resume timers.")
            return
        if first_log:
            logging.info(f"Timers paused: Jellyfin is scanning ({detail or 'library task running'})"
                         + (f" [reason: {reason}]" if reason else ""))
            first_log = False
        if MAX_SCAN_WAIT_MIN and (time.time() - start) > MAX_SCAN_WAIT_MIN * 60:
            logging.warning("Max wait for scan reached; resuming timers anyway.")
            return
        time.sleep(max(SCAN_RECHECK_DELAY_SEC, 1))

def _movie_poll_loop():
    while True:
        try:
            wait_until_scan_idle("movie poll")
            poll_recent_movies_once()
        except Exception as ex:
            logging.warning(f"Movie poll loop error: {ex}")
        time.sleep(MOVIE_POLL_INTERVAL_SEC)

def _series_poll_loop():
    while True:
        try:
            wait_until_scan_idle("series poll")
            poll_recent_episodes_once()
        except Exception as ex:
            logging.warning(f"Series poll loop error: {ex}")
        time.sleep(SERIES_POLL_INTERVAL_SEC)

def _wa_get_jid_from_env():
    """
    Возвращает JID из окружения.
    Если задана группа — возвращаем группу.
    Иначе личный чат из WHATSAPP_JID или WHATSAPP_NUMBER.
    """
    group_jid = WHATSAPP_GROUP_JID.strip()
    if group_jid:
        if not group_jid.endswith("@g.us"):
            # допустим, передали только id без @g.us
            group_jid = re.sub(r"[^\w\-]", "", group_jid) + "@g.us"
        return group_jid

    # Личный
    raw = (WHATSAPP_JID or WHATSAPP_NUMBER).strip()
    if not raw:
        return None
    if raw.endswith("@s.whatsapp.net"):
        return raw
    # очищаем до цифр и добавляем домен
    local = re.sub(r"\D", "", raw)
    return f"{local}@s.whatsapp.net" if local else None

def jellyfin_get_tmdb_id(item_id: str) -> str | None:
    """
    Возвращает TMDB ID для любого элемента Jellyfin по его Id.
    Читает Items?Ids=...&Fields=ProviderIds и берёт нужный ключ из ProviderIds.
    """
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "Ids": item_id,
            "Fields": "ProviderIds"
        }
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        items = (r.json() or {}).get("Items") or []
        if not items:
            return None
        prov = items[0].get("ProviderIds") or {}
        # разные сервера/версии могут звать ключ по-разному — учтём варианты
        return prov.get("Tmdb") or prov.get("TmdbId") or prov.get("TMDB") or None
    except Exception as ex:
        logging.warning(f"Failed to read ProviderIds for {item_id}: {ex}")
        return None


def fetch_mdblist_ratings(content_type: str, tmdb_id: str) -> str:
    """
    Запрос к https://api.mdblist.com/tmdb/{type}/{tmdbId}
    и формирование текста с найденными рейтингами.
    Возвращает строку вида:
      "- IMDb: 7.8\n- Rotten Tomatoes: 84%\n…"
    или пустую строку при ошибке/отсутствии данных.
    """
    url = f"https://api.mdblist.com/tmdb/{content_type}/{tmdb_id}?apikey={MDBLIST_API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        ratings = data.get("ratings")
        if not isinstance(ratings, list):
            return ""

        lines = []
        for r in ratings:
            source = r.get("source")
            value = r.get("value")
            if source is None or value is None:
                continue
            lines.append(f"- {source}: {value}")

        return "\n".join(lines)
    except requests.RequestException as e:
        app.logger.warning(f"MDblist API error for {content_type}/{tmdb_id}: {e}")
        return ""

def upload_image_to_imgbb(image_bytes):
    """
    Загружает изображение на imgbb.com (до 3 попыток) и устанавливает событие по завершении.
    """
    global uploaded_image_url
    uploaded_image_url = None
    imgbb_upload_done.clear()  # Сброс события

    # Проверка наличия ключа API
    if not IMGBB_API_KEY:
        logging.debug("IMGBB_API_KEY не задан — пропускаем загрузку на imgbb.")
        imgbb_upload_done.set()  # Сигнал о завершении (пропуск загрузки)
        return None

    url = "https://api.imgbb.com/1/upload"
    payload = {
        "key": IMGBB_API_KEY,
        "image": base64.b64encode(image_bytes).decode('utf-8')
    }

    for attempt in range(1, 4):
        try:
            logging.info(f"Попытка загрузки на imgbb #{attempt}")
            response = requests.post(url, data=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
            uploaded_image_url = data['data']['url']
            logging.info(f"Изображение успешно загружено на imgbb: {uploaded_image_url}")
            break
        except Exception as ex:
            logging.warning(f"Ошибка загрузки на imgbb (попытка {attempt}): {ex}")
            if attempt < 3:
                time.sleep(2)  # Пауза между попытками

    imgbb_upload_done.set()  # Сигнал, что загрузка завершена (успешно или нет)
    return uploaded_image_url

def wait_for_imgbb_upload():
    """
    Блокирует выполнение до завершения загрузки изображения.
    """
    imgbb_upload_done.wait()
    return uploaded_image_url


def get_jellyfin_image_and_upload_imgbb(photo_id):
    """
    Скачивает изображение из Jellyfin и загружает его на imgbb, возвращая публичную ссылку.
    """
    jellyfin_image_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    try:
        resp = requests.get(jellyfin_image_url)
        resp.raise_for_status()
        return upload_image_to_imgbb(resp.content)
    except Exception as ex:
        logging.warning(f"Ошибка скачивания из Jellyfin: {ex}")
        return None

def send_discord_message(photo_id, message, title="Jellyfin", uploaded_url=None):
    """
    Отправляет уведомление в Discord через Webhook.
    Картинку берём НАПРЯМУЮ из Jellyfin и прикрепляем как файл.
    Embed ссылается на неё через attachment://filename.
    """
    if not DISCORD_WEBHOOK_URL:
        logging.warning("DISCORD_WEBHOOK_URL not set, skipping Discord notification.")
        return None

    # 1) тянем постер из Jellyfin
    jellyfin_image_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    image_bytes = None
    filename = "poster.jpg"
    mimetype = "image/jpeg"
    try:
        r = requests.get(jellyfin_image_url, timeout=30)
        r.raise_for_status()
        image_bytes = r.content
        ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip().lower()
        if "png" in ct:
            filename, mimetype = "poster.png", "image/png"
        elif "webp" in ct:
            filename, mimetype = "poster.webp", "image/webp"
    except Exception as ex:
        logging.warning(f"Discord: failed to fetch image from Jellyfin: {ex}")

    # 2) готовим payload
    payload = {
        "username": title,
        "content": message
    }

    # если есть картинка — добавим embed, указывающий на attachment
    if image_bytes:
        payload["embeds"] = [{
            "image": {"url": f"attachment://{filename}"}
        }]

    try:
        if image_bytes:
            # multipart: payload_json + файл
            files = {
                "file": (filename, image_bytes, mimetype)
            }
            resp = requests.post(
                DISCORD_WEBHOOK_URL,
                data={"payload_json": json.dumps(payload, ensure_ascii=False)},
                files=files,
                timeout=30
            )
        else:
            # без картинки — обычный JSON
            resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=30)

        resp.raise_for_status()
        logging.info("Discord notification sent successfully")
        return resp
    except Exception as ex:
        logging.warning(f"Error sending to Discord: {ex}")
        return None

def clean_markdown_for_apprise(text):
    """
    Упрощает markdown-подобные уведомления для plain text и приводит ссылки к единому виду:
    - [текст](url) -> url
    - Убирает повторяющиеся подряд одинаковые url
    - Добавляет префикс '🎥 <перевод new_trailer>:' перед каждой ссылкой (без дублирования)
    - Очищает лишние пробелы по краям строк
    """
    if not text:
        return text

    # 0) Получаем локализованную метку для "Трейлер"
    try:
        trailer_label = t("new_trailer")
    except Exception:
        trailer_label = MESSAGES.get(LANG, {}).get("new_trailer", "Trailer")
    if not trailer_label:
        trailer_label = "Trailer"
    # 1) [текст](url) -> url
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\s)]+)\)', r'\2', text)

    # 2) Убираем подряд идущие повторы одного и того же URL
    text = re.sub(r'(https?://\S+)(\s*\1)+', r'\1', text)

    # 3) Сначала убираем уже проставленные префиксы, чтобы не получить дубликаты,
    #    затем добавим их единообразно
    prefix_pattern = rf'🎥\s*{re.escape(trailer_label)}[:]?\s*'
    text = re.sub(rf'{prefix_pattern}(https?://\S+)', r'\1', text)

    # 4) Префиксуем ТОЛЬКО не-musicbrainz ссылки (через колбэк)
    def _prefix_non_mb(m):
        url = m.group(1)
        if re.search(r'https?://(?:[^/\s)]+\.)*musicbrainz\.org(?=[/\s)]|$)', url, re.IGNORECASE):
            return url
        return f'🎥 {trailer_label}: {url}'

    text = re.sub(r'(https?://\S+)', _prefix_non_mb, text)
    # 5) Чистим лишние пробелы по краям строк (сохраняем переносы)
    text = '\n'.join(line.strip() for line in text.split('\n'))

    # Убрать *жирный* и _курсив_
    text = re.sub(r'(\*|_){1,3}(.+?)\1{1,3}', r'\2', text)

    return text

def sanitize_whatsapp_text(text: str) -> str:
    if not text:
        return text

    # Берём язык из переменной окружения
    lang = os.environ.get("LANGUAGE", "en")
    trailer_label = MESSAGES.get(lang, {}).get("new_trailer")

    # 1) Превращаем [любой текст](https://...) в просто https://...
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)', r'\2', text)

    # 2) Убираем подряд идущие повторы одного и того же URL
    text = re.sub(r'(https?://\S+)(\s*\1)+', r'\1', text)


    # 3) Сносим уже поставленные префиксы (на всякий)
    prefix_re = rf'🎥\s*{re.escape(trailer_label)}:?[\s]*'
    text = re.sub(rf'{prefix_re}(https?://\S+)', r'\1', text)

    # 4) Префиксуем ТОЛЬКО не-musicbrainz ссылки (через колбэк)
    def _prefix_non_mb(m):
        url = m.group(1)
        if re.search(r'https?://(?:[^/\s)]+\.)*musicbrainz\.org(?=[/\s)]|$)', url, re.IGNORECASE):
            return url
        return f'🎥 {trailer_label} {url}'

    text = re.sub(r'(https?://\S+)', _prefix_non_mb, text)

    # 5) Чистим лишние пробелы
    text = re.sub(r'[ \t]+', ' ', text).strip()

    return text

def send_email_with_image_jellyfin(photo_id: str, subject: str, body_markdown: str):
    """
    Отправляет email с:
      - text/plain (plain-версия текста)
      - text/html (Markdown → HTML)
      - inline-изображением из Jellyfin (через CID)
    Возвращает True/False.
    """
    if not (SMTP_HOST and SMTP_FROM and SMTP_TO):
        logging.debug("Email disabled or misconfigured; skip.")
        return False

    # plain-версия (без форматирования) — используем ваш очиститель
    body_plain = clean_markdown_for_apprise(body_markdown or "")

    # HTML-версия — рендерим из Markdown
    # extensions для более приятных списков/переносов
    body_html_rendered = markdown.markdown(
        body_markdown or "",
        extensions=["extra", "sane_lists", "nl2br"]
    )

    # Тянем картинку из Jellyfin (с повторами)
    img_bytes = None
    img_subtype = "jpeg"
    try:
        img_bytes = _fetch_jellyfin_image_with_retries(photo_id, attempts=3, timeout=10, delay=1.5)
        # subtype подберём осторожно (если есть headers в ретрае — можно хранить вместе)
        # здесь предполагаем jpeg; при желании можно расширить определение
    except Exception as ex:
        logging.warning(f"Email: failed to fetch Jellyfin image: {ex}")

    msg = EmailMessage()
    msg["Subject"] = subject or SMTP_SUBJECT
    msg["From"]    = SMTP_FROM
    recipients = [x.strip() for x in re.split(r"[,\s]+", SMTP_TO) if x.strip()]
    msg["To"]     = ", ".join(recipients)
    msg["Date"]   = formatdate(localtime=True)

    # 1) text/plain
    msg.set_content(body_plain or "")

    # 2) text/html (+ inline image при наличии)
    if img_bytes:
        cid = make_msgid()  # вида <...@domain>
        html_part = f"""\
<html>
  <body>
    <div>{body_html_rendered}</div>
    <p><img src="cid:{cid[1:-1]}" alt="poster"></p>
  </body>
</html>"""
        msg.add_alternative(html_part, subtype="html")
        try:
            # прикрепляем картинку к HTML-части как related
            msg.get_payload()[1].add_related(img_bytes, maintype="image", subtype=img_subtype, cid=cid)
        except Exception as ex:
            logging.warning(f"Email: cannot embed inline image (fallback as attachment): {ex}")
            msg.add_attachment(img_bytes, maintype="image", subtype=img_subtype, filename="poster.jpg")
    else:
        # нет картинки — просто HTML без тега <img>
        msg.add_alternative(f"<html><body>{body_html_rendered}</body></html>", subtype="html")

    # Отправка
    try:
        if SMTP_USE_SSL or SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_USER:
                    s.login(SMTP_USER, SMTP_PASS)
                s.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_USE_TLS:
                    s.starttls()
                if SMTP_USER:
                    s.login(SMTP_USER, SMTP_PASS)
                s.send_message(msg)
        logging.info("Email notification (Markdown->HTML) sent successfully")
        return True
    except Exception as ex:
        logging.warning(f"Email send failed: {ex}")
        return False

def _slack_try_join_channel(channel_id: str) -> bool:
    """
    Пытается добавить бота в PUBLIC-канал (требует scope channels:join).
    Для приватных каналов не сработает — нужно вручную /invite в Slack.
    """
    if not (SLACK_BOT_TOKEN and channel_id):
        return False
    try:
        resp = requests.post(
            "https://slack.com/api/conversations.join",
            headers={
                "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"channel": channel_id},
            timeout=15,
        )
        data = resp.json()
        if not data.get("ok"):
            logging.debug(f"Slack join failed/ignored: {data.get('error')}")
            return False
        return True
    except Exception as ex:
        logging.debug(f"Slack join error: {ex}")
        return False

def send_slack_text_only(message_markdown: str) -> bool:
    """
    Фоллбэк на чат без файла. Использует chat.postMessage.
    """
    if not (SLACK_BOT_TOKEN and SLACK_CHANNEL_ID):
        logging.debug("Slack disabled/misconfigured; skip text.")
        return False

    url = "https://slack.com/api/chat.postMessage"
    # Slack понимает mrkdwn (не совсем Markdown). Можно слегка «очистить» текст:
    text_plain = sanitize_whatsapp_text(message_markdown) or ""

    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "text": text_plain,
        "mrkdwn": True,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            logging.warning(f"Slack chat.postMessage error: {data}")
            return False
        logging.info("Slack text message sent successfully")
        return True
    except Exception as ex:
        logging.warning(f"Slack text send failed: {ex}")
        return False


def send_slack_message_with_image_from_jellyfin(photo_id: str, caption_markdown: str) -> bool:
    """
    Slack: загрузка файла по новому потоку:
      1) files.getUploadURLExternal (получаем upload_url и file_id)
      2) POST байтов картинки на upload_url
      3) files.completeUploadExternal (channel_id + initial_comment)
    Фоллбэк: отправляем просто текст через chat.postMessage.
    """
    if not (SLACK_BOT_TOKEN and SLACK_CHANNEL_ID):
        logging.debug("Slack disabled/misconfigured; skip.")
        return False

    # 1) достаём картинку из Jellyfin
    img_bytes = None
    filename = "poster.jpg"
    mimetype = "image/jpeg"
    try:
        if "_fetch_jellyfin_primary" in globals():
            b, mt, fn = _fetch_jellyfin_primary(photo_id)
            img_bytes, mimetype, filename = b, mt, fn
        else:
            jf_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
            r = requests.get(jf_url, timeout=30)
            r.raise_for_status()
            img_bytes = r.content
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip().lower()
            if "png" in ct:
                filename, mimetype = "poster.png", "image/png"
            elif "webp" in ct:
                filename, mimetype = "poster.webp", "image/webp"
    except Exception as ex:
        logging.warning(f"Slack: failed to fetch image from Jellyfin: {ex}")

    if not img_bytes:
        # нет картинки — отправим текст
        return send_slack_text_only(caption_markdown)

    # 2) files.getUploadURLExternal
    auth_h = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    try:
        resp = requests.post(
            "https://slack.com/api/files.getUploadURLExternal",
            headers=auth_h,
            data={"filename": filename, "length": str(len(img_bytes))},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            logging.warning(f"Slack getUploadURLExternal error: {data}")
            return send_slack_text_only(caption_markdown)
        upload_url = data["upload_url"]
        file_id    = data["file_id"]
    except Exception as ex:
        logging.warning(f"Slack getUploadURLExternal failed: {ex}")
        return send_slack_text_only(caption_markdown)

    # 3) POST файла на upload_url
    try:
        # можно сырыми байтами:
        up_headers = {"Content-Type": mimetype}
        up = requests.post(upload_url, data=img_bytes, headers=up_headers, timeout=60)
        # альтернативно: multipart (иногда помогает при прокси):
        # up = requests.post(upload_url, files={"filename": (filename, img_bytes, mimetype)}, timeout=60)
        if up.status_code != 200:
            logging.warning(f"Slack upload_url returned {up.status_code}: {up.text[:200]}")
            return send_slack_text_only(caption_markdown)
    except Exception as ex:
        logging.warning(f"Slack raw upload failed: {ex}")
        return send_slack_text_only(caption_markdown)

    # 4) files.completeUploadExternal (шарим файл в канал + комментарий)
    def _complete_upload():
        comp_payload = {
            "files": [{"id": file_id, "title": filename}],
            "channel_id": SLACK_CHANNEL_ID,
            "initial_comment": sanitize_whatsapp_text(caption_markdown) or "",
        }
        return requests.post(
            "https://slack.com/api/files.completeUploadExternal",
            headers={**auth_h, "Content-Type": "application/json; charset=utf-8"},
            json=comp_payload,
            timeout=30,
        )

    # попытка заранее присоединиться (на случай публичного канала)
    _slack_try_join_channel(SLACK_CHANNEL_ID)

    try:
        comp = _complete_upload()
        comp.raise_for_status()
        comp_data = comp.json()
        if not comp_data.get("ok"):
            if comp_data.get("error") == "not_in_channel":
                # пробуем присоединиться и повторить один раз
                if _slack_try_join_channel(SLACK_CHANNEL_ID):
                    comp = _complete_upload()
                    comp.raise_for_status()
                    comp_data = comp.json()
                    if comp_data.get("ok"):
                        logging.info("Slack image sent successfully (after join).")
                        return True
                logging.warning("Slack: bot is not in the channel. Invite the app (/invite @Bot) and retry.")
            else:
                logging.warning(f"Slack completeUploadExternal error: {comp_data}")
            return send_slack_text_only(caption_markdown)

        logging.info("Slack image (external upload flow) sent successfully")
        return True

    except Exception as ex:
        logging.warning(f"Slack completeUploadExternal failed: {ex}")
        return send_slack_text_only(caption_markdown)

def send_notification(photo_id, caption):
    uploaded_url = get_jellyfin_image_and_upload_imgbb(photo_id)
    """
    1. Всегда отправляет в Telegram напрямую (send_telegram_photo).
    2. Независимо — отправляет напрямую в Gotify (если включен).
    3. Остальные сервисы — через Apprise.
    """
    # Текст без Markdown (подходит для plain-транспорта, в т.ч. WhatsApp)
#    caption_plain = clean_markdown_for_apprise(caption)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        tg_response = send_telegram_photo(photo_id, caption)
        if tg_response and tg_response.ok:
            logging.info("Notification sent via Telegram")
        else:
            # ФОЛБЭК: разбиваем на два сообщения (фото -> текст)
            logging.warning("Telegram (photo+caption) failed; trying split: photo-only then text…")
            ok_photo = send_telegram_photo_only(photo_id)
            ok_text  = send_telegram_text(caption)
            if ok_photo and ok_text:
                logging.info("Telegram split (photo then text) sent successfully")
            else:
                logging.warning("Telegram split fallback failed")
#    tg_GOTIFY = send_gotify_message(photo_id, caption)

    # Gotify: только если параметры заданы
#    gotify_message = clean_markdown_for_apprise(caption)
#    gotify_response = None
    if GOTIFY_URL and GOTIFY_TOKEN:
        gotify_response = send_gotify_message(photo_id, caption, uploaded_url=uploaded_url)
        if gotify_response and gotify_response.ok:
            logging.info("Notification sent via Gotify")
        else:
            logging.warning("Notification failed via Gotify")

    # ======= ДОБАВЛЕНО ДЛЯ DISCORD =======
    if DISCORD_WEBHOOK_URL:
        discord_response = send_discord_message(photo_id, caption, uploaded_url=uploaded_url)
        if discord_response and discord_response.ok:
            logging.info("Notification sent via Discord")
        else:
            logging.warning("Notification failed via Discord")
    # =====================================
    # ======= SLACK: файл-изображение с комментарием =======
    try:
        if SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
            ok = send_slack_message_with_image_from_jellyfin(photo_id, caption)
            if ok:
                logging.info("Notification sent via Slack")
            else:
                logging.warning("Notification failed via Slack")
        else:
            logging.debug("Slack disabled or not configured; skip.")
    except Exception as sl_ex:
        logging.warning(f"Slack send failed: {sl_ex}")
    # ======================================================
    # ======= MATRIX (REST): СНАЧАЛА изображение из Jellyfin, затем текст =======
    try:
        if MATRIX_URL and MATRIX_ACCESS_TOKEN and MATRIX_ROOM_ID:
            ok = send_matrix_image_then_text_from_jellyfin(photo_id, caption)
            if ok:
                logging.info("Notification sent via Matrix (REST, image from Jellyfin then text)")
            else:
                logging.warning("Matrix (REST, Jellyfin): image+text flow failed; trying text-only fallback")
                send_matrix_text_rest(caption)
        else:
            logging.debug("Matrix disabled or not configured; skip.")
    except Exception as m_ex:
        logging.warning(f"Matrix send failed: {m_ex}")
    # ========================================================================
    # --- ОТПРАВКА В SIGNAL ---
    # Plain text для Signal (без Markdown)
    if SIGNAL_URL and SIGNAL_NUMBER:
        signal_resp = send_signal_message_with_image(
            photo_id,
            clean_markdown_for_apprise(caption),
            SIGNAL_NUMBER,
            SIGNAL_RECIPIENTS
        )
        if signal_resp and signal_resp.ok:
            logging.info("Notification sent via Signal")
        else:
            logging.warning("Notification failed via Signal")
    # --------------------------

    # ======= EMAIL: письмо с inline-картинкой из Jellyfin =======
    try:
        email_ok = send_email_with_image_jellyfin(photo_id, subject=SMTP_SUBJECT, body_markdown=caption)
        if email_ok:
            logging.info("Notification sent via Email")
        else:
            logging.warning("Notification failed via Email")
    except Exception as em_ex:
        logging.warning(f"Email send failed: {em_ex}")

    # ======= WHATSAPP: сначала картинка с подписью (с ретраями), при провале — текст =======
    try:
        wa_jid = _wa_get_jid_from_env()
        if WHATSAPP_API_URL and wa_jid:
            ok_img = send_whatsapp_image_with_retries(
                caption=caption,
                phone_jid=wa_jid,
                image_url=uploaded_url
            )
            if not ok_img:
                logging.warning("WhatsApp image failed after retries; sending text-only fallback")
                send_whatsapp_text_via_rest(caption, phone_jid=wa_jid)
        else:
            logging.debug("WhatsApp disabled or no JID; skip WhatsApp send.")
    except Exception as wa_ex:
        logging.warning(f"WhatsApp send block failed: {wa_ex}")

    other_services = [url for url in APPRISE_URLS.split() if url]  # убираем пустые строки
    if other_services:
        apprise_obj = Apprise()
        for url in other_services:
            apprise_obj.add(url)

        # Готовим временный файл для картинки (если фото есть)

    base_photo_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    attach_param = None
    try:
        image_response = requests.get(base_photo_url)
        if image_response.ok:
            # Сохраняем изображение во временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
                tmp.write(image_response.content)
                tmp_path = tmp.name
            attach_param = tmp_path
        else:
            attach_param = None
    except Exception as ex:
        logging.warning(f"Cannot download image: {ex}")
        attach_param = None

    caption_plain = clean_markdown_for_apprise(caption)
    result = apobj.notify(
        body=caption_plain,
        title="",
        attach=attach_param
    )

    if attach_param and os.path.exists(attach_param):
        try:
            os.remove(attach_param)
        except Exception as ex:
            logging.warning(f"Cannot remove temp image: {ex}")

    if result:
        logging.info("Notification sent via Apprise")
    else:
        logging.warning("Notification failed via Apprise")
    return None
def _fetch_jellyfin_image_with_retries(photo_id: str, attempts: int = 3, timeout: int = 10, delay: float = 1.5):
    """
    Пытается скачать Primary-постер из Jellyfin с повторами.
    Возвращает bytes или None.
    """
    url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    last_err = None
    for i in range(1, attempts + 1):
        try:
            # Быстрая проверка доступности (необязательно, но полезно)
            head = requests.head(url, timeout=timeout)
            if head.ok:
                resp = requests.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.content
            else:
                last_err = f"HTTP {head.status_code}"
        except Exception as ex:
            last_err = ex
        logging.warning(f"Jellyfin image try {i}/{attempts} failed: {last_err}")
        if i < attempts:
            time.sleep(delay)
    return None

def send_telegram_photo(photo_id, caption):
    try:
        # Ограничиваем caption до 1024 символов
    #    if caption and len(caption) > 1024:
    #        caption = caption[:1023] + "..."  # добавляем троеточие, если обрезаем

#        base_photo_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images"
#        primary_photo_url = f"{base_photo_url}/Primary"

        # Download the image from the jellyfin
#        image_response = requests.get(primary_photo_url)

        # Пытаемся получить картинку с повторами
        image_bytes = _fetch_jellyfin_image_with_retries(photo_id, attempts=3, timeout=10, delay=1.5)
        if not image_bytes:
            logging.warning("Telegram: Jellyfin image unavailable after retries")
            return None

        # Upload the image to the Telegram bot
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "caption": caption,
            "parse_mode": "Markdown"
        }

        files = {'photo': ('photo.jpg', image_bytes, 'image/jpeg')}
        response = requests.post(url, data=data, files=files, timeout=30)
        logging.info("Telegram notification sent successfully")
        return response

    except Exception as ex:
        logging.warning(f"Error sending to Telegram: {ex}")
        return None

def send_telegram_photo_only(photo_id):
    """
    Отправляет ТОЛЬКО фото (без caption) в Telegram.
    Возвращает response при успехе, иначе None.
    """
    try:
        image_bytes = _fetch_jellyfin_image_with_retries(photo_id, attempts=3, timeout=10, delay=1.5)
        if not image_bytes:
            logging.warning("Telegram(photo-only): Jellyfin image unavailable after retries")
            return None

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
        data = {"chat_id": TELEGRAM_CHAT_ID}
        files = {'photo': ('photo.jpg', image_bytes, 'image/jpeg')}
        resp = requests.post(url, data=data, files=files, timeout=30)
        resp.raise_for_status()
        logging.info("Telegram photo-only sent successfully")
        return resp
    except Exception as ex:
        logging.warning(f"Telegram photo-only failed: {ex}")
        return None


def send_telegram_text(message: str):
    """
    Отправляет ТОЛЬКО текст в Telegram.
    Сначала пробуем Markdown, при ошибке парсинга падаем в plain-text (очищенный).
    Возвращает response при успехе, иначе None.
    """
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return None

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        # Попытка №1: Markdown
        resp = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }, timeout=30)
        if resp.ok:
            logging.info("Telegram text sent (Markdown)")
            return resp

        # Если не ok — пробуем plain
        logging.warning(f"Telegram text markdown failed: {resp.status_code} {resp.text}")
        raise HTTPError(response=resp)

    except Exception as md_ex:
        try:
            # Попытка №2: plain (очищаем markdown, ссылки приводим к простому виду)
            plain = clean_markdown_for_apprise(message) or message
            resp2 = requests.post(url, data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": plain
            }, timeout=30)
            resp2.raise_for_status()
            logging.info("Telegram text sent (plain fallback)")
            return resp2
        except Exception as ex2:
            logging.warning(f"Telegram text send failed: {ex2}")
            return None

def send_matrix_text_rest(message_markdown: str):
    """
    Отправляет ТОЛЬКО текст в Matrix через REST (v3).
    1) Пытается правильный PUT по спецификации.
    2) Если прокси блокирует PUT (405) — делает POST фоллбэк на тот же путь.
    Возвращает объект response при успехе, иначе None.
    """
    if not (MATRIX_URL and MATRIX_ACCESS_TOKEN and MATRIX_ROOM_ID):
        logging.debug("Matrix not configured; skip.")
        return None

    try:
        # room_id вида "!MNddurK...:example.org" нужно URL-энкодить полностью
        room_enc = quote(MATRIX_ROOM_ID, safe="")
        base = f"{MATRIX_URL.rstrip('/')}/_matrix/client/v3/rooms/{room_enc}/send/m.room.message"

        headers = {
            "Authorization": f"Bearer {MATRIX_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }

        # Чистим Markdown для plain-текста (Matrix клиенты корректно покажут)
        body_plain = clean_markdown_for_apprise(message_markdown) or ""
        payload = {"msgtype": "m.text", "body": body_plain}

        # Уникальный txnId (в миллисекундах)
        txn_id = f"{int(time.time() * 1000)}txt"
        url = f"{base}/{txn_id}"

        # 1) Правильный путь: PUT (спецификация)
        try:
            resp = requests.put(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            logging.info("Matrix text sent successfully via PUT v3")
            return resp
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 405:
                # 2) Фоллбэк: POST тем же урлом (некоторые reverse-proxy режут PUT)
                logging.warning("Matrix PUT blocked (405). Trying POST fallback…")
                resp2 = requests.post(url, headers=headers, json=payload, timeout=30)
                resp2.raise_for_status()
                logging.info("Matrix text sent successfully via POST fallback")
                return resp2
            else:
                logging.warning(f"Matrix text send failed via PUT: {e}")
                return None

    except Exception as ex:
        logging.warning(f"Matrix text send failed: {ex}")
        return None

def matrix_upload_image_rest(image_bytes: bytes, filename: str, mimetype: str = "image/jpeg") -> str | None:
    """
    Загружает картинку в Matrix content repo и возвращает mxc:// URI.
    Пробуем v3, при 404/405/501 — фоллбэк на r0.
    """
    if not (MATRIX_URL and MATRIX_ACCESS_TOKEN):
        logging.debug("Matrix not configured for media upload; skip.")
        return None

    headers = {"Authorization": f"Bearer {MATRIX_ACCESS_TOKEN}", "Content-Type": mimetype}
    base = MATRIX_URL.rstrip("/")
    url_v3 = f"{base}/_matrix/media/v3/upload?filename={quote(filename)}"

    try:
        r = requests.post(url_v3, headers=headers, data=image_bytes, timeout=30)
        r.raise_for_status()
        return r.json().get("content_uri")
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if code in (404, 405, 501):
            logging.warning(f"media/v3/upload returned {code}, trying r0…")
            try:
                url_r0 = f"{base}/_matrix/media/r0/upload?filename={quote(filename)}"
                r2 = requests.post(url_r0, headers=headers, data=image_bytes, timeout=30)
                r2.raise_for_status()
                return r2.json().get("content_uri")
            except Exception as ex2:
                logging.warning(f"Matrix r0 upload failed: {ex2}")
                return None
        logging.warning(f"Matrix v3 upload failed: {e}")
        return None
    except Exception as ex:
        logging.warning(f"Matrix upload failed: {ex}")
        return None


def _matrix_send_event_rest(room_id: str, event_type: str, content: dict):
    """
    Отправляет событие в комнату:
      PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}
    При 405 — POST на тот же путь.
    Возвращает response или None.
    """
    if not (MATRIX_URL and MATRIX_ACCESS_TOKEN and room_id):
        return None

    room_enc = quote(room_id, safe="")
    base = f"{MATRIX_URL.rstrip('/')}/_matrix/client/v3/rooms/{room_enc}/send/{event_type}"
    txn_id = f"{int(time.time()*1000)}evt"
    url = f"{base}/{txn_id}"
    headers = {"Authorization": f"Bearer {MATRIX_ACCESS_TOKEN}", "Content-Type": "application/json"}

    try:
        resp = requests.put(url, headers=headers, json=content, timeout=30)
        resp.raise_for_status()
        return resp
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) == 405:
            logging.warning("PUT blocked (405). Trying POST fallback…")
            try:
                resp2 = requests.post(url, headers=headers, json=content, timeout=30)
                resp2.raise_for_status()
                return resp2
            except Exception as ex2:
                logging.warning(f"Matrix POST fallback failed: {ex2}")
                return None
        logging.warning(f"Matrix send event failed via PUT: {e}")
        return None
    except Exception as ex:
        logging.warning(f"Matrix send event failed: {ex}")
        return None

def _fetch_jellyfin_primary(photo_id: str):
    """
    Возвращает (bytes, mimetype, filename) для Primary-постера из Jellyfin.
    """
    url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    mimetype = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip().lower()
    ext = ".jpg"
    if "png" in mimetype:
        ext = ".png"
    elif "webp" in mimetype:
        ext = ".webp"
    filename = f"poster{ext}"
    return resp.content, mimetype, filename


def send_matrix_image_then_text_from_jellyfin(photo_id: str, caption_markdown: str) -> bool:
    """
    1) Тянем постер из Jellyfin
    2) Загружаем в Matrix (media repo) -> mxc://
    3) Отправляем m.image (body = имя файла)
    4) Отдельным сообщением отправляем текст (m.text)
    """
    if not (MATRIX_URL and MATRIX_ACCESS_TOKEN and MATRIX_ROOM_ID):
        logging.debug("Matrix not configured; skip.")
        return False

    # 1) картинка из Jellyfin
    try:
        img_bytes, mimetype, filename = _fetch_jellyfin_primary(photo_id)
    except Exception as ex:
        logging.warning(f"Matrix(JF): cannot fetch image from Jellyfin: {ex}")
        # хотя бы текст отправим
        resp_txt = send_matrix_text_rest(caption_markdown)
        return bool(resp_txt and resp_txt.ok)

    # 2) upload -> mxc://
    mxc_uri = matrix_upload_image_rest(img_bytes, filename, mimetype)
    if not mxc_uri:
        logging.warning("Matrix(JF): media upload failed; sending text only.")
        resp_txt = send_matrix_text_rest(caption_markdown)
        return bool(resp_txt and resp_txt.ok)

    # 3) m.image (ВАЖНО: body — имя файла)
    content_img = {
        "msgtype": "m.image",
        "body": filename,
        "url": mxc_uri,
        "info": {
            "mimetype": mimetype,
            "size": len(img_bytes),
        },
    }
    resp_img = _matrix_send_event_rest(MATRIX_ROOM_ID, "m.room.message", content_img)
    img_ok = bool(resp_img and resp_img.ok)

    # 4) затем текст отдельным сообщением
    resp_txt = send_matrix_text_rest(caption_markdown)
    txt_ok = bool(resp_txt and resp_txt.ok)

    if img_ok and txt_ok:
        logging.info("Matrix(JF): image then text sent successfully.")
    else:
        logging.warning("Matrix(JF): image+text flow partially/fully failed.")
    return img_ok and txt_ok

def send_gotify_message(photo_id, message, title="Jellyfin", priority=5, uploaded_url=None):
    img_url = wait_for_imgbb_upload()
    if not img_url:
        logging.warning("Изображение не загружено — пропускаем отправку в Gotify.")
        return
    """
    Отправка сообщения и картинки напрямую в Gotify.
    """
    if not GOTIFY_URL or not GOTIFY_TOKEN:
        logging.warning("GOTIFY_URL or GOTIFY_TOKEN not set, skipping Gotify notification.")
        return None

    if uploaded_url is None:
        uploaded_url = get_jellyfin_image_and_upload_imgbb(photo_id)
    if uploaded_url:
        message = f"![Poster]({uploaded_url})\n\n{message}"
        big_image_url = uploaded_url
    else:
        big_image_url = None

    gotify_url = GOTIFY_URL.rstrip('/')
    url = f"{gotify_url}/message?token={GOTIFY_TOKEN}"

    data = {
        "title": title,
        "message": message,
        "priority": priority,
        "extras": {
            "client::display": {"contentType": "text/markdown"}
        }
    }
    if big_image_url:
        data["extras"]["client::notification"] = {"bigImageUrl": big_image_url}
    headers = {"X-Gotify-Format": "markdown"}

    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        logging.info("Gotify notification sent successfully")
        return response
    except Exception as ex:
        logging.warning(f"Error sending to Gotify: {ex}")
        return None

def send_signal_message_with_image(photo_id, message, SIGNAL_NUMBER, SIGNAL_RECIPIENTS, api_url=SIGNAL_URL):
    """
    Отправляет текст и изображение из Jellyfin в Signal через base64_attachments.
    """
    # Скачиваем изображение из Jellyfin
    jellyfin_image_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images/Primary"
    try:
        image_resp = requests.get(jellyfin_image_url)
        image_resp.raise_for_status()
        image_bytes = image_resp.content
        # Кодируем в base64
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")

        data = {
            "message": message,
            "number": SIGNAL_NUMBER,
            "recipients": SIGNAL_RECIPIENTS if isinstance(SIGNAL_RECIPIENTS, list) else [SIGNAL_RECIPIENTS],
            "base64_attachments": [image_b64],
        }

        resp = requests.post(api_url, json=data)
        resp.raise_for_status()
        logging.info("Signal image message sent successfully")
        return resp
    except Exception as ex:
        logging.warning(f"Error sending Signal image message: {ex}")
        return None


def send_whatsapp_image_via_rest(
    caption: str,
    phone_jid: str = None,
    image_url: str = None,
    view_once: bool = False,
    compress: bool = False,
    duration: int = 0,
    is_forwarded: bool = False,
):
    img_url = wait_for_imgbb_upload()
    if not img_url:
        logging.warning("Изображение не загружено — пропускаем отправку в WhatsApp.")
        return
    if not WHATSAPP_API_URL:
        logging.warning("WHATSAPP_API_URL not set, skipping WhatsApp image.")
        return None

    phone_jid = phone_jid or _wa_get_jid_from_env()
    if not phone_jid:
        logging.warning("WhatsApp JID is empty, skip sending image.")
        return None

    url = f"{WHATSAPP_API_URL.rstrip('/')}/send/image"
    auth = (WHATSAPP_API_USERNAME, WHATSAPP_API_PWD)

    form = {
        "phone": phone_jid,
        "caption": sanitize_whatsapp_text(caption or ""),
        "view_once": str(bool(view_once)).lower(),
        "compress": str(bool(compress)).lower(),
        "duration": str(int(duration)),
        "is_forwarded": str(bool(is_forwarded)).lower(),
    }

    files = None
    jellyfin_used = False

    if image_url:
        form["image_url"] = image_url
    else:
        logging.warning("WhatsApp image: image_url не задан, пропускаем отправку изображения.")
        return None

    try:
        resp = requests.post(url, data=form, files=files, auth=auth, timeout=30)
        resp.raise_for_status()
        logging.info("WhatsApp image sent successfully")
        return resp
    except requests.exceptions.RequestException as e:
        logging.warning(f"Error sending WhatsApp image: {e}")
        return None

def send_whatsapp_text_via_rest(message: str, phone_jid: str | None = None):
    """
    Шлёт ТОЛЬКО текст. Сначала /send/text, при 404 — /send/message.
    Возвращает response или None.
    """
    if not WHATSAPP_API_URL:
        logging.debug("WhatsApp API URL not set; skip text.")
        return None

    phone_jid = phone_jid or _wa_get_jid_from_env()
    if not phone_jid:
        logging.debug("WhatsApp JID empty; skip text.")
        return None

    base = WHATSAPP_API_URL.rstrip("/")
    url_text = f"{base}/send/text"
    url_msg  = f"{base}/send/message"
    auth = (WHATSAPP_API_USERNAME, WHATSAPP_API_PWD) if (WHATSAPP_API_USERNAME or WHATSAPP_API_PWD) else None

    form = {
        "phone": phone_jid,
        "message": sanitize_whatsapp_text(message or "")
    }

    try:
        r = requests.post(url_text, data=form, auth=auth, timeout=20)
        if r.status_code == 404:
            r = requests.post(url_msg, data=form, auth=auth, timeout=20)
        r.raise_for_status()
        logging.info("WhatsApp text sent successfully")
        return r
    except Exception as ex:
        logging.warning(f"WhatsApp text send failed: {ex}")
        return None

def send_whatsapp_image_with_retries(
    caption: str,
    phone_jid: str | None,
    image_url: str | None = None
) -> bool:
    """
    Пытается отправить изображение с подписью несколько раз.
    True при успехе, False если все попытки провалились.
    """
    attempts = max(1, WHATSAPP_IMAGE_RETRY_ATTEMPTS)
    delay = max(0, WHATSAPP_IMAGE_RETRY_DELAY_SEC)

    for i in range(1, attempts + 1):
        try:
            resp = send_whatsapp_image_via_rest(
                caption=caption,
                phone_jid=phone_jid,
                image_url=image_url
            )
            ok = (resp is not None) and (getattr(resp, "ok", True))
            if ok:
                logging.info(f"WhatsApp image sent on attempt {i}")
                return True
            else:
                logging.warning(f"WhatsApp image attempt {i} failed (no/negative response)")
        except Exception as ex:
            logging.warning(f"WhatsApp image attempt {i} exception: {ex}")
        if i < attempts:
            time.sleep(delay)
    return False


def get_item_details(item_id):
    headers = {'accept': 'application/json', }
    params = {'api_key': JELLYFIN_API_KEY, }
    url = f"{JELLYFIN_BASE_URL}/emby/Items?Recursive=true&Fields=DateCreated, Overview&Ids={item_id}"
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Check if request was successful
    return response.json()


def is_within_last_x_days(date_str, x):
    days_ago = datetime.now() - timedelta(days=x)
    return date_str >= days_ago.isoformat()


def is_not_within_last_x_days(date_str, x):
    days_ago = datetime.now() - timedelta(days=x)
    return date_str < days_ago.isoformat()


def get_youtube_trailer_url(query):
    base_search_url = "https://www.googleapis.com/youtube/v3/search"
    if not YOUTUBE_API_KEY:
        return None
    api_key = YOUTUBE_API_KEY

    params = {
        'part': 'snippet',
        'q': query,
        'type': 'video',
        'key': api_key
    }

    response = requests.get(base_search_url, params=params)
    response.raise_for_status()  # Check for HTTP errors before processing the data
    response_data = response.json()
    video_id = response_data.get("items", [{}])[0].get('id', {}).get('videoId')

    return f"https://www.youtube.com/watch?v={video_id}" if video_id else "Video not found!"


def item_already_notified(item_type, item_name, release_year):
    # В режиме теста всегда считаем, что ещё не отправляли
    if DISABLE_DEDUP:
        logging.debug("Dedup is disabled: treating as NOT notified.")
        return False

    key = f"{item_type}:{item_name}:{release_year}"
    return notified_items.get(key) is True


def mark_item_as_notified(item_type, item_name, release_year, max_items=100):
    # В режиме теста ничего не пишем в файл
    if DISABLE_DEDUP:
        logging.debug("Dedup is disabled: NOT recording notified key.")
        return

    key = f"{item_type}:{item_name}:{release_year}"
    notified_items[key] = True

    # ограничиваем размер «памяти»
    if len(notified_items) > max_items:
        # если уже храните timestamp — удаляйте самый старый; иначе просто pop первого
        notified_items.pop(next(iter(notified_items)))
    save_notified_items(notified_items)

# Сравнение данных о видео

def _get_item_media_info_movie(item_id: str) -> dict:
    """
    Тянем MediaSources/MediaStreams для фильма и уплощаем в dict.
    Дополнительно возвращаем:
      - список аудио-дорожек: audio_tracks + их количество
      - профили изображения: image_profiles (['DV','HDR10',...]) и image_profile_str ("DV, HDR10")
    """
    try:
        headers = {'accept': 'application/json'}
        params = {'api_key': JELLYFIN_API_KEY}
        url = f"{JELLYFIN_BASE_URL}/emby/Items?Ids={item_id}&Fields=MediaSources,RunTimeTicks"
        r = requests.get(url, headers=headers, params=params, timeout=12)
        r.raise_for_status()
        data = r.json()
        item = (data.get("Items") or [{}])[0]
        sources = item.get("MediaSources") or []
        if not sources:
            return {}
        src = sources[0]

        container = src.get("Container")
        overall_bitrate = src.get("Bitrate")
        size_bytes = src.get("Size")
        duration_ticks = src.get("RunTimeTicks") or item.get("RunTimeTicks")
        duration_sec = duration_ticks / 10_000_000 if duration_ticks else None

        vcodec = None; vbitrate = None; width = None; height = None; dyn = None; vdepth = None; fps = None
        acodec = None; abitrate = None; channels = None

        audio_tracks = []
        image_profiles = None  # NEW

        for s in (src.get("MediaStreams") or []):
            stype = s.get("Type")
            if stype == "Video" and vcodec is None:
                # ... ваш код извлечения полей ...
                try:
                    image_profiles = _detect_image_profiles_from_fields(s)
                except Exception:
                    image_profiles = None
                # >>> ФОЛБЭК
                if not image_profiles:
                    image_profiles = ["SDR"]
                # <<<
                vcodec = s.get("Codec")
                vbitrate = s.get("BitRate") or s.get("bitrate") or overall_bitrate
                width = s.get("Width"); height = s.get("Height")
                fps = s.get("AverageFrameRate") or s.get("RealFrameRate")
                vdepth = s.get("BitDepth") or s.get("VideoBitDepth")
                dyn = s.get("ColorTransfer") or s.get("VideoRange") or s.get("ColorPrimaries")
                if isinstance(dyn, str):
                    u = dyn.upper()
                    dyn = "HDR" if ("PQ" in u or "HLG" in u or "HDR" in u or "BT2020" in u) else "SDR"

                # NEW: профили изображения (DV / HDR10+ / HDR10 / HLG / HDR / SDR)
                try:
                    image_profiles = _detect_image_profiles_from_fields(s)
                except Exception:
                    image_profiles = None

            elif stype == "Audio":
                # основной аудио-снимок (для сводки)
                if acodec is None:
                    acodec = s.get("Codec")
                    abitrate = s.get("BitRate") or s.get("bitrate")
                    channels = s.get("Channels")
                # читаем «человеческое» имя дорожки
                label = s.get("DisplayTitle") or s.get("Title")
                if not label:
                    lang = s.get("Language")
                    codec = s.get("Codec")
                    ch = s.get("Channels")
                    layout = s.get("ChannelLayout")
                    parts = []
                    if lang:   parts.append(str(lang).upper())
                    if codec:  parts.append(str(codec).upper())
                    if ch:     parts.append(f"{ch}ch")
                    if layout: parts.append(layout)
                    label = " ".join(parts) or "Audio"
                audio_tracks.append(label)

        approx_kbps = None
        if (not vbitrate) and size_bytes and duration_sec and duration_sec > 0:
            approx_kbps = int((size_bytes * 8) / duration_sec / 1000)

        return {
            "video_codec": vcodec,
            "video_bitrate": vbitrate,
            "approx_video_kbps": approx_kbps,
            "width": width,
            "height": height,
            "fps": fps,
            "bit_depth": vdepth,
            "dynamic_range": dyn or "SDR",
            "audio_codec": acodec,
            "audio_bitrate": abitrate,
            "audio_channels": channels,
            "container": container,
            "size_bytes": size_bytes,
            "duration_sec": duration_sec,
            # аудио-дорожки
            "audio_tracks": audio_tracks,
            "audio_track_count": len(audio_tracks),
            # NEW: профили изображения с фолбэком
            "image_profiles": image_profiles or ["SDR"],
            "image_profile_str": ", ".join(image_profiles or ["SDR"]),
        }
    except Exception as ex:
        logging.warning(f"Media info fetch failed for movie {item_id}: {ex}")
        return {}

def build_audio_tracks_block(q: dict) -> str:
    tracks = (q or {}).get("audio_tracks") or []
    if not tracks:
        return ""
    header = t("audio_tracks")
    lines = "\n".join(f"- {name}" for name in tracks)
    return f"\n\n*{header} ({len(tracks)})*\n{lines}"

def _quality_signature(q: dict) -> str:
    """
    Компактный ключ качества: достаточно чувствителен к реальной замене файла.
    """
    def part(x): return "-" if x in (None, "", 0) else str(x)
    vbr = q.get("video_bitrate") or q.get("approx_video_kbps")
    return "|".join([
        part(q.get("video_codec")),
        f"{part(q.get('width'))}x{part(q.get('height'))}",
        part(vbr),
        part(q.get("dynamic_range")),
        part(q.get("bit_depth")),
        part(q.get("fps")),
        part(q.get("audio_codec")),
        part(q.get("audio_channels")),
        part(q.get("audio_bitrate")),
        part(q.get("container")),
        part(q.get("size_bytes")),
    ])

def _quality_is_substantial(q: dict | None) -> bool:
    """False, если «пустой» снимок (Jellyfin ещё не распарсил потоки)."""
    if not q: return False
    return any([
        q.get("video_codec"),
        (q.get("width") and q.get("height")),
        q.get("audio_codec"),
        q.get("container"),
        q.get("size_bytes"),
    ])

def _fmt_mbps(q: dict) -> str:
    vbr = q.get("video_bitrate")
    if vbr:
        try: return f"{int(vbr)/1000:.1f} Mbps"
        except: return f"{vbr} kbps"
    kbps = q.get("approx_video_kbps")
    return f"{kbps/1000:.1f} Mbps (≈)" if kbps else "-"

def _movie_logical_key(*, tmdb_id: str | None, imdb_id: str | None, name: str, year: int | None) -> str:
    if tmdb_id: return f"movie:tmdb:{tmdb_id}"
    if imdb_id: return f"movie:imdb:{imdb_id}"
    # фолбэк: name+year в нижнем регистре
    key_name = re.sub(r"\s+", " ", (name or "").strip().lower())
    return f"movie:nameyear:{key_name}:{year or ''}"

def store_quality_snapshot_movie(*, item_id: str, name: str, year: int | None,
                                 tmdb_id: str | None, imdb_id: str | None) -> dict:
    """
    1) Тянем качество из Jellyfin
    2) Upsert в media_quality (по ItemId)
    3) Сравниваем и upsert в content_quality (по логическому ключу)
    Возвращаем флаги: logical_inserted, logical_changed, old_quality, new_quality
    """
    q = _get_item_media_info_movie(item_id)
    sig = _quality_signature(q)
    now = datetime.now().isoformat(timespec='seconds')

    profiles_str = (q.get("image_profile_str") or
                    ",".join(_profiles_from_q(q)))  # "DV,HDR10" или "SDR"

    result = {
        "logical_inserted": False,
        "logical_changed": False,
        "old_quality": None,
        "new_quality": q,
        "old_signature": None,
        "new_signature": sig,
        "logical_key": None
    }

    logical_key = _movie_logical_key(tmdb_id=tmdb_id, imdb_id=imdb_id, name=name, year=year)
    result["logical_key"] = logical_key

    conn = sqlite3.connect(QUALITY_DB_FILE)
    try:
        cur = conn.cursor()
        # --- media_quality по ItemId
        cur.execute("SELECT signature FROM media_quality WHERE item_id=?", (item_id,))
        if cur.fetchone() is None:
            cur.execute("""INSERT INTO media_quality
                           (item_id, movie_name, year, video_codec, video_bitrate, width, height, fps, bit_depth,
                            dynamic_range,
                            audio_codec, audio_bitrate, audio_channels, container, size_bytes, duration_sec, signature,
                            date_seen, image_profiles)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (item_id, name, year, q.get("video_codec"), q.get("video_bitrate"),
                         q.get("width"), q.get("height"), q.get("fps"), q.get("bit_depth"), q.get("dynamic_range"),
                         q.get("audio_codec"), q.get("audio_bitrate"), q.get("audio_channels"),
                         q.get("container"), q.get("size_bytes"), q.get("duration_sec"), sig, now, profiles_str)
                        )
        else:
            cur.execute("""UPDATE media_quality
                           SET movie_name=?,
                               year=?,
                               video_codec=?,
                               video_bitrate=?,
                               width=?,
                               height=?,
                               fps=?,
                               bit_depth=?,
                               dynamic_range=?,
                               audio_codec=?,
                               audio_bitrate=?,
                               audio_channels=?,
                               container=?,
                               size_bytes=?,
                               duration_sec=?,
                               signature=?,
                               date_seen=?,
                               image_profiles=?
                           WHERE item_id = ?""",
                        (name, year, q.get("video_codec"), q.get("video_bitrate"),
                         q.get("width"), q.get("height"), q.get("fps"), q.get("bit_depth"), q.get("dynamic_range"),
                         q.get("audio_codec"), q.get("audio_bitrate"), q.get("audio_channels"),
                         q.get("container"), q.get("size_bytes"), q.get("duration_sec"), sig, now, profiles_str,
                         item_id)
                        )

        # --- content_quality по logical_key
        cur.execute("""SELECT signature,
                              last_item_id,
                              video_codec,
                              video_bitrate,
                              width,
                              height,
                              fps,
                              bit_depth,
                              dynamic_range,
                              image_profiles,
                              audio_codec,
                              audio_bitrate,
                              audio_channels,
                              container,
                              size_bytes,
                              duration_sec
                       FROM content_quality
                       WHERE logical_key = ?""", (logical_key,))
        row = cur.fetchone()
        if row is None:
            if _quality_is_substantial(q):
                cur.execute("""INSERT INTO content_quality
                               (logical_key, last_item_id, movie_name, year, video_codec, video_bitrate, width, height,
                                fps, bit_depth,
                                dynamic_range, image_profiles, audio_codec, audio_bitrate, audio_channels, container,
                                size_bytes, duration_sec, signature, date_seen)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (logical_key, item_id, name, year, q.get("video_codec"), q.get("video_bitrate"),
                             q.get("width"), q.get("height"), q.get("fps"), q.get("bit_depth"), q.get("dynamic_range"),
                             profiles_str, q.get("audio_codec"), q.get("audio_bitrate"), q.get("audio_channels"),
                             q.get("container"), q.get("size_bytes"), q.get("duration_sec"), sig, now)
                            )
                result["logical_inserted"] = True
        else:
            old_sig, old_item_id = row[0], row[1]
            old_q = {
                "video_codec": row[2], "video_bitrate": row[3], "width": row[4], "height": row[5],
                "fps": row[6], "bit_depth": row[7], "dynamic_range": row[8],
                "image_profiles": ([p.strip() for p in row[9].split(",")] if row[9] else None),
                "audio_codec": row[10], "audio_bitrate": row[11], "audio_channels": row[12],
                "container": row[13], "size_bytes": row[14], "duration_sec": row[15]
            }
            result["old_signature"] = old_sig
            result["old_quality"] = old_q

            if old_sig != sig and _quality_is_substantial(old_q) and _quality_is_substantial(q):
                result["logical_changed"] = True
                cur.execute("""UPDATE content_quality
                               SET last_item_id=?,
                                   movie_name=?,
                                   year=?,
                                   video_codec=?,
                                   video_bitrate=?,
                                   width=?,
                                   height=?,
                                   fps=?,
                                   bit_depth=?,
                                   dynamic_range=?,
                                   image_profiles=?,
                                   audio_codec=?,
                                   audio_bitrate=?,
                                   audio_channels=?,
                                   container=?,
                                   size_bytes=?,
                                   duration_sec=?,
                                   signature=?,
                                   date_seen=?
                               WHERE logical_key = ?""",
                            (item_id, name, year, q.get("video_codec"), q.get("video_bitrate"),
                             q.get("width"), q.get("height"), q.get("fps"), q.get("bit_depth"), q.get("dynamic_range"),
                             profiles_str, q.get("audio_codec"), q.get("audio_bitrate"), q.get("audio_channels"),
                             q.get("container"), q.get("size_bytes"), q.get("duration_sec"),
                             sig, now, logical_key)
                            )
        conn.commit()
    finally:
        conn.close()
    return result

def _labels():
    if LANG == "ru":
        return {
            "changes": "Изменения качества",
            "resolution": "Разрешение",
            "video_codec": "Видео-кодек",
            "bitrate": "Битрейт (видео)",
            "dynamic_range": "Динамический диапазон",
            "audio": "Аудио",
            "container": "Контейнер",
            "fps": "Кадровая частота",
            "bit_depth": "Битовая глубина",
        }
    return {
        "changes": "Quality changes",
        "resolution": "Resolution",
        "video_codec": "Video codec",
        "bitrate": "Bitrate (video)",
        "dynamic_range": "Dynamic range",
        "audio": "Audio",
        "container": "Container",
        "fps": "Frame rate",
        "bit_depth": "Bit depth",
    }

def build_quality_changes_block(old_q: dict, new_q: dict) -> str:
    L = _labels()
    lines = []

    def arrow(a, b):
        return f"{a} → {b}"

    # Разрешение
    # --- Resolution (с ярлыками) ---
    res_old = _res_display_from_q(old_q)
    res_new = _res_display_from_q(new_q)
    if res_old != res_new:
        lines.append(f"- {L['resolution']}: {arrow(res_old, res_new)}")

    # Видео-кодек
    vc_old = (old_q.get("video_codec") or "-").upper()
    vc_new = (new_q.get("video_codec") or "-").upper()
    if vc_old != vc_new:
        lines.append(f"- {L['video_codec']}: {arrow(vc_old, vc_new)}")

    # Динамический диапазон (SDR/HDR и т.п.)
    dr_old = old_q.get("dynamic_range") or "-"
    dr_new = new_q.get("dynamic_range") or "-"
    if dr_old != dr_new:
        lines.append(f"- {L['dynamic_range']}: {arrow(dr_old, dr_new)}")

    # Профили изображения (SDR/HDR/HDR10/HDR10+/DV/HLG)
    old_profiles = ", ".join(_profiles_from_q(old_q))
    new_profiles = ", ".join(_profiles_from_q(new_q))
    if old_profiles != new_profiles:
        lines.append(f"- {t('image_profiles')}: {old_profiles} → {new_profiles}")
    logging.debug(f"Quality delta profiles: old='{old_profiles}' new='{new_profiles}'")
    if not lines:
        return ""
    return f"\n\n*{L['changes']}*\n" + "\n".join(lines)

def build_initial_quality_changes_block(new_q: dict) -> str:
    """
    Блок качества для НОВОГО фильма без стрелок и без 'Dynamic range'.
    Показываем: Resolution, Video codec, Image profiles.
    """
    L = _labels()
    lines = []

    # Resolution
    # Resolution -> ярлык (или WxH при неизвестном)
    res_new = _res_display_from_q(new_q)
    if res_new != "-":
        lines.append(f"- {L['resolution']}: {res_new}")

    # Video codec
    vc_new = (new_q.get("video_codec") or "-").upper()
    if vc_new != "-":
        lines.append(f"- {L['video_codec']}: {vc_new}")

    # Image profiles (SDR/HDR/HDR10/HDR10+/DV/HLG)
    profiles = ", ".join(_profiles_from_q(new_q))
    lines.append(f"- {t('image_profiles')}: {profiles}")

    if not lines:
        return ""
    return f"\n\n*{L['changes']}*\n" + "\n".join(lines)



def maybe_notify_movie_quality_change(*, item_id: str, movie_name_cleaned: str, release_year: int | None,
                                      tmdb_id: str | None, imdb_id: str | None,
                                      overview: str | None, runtime: str | None) -> bool:
    """
    Если качество фильма изменилось (по логическому ключу) — отправляем уведомление
    тем же шаблоном, что и для нового фильма, + блок 'Изменения качества'.
    Возвращает True, если отправили (обычное сообщение 'New Movie' слать не надо).
    """
    res = store_quality_snapshot_movie(
        item_id=item_id, name=movie_name_cleaned, year=release_year,
        tmdb_id=tmdb_id, imdb_id=imdb_id
    )
    if not res.get("logical_changed"):
        return False

    old_q = res.get("old_quality")
    new_q = res.get("new_quality")
    # Если старый снимок «пустой» — это первый нормальный парс нового фильма; считаем, что это НЕ апгрейд.
    if not _quality_is_substantial(old_q):
        logging.info("(Movie guard) Old quality is empty -> treat as NEW content, not a quality update.")
        return False

    # Собираем «как при добавлении»
    notification_message = (
        f"*{t('quality_updated')}*\n\n*{movie_name_cleaned}* *({release_year})*\n\n{overview or ''}\n\n"
        f"*{t('new_runtime')}*\n{runtime or ''}"
    )

    # рейтинги (если есть tmdb_id)
    if tmdb_id:
        ratings_text = safe_fetch_mdblist_ratings("movie", tmdb_id)
        if ratings_text:
            notification_message += f"\n\n*{t('new_ratings_movie')}*\n{ratings_text}"

    # трейлер
    trailer_url = safe_get_trailer_prefer_tmdb(f"{movie_name_cleaned} Trailer {release_year}",
                                context="webhook", subkind="movie", tmdb_id=tmdb_id)
    if trailer_url:
        notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

    # добавим блок «что изменилось»
    # добавим блок «что изменилось»
    delta = build_quality_changes_block(old_q, new_q)
    if delta:
        notification_message += delta

    # ДОБАВЬ УСЛОВИЕ:
    if INCLUDE_AUDIO_TRACKS:
        tracks_block = build_audio_tracks_block(new_q)
        if tracks_block:
            notification_message += tracks_block

    send_notification(item_id, notification_message)
    touch_quality_update_marker(res.get("logical_key") or _movie_logical_key(
        tmdb_id=tmdb_id, imdb_id=imdb_id, name=movie_name_cleaned, year=release_year
    ), item_id=item_id)
    logging.info(f"(Movie) Quality update sent for {movie_name_cleaned} ({release_year}); logical_key={res.get('logical_key')}")
    return True

def _format_runtime_from_ticks(runtime_ticks) -> str:
    if not runtime_ticks:
        return ""
    try:
        total_sec = int(runtime_ticks) // 10_000_000
        h = total_sec // 3600
        m = (total_sec % 3600) // 60
        if h > 0:
            return f"{h}h {m}m"
        return f"{m}m"
    except Exception:
        return ""
def poll_recent_movies_once():
    """
    Пагинированно тянем фильмы и проверяем апгрейды качества.
    Новые (очень свежие) пропускаем — их объявит вебхук.
    """
    page_size = MOVIE_POLL_PAGE_SIZE
    # совместимость: если задан старый MOVIE_POLL_LIMIT и MAX_TOTAL == 0, используем его как предел
    max_total = MOVIE_POLL_MAX_TOTAL  # 0 = без ограничения

    start = 0
    fetched = 0
    now_utc = datetime.now(timezone.utc)

    while True:
        # Ограничение на последнюю страницу, если нужно
        current_limit = page_size
        if max_total and (max_total - fetched) < page_size:
            current_limit = max_total - fetched
            if current_limit <= 0:
                break

        try:
            params = {
                "api_key": JELLYFIN_API_KEY,
                "IncludeItemTypes": "Movie",
                "Recursive": "true",
                "SortBy": "DateModified,DateCreated",
                "SortOrder": "Descending",
                "Limit": str(current_limit),
                "StartIndex": str(start),
                # DateCreated нужен для грейс-фильтра (чтобы вебхук объявлял «новые»)
                "Fields": "MediaSources,RunTimeTicks,ProviderIds,ProductionYear,Overview,DateCreated"
            }
            url = f"{JELLYFIN_BASE_URL}/emby/Items"
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json() or {}
            items = payload.get("Items") or []
        except Exception as ex:
            logging.warning(f"Movie poll: failed page start={start}: {ex}")
            break

        if not items:
            break

        for it in items:
            try:
                # --- грейс: свежие новинки не трогаем (пусть вебхук пошлёт 'New Movie Added')
                created_iso = it.get("DateCreated")
                created_dt = _parse_iso_utc(created_iso)
                if created_dt and (now_utc - created_dt) < timedelta(minutes=MOVIE_POLL_GRACE_MIN):
                    logging.debug(f"Movie poll: skip fresh item (created {created_dt.isoformat()}): {it.get('Name')}")
                    continue
                # -------------------------------------------------------------

                item_id = it.get("Id")
                name = it.get("Name") or ""
                year = it.get("ProductionYear")
                prov = it.get("ProviderIds") or {}
                tmdb_id = prov.get("Tmdb") or prov.get("TmdbId")
                imdb_id = prov.get("Imdb") or prov.get("ImdbId")

                # Имя без года в скобках (как в вебхуке)
                name_clean = name.replace(f" ({year})", "").strip()

                # Overview/Runtime для текста
                overview = it.get("Overview") or ""
                runtime_str = _format_runtime_from_ticks(it.get("RunTimeTicks"))

                # Проверяем и отправляем ТОЛЬКО апдейты качества (не «новый фильм»)
                sent = maybe_notify_movie_quality_change(
                    item_id=item_id,
                    movie_name_cleaned=name_clean,
                    release_year=year,
                    tmdb_id=tmdb_id,
                    imdb_id=imdb_id,
                    overview=overview,
                    runtime=runtime_str
                )
                if sent:
                    # запись в БД уже обновлена; повтора на следующем проходе не будет
                    continue
            except Exception as ex:
                logging.warning(f"Movie poll: item {it.get('Id')} failed: {ex}")

        n = len(items)
        fetched += n
        start += n
        logging.debug(f"Movie poll: page fetched {n} items (total {fetched})")

        # если страница неполная — дальше элементов нет
        if n < current_limit:
            break

        # мягкое дыхание между страницами (не обязательно)
        time.sleep(0.1)

def _detect_image_profiles_from_fields(s: dict) -> list[str]:
    """
    Детект DV / HDR10+ / HDR10 / HLG / HDR / SDR по полям видео-потока.
    Если ничего не нашли — возвращаем ['SDR'].
    """
    txt_parts = []
    for k in ("ColorTransfer","VideoRange","VideoRangeType","ColorPrimaries","ColorSpace",
              "Profile","Hdr","Hdr10Plus","DolbyVision","DoVi","VideoDoViProfile"):
        v = s.get(k)
        if isinstance(v, bool):
            v = "1" if v else "0"
        if v is not None:
            txt_parts.append(str(v))
    txt = " ".join(txt_parts).upper()

    prof = []
    def add(tag):
        if tag not in prof:
            prof.append(tag)

    if "DOLBY VISION" in txt or "DOVI" in txt or "VIDEO DOVIPROFILE" in txt or re.search(r"\bDV\b", txt or ""):
        add("DV")
    if "HDR10+" in txt or "HDR10PLUS" in txt or "HDR10 PLUS" in txt:
        add("HDR10+")
    if "HDR10" in txt:
        add("HDR10")
    if "HLG" in txt:
        add("HLG")
    if ("HDR" in txt or "PQ" in txt or "BT2020" in txt) and not any(p in prof for p in ("DV","HDR10+","HDR10","HLG")):
        add("HDR")
    if not prof:
        add("SDR")

    order = {"DV":0,"HDR10+":1,"HDR10":2,"HLG":3,"HDR":4,"SDR":5}
    prof.sort(key=lambda x: order.get(x, 99))
    return prof

def _profiles_from_q(q: dict | None) -> list[str]:
    order = {"DV": 0, "HDR10+": 1, "HDR10": 2, "HLG": 3, "HDR": 4, "SDR": 5}
    if not q:
        return ["SDR"]

    profs = (q.get("image_profiles") or [])
    if not profs:
        dr = (q.get("dynamic_range") or "").upper()
        if "DV" in dr or "DOLBY" in dr:
            profs = ["DV"]
        elif "HDR10+" in dr:
            profs = ["HDR10+"]
        elif "HDR10" in dr:
            profs = ["HDR10"]
        elif "HLG" in dr:
            profs = ["HLG"]
        elif "HDR" in dr:
            profs = ["HDR"]
        else:
            profs = ["SDR"]

    # ← вот эти две строки ключевые:
    profs = [str(p).strip().upper() for p in profs if str(p).strip()]
    profs = list(dict.fromkeys(profs))

    profs.sort(key=lambda p: order.get(p, 99))
    return profs

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')

def _iso_to_dt(s: str | None) -> datetime | None:
    if not s: return None
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except Exception:
        return None

def touch_quality_update_marker(logical_key: str, item_id: str | None = None):
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        cur.execute("""INSERT INTO recent_quality_updates (logical_key, notified_at, item_id)
                       VALUES (?, ?, ?)
                       ON CONFLICT(logical_key) DO UPDATE SET
                         notified_at=excluded.notified_at,
                         item_id=excluded.item_id
                    """, (logical_key, _now_utc_iso(), item_id))
        conn.commit()
    except Exception as ex:
        logging.warning(f"touch_quality_update_marker failed: {ex}")
    finally:
        try: conn.close()
        except: pass

def was_quality_update_recent(logical_key: str) -> bool:
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT notified_at FROM recent_quality_updates WHERE logical_key=?", (logical_key,))
        row = cur.fetchone()
    except Exception as ex:
        logging.warning(f"was_quality_update_recent check failed: {ex}")
        return False
    finally:
        try: conn.close()
        except: pass

    if not row:
        return False
    ts = _iso_to_dt(row[0])
    if not ts:
        return False
    return (datetime.now(timezone.utc) - ts) < timedelta(minutes=SUPPRESS_WEBHOOK_AFTER_QUALITY_UPDATE_MIN)

def _parse_iso_utc(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00')).astimezone(timezone.utc)
    except Exception:
        return None


def _movie_poll_loop():
    while True:
        try:
            poll_recent_movies_once()
        except Exception as ex:
            logging.warning(f"Movie poll loop error: {ex}")
        time.sleep(MOVIE_POLL_INTERVAL_SEC)

if MOVIE_POLL_ENABLED:
    threading.Thread(target=_movie_poll_loop, name="movie-poll", daemon=True).start()
    logging.info(f"Movie quality polling enabled every {MOVIE_POLL_INTERVAL_SEC}s (limit={MOVIE_POLL_MAX_TOTAL})")

def _resolution_label(width: int | None, height: int | None) -> str | None:
    """
    Возвращает текстовый ярлык разрешения (8K, 5K, 4K (UltraHD), 1440p, 1080p, 720p, 576p, 480p, 360p, 240p).
    Использует высоту кадра с допуском (~8%) на «неканонические» значения (например, 2026 ≈ 2160).
    Если не попадает ни в один диапазон — вернёт None.
    """
    if not height:
        return None

    # (target_height, label)
    targets = [
        (4320, "8K"),
        (2880, "5K"),
        (2160, "4K (UltraHD)"),
        (1440, "1440p"),
        (1080, "1080p"),
        (720,  "720p"),
        (576,  "576p"),
        (480,  "480p"),
        (360,  "360p"),
        (240,  "240p"),
    ]
    for h, label in targets:
        tol = max(int(h * 0.08), 12)  # ~8% или минимум 12px
        if abs(height - h) <= tol:
            return label
    return None

def _res_display_from_q(q: dict | None) -> str:
    """
    Человекочитаемое разрешение для отображения:
    - если удалось сопоставить ярлык 8K/4K/... -> вернуть его
    - иначе вернуть 'WxH'
    - если данных нет -> '-'
    """
    if not q:
        return "-"
    w, h = q.get("width"), q.get("height")
    if not (w and h):
        return "-"
    label = _resolution_label(w, h)
    return label or f"{w}x{h}"

#Очистка базы данных
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z")

def _iso_to_dt(s: str | None) -> datetime | None:
    if not s: return None
    try: return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception: return None

def _collect_current_movie_keys_and_ids() -> tuple[set[str], set[str]]:
    """
    Возвращает (set логических ключей, set ItemId) для ВСЕХ фильмов в Jellyfin.
    Ключ строим через _movie_logical_key(...) по ProviderIds/Tmdb/Imdb -> name+year.
    """
    current_keys: set[str] = set()
    current_ids: set[str] = set()

    start = 0
    page_size = QUALITY_GC_PAGE_SIZE

    while True:
        try:
            params = {
                "api_key": JELLYFIN_API_KEY,
                "IncludeItemTypes": "Movie",
                "Recursive": "true",
                "SortBy": "DateCreated",
                "SortOrder": "Descending",
                "Limit": str(page_size),
                "StartIndex": str(start),
                "Fields": "ProviderIds,ProductionYear"
            }
            url = f"{JELLYFIN_BASE_URL}/emby/Items"
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json() or {}
            items = payload.get("Items") or []
        except Exception as ex:
            logging.warning(f"Quality GC: failed to list movies page start={start}: {ex}")
            break

        if not items:
            break

        for it in items:
            item_id = it.get("Id")
            name = it.get("Name") or ""
            year = it.get("ProductionYear")
            prov = it.get("ProviderIds") or {}
            tmdb_id = prov.get("Tmdb") or prov.get("TmdbId")
            imdb_id = prov.get("Imdb") or prov.get("ImdbId")
            # имя без суффикса "(year)"
            name_clean = name.replace(f" ({year})", "").strip()

            key = _movie_logical_key(
                tmdb_id=tmdb_id,
                imdb_id=imdb_id,
                name=name_clean,
                year=year
            )
            current_keys.add(key)
            if item_id:
                current_ids.add(item_id)

        n = len(items)
        start += n
        if n < page_size:
            break

    return current_keys, current_ids

def _collect_current_movie_keys_and_ids() -> tuple[set[str], set[str]]:
    """
    Возвращает (set логических ключей, set ItemId) для ВСЕХ фильмов в Jellyfin.
    Ключ строим через _movie_logical_key(...) по ProviderIds/Tmdb/Imdb -> name+year.
    """
    current_keys: set[str] = set()
    current_ids: set[str] = set()

    start = 0
    page_size = QUALITY_GC_PAGE_SIZE

    while True:
        try:
            params = {
                "api_key": JELLYFIN_API_KEY,
                "IncludeItemTypes": "Movie",
                "Recursive": "true",
                "SortBy": "DateCreated",
                "SortOrder": "Descending",
                "Limit": str(page_size),
                "StartIndex": str(start),
                "Fields": "ProviderIds,ProductionYear"
            }
            url = f"{JELLYFIN_BASE_URL}/emby/Items"
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json() or {}
            items = payload.get("Items") or []
        except Exception as ex:
            logging.warning(f"Quality GC: failed to list movies page start={start}: {ex}")
            break

        if not items:
            break

        for it in items:
            item_id = it.get("Id")
            name = it.get("Name") or ""
            year = it.get("ProductionYear")
            prov = it.get("ProviderIds") or {}
            tmdb_id = prov.get("Tmdb") or prov.get("TmdbId")
            imdb_id = prov.get("Imdb") or prov.get("ImdbId")
            # имя без суффикса "(year)"
            name_clean = name.replace(f" ({year})", "").strip()

            key = _movie_logical_key(
                tmdb_id=tmdb_id,
                imdb_id=imdb_id,
                name=name_clean,
                year=year
            )
            current_keys.add(key)
            if item_id:
                current_ids.add(item_id)

        n = len(items)
        start += n
        if n < page_size:
            break

    return current_keys, current_ids

def gc_quality_db_once():
    """
    Удаляет устаревшие записи:
      - content_quality: логические ключи, которых нет в библиотеке и last seen старше GRACE
      - media_quality: item_id, которых нет в библиотеке и last seen старше GRACE
      - recent_quality_updates: маркеры по отсутствующим ключам
    """
    try:
        current_keys, current_ids = _collect_current_movie_keys_and_ids()
        cutoff = datetime.now(timezone.utc) - timedelta(days=QUALITY_GC_GRACE_DAYS)

        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()

        # --- content_quality
        cur.execute("SELECT logical_key, date_seen FROM content_quality")
        rows = cur.fetchall()
        to_del_keys = []
        for logical_key, date_seen in rows:
            if logical_key in current_keys:
                continue
            dt = _iso_to_dt(date_seen)
            if (dt is None) or (dt < cutoff):
                to_del_keys.append(logical_key)

        if to_del_keys:
            for key in to_del_keys:
                cur.execute("DELETE FROM content_quality WHERE logical_key=?", (key,))
            logging.info(f"Quality GC: removed {len(to_del_keys)} content_quality rows")

        # --- media_quality
        cur.execute("SELECT item_id, date_seen FROM media_quality")
        rows = cur.fetchall()
        to_del_ids = []
        for item_id, date_seen in rows:
            if item_id in current_ids:
                continue
            dt = _iso_to_dt(date_seen)
            if (dt is None) or (dt < cutoff):
                to_del_ids.append(item_id)

        if to_del_ids:
            for iid in to_del_ids:
                cur.execute("DELETE FROM media_quality WHERE item_id=?", (iid,))
            logging.info(f"Quality GC: removed {len(to_del_ids)} media_quality rows")

        # --- recent_quality_updates (маркеры подавления вебхука)
        if to_del_keys:
            for key in to_del_keys:
                cur.execute("DELETE FROM recent_quality_updates WHERE logical_key=?", (key,))

        conn.commit()

        # по желанию можно иногда делать VACUUM (редко)
        # cur.execute("VACUUM")  # если база компактность важна

    except Exception as ex:
        logging.warning(f"Quality GC error: {ex}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

_init_quality_db()

# --- Форсированная очистка при старте (одноразово) ---
if FORCE_QUALITY_GC_ON_START:
    old_grace = QUALITY_GC_GRACE_DAYS
    try:
        # Если не задано явно — чистим без «грейса» (сразу)
        QUALITY_GC_GRACE_DAYS = int(FORCE_QUALITY_GC_GRACE_DAYS) if FORCE_QUALITY_GC_GRACE_DAYS is not None else 0
    except Exception:
        logging.warning(f"FORCE_QUALITY_GC_GRACE_DAYS is not an int: {FORCE_QUALITY_GC_GRACE_DAYS}")
        QUALITY_GC_GRACE_DAYS = 0

    logging.info(f"Quality DB GC (startup forced): grace={QUALITY_GC_GRACE_DAYS}d, vacuum={FORCE_QUALITY_GC_VACUUM}")
    try:
        gc_quality_db_once()  # удалит записи по фильмам, которых уже нет в Jellyfin
        if FORCE_QUALITY_GC_VACUUM:
            try:
                conn = sqlite3.connect(QUALITY_DB_FILE)
                conn.execute("VACUUM")
                conn.close()
                logging.info("Quality DB GC (startup) VACUUM done.")
            except Exception as ex:
                logging.warning(f"Quality DB GC (startup) VACUUM failed: {ex}")
    finally:
        # вернём обычный grace для фоновой очистки
        QUALITY_GC_GRACE_DAYS = old_grace

#Работа я youtube и рейтингом

# --- SAFE trailer & ratings helpers ---
_youtube_forbid_until = 0.0
_trailer_cache = {}   # на процесс
_ratings_cache = {}   # на процесс

def safe_get_trailer(query: str, *, context: str = "", subkind: str | None = None, tmdb_id: str | None = None) -> str | None:
    """
    Ищем трейлер безопасно + кэшируем в БД.
    Идентификатор кэша берём по приоритету: tmdb_id → нормализованный query.
    subkind: 'movie' | 'show' (для удобства TTL/аналитики; к ключу не обязательно)
    """
    # правила отключения
    try:
        if os.getenv("TRAILER_FETCH_ENABLED", "1").lower() not in ("1","true","yes","on"):
            return None
        if os.getenv("DISABLE_TRAILER_IN_POLLS", "1").lower() in ("1","true","yes","on") and context == "series_poll":
            return None
    except Exception:
        pass

    # ключ кэша
    identity = tmdb_id or query.strip()
    # 1) читаем кэш
    cached_val, cached_at = _extcache_read("trailer", subkind, identity)
    if _is_fresh(cached_at, TRAILER_CACHE_TTL_DAYS) and cached_val:
        return cached_val

    # 2) не свежий — пробуем обновить из сети (с 403-предохранителем)
    try:
        # локальный «стоп» по 403
        import time as _t
        forbid_until = globals().get("_youtube_forbid_until", 0.0)
        if _t.time() < forbid_until:
            return cached_val or None

        url = get_youtube_trailer_url(query)  # твоя исходная функция
        if url:
            _extcache_write("trailer", subkind, identity, url)
            return url
        # если не нашли — вернём устаревшее, чтобы не ломать текст
        return cached_val or None

    except requests.HTTPError as ex:
        resp = getattr(ex, "response", None)
        if getattr(resp, "status_code", None) == 403:
            # при 403 — ставим «форбид» и используем кэш
            import time as _t
            suspend_min = int(os.getenv("TRAILER_FORBID_SUSPEND_MIN", "60"))
            globals()["_youtube_forbid_until"] = _t.time() + suspend_min * 60
            logging.warning(f"YouTube 403; suspend {suspend_min} min; use cache if any")
            return cached_val or None
        logging.warning(f"YouTube HTTP error: {ex}")
        return cached_val or None
    except Exception as ex:
        logging.warning(f"YouTube trailer fetch failed: {ex}")
        return cached_val or None

def safe_fetch_mdblist_ratings(kind: str, tmdb_id: str | None) -> str:
    """
    Возвращает текст рейтингов. Сначала читаем кэш из БД, если он свежий.
    Если кэш просрочен — пробуем обновить; при ошибке отдаём устаревшее.
    kind: 'movie' | 'show'
    """
    if not tmdb_id:
        return ""
    # 1) читаем кэш
    cached_val, cached_at = _extcache_read("ratings", kind, tmdb_id)
    if _is_fresh(cached_at, RATINGS_CACHE_TTL_DAYS) and cached_val:
        return cached_val

    # 2) пробуем обновить из сети
    fresh = ""
    try:
        fresh = fetch_mdblist_ratings(kind, tmdb_id) or ""
    except Exception as ex:
        logging.warning(f"MDblist ratings fetch failed: {ex}")

    if fresh:
        _extcache_write("ratings", kind, tmdb_id, fresh)
        return fresh

    # 3) если не удалось обновить — вернём устаревшее (не ломаем уведомления)
    return cached_val or ""

#Работа с сезонными уведомлениями
def jellyfin_count_present_episodes_in_season(season_id: str) -> int | None:
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "ParentId": season_id,
            "IncludeItemTypes": "Episode",
            "Recursive": "false",
            "LocationTypes": "FileSystem",
            "IsMissing": "false",
            "Limit": "1",
        }
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json() or {}
        cnt = data.get("TotalRecordCount")
        return int(cnt) if isinstance(cnt, int) else len(data.get("Items") or [])
    except requests.HTTPError as ex:
        status = getattr(getattr(ex, "response", None), "status_code", None)
        if status in (400, 404):
            # сигнал «сезон удалён»
            return -1
        logging.warning(f"Failed to count PRESENT episodes for season {season_id}: {ex}")
        return None
    except Exception as ex:
        logging.warning(f"Failed to count PRESENT episodes for season {season_id}: {ex}")
        return None

def jellyfin_count_missing_episodes_in_season(season_id: str) -> int | None:
    try:
        params = {
            "api_key": JELLYFIN_API_KEY,
            "ParentId": season_id,
            "IncludeItemTypes": "Episode",
            "Recursive": "false",
            "IsMissing": "true",
            "IsUnaired": "false",
            "IsVirtualUnaired": "false",
            "LocationTypes": "Virtual",
            "Limit": "1",
        }
        url = f"{JELLYFIN_BASE_URL}/emby/Items"
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json() or {}
        cnt = data.get("TotalRecordCount")
        return int(cnt) if isinstance(cnt, int) else len(data.get("Items") or [])
    except requests.HTTPError as ex:
        status = getattr(getattr(ex, "response", None), "status_code", None)
        if status in (400, 404):
            # сигнал «сезон удалён»
            return -1
        logging.warning(f"Failed to count MISSING episodes for season {season_id}: {ex}")
        return None
    except Exception as ex:
        logging.warning(f"Failed to count MISSING episodes for season {season_id}: {ex}")
        return None

def jellyfin_get_season_counts_resilient(season_id: str) -> tuple[int, int] | tuple[int, int, bool]:
    attempts = max(int(os.getenv("SEASON_EP_COUNT_RETRY_ATTEMPTS", "5")), 1)
    delay = max(int(os.getenv("SEASON_EP_COUNT_RETRY_DELAY_SEC", "3")), 0)

    present, total = 0, 0
    for i in range(1, attempts + 1):
        p = jellyfin_count_present_episodes_in_season(season_id)
        if p == -1:
            # сезон удалён — чистим прогресс и прекращаем
            _sp_delete(season_id)
            logging.info(f"Season {season_id} removed from Jellyfin — purged from DB.")
            return (-1, -1)  # сигнал наверх

        m = jellyfin_count_missing_episodes_in_season(season_id)
        if m == -1:
            _sp_delete(season_id)
            logging.info(f"Season {season_id} removed from Jellyfin — purged from DB.")
            return (-1, -1)

        if isinstance(p, int):
            present = p
        if isinstance(m, int):
            total = present + m
        else:
            total = present

        if total > 0 and (present > 0 or i == attempts):
            if i > 1:
                logging.debug(f"Season counts after {i} attempts: present={present}, total={total}")
            break
        time.sleep(delay)

    return (present, total)

def poll_recent_episodes_once():
    """
    Ищем свежие эпизоды постранично, группируем по сезону и шлём ОДНО уведомление «Новый сезон: добавлено N из M».
    Свежие (моложе SERIES_POLL_GRACE_MIN) пропускаем — пусть их анонсирует вебхук.
    """
    page_size = SERIES_POLL_PAGE_SIZE
    max_total = SERIES_POLL_MAX_TOTAL or 0  # 0 = без ограничения
    start = 0
    fetched = 0
    now_utc = datetime.now(timezone.utc)

    processed_seasons: set[str] = set()

    while True:
        # ограничим последнюю страницу при max_total
        current_limit = page_size if (not max_total or (max_total - fetched) >= page_size) else (max_total - fetched)
        if current_limit <= 0:
            break

        try:
            params = {
                "api_key": JELLYFIN_API_KEY,
                "IncludeItemTypes": "Episode",
                "Recursive": "true",
                "SortBy": "DateCreated,DateModified",
                "SortOrder": "Descending",
                "Limit": str(current_limit),
                "StartIndex": str(start),
                "Fields": "ParentId,SeriesId,SeasonName,DateCreated,ProductionYear,Overview"
            }
            url = f"{JELLYFIN_BASE_URL}/emby/Items"
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json() or {}
            items = payload.get("Items") or []
        except Exception as ex:
            logging.warning(f"Series poll: failed page start={start}: {ex}")
            break

        if not items:
            break

        # сгруппируем эпизоды по сезону
        for ep in items:
            try:
                season_id = ep.get("ParentId") or ep.get("SeasonId")
                if not season_id or season_id in processed_seasons:
                    continue

                # грейс: если эпизод совсем свежий — пропускаем сезон, пусть вебхук объявит
                created_iso = ep.get("DateCreated")
                created_dt = _parse_iso_utc(created_iso) if ' _parse_iso_utc' in globals() else None
                if created_dt and (now_utc - created_dt) < timedelta(minutes=SERIES_POLL_GRACE_MIN):
                    logging.debug(f"Series poll: skip fresh season (ep created {created_dt.isoformat()}) season={season_id}")
                    continue

                # получаем детали сезона/сериала
                season_details = get_item_details(season_id)
                s_item = (season_details.get("Items") or [{}])[0]
                series_id = s_item.get("SeriesId")
                season_name = s_item.get("Name") or ep.get("SeasonName") or "Season"
                release_year = s_item.get("ProductionYear") or ep.get("ProductionYear")

                series_details = get_item_details(series_id) if series_id else {"Items": [{}]}
                series_item = (series_details.get("Items") or [{}])[0]
                series_name = series_item.get("Name") or ""
                overview_to_use = s_item.get("Overview") or series_item.get("Overview") or ""

                # антиспам-ключ, как в вебхуке
                series_name_cleaned = series_name.replace(f" ({release_year})", "").strip()
                key_name = f"{series_name_cleaned} {season_name}".strip()

                if item_already_notified("Season", key_name, release_year):
                    processed_seasons.add(season_id)
                    continue

                # считаем «сколько есть / сколько всего» по сезону (используй твой resilient-хелпер)
                # в poll_recent_episodes_once(), прямо перед подсчётом present/total:
                wait_until_scan_idle("season counts build")
                present, total = jellyfin_get_season_counts_resilient(season_id)
                # сезон удалён — пропускаем
                if isinstance(present, int) and isinstance(total, int) and present == -1 and total == -1:
                    processed_seasons.add(season_id)
                    continue

                # --- Срез по дате создания БД: baseline только ОДИН РАЗ, если сезона ещё нет в БД ---
                row_existing = _sp_get(season_id)
                if row_existing is None:
                    try:
                        db_created_iso = _db_get_created_at_iso()
                        db_created_dt = _parse_iso_dt(db_created_iso)

                        # DateCreated у сезона берём из уже полученного s_item; если вдруг нет — дёрнем детали
                        season_created_iso = s_item.get("DateCreated")
                        if not season_created_iso:
                            s_det_fallback = get_item_details(season_id)
                            season_created_iso = ((s_det_fallback.get("Items") or [{}])[0]).get("DateCreated")
                        season_created_dt = _parse_iso_dt(season_created_iso)

                        if db_created_dt and season_created_dt and (season_created_dt < db_created_dt):
                            # Сезон был ДО создания БД — пишем baseline и НЕ шлём уведомление
                            _sp_upsert(
                                season_id,
                                present=present, total=total,
                                series_id=series_id,
                                season_number=int(s_item.get("IndexNumber")) if s_item.get(
                                    "IndexNumber") is not None else None,
                                series_name=series_name_cleaned,
                                release_year=release_year,
                                mark_notified=True  # baseline: сразу считаем «объявленным»
                            )
                            logging.info(
                                f"(Series poll) Season pre-DB cutoff baseline: {series_name_cleaned} {season_name} — {present}/{total}")
                            processed_seasons.add(season_id)
                            continue
                    except Exception as ex:
                        logging.warning(f"Season cutoff check failed for {season_id}: {ex}")
                # --- конец среза ---

                # 1) сохраняем «наблюдение» (без mark_notified) — чтобы иметь базу для следующего раза
                _sp_upsert(
                    season_id,
                    present=present, total=total,
                    series_id=series_id,
                    season_number=int(s_item.get("IndexNumber")) if s_item.get("IndexNumber") is not None else None,
                    series_name=series_name_cleaned,
                    release_year=release_year,
                    mark_notified=False
                )

                # 2) решаем, отправлять ли: только если present вырос со времени прошлого уведомления
                if not _sp_should_notify(season_id, present):
                    processed_seasons.add(season_id)
                    continue
                # мы решили отправлять: сразу «закрываем» сезон на этот прогон,
                # чтобы следующие эпизоды не повторяли внешние вызовы
                processed_seasons.add(season_id)

                # рейтинги/трейлер (опционально)
                tmdb_id = jellyfin_get_tmdb_id(series_id) if 'jellyfin_get_tmdb_id' in globals() else None
                trailer_url = safe_get_trailer_prefer_tmdb(f"{series_name_cleaned} Trailer {release_year}",
                                subkind="show", tmdb_id=tmdb_id, context="")

                # 3) формируем сообщение
                notification_message = (
                    f"*{t('new_season_title')}*\n\n*{series_name_cleaned}* *({release_year})*\n\n"
                    f"*{season_name}*"
                )
                if total >= present and total > 0:
                    notification_message += f"\n\n{t('season_added_progress').format(added=present, total=total)}"
                elif present > 0:
                    notification_message += f"\n\n{t('season_added_count_only').format(added=present)}"
                if overview_to_use:
                    notification_message += f"\n\n{overview_to_use}"
                if tmdb_id:
                    ratings_text = safe_fetch_mdblist_ratings("show", tmdb_id)
                    if ratings_text:
                        notification_message += f"\n\n*{t('new_ratings_show')}*\n{ratings_text}"
                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

                if INCLUDE_AUDIO_TRACKS:
                    tracks_block = build_audio_tracks_block_for_season(season_id)
                    if tracks_block:
                        notification_message += tracks_block

                # 4) отправляем и фиксируем «до куда сообщили»
                if _fetch_jellyfin_image_with_retries(season_id, attempts=1, timeout=3):
                    send_notification(season_id, notification_message)
                else:
                    send_notification(series_id, notification_message)
                    logging.warning(
                        f"(Series poll) {series_name_cleaned} {season_name} image missing; using series image")

                # помечаем прогресс: теперь last_notified_present = present
                _sp_upsert(
                    season_id,
                    present=present, total=total,
                    series_id=series_id,
                    season_number=int(s_item.get("IndexNumber")) if s_item.get("IndexNumber") is not None else None,
                    series_name=series_name_cleaned,
                    release_year=release_year,
                    mark_notified=True
                )

                logging.info(
                    f"(Series poll) Season announced: {series_name_cleaned} {season_name} — {present} / {total}")
                processed_seasons.add(season_id)

            except Exception as ex:
                logging.warning(f"Series poll: season from ep {ep.get('Id')} failed: {ex}")

        n = len(items)
        fetched += n
        start += n
        logging.debug(f"Series poll: page fetched {n} episodes (total {fetched})")
        if n < current_limit:
            break  # последняя страница

def _series_poll_loop():
    while True:
        try:
            poll_recent_episodes_once()
        except Exception as ex:
            logging.warning(f"Series poll loop error: {ex}")
        time.sleep(SERIES_POLL_INTERVAL_SEC)

if SERIES_POLL_ENABLED:
    threading.Thread(target=_series_poll_loop, name="series-poll", daemon=True).start()
    logging.info(f"Series polling enabled every {SERIES_POLL_INTERVAL_SEC}s "
                 f"(page={SERIES_POLL_PAGE_SIZE}, max_total={SERIES_POLL_MAX_TOTAL}, grace={SERIES_POLL_GRACE_MIN}m)")


def _sq_get(season_id: str) -> dict | None:
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        cur.execute("""
            SELECT season_id, series_id, series_name, season_number, release_year, signature, updated_at, episode_count
            FROM season_quality WHERE season_id=?""", (season_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "season_id": row[0],
            "series_id": row[1],
            "series_name": row[2],
            "season_number": row[3],
            "release_year": row[4],
            "signature": row[5],
            "updated_at": row[6],
            "episode_count": row[7],
        }
    except Exception as ex:
        logging.warning(f"_sq_get failed: {ex}")
        return None
    finally:
        try: conn.close()
        except: pass


def _sq_upsert(season_id: str, *, signature: str,
               episode_count: int | None,
               series_id: str | None = None,
               series_name: str | None = None,
               season_number: int | None = None,
               release_year: int | None = None):
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        nowz = datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')
        cur.execute("""
            INSERT INTO season_quality (season_id, series_id, series_name, season_number, release_year, signature, updated_at, episode_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(season_id) DO UPDATE SET
              signature=excluded.signature,
              updated_at=excluded.updated_at,
              episode_count=excluded.episode_count,
              series_id=COALESCE(excluded.series_id, season_quality.series_id),
              series_name=COALESCE(excluded.series_name, season_quality.series_name),
              season_number=COALESCE(excluded.season_number, season_quality.season_number),
              release_year=COALESCE(excluded.release_year, season_quality.release_year)
        """, (season_id, series_id, series_name, season_number, release_year, signature, nowz, episode_count))
        conn.commit()
    except Exception as ex:
        logging.warning(f"_sq_upsert failed: {ex}")
    finally:
        try: conn.close()
        except: pass


def _sp_get(season_id: str) -> dict | None:
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        cur.execute("""
            SELECT season_id, series_id, series_name, season_number, release_year,
                   present, total, last_notified_present, updated_at
            FROM season_progress WHERE season_id=?""", (season_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "season_id": row[0], "series_id": row[1], "series_name": row[2],
            "season_number": row[3], "release_year": row[4],
            "present": row[5], "total": row[6],
            "last_notified_present": row[7], "updated_at": row[8],
        }
    except Exception as ex:
        logging.warning(f"_sp_get failed: {ex}")
        return None
    finally:
        try: conn.close()
        except: pass

def _sp_upsert(season_id: str, *, present: int, total: int,
               series_id: str | None = None, season_number: int | None = None,
               series_name: str | None = None, release_year: int | None = None,
               mark_notified: bool = False):
    """
    Записывает прогресс сезона ТОЛЬКО при фактических изменениях.
    - без mark_notified: обновляем, если изменились present/total
    - с mark_notified: дополнительно пишем last_notified_present=present (если отличается)
    Это предотвращает бессмысленные перезаписи и «мигание» mtime у файла БД.
    """
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        nowz = datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')

        # читаем текущее состояние
        cur.execute("""
            SELECT present, total, last_notified_present
            FROM season_progress WHERE season_id=?
        """, (season_id,))
        row = cur.fetchone()

        if row:
            old_present, old_total, old_last = (row[0] or 0), (row[1] or 0), (row[2] or 0)

            # решаем, нужно ли писать
            need_update = False
            set_last = None

            if present != old_present or total != old_total:
                need_update = True
            if mark_notified and old_last != present:
                need_update = True
                set_last = present

            if not need_update:
                # ничего не изменилось — выходим без записи
                return

            if mark_notified:
                cur.execute("""
                    UPDATE season_progress
                    SET present=?, total=?, last_notified_present=?, updated_at=?,
                        series_id=COALESCE(?, series_id),
                        series_name=COALESCE(?, series_name),
                        season_number=COALESCE(?, season_number),
                        release_year=COALESCE(?, release_year)
                    WHERE season_id=?
                """, (present, total, (set_last if set_last is not None else old_last), nowz,
                      series_id, series_name, season_number, release_year, season_id))
            else:
                cur.execute("""
                    UPDATE season_progress
                    SET present=?, total=?, updated_at=?,
                        series_id=COALESCE(?, series_id),
                        series_name=COALESCE(?, series_name),
                        season_number=COALESCE(?, season_number),
                        release_year=COALESCE(?, release_year)
                    WHERE season_id=?
                """, (present, total, nowz,
                      series_id, series_name, season_number, release_year, season_id))
        else:
            # первая запись по сезону
            cur.execute("""
                INSERT INTO season_progress (
                    season_id, series_id, series_name, season_number, release_year,
                    present, total, last_notified_present, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (season_id, series_id, series_name, season_number, release_year,
                  int(present), int(total),
                  int(present) if mark_notified else 0,
                  nowz))
        conn.commit()
    except Exception as ex:
        logging.warning(f"_sp_upsert failed: {ex}")
    finally:
        try: conn.close()
        except: pass

def _sp_should_notify(season_id: str, present_now: int) -> bool:
    row = _sp_get(season_id)
    if row is None:
        # впервые видим сезон: слать только если разрешено и есть хоть что-то
        return SERIES_POLL_INITIAL_ANNOUNCE and present_now > 0
    last = int(row.get("last_notified_present") or 0)
    return present_now > last

#Сохранение трейлеров и рейтингов в базу данных
def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')

def _extcache_key(kind: str, subkind: str | None, identity: str) -> str:
    # Единый формат ключа
    s = subkind or "-"
    return f"{kind}:{s}:{identity}".strip()

def _extcache_read(kind: str, subkind: str | None, identity: str) -> tuple[str | None, str | None]:
    """
    Возвращает (value, updated_at_iso) из external_cache или (None, None).
    """
    if not EXTERNAL_CACHE_ENABLED:
        return (None, None)
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        ck = _extcache_key(kind, subkind, identity)
        cur.execute("SELECT value, updated_at FROM external_cache WHERE cache_key=?", (ck,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)
    except Exception as ex:
        logging.warning(f"_extcache_read fail: {ex}")
        return (None, None)
    finally:
        try: conn.close()
        except: pass

def _extcache_write(kind: str, subkind: str | None, identity: str, value: str | None):
    if not EXTERNAL_CACHE_ENABLED:
        return
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        ck = _extcache_key(kind, subkind, identity)
        cur.execute("""
            INSERT INTO external_cache (cache_key, kind, subkind, value, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """, (ck, kind, subkind or "", value or "", _utcnow_iso()))
        conn.commit()
    except Exception as ex:
        logging.warning(f"_extcache_write fail: {ex}")
    finally:
        try: conn.close()
        except: pass

def _is_fresh(updated_iso: str | None, ttl_days: int) -> bool:
    if not updated_iso:
        return False
    try:
        dt = datetime.fromisoformat(updated_iso.replace('Z', '+00:00'))
        return datetime.now(timezone.utc) - dt <= timedelta(days=max(ttl_days, 0))
    except Exception:
        return False

#Поиск трейлеров на tmdb
def _tmdb_pick_best_video(results: list[dict]) -> str | None:
    """
    Выбираем лучшую ссылку на трейлер из TMDB /videos.
    Приоритет: YouTube + type=Trailer + official=true → YouTube + Trailer → YouTube → Vimeo.
    """
    if not results:
        return None

    def to_url(site: str | None, key: str | None) -> str | None:
        if not site or not key:
            return None
        s = site.lower()
        if s == "youtube":
            return f"https://www.youtube.com/watch?v={key}"
        if s == "vimeo":
            return f"https://vimeo.com/{key}"
        return None

    # нормализуем
    vids = []
    for v in results:
        vids.append({
            "site": (v.get("site") or v.get("Site") or "").strip(),
            "type": (v.get("type") or v.get("Type") or "").strip(),
            "official": bool(v.get("official") if v.get("official") is not None else v.get("Official")),
            "key": v.get("key") or v.get("Key"),
            "size": v.get("size") or v.get("Size") or 0,
            "published_at": v.get("published_at") or v.get("PublishedAt") or "",
        })

    # 1) YouTube + Trailer + official
    for v in vids:
        if v["site"].lower() == "youtube" and v["type"].lower() == "trailer" and v["official"]:
            u = to_url(v["site"], v["key"])
            if u: return u
    # 2) YouTube + Trailer
    for v in vids:
        if v["site"].lower() == "youtube" and v["type"].lower() == "trailer":
            u = to_url(v["site"], v["key"])
            if u: return u
    # 3) любой YouTube
    for v in vids:
        if v["site"].lower() == "youtube":
            u = to_url(v["site"], v["key"])
            if u: return u
    # 4) Vimeo (на всякий случай)
    for v in vids:
        if v["site"].lower() == "vimeo":
            u = to_url(v["site"], v["key"])
            if u: return u
    return None


def _tmdb_fetch_trailer_url(subkind: str, tmdb_id: str, season_number: int | None = None) -> str | None:
    """
    subkind: 'movie' | 'show'
    Для фильмов: /movie/{id}/videos
    Для сериалов: /tv/{id}/videos, при необходимости пробуем /tv/{id}/season/{n}/videos
    """
    if not TMDB_API_KEY or not tmdb_id:
        return None
    try:
        params = {
            "api_key": TMDB_API_KEY,
            "language": TMDB_LANGUAGE,
            # включить ролики без языковой метки
            "include_video_language": f"{TMDB_LANGUAGE},null"
        }
        if subkind == "movie":
            url = f"{TMDB_BASE}/movie/{tmdb_id}/videos"
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json() or {}
            return _tmdb_pick_best_video(data.get("results") or [])
        else:
            # пробуем уровень сериала
            url = f"{TMDB_BASE}/tv/{tmdb_id}/videos"
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json() or {}
            url_pick = _tmdb_pick_best_video(data.get("results") or [])
            if url_pick:
                return url_pick
            # при необходимости — уровень сезона
            if season_number is not None:
                url = f"{TMDB_BASE}/tv/{tmdb_id}/season/{int(season_number)}/videos"
                r = requests.get(url, params=params, timeout=10)
                r.raise_for_status()
                data = r.json() or {}
                return _tmdb_pick_best_video(data.get("results") or [])
            return None
    except Exception as ex:
        logging.warning(f"TMDB trailer fetch failed ({subkind}:{tmdb_id} s{season_number}): {ex}")
        return None

def safe_get_trailer_prefer_tmdb(
    title: str,
    *,
    year: int | None = None,
    subkind: str,                 # 'movie' | 'show'
    tmdb_id: str | None = None,
    season_number: int | None = None,
    context: str = ""
) -> str | None:
    """
    1) Читаем кэш external_cache('trailer', subkind, identity) — identity=tmdb_id или title+year.
    2) Если кэш свежий — отдаём.
    3) Иначе пробуем TMDB → если нашли — пишем в кэш и отдаём.
    4) Иначе fallback: YouTube-поиск через safe_get_trailer(query, ...), тоже кладём в кэш.
    """
    # формируем identity и query для кэша/фоллбэка
    identity = (tmdb_id or "").strip() or f"{title.strip()} ({year})".strip()
    cached_val, cached_at = _extcache_read("trailer", subkind, identity)
    if _is_fresh(cached_at, TRAILER_CACHE_TTL_DAYS) and cached_val:
        return cached_val

    # 1) TMDB
    url_tmdb = None
    try:
        url_tmdb = _tmdb_fetch_trailer_url(subkind, tmdb_id, season_number) if tmdb_id else None
    except Exception as ex:
        logging.warning(f"safe_get_trailer_prefer_tmdb: TMDB branch failed: {ex}")

    if url_tmdb:
        _extcache_write("trailer", subkind, identity, url_tmdb)
        return url_tmdb

    # 2) Fallback: YouTube по названию + году
    q_year = f" {year}" if year else ""
    query = f"{title} Trailer{q_year}".strip()
    url_yt = safe_get_trailer(query, context=context, subkind=subkind, tmdb_id=tmdb_id)
    if url_yt:
        _extcache_write("trailer", subkind, identity, url_yt)
        return url_yt

    # 3) Ничего не нашли: вернём устаревшее, если было
    return cached_val or None

#Получение звуковых дорожек для сериалов
def _label_audio_stream(stream: dict) -> str:
    """
    Формирует «человеческую» подпись дорожки как в фильмах:
    DisplayTitle/Title -> иначе LANG CODEC Ch Layout (например: ENG AC3 6ch 5.1)
    """
    label = stream.get("DisplayTitle") or stream.get("Title")
    if label:
        return str(label)
    lang = stream.get("Language")
    codec = stream.get("Codec")
    ch = stream.get("Channels")
    layout = stream.get("ChannelLayout")
    parts = []
    if lang:   parts.append(str(lang).upper())
    if codec:  parts.append(str(codec).upper())
    if ch:     parts.append(f"{ch}ch")
    if layout: parts.append(str(layout))
    return " ".join(parts) or "Audio"

def _collect_season_audio_labels(season_id: str) -> list[str]:
    """
    Собирает уникальные названия аудио-дорожек из фактически присутствующих эпизодов сезона.
    Берём не более SEASON_AUDIO_SCAN_LIMIT эпизодов и не более SEASON_AUDIO_TRACKS_MAX уникальных дорожек.
    """
    labels_seen = []
    label_set = set()

    # запросим эпизоды сезона с MediaSources (как у тебя уже делается)
    eps = _season_fetch_episodes(season_id)  # должен возвращать Items с MediaSources/LocationType/Path
    # фильтруем только присутствующие (есть файл)
    present_eps = [ep for ep in eps if _episode_has_file(ep)]

    # ограничим количество эпизодов для разбора, чтобы не грузить лишнее
    for ep in present_eps[:max(SEASON_AUDIO_SCAN_LIMIT, 1)]:
        sources = ep.get("MediaSources") or []
        if not sources:
            continue
        # возьмём первую «основную» дорожку источника
        src = sources[0]
        for s in (src.get("MediaStreams") or []):
            if s.get("Type") != "Audio":
                continue
            lbl = _label_audio_stream(s)
            if lbl not in label_set:
                label_set.add(lbl)
                labels_seen.append(lbl)
                if len(labels_seen) >= SEASON_AUDIO_TRACKS_MAX:
                    return labels_seen
    return labels_seen

def _season_fetch_episodes(season_id: str, *, max_items: int | None = None) -> list[dict]:
    """
    Возвращает список эпизодов сезона с нужными полями для анализа аудио:
    - Берём ТОЛЬКО фактически присутствующие эпизоды (IsMissing=false, LocationTypes=FileSystem)
    - Тянем поля MediaSources/LocationType/Path/IndexNumber/Name
    - Пагинация до max_items (по умолчанию SEASON_AUDIO_SCAN_LIMIT или 50)
    """
    try:
        per_page = 200
        # ограничим объём: нам для списка дорожек достаточно просканировать часть сезона
        default_scan_limit = 50
        try:
            default_scan_limit = max(int(globals().get("SEASON_AUDIO_SCAN_LIMIT", 50)), 1)
        except Exception:
            pass
        cap = int(max_items) if isinstance(max_items, int) and max_items > 0 else default_scan_limit

        all_eps: list[dict] = []
        start = 0
        while True:
            # не запрашиваем больше, чем осталось до cap
            limit = per_page if (len(all_eps) + per_page) <= cap else (cap - len(all_eps))
            if limit <= 0:
                break

            params = {
                "api_key": JELLYFIN_API_KEY,
                "ParentId": season_id,
                "IncludeItemTypes": "Episode",
                "Recursive": "false",
                # ключевые фильтры: только реальные файлы
                "IsMissing": "false",
                "LocationTypes": "FileSystem",
                # сортируем по номеру эпизода
                "SortBy": "IndexNumber,DateCreated",
                "SortOrder": "Ascending",
                "StartIndex": str(start),
                "Limit": str(limit),
                # поля, нужные для аудио-аналитики
                "Fields": "MediaSources,LocationType,Path,IndexNumber,Name"
            }
            url = f"{JELLYFIN_BASE_URL}/emby/Items"
            r = requests.get(url, params=params, timeout=12)
            r.raise_for_status()
            data = r.json() or {}
            items = data.get("Items") or []
            if not items:
                break

            all_eps.extend(items)
            start += len(items)
            if len(items) < limit:
                break  # последняя страница

        return all_eps
    except requests.HTTPError as ex:
        status = getattr(getattr(ex, "response", None), "status_code", None)
        if status in (400, 404):
            return []  # сезон удалён — молча возвращаем пусто
        logging.warning(f"_season_fetch_episodes failed (season {season_id}): {ex}")
        return []
    except Exception as ex:
        logging.warning(f"_season_fetch_episodes failed (season {season_id}): {ex}")
        return []


def _episode_has_file(ep: dict) -> bool:
    """
    Возвращает True, если у эпизода есть реальный файл.
    Проверяем:
      - LocationType == FileSystem/File (на всякий случай)
      - или задан Path
      - или есть MediaSources (не пусто) с признаками файла
    """
    try:
        lt = (ep.get("LocationType") or "").strip().lower()
        if lt in ("filesystem", "file"):
            return True

        if ep.get("Path"):
            return True

        ms = ep.get("MediaSources") or []
        if ms:
            for src in ms:
                # Признаки реального файла в источнике
                src_lt = (src.get("LocationType") or "").strip().lower()
                if src_lt in ("filesystem", "file"):
                    return True
                if src.get("Path"):
                    return True
                # Наличие контейнера/размера часто говорит о локальном файле
                if src.get("Container") or src.get("Size"):
                    return True
        return False
    except Exception:
        return False

def _plural_episodes(n: int, lang: str) -> str:
    lang = (lang or "").lower()
    if lang.startswith("ru"):
        n10, n100 = n % 10, n % 100
        if n10 == 1 and n100 != 11:
            return "эпизод"
        if 2 <= n10 <= 4 and not (12 <= n100 <= 14):
            return "эпизода"
        return "эпизодов"
    return "episode" if n == 1 else "episodes"

def _collect_season_audio_label_counts(season_id: str) -> tuple[OrderedDict[str, int], int]:
    """
    Возвращает (OrderedDict[display_label -> count], present_episodes_count).
    Группирует метки дорожек с учётом нормализации (_normalize_audio_label),
    чтобы 'HDRezka' и 'HDrezka' считались одной дорожкой.
    """
    try:
        eps = _season_fetch_episodes(season_id)
        present_eps = [ep for ep in eps if _episode_has_file(ep)]
        scan_limit = max(int(globals().get("SEASON_AUDIO_SCAN_LIMIT", 50)), 1)

        # norm_label -> [display_label (первая встреченная), count]
        groups: dict[str, list] = {}

        for ep in present_eps[:scan_limit]:
            sources = ep.get("MediaSources") or []
            if not sources:
                continue
            src = sources[0]
            for s in (src.get("MediaStreams") or []):
                if s.get("Type") != "Audio":
                    continue
                raw_label = _label_audio_stream(s)
                norm = _normalize_audio_label(raw_label)
                if norm not in groups:
                    # сохраним человекочитаемую метку «как встретилась впервые»
                    groups[norm] = [raw_label.strip(), 1]
                else:
                    groups[norm][1] += 1

        # Сортировка: по убыванию счётчика, затем по метке (нормализованной)
        sorted_items = sorted(groups.items(), key=lambda kv: (-kv[1][1], kv[0]))
        ordered = OrderedDict((disp, cnt) for (_norm, (disp, cnt)) in sorted_items)
        return ordered, len(present_eps)
    except Exception as ex:
        logging.warning(f"_collect_season_audio_label_counts failed for {season_id}: {ex}")
        return OrderedDict(), 0

def build_audio_tracks_block_for_season(season_id: str) -> str:
    """
    Формирует текстовый блок аудио-дорожек для сезона в виде:
      *Audio tracks* (N)
      - RUS AC3 5.1 × 5 эпизодов
      - ENG EAC3 6ch × 3 эпизода
      ...
    """
    try:
        labels_counts, present_cnt = _collect_season_audio_label_counts(season_id)
        if not labels_counts:
            return ""

        lang = os.environ.get("LANGUAGE", "en")
        # заголовок (fallback, если нет ключа локализации)
        header = (MESSAGES.get(LANG, {}) or {}).get("audio_tracks_header") or \
                 ( "Аудио-дорожки" if lang.lower().startswith("ru") else "Audio tracks" )

        max_labels = max(int(globals().get("SEASON_AUDIO_TRACKS_MAX", 12)), 1)
        lines = [f"\n\n*{header}* ({min(len(labels_counts), max_labels)})"]

        i = 0
        for label, count in labels_counts.items():
            if i >= max_labels:
                break
            lines.append(f"- {label} × {count} {_plural_episodes(count, lang)}")
            i += 1

        return "\n".join(lines)
    except Exception as ex:
        logging.warning(f"Season audio block build failed for {season_id}: {ex}")
        return ""

def _normalize_audio_label(label: str) -> str:
    """
    Приводит метку к каноническому виду для сравнения:
    - casefold (регистронезависимо)
    - нормализация длинных тире к '-'
    - единые пробелы вокруг дефиса и внутри строки
    """
    s = (label or "").strip()
    s = re.sub(r"[–—−]", "-", s)              # все тире -> '-'
    s = re.sub(r"\s*-\s*", " - ", s)          # пробелы вокруг дефиса
    s = re.sub(r"\s+", " ", s)                # схлопнуть пробелы
    return s.casefold()                        # регистронезависимо

#Контроль базы данных (дата создания)
def _db_get_created_at_iso() -> str | None:
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_meta WHERE key='db_created_at'")
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as ex:
        logging.warning(f"db_created_at read failed: {ex}")
        return None
    finally:
        try: conn.close()
        except: pass

def _parse_iso_dt(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except Exception:
        return None

def _sp_delete(season_id: str):
    try:
        conn = sqlite3.connect(QUALITY_DB_FILE)
        cur = conn.cursor()
        cur.execute("DELETE FROM season_progress WHERE season_id=?", (season_id,))
        conn.commit()
    except Exception as ex:
        logging.warning(f"_sp_delete failed for {season_id}: {ex}")
    finally:
        try: conn.close()
        except: pass

#Уведомление об обновлении сезонов
def _episode_media_quality_signature_from_ep(ep: dict) -> str:
    """
    Строит сигнатуру качества эпизода по первому MediaSource (без сетевых запросов).
    """
    try:
        sources = ep.get("MediaSources") or []
        if not sources:
            return ""
        src = sources[0]
        streams = src.get("MediaStreams") or []

        v = next((s for s in streams if s.get("Type")=="Video"), None)
        a = next((s for s in streams if s.get("Type")=="Audio"), None)

        q = {}
        # контейнер/размер/битрейт
        q["container"] = (src.get("Container") or "").lower()
        try: q["size_bytes"] = int(src.get("Size") or 0)
        except Exception: q["size_bytes"] = 0
        try: q["video_bitrate_kbps"] = int((src.get("Bitrate") or 0)) // 1000
        except Exception: q["video_bitrate_kbps"] = None

        if v:
            q["video_codec"] = (v.get("Codec") or "").lower()
            q["width"]  = v.get("Width") or v.get("PixelWidth")
            q["height"] = v.get("Height") or v.get("PixelHeight")
            q["bit_depth"] = v.get("BitDepth")
            # fps
            fps = v.get("RealFrameRate") or v.get("AverageFrameRate") or v.get("FrameRate")
            try: q["fps"] = float(fps) if fps is not None else None
            except Exception: q["fps"] = None
            # профили HDR/DV
            q["image_profiles"] = _detect_image_profiles_from_fields(v)

        if a:
            q["audio_codec"] = (a.get("Codec") or "").lower()
            try: q["audio_channels"] = int(a.get("Channels") or 0)
            except Exception: q["audio_channels"] = None
            try: q["audio_bitrate_kbps"] = int((a.get("BitRate") or 0)) // 1000
            except Exception: q["audio_bitrate_kbps"] = None

        sig = _quality_signature(q)  # используем вашу нормализацию
        return sig or ""
    except Exception as ex:
        logging.debug(f"episode quality signature failed: {ex}")
        return ""

def _season_quality_signature(season_id: str, *, scan_limit: int | None = None) -> str:
    """
    Агрегированная сигнатура сезона = sha1 от отсортированного списка сигнатур эпизодов (с файлами).
    """
    eps = _season_fetch_episodes(season_id)
    present_eps = [ep for ep in eps if _episode_has_file(ep)]
    lim = max(int(globals().get("SEASON_QUALITY_SIG_LIMIT", 80)), 1)
    if scan_limit is not None:
        lim = max(int(scan_limit), 1)

    sigs = []
    for ep in present_eps[:lim]:
        s = _episode_media_quality_signature_from_ep(ep)
        if s:
            sigs.append(s)

    if not sigs:
        return ""

    sigs.sort()
    joined = "||".join(sigs).encode("utf-8", errors="ignore")
    return hashlib.sha1(joined).hexdigest()


def _season_quality_snapshot(season_id: str, *, scan_limit: int | None = None) -> tuple[str, int]:
    sig = _season_quality_signature(season_id, scan_limit=scan_limit)
    eps = _season_fetch_episodes(season_id)
    present = len([ep for ep in eps if _episode_has_file(ep)])
    return (sig, present)

def _notify_season_quality_updated(season_id: str):
    # детали сезона/сериала
    season_details = get_item_details(season_id)
    s_item = (season_details.get("Items") or [{}])[0]
    series_id = s_item.get("SeriesId")
    season_name = s_item.get("Name") or "Season"
    release_year = s_item.get("ProductionYear")

    series_details = get_item_details(series_id) if series_id else {"Items":[{}]}
    series_item = (series_details.get("Items") or [{}])[0]
    series_name = series_item.get("Name") or ""
    overview = s_item.get("Overview") or series_item.get("Overview") or ""

    series_name_cleaned = series_name.replace(f" ({release_year})","").strip()

    # рейтинги + трейлер
    tmdb_id = jellyfin_get_tmdb_id(series_id) if 'jellyfin_get_tmdb_id' in globals() else None
    trailer_url = safe_get_trailer_prefer_tmdb(f"{series_name_cleaned} Trailer {release_year}",
                                               subkind="show", tmdb_id=tmdb_id, context="")

    msg = f"*{t('quality_updated')}*\n\n*{series_name_cleaned}* *({release_year})*\n\n*{season_name}*"
    if overview:
        msg += f"\n\n{overview}"
    if tmdb_id:
        ratings_text = safe_fetch_mdblist_ratings("show", tmdb_id)
        if ratings_text:
            msg += f"\n\n*{t('new_ratings_show')}*\n{ratings_text}"
    if trailer_url:
        msg += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

    if INCLUDE_AUDIO_TRACKS:
        tracks_block = build_audio_tracks_block_for_season(season_id)
        if tracks_block:
            msg += tracks_block

    # постер сезона, если нет — постер сериала
    if _fetch_jellyfin_image_with_retries(season_id, attempts=1, timeout=3):
        send_notification(season_id, msg)
    else:
        send_notification(series_id, msg)
        logging.warning(f"(EpQuality poll) season image missing; used series image for {series_name_cleaned} {season_name}")

def _maybe_notify_season_quality_change(season_id: str) -> bool:
    # Текущий снимок
    new_sig, new_count = _season_quality_snapshot(season_id)
    if not new_sig:
        return False  # ждём, когда Jellyfin отдаст MediaSources/файлы

    row = _sq_get(season_id)
    if row is None:
        # Baseline: фиксируем сигнатуру и count, без уведомления
        try:
            sd = get_item_details(season_id)
            s_item = (sd.get("Items") or [{}])[0]
            _sq_upsert(
                season_id,
                signature=new_sig,
                episode_count=new_count,
                series_id=s_item.get("SeriesId"),
                series_name=None,
                season_number=int(s_item.get("IndexNumber")) if s_item.get("IndexNumber") is not None else None,
                release_year=s_item.get("ProductionYear"),
            )
        except Exception:
            _sq_upsert(season_id, signature=new_sig, episode_count=new_count)
        return False

    old_sig = row.get("signature") or ""
    old_count = row.get("episode_count")
    # 1) Если изменилось число эпизодов в сезоне — обновляем baseline и выходим БЕЗ уведомления
    if (old_count is None) or (old_count != new_count):
        _sq_upsert(season_id, signature=new_sig, episode_count=new_count)
        logging.info(f"(EpQuality) suppressed due to episode_count change: {old_count} -> {new_count} for season {season_id}")
        return False

    # 2) Если сигнатура не изменилась — выходим
    if old_sig == new_sig:
        return False

    # 3) Чистое изменение качества при стабильном числе эпизодов — отправляем уведомление
    _notify_season_quality_updated(season_id)
    _sq_upsert(season_id, signature=new_sig, episode_count=new_count)
    return True

_last_epq_since = datetime.now(timezone.utc)

def poll_episode_quality_once():
    """
    Ищем эпизоды по DateModified (самые свежие изменения), собираем уникальные сезоны,
    и для каждого сезона проверяем изменения агрегированного качества.
    Новые (очень свежие) эпизоды пропускаем — их анонсирует вебхук/серийный поллер.
    """
    page_size = EP_QUALITY_POLL_PAGE_SIZE
    max_total = EP_QUALITY_POLL_MAX_TOTAL or 0
    start = 0
    fetched = 0
    now_utc = datetime.now(timezone.utc)
    processed_seasons: set[str] = set()
    triggered = 0

    while True:
        current_limit = page_size if (not max_total or (max_total - fetched) >= page_size) else (max_total - fetched)
        if current_limit <= 0:
            break
        try:
            params = {
                "api_key": JELLYFIN_API_KEY,
                "IncludeItemTypes": "Episode",
                "Recursive": "true",
                "SortBy": "DateModified,DateCreated",
                "SortOrder": "Descending",
                "Limit": str(current_limit),
                "StartIndex": str(start),
                "Fields": "ParentId,DateCreated"
            }
            url = f"{JELLYFIN_BASE_URL}/emby/Items"
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json() or {}
            items = payload.get("Items") or []
        except Exception as ex:
            logging.warning(f"EpQuality poll: failed page start={start}: {ex}")
            break

        if not items:
            break

        for it in items:
            season_id = it.get("ParentId") or it.get("SeasonId")
            if not season_id or season_id in processed_seasons:
                continue

            # грейс для «совсем новых» эпизодов
            created_iso = it.get("DateCreated")
            created_dt = _parse_iso_utc(created_iso)
            if created_dt and (now_utc - created_dt) < timedelta(minutes=SERIES_POLL_GRACE_MIN):
                continue

            try:
                if _maybe_notify_season_quality_change(season_id):
                    triggered += 1
                processed_seasons.add(season_id)
            except Exception as ex:
                logging.warning(f"EpQuality poll: season {season_id} failed: {ex}")

        n = len(items)
        fetched += n
        start += n
        if n < current_limit:
            break  # последняя страница

    global _last_epq_since
#    logging.info(f"(EpQuality poll) processed={len(processed_seasons)}, triggered={triggered}, since={_last_epq_since.isoformat()}")
    _last_epq_since = now_utc

def _ep_quality_poll_loop():
    while True:
        try:
            poll_episode_quality_once()
        except Exception as ex:
            logging.warning(f"EpQuality poll loop error: {ex}")
        time.sleep(EP_QUALITY_POLL_INTERVAL_SEC)

if EP_QUALITY_POLL_ENABLED:
    threading.Thread(target=_ep_quality_poll_loop, name="ep-quality-poll", daemon=True).start()
    logging.info(f"Episode/Season quality polling enabled every {EP_QUALITY_POLL_INTERVAL_SEC}s "
                 f"(page={EP_QUALITY_POLL_PAGE_SIZE}, max_total={EP_QUALITY_POLL_MAX_TOTAL}, grace={SERIES_POLL_GRACE_MIN}m)")


@app.route("/webhook", methods=["POST"])
def announce_new_releases_from_jellyfin():
    try:
        payload = json.loads(request.data)
        item_type = payload.get("ItemType")
        tmdb_id = payload.get("Provider_tmdb")
        item_name = payload.get("Name")
        release_year = payload.get("Year")
        series_name = payload.get("SeriesName")
        season_epi = payload.get("EpisodeNumber00")
        season_num = payload.get("SeasonNumber00")

        if item_type == "Movie":
            movie_id = payload.get("ItemId")
            overview = payload.get("Overview")
            runtime = payload.get("RunTime")

            movie_name = item_name
            movie_name_cleaned = movie_name.replace(f" ({release_year})", "").strip()

            tmdb_id_payload = payload.get("Provider_tmdb") or payload.get("TmdbId")
            imdb_id_payload = payload.get("Provider_imdb") or payload.get("ImdbId")

            # --- НОВОЕ: если недавно отправляли quality-update по сканеру/гварду — глушим вебхук
            logical_key = _movie_logical_key(tmdb_id=tmdb_id_payload, imdb_id=imdb_id_payload,
                                             name=movie_name_cleaned, year=release_year)
            if was_quality_update_recent(logical_key):
                logging.info(
                    f"(Webhook/Movie) Suppressed 'new movie' due to recent quality update (logical_key={logical_key})")
                return "Suppressed: recent quality update"

            # 1) Сначала проверим апгрейд качества — это должно срабатывать даже если фильм уже «был отправлен»
            if maybe_notify_movie_quality_change(
                    item_id=movie_id,
                    movie_name_cleaned=movie_name_cleaned,
                    release_year=release_year,
                    tmdb_id=tmdb_id_payload,
                    imdb_id=imdb_id_payload,
                    overview=overview,
                    runtime=runtime
            ):
                return "Movie quality update sent"

            # 2) Иначе — стандартное уведомление о новом фильме (как было)
            if not item_already_notified(item_type, item_name, release_year):
                trailer_url = safe_get_trailer_prefer_tmdb(f"{movie_name_cleaned} Trailer {release_year}",
                                context="webhook", subkind="movie", tmdb_id=tmdb_id)

                notification_message = (
                    f"*{t('new_movie_title')}*\n\n*{movie_name_cleaned}* *({release_year})*\n\n{overview}\n\n"
                    f"*{t('new_runtime')}*\n{runtime}"
                )

                if tmdb_id:
                    mdblist_type = item_type.lower()
                    ratings_text = safe_fetch_mdblist_ratings(mdblist_type, tmdb_id)
                    if ratings_text:
                        notification_message += f"\n\n*{t('new_ratings_movie')}*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

                # --- Quality changes в сообщении о НОВОМ фильме ---
                try:
                    # текущее качество
                    new_q = _get_item_media_info_movie(movie_id)

                    # попробуем найти старый слепок по логическому ключу (если фильм когда-то был)
                    logical_key = _movie_logical_key(
                        tmdb_id=tmdb_id_payload,
                        imdb_id=imdb_id_payload,
                        name=movie_name_cleaned,
                        year=release_year
                    )
                    old_q = None
                    try:
                        conn = sqlite3.connect(QUALITY_DB_FILE)
                        cur = conn.cursor()
                        cur.execute("""SELECT video_codec,
                                              video_bitrate,
                                              width,
                                              height,
                                              fps,
                                              bit_depth,
                                              dynamic_range,
                                              image_profiles,
                                              audio_codec,
                                              audio_bitrate,
                                              audio_channels,
                                              container,
                                              size_bytes,
                                              duration_sec
                                       FROM content_quality
                                       WHERE logical_key = ?""", (logical_key,))
                        row = cur.fetchone()
                        if row:
                            old_q = {
                                "video_codec": row[0], "video_bitrate": row[1], "width": row[2], "height": row[3],
                                "fps": row[4], "bit_depth": row[5], "dynamic_range": row[6],
                                "image_profiles": ([p.strip() for p in row[7].split(",")] if row[7] else None),
                                "audio_codec": row[8], "audio_bitrate": row[9], "audio_channels": row[10],
                                "container": row[11], "size_bytes": row[12], "duration_sec": row[13],
                            }
                    except Exception as ex:
                        logging.warning(f"Quality (new movie) old snapshot read failed: {ex}")
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass

                    # Сформируем блок:
                    delta_block = build_quality_changes_block(old_q,
                                                              new_q) if old_q else build_initial_quality_changes_block(
                        new_q)
                    if not delta_block:
                        # на всякий случай: если почему-то блок пуст, покажем инициализационный
                        delta_block = build_initial_quality_changes_block(new_q)
                    notification_message += delta_block

                    # (по желанию) список аудио-дорожек, если разрешён флагом
                    if INCLUDE_AUDIO_TRACKS:
                        tracks_block = build_audio_tracks_block(new_q)
                        if tracks_block:
                            notification_message += tracks_block

                except Exception as ex:
                    logging.warning(f"Quality (new movie) block build failed: {ex}")
                # --- /Quality changes ---

                send_notification(movie_id, notification_message)
                mark_item_as_notified(item_type, item_name, release_year)
                logging.info(f"(Movie) {movie_name} {release_year} notification was sent.")
                return "Movie notification was sent"

        if item_type == "Season":
            # формируем более информативный ключ для антиспама
            season = item_name  # например, "Сезон 1"
            series_title_for_key = (series_name or "").strip()
            key_name = f"{series_title_for_key} {season}".strip()

            if not item_already_notified(item_type, key_name, release_year):
                season_id = payload.get("ItemId")
                season = item_name
                season_details = get_item_details(season_id)
                series_id = season_details["Items"][0].get("SeriesId")
                series_details = get_item_details(series_id)
                # Remove release_year from series_name if present
                series_name_cleaned = series_name.replace(f" ({release_year})", "").strip()

                # Get TMDb ID via external API
                tmdb_id = jellyfin_get_tmdb_id(series_id)
                trailer_url = safe_get_trailer_prefer_tmdb(f"{series_name_cleaned} Trailer {release_year}",
                                subkind="show", tmdb_id=tmdb_id, context="")

                # **Новые строки**: получаем рейтинги для сериала
                ratings_text = safe_fetch_mdblist_ratings("show", tmdb_id)
                # Если есть рейтинги — добавляем пустую строку после них
                ratings_section = f"{ratings_text}\n\n" if ratings_text else ""

                # Get series overview if season overview is empty
                overview_to_use = payload.get("Overview") if payload.get("Overview") else series_details["Items"][0].get(
                    "Overview")

                # считаем «сколько есть / сколько всего» по сезону (используй твой resilient-хелпер)
                present, total = jellyfin_get_season_counts_resilient(season_id)

                if total >= present and total > 0:
                    episodes_segment = f"\n\n{t('season_added_progress').format(added=present, total=total)}"
                elif present > 0:
                    episodes_segment = f"\n\n{t('season_added_count_only').format(added=present)}"
                else:
                    episodes_segment = ""

                notification_message = (
                    f"*{t('new_season_title')}*\n\n*{series_name_cleaned}* *({release_year})*\n\n"
                    f"*{season}*{episodes_segment}\n\n{overview_to_use}")

                if ratings_text:
                    notification_message += f"\n\n*{t('new_ratings_show')}*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

                # Проверим, есть ли постер сезона — если нет, шлём с постером сериала
                if _fetch_jellyfin_image_with_retries(season_id, attempts=1, timeout=3):
                    send_notification(season_id, notification_message)
                else:
                    send_notification(series_id, notification_message)
                    logging.warning(
                        f"{series_name_cleaned} {season} image does not exist, falling back to series image")

                mark_item_as_notified(item_type, key_name, release_year)
                logging.info(f"(Season) {series_name_cleaned} {season} notification was sent")
                return "Season notification was sent"

        if item_type == "Episode":
            if not item_already_notified(item_type, item_name, release_year):
                item_id = payload.get("ItemId")
                file_details = get_item_details(item_id)
                season_id = file_details["Items"][0].get("SeasonId")
                episode_premiere_date = file_details["Items"][0].get("PremiereDate", "0000-00-00T").split("T")[0]
                season_details = get_item_details(season_id)
                series_id = season_details["Items"][0].get("SeriesId")
                season_date_created = season_details["Items"][0].get("DateCreated", "0000-00-00T").split("T")[0]
                epi_name = item_name
                overview = payload.get("Overview")

#                if not DEBUG_DISABLE_DATE_CHECKS:
                if not is_not_within_last_x_days(season_date_created, SEASON_ADDED_WITHIN_X_DAYS):
                    logging.info(f"(Episode) {series_name} Season {season_num} "
                                 f"was added within the last {SEASON_ADDED_WITHIN_X_DAYS} "
                                 f"days. Not sending notification.")
                    return (f"Season was added within the last {SEASON_ADDED_WITHIN_X_DAYS} "
                            f"days. Not sending notification.")

                if episode_premiere_date and is_within_last_x_days(episode_premiere_date,
                                                                   EPISODE_PREMIERED_WITHIN_X_DAYS):

                    notification_message = (
                        f"*{t('new_episode_title')}*\n\n*{t('new_release_date')}*: {episode_premiere_date}\n\n*{t('new_series')}*: {series_name} *S*"
                        f"{season_num}*E*{season_epi}\n*{t('new_episode_t')}*: {epi_name}\n\n{overview}\n\n"
                    )
                    # Постер сезона может отсутствовать — проверим заранее и при необходимости уйдём на постер сериала
                    if _fetch_jellyfin_image_with_retries(season_id, attempts=1, timeout=3):
                        send_notification(season_id, notification_message)
                    else:
                        send_notification(series_id, notification_message)
                        logging.warning(
                            f"(Episode) {series_name} season image does not exist, falling back to series image")

                    mark_item_as_notified(item_type, item_name, release_year)
                    logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} notification sent!")
                    return "Notification sent!"

                else:
                    logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} "
                                 f"was premiered more than {EPISODE_PREMIERED_WITHIN_X_DAYS} "
                                 f"days ago. Not sending notification.")
                    return (f"Episode was added more than {EPISODE_PREMIERED_WITHIN_X_DAYS} "
                            f"days ago. Not sending notification.")

        if item_type == "MusicAlbum":
            # читаем исполнителя/альбом заранее, чтобы сформировать ключ
            album_name = payload.get("Name")
            artist = payload.get("Artist")
            key_name = f"{artist} – {album_name}".strip()

            if not item_already_notified(item_type, key_name, release_year):
                album_id = payload.get("ItemId")
                album_name = payload.get("Name")
                artist = payload.get("Artist")
                year = payload.get("Year")
                overview = payload.get("Overview")
                runtime = payload.get("RunTime")
                musicbrainzalbum_id = payload.get("Provider_musicbrainzalbum")

                # Формируем ссылку на MusicBrainz, если есть ID
                mb_link = f"https://musicbrainz.org/release/{musicbrainzalbum_id}" if musicbrainzalbum_id else ""

                # Шаблон уведомления
                notification_message = (
                    f"*{t('new_album_title')}*\n\n"
                    f"*{artist}*\n\n"
                    f"*{album_name} ({year})*\n\n"
                    f"{overview and overview + '\n\n' or ''}"
                    f"*{t('new_runtime')}*\n{runtime}\n\n"
                    f"{f'[MusicBrainz]({mb_link})' if mb_link else ''}\n"
                )

                send_notification(album_id, notification_message)
                mark_item_as_notified(item_type, key_name, release_year)
                logging.info(f"(Album) {artist} – {album_name} ({year}) notification sent.")
                return "Album notification was sent to telegram"

        if item_type == "Movie":
            logging.info(f"(Movie) {item_name} Notification Was Already Sent")
        elif item_type == "Season":
            logging.info(f"(Season) {series_name} {item_name} Notification Was Already Sent")
        elif item_type == "Episode":
            logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} Notification Was Already Sent")
        else:
            logging.error('Item type not supported')
        return "Item type not supported."

    # Handle specific HTTP errors
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        return str(http_err)

    # Handle generic exceptions
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return f"Error: {str(e)}"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
