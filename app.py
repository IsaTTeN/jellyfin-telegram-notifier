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
from datetime import datetime, timedelta

load_dotenv()
app = Flask(__name__)

# Set up logging
log_directory = '/app/log'
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
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/tv"
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
#выключить логику пропуска по датам
#DEBUG_DISABLE_DATE_CHECKS = True
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
notified_items_file = '/app/data/notified_items.json'

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

def get_tmdb_id(series_name: str, release_year: int) -> str:
    """
    Поиск сериала в TMDb и возврат первого найденного TV ID.
    Если ничего не найдено — возвращает "N/A".
    """
    params = {
        "api_key": TMDB_API_KEY,
        "query": series_name,
        "first_air_date_year": release_year,
        "language": "en-US",
        "page": 1
    }
    try:
        resp = requests.get(TMDB_SEARCH_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            logging.warning(f"TMDb: не найден сериал «{series_name} ({release_year})»")
            return "N/A"
        return str(results[0]["id"])
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе TMDb для «{series_name}»: {e}")
        return "N/A"

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

    # 1) [текст](url) -> url
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\s)]+)\)', r'\2', text)

    # 2) Убираем подряд идущие повторы одного и того же URL
    text = re.sub(r'(https?://\S+)(\s*\1)+', r'\1', text)

    # 3) Сначала убираем уже проставленные префиксы, чтобы не получить дубликаты,
    #    затем добавим их единообразно
    prefix_pattern = rf'🎥\s*{re.escape(trailer_label)}[:]?\s*'
    text = re.sub(rf'{prefix_pattern}(https?://\S+)', r'\1', text)

    # 4) Добавляем "🎥 <label>:" перед каждой ссылкой
    text = re.sub(r'(https?://\S+)', rf'🎥 {trailer_label}: \1', text)

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

    # 3) Добавляем 🎥 <перевод new_trailer> перед ссылкой (если ещё нет)
    pattern = rf'(?<!🎥 {re.escape(trailer_label)}\s)(https?://\S+)'
    replacement = rf'🎥 {trailer_label} \1'
    text = re.sub(pattern, replacement, text)

    # 4) Чистим лишние пробелы
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
            logging.warning("Notification failed via Telegram")
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

    # ======= WHATSAPP: отправляем картинку с подписью =======
    try:
        wa_jid = _wa_get_jid_from_env()
        if WHATSAPP_API_URL and wa_jid:
            # view_once, compress, duration, is_forwarded возьмутся из дефолтов
            send_whatsapp_image_via_rest(
                caption=caption,
                phone_jid=wa_jid,
                image_url=uploaded_url
            )
        else:
            logging.debug("WhatsApp disabled or no JID; skip image send.")
    except Exception as wa_ex:
        logging.warning(f"WhatsApp image send failed: {wa_ex}")

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


#def send_matrix_image_from_imgbb(photo_id: str, caption_markdown: str, uploaded_url: str | None = None):
#    """
#    Отправляет В MATRIX изображение, которое было загружено на imgbb:
#      1) ждём готовности imgbb (если uploaded_url не передан) -> берём HTTP-URL
#      2) скачиваем картинку с imgbb
#      3) загружаем в Matrix media-repo (получаем mxc://…)
#      4) отправляем m.image с подписью в body
#    Возвращает response или None.
#    """
#    # 1) Берём URL из imgbb
#    try:
#        img_http_url = uploaded_url or wait_for_imgbb_upload()
#        if not img_http_url:
#            logging.warning("Matrix image: imgbb URL is empty; skip.")
#            return None
#    except Exception as ex:
#        logging.warning(f"Matrix image: waiting imgbb failed: {ex}")
#        return None

    # 2) Скачиваем картинку с imgbb
#    try:
#        r = requests.get(img_http_url, timeout=30)
#        r.raise_for_status()
#        image_bytes = r.content
#        mimetype = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip().lower()
#        ext = ".jpg"
#        if "png" in mimetype: ext = ".png"
#        elif "webp" in mimetype: ext = ".webp"
#        filename = f"poster{ext}"
#    except Exception as ex:
#        logging.warning(f"Matrix image: cannot download from imgbb: {ex}")
#        return None

    # 3) Загружаем в Matrix → получаем mxc://
#    mxc = matrix_upload_image_rest(image_bytes, filename, mimetype)
#    if not mxc:
#        return None

    # 👇 ВАЖНО: body = ИМЯ ФАЙЛА, НЕ caption
#   content = {
#        "msgtype": "m.image",
#        "body": filename,     # <-- раньше здесь был caption; поменяли на имя файла
#        "url": mxc,
#        "info": {
#            "mimetype": mimetype,
#            "size": len(image_bytes),
#        },
#    }
#    return _matrix_send_event_rest(MATRIX_ROOM_ID, "m.room.message", content)


#def send_matrix_image_then_text_from_imgbb(photo_id: str, caption_markdown: str, uploaded_url: str | None = None) -> bool:
#    """
#    Сначала отправляет в Matrix изображение (берём именно то, что лежит на imgbb),
#    затем отдельным сообщением отправляет текст (используем send_matrix_text_rest).
#    """
#    img_ok = False
#    try:
#        r_img = send_matrix_image_from_imgbb(photo_id, caption_markdown, uploaded_url=uploaded_url)
#        if r_img and r_img.ok:
#            img_ok = True
#            logging.info("Matrix: image (from imgbb) sent successfully.")
#        else:
#            logging.warning("Matrix: image (from imgbb) failed to send.")
#    except Exception as ex:
#        logging.warning(f"Matrix: image-from-imgbb pipeline failed: {ex}")

#    txt_ok = False
#    try:
#        r_txt = send_matrix_text_rest(caption_markdown)
#        if r_txt and r_txt.ok:
#            txt_ok = True
#            logging.info("Matrix: text sent successfully after image.")
#        else:
#            logging.warning("Matrix: text failed to send after image.")
#    except Exception as ex:
#       logging.warning(f"Matrix: text pipeline failed: {ex}")

#    return img_ok and txt_ok

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
#    photo_id: str = None,   # теперь необязательный
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
    key = f"{item_type}:{item_name}:{release_year}"
    return key in notified_items


def mark_item_as_notified(item_type, item_name, release_year, max_entries=100):
    key = f"{item_type}:{item_name}:{release_year}"
    notified_items[key] = True

    # Check if the number of entries in notified_items exceeds the limit
    if len(notified_items) > max_entries:
        # Get a list of keys (notification identifiers) sorted by their insertion order (oldest first)
        keys_sorted_by_insertion_order = sorted(notified_items, key=notified_items.get)

        # Remove the oldest entry from the dictionary
        oldest_key = keys_sorted_by_insertion_order[0]
        del notified_items[oldest_key]
        logging.info(f"Key '{oldest_key}' has been deleted from notified_items")
    # Save the updated notified items to the JSON file
    save_notified_items(notified_items)


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
            if not item_already_notified(item_type, item_name, release_year):
                movie_id = payload.get("ItemId")
                overview = payload.get("Overview")
                runtime = payload.get("RunTime")
                # Remove release_year from movie_name if present
                movie_name = item_name
                movie_name_cleaned = movie_name.replace(f" ({release_year})", "").strip()

                trailer_url = get_youtube_trailer_url(f"{movie_name_cleaned} Trailer {release_year}")

                notification_message = (
                    f"*{t('new_movie_title')}*\n\n*{movie_name_cleaned}* *({release_year})*\n\n{overview}\n\n"
                    f"*{t('new_runtime')}*\n{runtime}")

                if tmdb_id:
                    # приводим тип к тому, что ждёт MDblist: movie или series
                    mdblist_type = item_type.lower()
                    ratings_text = fetch_mdblist_ratings(mdblist_type, tmdb_id)
                    if ratings_text:
                        notification_message += f"\n\n*{t('new_ratings_movie')}*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

#                notification_message_plain = clean_markdown_for_apprise(notification_message)

                send_notification(movie_id, notification_message)
                mark_item_as_notified(item_type, item_name, release_year)
                logging.info(f"(Movie) {movie_name} {release_year} "
                             f"notification was sent.")
                return "Movie notification was sent"

        if item_type == "Season":
            if not item_already_notified(item_type, item_name, release_year):
                season_id = payload.get("ItemId")
                season = item_name
                season_details = get_item_details(season_id)
                series_id = season_details["Items"][0].get("SeriesId")
                series_details = get_item_details(series_id)
                # Remove release_year from series_name if present
                series_name_cleaned = series_name.replace(f" ({release_year})", "").strip()

                trailer_url = get_youtube_trailer_url(f"{series_name_cleaned} Trailer {release_year}")

                # Get TMDb ID via external API
                tmdb_id = get_tmdb_id(series_name_cleaned, release_year)

                # **Новые строки**: получаем рейтинги для сериала
                ratings_text = fetch_mdblist_ratings("show", tmdb_id)
                # Если есть рейтинги — добавляем пустую строку после них
                ratings_section = f"{ratings_text}\n\n" if ratings_text else ""

                # Get series overview if season overview is empty
                overview_to_use = payload.get("Overview") if payload.get("Overview") else series_details["Items"][0].get(
                    "Overview")

                notification_message = (
                    f"*{t('new_season_title')}*\n\n*{series_name_cleaned}* *({release_year})*\n\n"
                    f"*{season}*\n\n{overview_to_use}")

                if ratings_text:
                    notification_message += f"\n\n*{t('new_ratings_show')}*\n{ratings_text}"

                if trailer_url:
                    notification_message += f"\n\n[🎥]({trailer_url})[{t('new_trailer')}]({trailer_url})"

                response = send_notification(season_id, notification_message)

                if response.status_code == 200:
                    mark_item_as_notified(item_type, item_name, release_year)
                    logging.info(f"(Season) {series_name_cleaned} {season} "
                                 f"notification was sent")
                    return "Season notification was sent"
                else:
                    send_notification(series_id, notification_message)
                    mark_item_as_notified(item_type, item_name, release_year)
                    logging.warning(f"{series_name_cleaned} {season} image does not exists, falling back to series image")
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
                    response = send_notification(season_id, notification_message)

                    if response.status_code == 200:
                        mark_item_as_notified(item_type, item_name, release_year)
                        logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} notification sent!")
                        return "Notification sent!"
                    else:
                        send_notification(series_id, notification_message)
                        logging.warning(f"(Episode) {series_name} season image does not exists, "
                                        f"falling back to series image")
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
            if not item_already_notified(item_type, item_name, release_year):
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

                # Отправляем обложку альбома, если есть, иначе ничего страшного
                response = send_notification(album_id, notification_message)

                # Фиксируем уведомление как отправленное
                mark_item_as_notified(item_type, item_name, release_year)

                if response.status_code == 200:
                    logging.info(f"(Album) {artist} – {album_name} ({year}) notification sent.")
                    return "Album notification was sent to telegram"
                else:
                    # можно при падении картинки просто залогировать и вернуть успех, чтобы не спамить
                    logging.warning(f"Album cover not found for {album_name}, sent text-only message.")
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
