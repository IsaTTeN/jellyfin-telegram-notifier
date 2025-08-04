import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta
import os
import json
import requests
import tempfile
import re
import base64
from requests.exceptions import HTTPError
from flask import Flask, request
from dotenv import load_dotenv
from apprise import Apprise

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
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GOTIFY_URL = os.environ.get("GOTIFY_URL", "")
GOTIFY_TOKEN = os.environ.get("GOTIFY_TOKEN", "")
JELLYFIN_BASE_URL = os.environ["JELLYFIN_BASE_URL"]
JELLYFIN_API_KEY = os.environ["JELLYFIN_API_KEY"]
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
MDBLIST_API_KEY = os.environ["MDBLIST_API_KEY"]
IMGBB_API_KEY = os.environ["IMGBB_API_KEY"]
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
TMDB_API_KEY = os.environ["TMDB_API_KEY"]
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/tv"
LANGUAGE = os.environ["LANGUAGE"]
EPISODE_PREMIERED_WITHIN_X_DAYS = int(os.environ["EPISODE_PREMIERED_WITHIN_X_DAYS"])
SEASON_ADDED_WITHIN_X_DAYS = int(os.environ["SEASON_ADDED_WITHIN_X_DAYS"])
#выключить логику пропуска по датам
#DEBUG_DISABLE_DATE_CHECKS = True

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
    Загружает изображение на imgbb.com и возвращает прямую ссылку на изображение.
    """
    url = "https://api.imgbb.com/1/upload"
    payload = {
        "key": IMGBB_API_KEY,
        "image": base64.b64encode(image_bytes).decode('utf-8')
    }
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        data = response.json()
        img_url = data['data']['url']
        logging.info(f"Изображение успешно загружено на imgbb: {img_url}")
        return img_url
    except Exception as ex:
        logging.warning(f"Ошибка загрузки на imgbb: {ex}")
        return None

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
    Отправляет уведомление (текст и картинку) в Discord через Webhook.
    Картинка — из imgbb, как и для Gotify.
    """
    if not DISCORD_WEBHOOK_URL:
        logging.warning("DISCORD_WEBHOOK_URL not set, skipping Discord notification.")
        return None

    if uploaded_url is None:
        uploaded_url = get_jellyfin_image_and_upload_imgbb(photo_id)

    data = {
        "username": title,
        "content": message
    }

    # Если есть картинка — добавляем embed с изображением
    if uploaded_url:
        data["embeds"] = [
            {
                "image": {"url": uploaded_url}
            }
        ]

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=data)
        response.raise_for_status()
        logging.info("Discord notification sent successfully")
        return response
    except Exception as ex:
        logging.warning(f"Error sending to Discord: {ex}")
        return None


def clean_markdown_for_apprise(text):
    """
    Упрощает markdown-подобные уведомления для plain text:
    - убирает *жирный* и _курсив_
    - превращает ссылки [название](url) в 'название: url'
    - убирает двойные/одиночные пробелы у краёв строк
    - оставляет эмодзи
    """
    # Преобразовать [текст](ссылка) в 'текст: ссылка'
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)', r'\1: \2', text)

    # Убрать *жирный* и _курсив_
    text = re.sub(r'(\*|_){1,3}(.+?)\1{1,3}', r'\2', text)

    # Убрать лишние пробелы по краям
    text = '\n'.join([line.strip() for line in text.split('\n')])

    # При желании можно убрать любые другие "технические" символы

    return text

def send_notification(photo_id, caption):
    uploaded_url = get_jellyfin_image_and_upload_imgbb(photo_id)
    """
    1. Всегда отправляет в Telegram напрямую (send_telegram_photo).
    2. Независимо — отправляет напрямую в Gotify (если включен).
    3. Остальные сервисы — через Apprise.
    """
    tg_response = send_telegram_photo(photo_id, caption)
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
    return tg_response

def send_telegram_photo(photo_id, caption):
    base_photo_url = f"{JELLYFIN_BASE_URL}/Items/{photo_id}/Images"
    primary_photo_url = f"{base_photo_url}/Primary"

    # Download the image from the jellyfin
    image_response = requests.get(primary_photo_url)

    # Upload the image to the Telegram bot
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "caption": caption,
        "parse_mode": "Markdown"
    }

    files = {'photo': ('photo.jpg', image_response.content, 'image/jpeg')}
    response = requests.post(url, data=data, files=files)
    return response

def send_gotify_message(photo_id, message, title="Jellyfin", priority=5, uploaded_url=None):
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
                    f"{t('new_runtime')}\n{runtime}")

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
                             f"notification was sent to telegram.")
                return "Movie notification was sent to telegram"

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
                                 f"notification was sent to telegram.")
                    return "Season notification was sent to telegram"
                else:
                    send_notification(series_id, notification_message)
                    mark_item_as_notified(item_type, item_name, release_year)
                    logging.warning(f"{series_name_cleaned} {season} image does not exists, falling back to series image")
                    logging.info(f"(Season) {series_name_cleaned} {season} notification was sent to telegram")
                    return "Season notification was sent to telegram"

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
                        logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} notification sent to Telegram!")
                        return "Notification sent to Telegram!"
                    else:
                        send_notification(series_id, notification_message)
                        logging.warning(f"(Episode) {series_name} season image does not exists, "
                                        f"falling back to series image")
                        mark_item_as_notified(item_type, item_name, release_year)
                        logging.info(f"(Episode) {series_name} S{season_num}E{season_epi} notification sent to Telegram!")
                        return "Notification sent to Telegram!"

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
                    "*{t('new_album_title')}*\n\n"
                    f"*{artist}*\n\n"
                    f"*{album_name} ({year})*\n\n"
                    f"{overview and overview + '\n\n' or ''}"
                    f"{t('new_runtime')}\n{runtime}\n\n"
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
