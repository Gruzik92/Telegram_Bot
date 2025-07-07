
import logging
import os
import telebot
from flask import Flask, request, abort
import psycopg2
from datetime import datetime, timedelta, date as dt_date
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import io
import threading
import schedule
import time
import openai
import json
import urllib.parse
import re
import sys
import random

# --- ПОЧАТКОВЕ НАЛАШТУВАННЯ ЛОГУВАННЯ ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set specific log levels for telebot components
telebot.logger.setLevel(logging.INFO)
logging.getLogger('telebot.dispatcher').setLevel(logging.INFO)
logging.getLogger('telebot.handler_backends').setLevel(logging.INFO)
logging.getLogger('telebot.apihelper').setLevel(logging.INFO)

print(f"[{datetime.now()}] !!! APPLICATION STARTUP - main.py initialized !!!")

# === Configuration Constants ===
class Config:
    """Class to hold and validate application configuration."""
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
    WEBHOOK_BASE_URL = os.environ.get("WEBHOOK_BASE_URL")
    WEBHOOK_PATH = "/webhook"
    # Default GROUP_REPORT_CHAT_ID for reports, can be overridden by environment variable
    # NOTE: This should be your GROUP CHAT ID, typically a negative number like -100XXXXXXXXXX
    GROUP_REPORT_CHAT_ID = int(os.environ.get("CHAT_ID")) if os.environ.get("CHAT_ID") else -12345678910 #
    # OWNER_TELEGRAM_USER_ID: This is your personal Telegram user ID (the one you used in the logs)
    OWNER_TELEGRAM_USER_ID = 12345678 # Your personal user ID, as per logs
    RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")
    RAPIDAPI_HOST = os.environ.get("RAPIDAPI_HOST")
    DATABASE_URL = os.environ.get("DATABASE_URL")

    @classmethod
    def validate(cls):
        """Validates essential environment variables."""
        if not cls.TELEGRAM_BOT_TOKEN:
            logging.critical(f"[{datetime.now()}] CRITICAL: TELEGRAM_BOT_TOKEN is missing or empty. Bot will not work. Exiting.")
            sys.exit(1)
        if not cls.DATABASE_URL:
            logging.critical(f"[{datetime.now()}] CRITICAL: DATABASE_URL environment variable is not set. Cannot connect to DB. Exiting.")
            sys.exit(1)

# Validate configuration at startup
Config.validate()


# --- Global Instances (initialized once) ---
bot = telebot.TeleBot(Config.TELEGRAM_BOT_TOKEN)
openai_client = openai.OpenAI(api_key=Config.OPENAI_API_KEY)
# Global variable for database connection, managed by DatabaseManager
db_connection_global = None

# --- Idempotency Cache for Webhook Updates ---
processed_updates = {}
IDEMPOTENCY_WINDOW_SECONDS = 60 # Time-to-live for cache entries in seconds

def clean_processed_updates_cache():
    """Removes old entries from the processed updates cache."""
    now = time.time()
    keys_to_delete = [
        update_id for update_id, timestamp in processed_updates.items()
        if now - timestamp > IDEMPOTENCY_WINDOW_SECONDS
    ]
    for update_id in keys_to_delete:
        del processed_updates[update_id]
        logging.debug(f"[{datetime.now()}] Idempotency: Cleaned old entry for update_id {update_id}.")

# Function to run the cache cleaner in a loop
def run_cleaner_job():
    while True:
        clean_processed_updates_cache()
        time.sleep(IDEMPOTENCY_WINDOW_SECONDS / 2) # Run twice as often as the window size, e.g., every 30 seconds for a 60-second window

# Start cache cleaning in a separate thread
cleaner_thread = threading.Thread(target=run_cleaner_job)
cleaner_thread.daemon = True # Allow the main program to exit even if this thread is still running
cleaner_thread.start()
logging.info(f"[{datetime.now()}] Idempotency: Cache cleaner thread started.")


# === Utility Functions ===
def escape_markdown_v2(text):
    """
    Escapes characters that have special meaning in MarkdownV2.
    This function is used for *dynamic content* that should NOT contain Markdown formatting.
    https://core.telegram.org/bots/api#markdownv2-style
    """
    if not isinstance(text, str):
        return str(text)

    # Characters that need to be escaped in MarkdownV2
    # Note: '*' is included here because this function is specifically for *content*,
    # not for applying formatting. Formatting characters like '**' for bold must be
    # manually placed in the f-string and not passed through this function if they
    # are intended as formatting.
    chars_to_escape = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']

    escaped_text = text
    for char in chars_to_escape:
        escaped_text = escaped_text.replace(char, f"\\{char}")
    return escaped_text

# Regular expressions for swear words
SWEAR_WORDS_REGEX_PATTERNS = [
    r'\bбл[яя]ть\b', r'\bху[ййиюяе]\b', r'\bп[іие]зд[ауеоіиь]\b',
    r'\b[їие]б[аеи][тть]\b', r'\bсук[ауоие]\b', r'\bнаху[йяею]\b',
    r'\bдрищ[іауое]?\b', r'\bкурв[ауоие]\b', r'\bг[іи]мн[оауе]\b',
    r'\bлайн[оауое]\b', r'\bпадл[оауе]\b', r'\bганд[оо]н[ауоие]?\b',
    r'\bмуд[аа]к[ауоие]?\b', r'\bвирод[оо]к[ауоие]?\b', r'\bсвол[оо]т[ауоие]\b',
    r'\bгнид[ауоие]\b', r'\b[іие]д[іи][оо]т[ауоие]?\b', r'\bбовдур[ауоие]?\b',
    r'\bп[іи]дор[ауоие]?\b', r'\bху[ййи]л[оауе]\b', r'\bпут[іи]н[ауоие]?\b',
    r'\bп[ее][тту]ш[аа]р[уоие]\b', r'\bху[ййи]н[яяею]\b', r'\bд[ии][бб][іи]л[ауоие]?\b',
    r'\bдаун[ауоие]?\b', r'\b[їие]бал[оауое]\b', r'\bза[їие]б[аа]в[ауоие]?\b',
    r'\bза[їие]бал[оауое]\b', r'\bп[іи]зд[ее]ць\b', r'\bєб[аи][тть]\b',
    r'\bйо[ба]ний?\b', r'\bтрах[аеи][тть]\b', r'\bшмар[ауоие]\b',
    r'\bдристун[ауоие]?\b', r'\bчмо[шн]?[ик]?\b', r'\bлох[ауоие]?\b',
    r'\bмуд[іи]л[оауое]\b', r'\bгандош[ауоие]\b', r'\bхер[ауоие]?\b',
    r'\b[ауоие]ху[ййиюяе]\b', r'\b[їие]бан[ауоие][ауоие]?\b',
    r'\b[їие]буч[ийаео]\b', r'\bбляха\b'
]


# === Database Manager Class ===
class DatabaseManager:
    def __init__(self, database_url):
        self.database_url = database_url
        self._connection = None # Internal connection state

    def _get_connection(self, max_retries=5, retry_delay_seconds=5):
        """Establishes and returns a new database connection with retries."""
        # Use existing connection if healthy and not closed
        if self._connection and not self._connection.closed:
            try:
                cursor = self._connection.cursor()
                cursor.execute("SELECT 1") # Simple query to check connection health
                cursor.close()
                logging.debug(f"[{datetime.now()}] DB: Reusing existing healthy database connection.")
                return self._connection
            except psycopg2.OperationalError as e:
                logging.warning(f"[{datetime.now()}] DB: Existing connection stale or closed ({e}). Attempting reconnect.")
                self._connection = None # Invalidate stale connection

        url = urllib.parse.urlparse(self.database_url)
        conn_params = {
            "host": url.hostname,
            "database": url.path[1:],
            "user": url.username,
            "password": url.password,
            "port": url.port if url.port else 5432
        }
        query_params = urllib.parse.parse_qs(url.query)
        for key, value in query_params.items():
            conn_params[key] = value[0]

        for attempt in range(1, max_retries + 1):
            logging.info(f"[{datetime.now()}] DB: Attempting database connection (Attempt {attempt}/{max_retries})... Host: {conn_params.get('host')}, DB: {conn_params.get('database')}")
            try:
                conn = psycopg2.connect(**conn_params)
                conn.autocommit = True
                logging.info(f"[{datetime.now()}] DB: Successfully connected to database on attempt {attempt}.")
                self._connection = conn # Store the healthy connection
                return conn
            except psycopg2.OperationalError as e:
                logging.error(f"[{datetime.now()}] DB: Database connection error on attempt {attempt}: {e}", exc_info=False)
                if "could not translate host name" in str(e):
                    logging.error(f"[{datetime.now()}] DB: DNS resolution for database host failed. Check hostname and network access.")
                if attempt < max_retries:
                    logging.info(f"[{datetime.now()}] DB: Retrying connection in {retry_delay_seconds} seconds...")
                    time.sleep(retry_delay_seconds)
                else:
                    logging.critical(f"[{datetime.now()}] DB: Failed to connect to database after {max_retries} attempts.")
                    self._connection = None
                    return None
            except Exception as e:
                logging.critical(f"[{datetime.now()}] DB: Unexpected error during database connection on attempt {attempt}: {e}", exc_info=True)
                self._connection = None
                return None
        return None

    def _create_messages_table(self, cursor):
        """Creates or updates the messages table."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                telegram_message_id BIGINT UNIQUE,
                user_id BIGINT NOT NULL,
                username VARCHAR(255),
                message TEXT,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                is_bot BOOLEAN DEFAULT FALSE,
                chat_id BIGINT,
                bot_message_type TEXT
            );
        """)
        # Add columns if they don't exist
        try:
            cursor.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS telegram_message_id BIGINT;")
            cursor.execute("ALTER TABLE messages ADD CONSTRAINT unique_telegram_message_id UNIQUE (telegram_message_id);")
            logging.info("[DBManager] Додано UNIQUE Constraint на 'telegram_message_id'.")
        except psycopg2.ProgrammingError as e:
            if "already exists" in str(e) or "could not create unique index" in str(e):
                logging.warning(f"[DBManager] UNIQUE Constraint на 'telegram_message_id' вже існує або не може бути створений через дублікати: {e}. Пропускаємо додавання.")
                self._connection.rollback()
            else:
                raise
        logging.info("[DBManager] Додано колонку 'telegram_message_id' до таблиці 'messages'.")

        cursor.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_bot BOOLEAN DEFAULT FALSE;")
        cursor.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS chat_id BIGINT;")
        try:
            cursor.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS bot_message_type TEXT;")
            logging.info("[DBManager] Додано колонку 'bot_message_type' до таблиці 'messages'.")
        except psycopg2.ProgrammingError as e:
            if "column \"bot_message_type\" already exists" in str(e):
                logging.info("[DBManager] Колонка 'bot_message_type' вже існує в таблиці 'messages'. Пропускаємо додавання.")
                self._connection.rollback()
            else:
                raise

    def _create_swear_counts_table(self, cursor):
        """Creates the swear_counts table."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS swear_counts (
                chat_id BIGINT NOT NULL,
                swear_date DATE NOT NULL,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, swear_date)
            );
        """)

    def _create_scheduled_announcements_table(self, cursor):
        """Creates the scheduled_announcements table."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_announcements (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                message_text TEXT NOT NULL,
                schedule_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
                sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def _create_scheduled_job_executions_table(self, cursor):
        """Creates the scheduled_job_executions_v2 table."""
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scheduled_job_executions_v2 (
                    job_name VARCHAR(255) NOT NULL,
                    execution_date DATE NOT NULL,
                    execution_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (job_name, execution_date)
                );
            """)
        except psycopg2.errors.UniqueViolation as e:
            logging.warning(f"[{datetime.now()}] DB: Suppressed (benign) UniqueViolation during scheduled_job_executions_v2 creation: {e}")
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error during scheduled_job_executions_v2 creation: {e}", exc_info=True)
            raise

    def create_tables(self):
        """
        Creates all necessary tables if they don't exist and adds missing columns.
        """
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if conn:
                cursor = conn.cursor()
                self._create_messages_table(cursor)
                self._create_swear_counts_table(cursor)
                self._create_scheduled_announcements_table(cursor)
                self._create_scheduled_job_executions_table(cursor)
                conn.commit()
                logging.info(f"[{datetime.now()}] DB: Tables 'messages', 'swear_counts', 'scheduled_announcements', 'scheduled_job_executions_v2' checked/created/updated successfully.")
            else:
                logging.warning(f"[{datetime.now()}] DB: Could not get DB connection to create/update tables. Database functionality will be limited.")
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Error creating/updating tables: {e}", exc_info=True)
        finally:
            if cursor:
                cursor.close()

    def save_message(self, telegram_message_id, user_id, username, message_content, message_date, chat_id_to_save, is_bot_message=False, bot_message_type=None):
        """
        Saves message information to the database.
        Now uses 'telegram_message_id' for mapping to Telegram messages.
        """
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: save_message did not get DB connection. Message not saved.")
            return

        cur = None
        try:
            cur = conn.cursor()
            message_content_str = str(message_content) if message_content is not None else 'No content'

            logging.info(f"[{datetime.now()}] DB: Attempting to save message (Bot: {is_bot_message}, Type: {bot_message_type}) from User ID: {user_id}, Username: {username}, Chat ID: {chat_id_to_save}, Text: '{message_content_str[:50]}'")

            cur.execute(
                """INSERT INTO messages (telegram_message_id, user_id, username, message, timestamp, is_bot, chat_id, bot_message_type)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (telegram_message_id) DO UPDATE SET
                       user_id = EXCLUDED.user_id,
                       username = EXCLUDED.username,
                       message = EXCLUDED.message,
                       timestamp = EXCLUDED.timestamp,
                       is_bot = EXCLUDED.is_bot,
                       chat_id = EXCLUDED.chat_id,
                       bot_message_type = EXCLUDED.bot_message_type;""",
                (telegram_message_id, user_id, username, message_content_str, message_date, is_bot_message, chat_id_to_save, bot_message_type)
            )
            conn.commit()
            logging.info(f"[{datetime.now()}] DB: Message from User ID: {user_id} (Bot: {is_bot_message}, Type: {bot_message_type}) successfully saved to DB (Telegram ID: {telegram_message_id}).")

        except psycopg2.errors.UniqueViolation as e:
            logging.warning(f"[{datetime.now()}] DB: UniqueViolation (duplicate telegram_message_id) suppressed in save_message: {e}. Message Telegram ID: {telegram_message_id}")
            if conn: conn.rollback()
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error saving message to DB (psycopg2): {e}", exc_info=True)
            if conn: conn.rollback()
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error in save_message: {e}", exc_info=True)
            if conn: conn.rollback()
        finally:
            if cur: cur.close()

    def get_message_by_id(self, telegram_message_id):
        """Retrieves a message by its Telegram message_id, including bot_message_type."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to retrieve message by Telegram ID.")
            return None

        cur = None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, user_id, username, message, timestamp, is_bot, chat_id, bot_message_type, telegram_message_id
                FROM messages
                WHERE telegram_message_id = %s;
            """, (telegram_message_id,))
            row = cur.fetchone()
            if row:
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))
            return None
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error retrieving message by Telegram ID {telegram_message_id}: {e}", exc_info=True)
            return None
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error retrieving message by Telegram ID {telegram_message_id}: {e}", exc_info=True)
            return None
        finally:
            if cur: cur.close()

    def increment_swear_count(self, chat_id, current_date, increment_by=1):
        """Increments the swear count for a given chat and date, returns new count."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to update swear count.")
            return 0

        cur = None
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO swear_counts (chat_id, swear_date, count)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id, swear_date) DO UPDATE SET count = swear_counts.count + %s
                RETURNING count;
            """, (chat_id, current_date, increment_by, increment_by))
            updated_count = cur.fetchone()[0]
            conn.commit()
            logging.info(f"[{datetime.now()}] DB: Swear count for chat {chat_id} on {current_date} incremented by {increment_by} to {updated_count}.")
            return updated_count
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error updating swear count: {e}", exc_info=True)
            return 0
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error in increment_swear_count: {e}", exc_info=True)
            return 0
        finally:
            if cur: cur.close()

    def get_swear_count(self, chat_id, current_date):
        """Returns the current swear count for a given chat and date."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to get swear count.")
            return 0

        cur = None
        try:
            cur = conn.cursor()
            cur.execute("SELECT count FROM swear_counts WHERE chat_id = %s AND swear_date = %s", (chat_id, current_date))
            result = cur.fetchone()
            logging.info(f"[{datetime.now()}] DB: Swear count for chat {chat_id} on {current_date} retrieved.")
            return result[0] if result else 0
        finally:
            if cur: cur.close()

    def add_scheduled_announcement(self, chat_id, message_text, schedule_time_str):
        """Adds an announcement to the database for future scheduled sending."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to add announcement.")
            return "Failed to connect to the database."

        cur = None
        try:
            cur = conn.cursor()
            try:
                hour, minute = map(int, schedule_time_str.split(':'))
                if not (0 <= hour <= 23 and 0 <= minute <= 59):
                    return "Incorrect time format. Use HH:MM (e.g., 18:00)."
            except ValueError:
                return "Incorrect time format. Use HH:MM (e.g., 18:00)."

            now_utc = datetime.utcnow()
            schedule_datetime_utc = datetime(now_utc.year, now_utc.month, now_utc.day, hour, minute, 0)
            if schedule_datetime_utc <= now_utc:
                schedule_datetime_utc += timedelta(days=1)

            # Екрануємо текст анонсу ПЕРЕД збереженням у БД, щоб він був безпечним для MarkdownV2 при відправці.
            escaped_message_text_for_db = escape_markdown_v2(message_text)

            cur.execute("""
                INSERT INTO scheduled_announcements (chat_id, message_text, schedule_datetime, sent)
                VALUES (%s, %s, %s, FALSE) RETURNING id;
            """, (chat_id, escaped_message_text_for_db, schedule_datetime_utc))
            new_id = cur.fetchone()[0]
            conn.commit()
            logging.info(f"[{datetime.now()}] DB: Announcement ID {new_id} scheduled for chat {chat_id} at {schedule_datetime_utc.strftime('%d.%m.%Y at %H:%M UTC')}.")
            return (
                f"Анонс заплановано на **{escape_markdown_v2(schedule_datetime_utc.strftime('%d.%m.%Y'))}** о "
                f"**{escape_markdown_v2(schedule_datetime_utc.strftime('%H:%M UTC'))}**\\. ID анонсу\\: `{escape_markdown_v2(str(new_id))}`"
            )
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Database error adding announcement: {e}", exc_info=True)
            return f"Помилка бази даних при плануванні анонсу: {escape_markdown_v2(str(e))}"
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Несподівана помилка при плануванні анонсу: {e}", exc_info=True)
            return f"Несподівана помилка при плануванні анонсу: {escape_markdown_v2(str(e))}"
        finally:
            if cur: cur.close()

    def get_messages_for_summary(self):
        """Retrieves messages for daily summary."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to get messages for summary.")
            return []

        cur = None
        try:
            cur = conn.cursor()
            today = datetime.utcnow().date()
            tomorrow = today + timedelta(days=1)
            cur.execute("""
                SELECT username, message FROM messages
                WHERE timestamp >= %s AND timestamp < %s AND is_bot = FALSE AND message IS NOT NULL
                ORDER BY timestamp ASC;
            """, (today, tomorrow))
            rows = cur.fetchall()
            logging.info(f"[{datetime.now()}] DB: Retrieved {len(rows)} messages for summary.")
            return rows
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error getting messages for summary: {e}", exc_info=True)
            return []
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error in get_messages_for_summary: {e}", exc_info=True)
            return []
        finally:
            if cur: cur.close()

    def get_recent_messages_for_context(self, chat_id, limit=10):
        """
        Retrieves recent messages for conversation context.
        For bot messages (is_bot=True), it will only return the message content, not "bot_username: message".
        """
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to get recent messages for context.")
            return []

        cur = None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT username, message, is_bot
                FROM messages
                WHERE chat_id = %s AND message IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT %s;
            """, (chat_id, limit))
            rows = cur.fetchall()
            logging.info(f"[{datetime.now()}] DB: Retrieved {len(rows)} recent messages for context.")

            formatted_history = []
            for username, message, is_bot_flag in reversed(rows): # Process in chronological order
                if message is None:
                    continue

                if is_bot_flag:
                    formatted_history.append({"role": "assistant", "content": message})
                else:
                    formatted_history.append({"role": "user", "content": f"{username if username else 'Unknown user'}: {message}"})

            return formatted_history
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error getting recent messages for context: {e}", exc_info=True)
            return []
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error in get_recent_messages_for_context: {e}", exc_info=True)
            return []
        finally:
            if cur: cur.close()

    def get_daily_stats(self):
        """
        Retrieves daily message statistics for the report,
        separating user messages from bot messages.
        """
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to get daily stats.")
            return 0, [], 0

        cur = None
        try:
            cur = conn.cursor()
            today = datetime.utcnow().date()
            tomorrow = today + timedelta(days=1)

            cur.execute("SELECT COUNT(*) FROM messages WHERE timestamp >= %s AND timestamp < %s", (today, tomorrow))
            total_messages = cur.fetchone()[0]

            cur.execute("""
                SELECT username, COUNT(*) FROM messages
                WHERE timestamp >= %s AND timestamp < %s AND is_bot = FALSE
                GROUP BY username ORDER BY COUNT(*) DESC LIMIT 5
            """, (today, tomorrow))
            top_users = cur.fetchall()

            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE timestamp >= %s AND timestamp < %s AND is_bot = TRUE
            """, (today, tomorrow))
            bot_messages_count = cur.fetchone()[0]

            return total_messages, top_users, bot_messages_count
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error getting daily stats: {e}", exc_info=True)
            return 0, [], 0
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error in get_daily_stats: {e}", exc_info=True)
            return 0, [], 0
        finally:
            if cur: cur.close()

    def get_all_texts_for_wordcloud(self):
        """Retrieves all message texts for word cloud generation, excluding bot messages, links, and the word 'content'."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to get texts for wordcloud.")
            return []

        cur = None
        try:
            cur = conn.cursor()
            today = datetime.utcnow().date()
            tomorrow = today + timedelta(days=1)
            cur.execute("SELECT message FROM messages WHERE timestamp >= %s AND timestamp < %s AND is_bot = FALSE", (today, tomorrow))

            filtered_texts = []
            for row in cur.fetchall():
                message = row[0]
                if message:
                    if re.search(r'https?://\S+', message, re.IGNORECASE):
                        continue

                    cleaned_message = re.sub(r'\bcontent\b', '', message, flags=re.IGNORECASE).strip()

                    if cleaned_message and len(cleaned_message) >= 4:
                        filtered_texts.append(cleaned_message)

            return filtered_texts
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error getting texts for wordcloud: {e}", exc_info=True)
            return []
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error in get_all_texts_for_wordcloud: {e}", exc_info=True)
            return []
        finally:
            if cur: cur.close()

    def get_scheduled_announcements_to_send(self):
        """Retrieves scheduled announcements that are due."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to get scheduled announcements.")
            return []

        cur = None
        try:
            cur = conn.cursor()
            now_utc = datetime.utcnow()
            cur.execute("""
                SELECT id, chat_id, message_text FROM scheduled_announcements
                WHERE schedule_datetime <= %s AND sent = FALSE;
            """, (now_utc,))
            announcements = cur.fetchall()
            return announcements
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error getting scheduled announcements: {e}", exc_info=True)
            return []
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error in get_scheduled_announcements_to_send: {e}", exc_info=True)
            return []
        finally:
            if cur: cur.close()

    def mark_announcement_sent(self, ann_id):
        """Marks a scheduled announcement as sent."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to mark announcement sent.")
            return

        cur = None
        try:
            cur = conn.cursor()
            cur.execute("UPDATE scheduled_announcements SET sent = TRUE WHERE id = %s", (ann_id,))
            conn.commit()
            logging.info(f"[{datetime.now()}] DB: Announcement ID {ann_id} marked as sent.")
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error marking announcement {ann_id} as sent: {e}", exc_info=True)
            if conn: conn.rollback()
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error marking announcement {ann_id} sent: {e}", exc_info=True)
            if conn: conn.rollback()
        finally:
            if cur: cur.close()

    def has_job_executed_today(self, job_name_base, current_date, slot=None):
        """
        Checks if a scheduled job has already been executed for the given date and optional slot.
        Combines job_name_base and slot to form a unique job_name.
        """
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to check job execution status for {job_name_base}.")
            return False

        cur = None
        try:
            job_name = f"{job_name_base}_{slot}" if slot else job_name_base

            cur = conn.cursor()
            cur.execute("""
                SELECT 1 FROM scheduled_job_executions_v2
                WHERE job_name = %s AND execution_date = %s;
            """, (job_name, current_date))
            result = cur.fetchone()
            if result:
                logging.info(f"[{datetime.now()}] DB: Scheduled job '{job_name}' already executed for {current_date}.")
                return True
            return False
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error checking job execution status for {job_name_base} (slot: {slot}): {e}", exc_info=True)
            return False
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error checking job execution status for {job_name_base} (slot: {slot}): {e}", exc_info=True)
            return False
        finally:
            if cur: cur.close()

    def record_job_execution(self, job_name_base, execution_date, slot=None):
        """
        Records that a scheduled job has been executed for the given date and optional slot.
        Combines job_name_base and slot to form a unique job_name.
        Returns True if the execution was recorded, False if it was already recorded.
        """
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to record job execution for {job_name_base}.")
            return False

        cur = None
        try:
            cur = conn.cursor()
            job_name = f"{job_name_base}_{slot}" if slot else job_name_base

            cur.execute("""
                INSERT INTO scheduled_job_executions_v2 (job_name, execution_date)
                VALUES (%s, %s)
                ON CONFLICT (job_name, execution_date) DO NOTHING;
            """, (job_name, execution_date))
            conn.commit()
            if cur.rowcount > 0:
                logging.info(f"[{datetime.now()}] DB: Recorded execution for scheduled job '{job_name}' on {execution_date}.")
                return True
            else:
                logging.info(f"[{datetime.now()}] DB: Execution for scheduled job '{job_name}' on {execution_date} already recorded (duplicate attempt).")
                return False
        except psycopg2.Error as e:
            logging.error(f"[{datetime.now()}] DB: Error recording job execution for {job_name_base} (slot: {slot}): {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        except Exception as e:
            logging.error(f"[{datetime.now()}] DB: Unexpected error recording job execution for {job_name_base} (slot: {slot}): {e}", exc_info=True)
            if conn: conn.rollback()
            return False
        finally:
            if cur: cur.close()

    def table_exists(self, table_name):
        """Checks if a given table exists in the database."""
        conn = self._get_connection()
        if not conn:
            logging.warning(f"[{datetime.now()}] DB: No connection to check for table '{table_name}'.")
            return False

        cur = None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                );
            """, (table_name,))
            exists = cur.fetchone()[0]
            logging.info(f"[{datetime.now()}] DB: Table '{table_name}' existence check: {exists}.")
            return exists
        finally:
            if cur: cur.close()


# Instantiate DatabaseManager
db_manager = DatabaseManager(Config.DATABASE_URL)


# === OpenAI Service Class ===
class OpenAIService:
    def __init__(self, client):
        self.client = client
        self.expert_roles = [
            "Ти — шановний історик-дослідник, що мандрує крізь часи, щоб розкрити правду.",
            "Мої думки ширять у завтрашньому дні. Я — футуролог, що бачить можливі шляхи майбутнього.",
            "Сидячи на вершині гори знань, я споглядаю глибини буття. Я — філософ-самітник.",
            "Справа заплутана, але жодна загадка не вистоїть проти гострого розуму. Я — детектив-логік.",
            "Мої слова — це барвисті нитки, що тчуть гобелен уяви. Я — поет-мрійник.",
            "Через роки досвіду я накопичив мудрість. Я тут, щоб поділитися знаннями та допомогти вам знайти свій шлях. Я — наставник-мудрець.",
            "Життя коротке, тож давайте посміхатися! Я — гуморист-оптиміст.",
            "Моя лабораторія — весь світ, а мої інструменти — знання. Я — науковець-експериментатор.",
            "Світ — це чисте полотно, а я — художник, що розфарбовує його уявою. Я — митець-фантазер.",
            "Я слухаю серцем і розумію душевні глибини. Я — психолог-емпат."
        ]

        self.translator_system_prompt = lambda lang: f"""
Ти є високоякісним перекладачем. Переклади наданий текст на {lang} мову.

Якщо наданий текст є анекдотом або містить гумор, то головна мета — щоб переклад був смішним і зрозумілим для аудиторії, яка розмовляє цією мовою. У такому випадку, адаптуй гру слів, культурні посилання або контекст, щоб гумор не втрачався.

Якщо текст не є гумористичним (наприклад, факт, новина, загальний текст), переклади його точно та природно, зберігаючи оригінальний зміст та стиль. Не додавай гумору там, де його немає.
"""
        self.random_fact_prompt = "Потрібен цікавий випадковий факт (1-2 речення) з будь-якої області: життя, спорт, наука, історія, мистецтво тощо"
        self.ukrainian_history_fact_prompt = "Згенеруй короткий (2-3 речення) цікавий історичний факт з історії будь-якої країни світу. Перевіряй додатково його на достовірність та правдивість"


    def _get_summary_system_prompt(self, role_prompt):
        """Generates the system prompt for summary based on a given role."""
        return f"""
{role_prompt}

Твоя задача — створювати стислі, інформативні та об'єктивні огляди групових чатів.
Огляд повинен:
- Виділяти 3-6 НАЙВАЖЛИВІШИХ тим або подій, що обговорювались.
- Для кожної теми: коротко описати суть обговорення та ключові рішення/висновки, якщо такі були.
- Ігнорувати привітання, флуд, меми та несуттєві повідомлення.
- Бути викладеним у формі коротких, чітких булетів або нумерованого списку.
- Загальний обсяг огляду не повинен перевищувати 300 токенів.
- Зосереджуйся на фактах та основній інформації. Обов'язково згадуй username або імена користувачів, які брали участь в обговоренні ключових тем, використовуючи їхні username або імена.
- Дій відповідно до своєї ролі, формуючи огляд чату. Додай до огляду легкий тон, що відповідає твоїй обраній ролі, але зберігай професіоналізм та об'єктивність.
"""

    def generate_summary(self, messages_data):
        """Generates a summary from a list of messages using OpenAI."""
        if not messages_data:
            return "No messages available for summary."

        random_role_for_summary = random.choice(self.expert_roles)
        summary_system_prompt = self._get_summary_system_prompt(random_role_for_summary)

        messages_for_openai = [{"role": "system", "content": summary_system_prompt}]
        for user, msg in messages_data:
            messages_for_openai.append({"role": "user", "content": f"{user if user else 'Unknown user'}: {msg}"})

        logging.info(f"[{datetime.now()}] OpenAI: Sending request for summary with role: {random_role_for_summary}...")
        try:
            response = self.client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=messages_for_openai,
                max_tokens=300,
                temperature=0.8
            )
            summary = response.choices[0].message.content
            logging.info(f"[{datetime.now()}] OpenAI: Summary successfully generated.")
            return summary
        except openai.APIError as e:
            logging.error(f"[{datetime.now()}] OpenAI: API Error during summary generation: {e}", exc_info=True)
            return f"Error generating summary: {e}"
        except Exception as e:
            logging.error(f"[{datetime.now()}] OpenAI: Unexpected error during summary generation: {e}", exc_info=True)
            return f"Unexpected error creating summary: {e}"

    def get_expert_answer(self, chat_id, current_query_text):
        """Generates an expert answer with conversation context using OpenAI, with a random role."""
        logging.info(f"[{datetime.now()}] OpenAI: Generating expert answer for chat {chat_id}: '{current_query_text[:50]}...'")

        random_role_prompt = random.choice(self.expert_roles)

        dynamic_expert_system_prompt = f"""
{random_role_prompt}

Твоя відповідь має бути короткою і лаконічною, не перевищуючи 180 токенів.
Завжди давай фактично коректну інформацію. Додай у відповідь тон, який відповідає обраній ролі.
Будь ласка, НЕ починай відповідь з фраз типу "Ось експертна думка з цього питання:", "Моя думка:", "Відповідь:" тощо. Одразу переходь до суті питання.
"""
        dynamic_expert_system_prompt = dynamic_expert_system_prompt.strip()

        raw_history = db_manager.get_recent_messages_for_context(chat_id, limit=10)
        messages_for_openai = [{"role": "system", "content": dynamic_expert_system_prompt}]
        for msg_entry in raw_history:
            messages_for_openai.append({"role": msg_entry["role"], "content": msg_entry["content"]})


        messages_for_openai.append({"role": "user", "content": current_query_text})

        try:
            response = self.client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=messages_for_openai,
                max_tokens=180,
                temperature=0.8
            )
            expert_answer = response.choices[0].message.content
            logging.info(f"[{datetime.now()}] OpenAI: Expert answer successfully generated.")
            return expert_answer
        except openai.APIError as e:
            logging.error(f"[{datetime.now()}] OpenAI: API Error during expert answer generation: {e}", exc_info=True)
            return f"Expert on break. Questions too complex. Reason: {e}. Try simplifying, if you can."
        except Exception as e:
            logging.error(f"[{datetime.now()}] OpenAI: Unexpected error during expert answer generation: {e}", exc_info=True)
            return f"Something went wrong getting expert opinion. Perhaps your question was too silly for me. Reason: {e}."

    def translate_text(self, text, target_language="українську"):
        """Translates text to the specified language using OpenAI API."""
        logging.info(f"[{datetime.now()}] OpenAI: Attempting to translate text: '{text[:50]}...' to {target_language}")
        try:
            response = self.client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[
                    {"role": "system", "content": self.translator_system_prompt(target_language)},
                    {"role": "user", "content": text}
                ],
                max_tokens=500
            )
            translated_text = response.choices[0].message.content
            logging.info(f"[{datetime.now()}] OpenAI: Text successfully translated.")
            return translated_text
        except openai.APIError as e:
            logging.error(f"[{datetime.now()}] OpenAI: API Error during translation: {e}", exc_info=True)
            return f"Failed to translate text due to an error: {e}"
        except Exception as e:
            logging.error(f"[{datetime.now()}] OpenAI: Unexpected error during text translation: {e}", exc_info=True)
            return f"Несподівана помилка при перекладі: {e}"

    def generate_random_fact(self):
        """Generates a random interesting fact using OpenAI."""
        logging.info(f"[{datetime.now()}] OpenAI: Generating random fact from various fields...")
        try:
            response = self.client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[
                    {"role": "system", "content": self.random_fact_prompt},
                    {"role": "user", "content": "Надай мені один цікавий випадковий факт (1-2 речення) з будь-якої області: життя, спорт, наука, історія, мистецтво тощо"}
                ],
                max_tokens=100,
                temperature=0.9
            )
            fact = response.choices[0].message.content
            logging.info(f"[{datetime.now()}] OpenAI: Random fact successfully generated.")
            return fact
        except openai.APIError as e:
            logging.error(f"[{datetime.now()}] OpenAI: API Error during random fact generation: {e}", exc_info=True)
            return f"Вибачте, не вдалося згенерувати цікавий факт: {e}"
        except Exception as e:
            logging.error(f"[{datetime.now()}] OpenAI: Unexpected error during random fact generation: {e}", exc_info=True)
            return f"Виникла несподівана помилка при генерації факту: {e}"

    def generate_ukrainian_history_fact(self):
        """Generates a random interesting historical fact about Ukraine using OpenAI."""
        logging.info(f"[{datetime.now()}] OpenAI: Generating random Ukrainian historical fact...")
        try:
            response = self.client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[
                    {"role": "system", "content": self.ukrainian_history_fact_prompt},
                    {"role": "user", "content": "Надай мені один короткий (2-3 речення) цікавий історичний факт з історії будь-якої країни світу. Перевіряй додатково його на достовірність та правдивість"}
                ],
                max_tokens=150,
                temperature=0.8
            )
            fact = response.choices[0].message.content
            logging.info(f"[{datetime.now()}] OpenAI: Ukrainian historical fact successfully generated.")
            return fact
        except openai.APIError as e:
            logging.error(f"[{datetime.now()}] OpenAI: API Error during Ukrainian historical fact generation: {e}", exc_info=True)
            return f"Вибачте, не вдалося згенерувати історичний факт: {e}"
        except Exception as e:
            logging.error(f"[{datetime.now()}] OpenAI: Unexpected error during Ukrainian historical fact generation: {e}", exc_info=True)
            return f"Виникла несподівана помилка при генерації історичного факту: {e}"


# Instantiate OpenAIService
openai_service = OpenAIService(openai_client)


# === Social Downloader Class ===
class SocialDownloader:
    def __init__(self, rapidapi_key, rapidapi_host):
        self.api_url = "https://social-download-all-in-one.p.rapidapi.com/v1/social/autolink"
        self.headers = {
            "x-rapidapi-key": rapidapi_key,
            "x-rapidapi-host": rapidapi_host,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def download_video(self, url):
        """Downloads a social media video from the given URL using RapidAPI."""
        logging.info(f"[{datetime.now()}] SocialDownloader: Attempting to download video from URL: {url}")
        payload = {"url": url}
        try:
            response = requests.post(self.api_url, json=payload, headers=self.headers, timeout=60)
            response.raise_for_status()
            data = response.json()
            logging.info(f"[{datetime.now()}] SocialDownloader: RapidAPI response received.")

            if "medias" in data and len(data["medias"]) > 0:
                best_video_url = None
                any_video_url = None

                for media in data["medias"]:
                    if media.get("type") == "video":
                        if any_video_url is None:
                            any_video_url = media["url"]

                        quality = str(media.get("quality", "")).lower()

                        if "tiktok" in url.lower():
                            if "hd_no_watermark" in quality: return media["url"]
                            elif "no_watermark" in quality: best_video_url = media["url"]
                        elif "instagram" in url.lower():
                            if "p" in quality: return media["url"]
                            elif "hd" in quality or "high" in quality: return media["url"]
                        elif "facebook" in url.lower():
                            if "hd" in quality: return media["url"]

                        if "hd" in quality or "high" in quality: best_video_url = media["url"]
                        elif "sd" in quality or "medium" in quality:
                            if best_video_url is None: best_video_url = media["url"]

                if best_video_url:
                    logging.info(f"[{datetime.now()}] SocialDownloader: Found preferred quality video URL: {best_video_url}")
                    return best_video_url
                elif any_video_url:
                    logging.info(f"[{datetime.now()}] SocialDownloader: No preferred quality, using first available video URL: {any_video_url}")
                    return any_video_url
            else:
                logging.warning(f"[{datetime.now()}] SocialDownloader: RapidAPI returned media, but no video URL found.")
                return "Could not find video link."
        except requests.exceptions.HTTPError as e:
            logging.error(f"[{datetime.now()}] SocialDownloader: HTTP Error from RapidAPI: {e.response.status_code} - {e.response.text}", exc_info=True)
            if e.response.status_code == 404: return "Video not found or private."
            elif e.response.status_code == 400: return "Invalid link or API request error."
            else: return f"Error from RapidAPI: {e.response.status_code} - {e.response.text}"
        except requests.exceptions.ConnectionError as e:
            logging.error(f"[{datetime.now()}] SocialDownloader: Connection error to RapidAPI: {e}", exc_info=True)
            return "Connection error to video download service."
        except requests.exceptions.Timeout as e:
            logging.error(f"[{datetime.now()}] SocialDownloader: Request to RapidAPI timed out: {e}", exc_info=True)
            return "Request to video service timed out."
        except Exception as e:
            logging.error(f"[{datetime.now()}] SocialDownloader: Unexpected error in RapidAPI request: {e}", exc_info=True)
            return f"An unexpected error occurred while processing the request: {e}"

# Instantiate SocialDownloader
social_downloader = SocialDownloader(Config.RAPIDAPI_KEY, Config.RAPIDAPI_HOST)


# === News and Weather Service Class ===
class NewsWeatherService:
    def get_weather_meteo(self, city_url):
        """Fetches weather from meteo.ua for a given city URL."""
        try:
            r = requests.get(city_url, timeout=10)
            soup = BeautifulSoup(r.content, "html.parser")
            temp = soup.find(class_="menu-basic__degree")
            return temp.get_text(strip=True) if temp else "N/A"
        except requests.exceptions.RequestException as e:
            logging.error(f"[{datetime.now()}] NewsWeather: Error getting weather for {city_url}: {e}", exc_info=True)
            return "N/A"
        except Exception as e:
            logging.error(f"[{datetime.now()}] NewsWeather: Unexpected error in get_weather_meteo: {e}", exc_info=True)
            return "N/A"

    def get_daily_weather_report(self):
        """Generates a daily weather report for predefined cities."""
        cities = {
            "Київ": "https://meteo.ua/ua/34/kiev",
            "Рівне": "https://meteo.ua/ua/28/rovno",
            "Косів": "https://meteo.ua/ua/16532/kosov",
            "Одеса": "https://meteo.ua/ua/111/odessa"
        }
        msg = "\U0001F324️ Прогноз погоди на сьогодні\\:\n"
        for city, url in cities.items():
            escaped_city = escape_markdown_v2(city)
            escaped_weather = escape_markdown_v2(self.get_weather_meteo(url))
            msg += f" \u2022 {escaped_city}\\: {escaped_weather}\n"
        return msg

    def get_top3_news_pravda(self):
        """Fetches top 3 news from pravda.ua."""
        try:
            r = requests.get("https://www.pravda.ua/", timeout=10)
            soup = BeautifulSoup(r.content, "html.parser")
            block = soup.find("div", {"data-vr-zone": "Popular by views"})
            articles = block.find_all("div", class_="article_popular", limit=3)
            text = "\U0001F4F0 Найбільш важливі новини за вчора\\:\n"
            for i, a in enumerate(articles, 1):
                title = a.find("a").get_text(strip=True)
                escaped_title = escape_markdown_v2(title)
                text += f"{i}\\. {escaped_title}\n"
            return text
        except requests.exceptions.RequestException as e:
            logging.error(f"[{datetime.now()}] NewsWeather: Error getting news: {e}", exc_info=True)
            return "Новини недоступні\\."
        except Exception as e:
            logging.error(f"[{datetime.now()}] NewsWeather: Unexpected error in get_top3_news_pravda: {e}", exc_info=True)
            return "Новини недоступні\\."

    def get_official_usd_rate(self):
        """Fetches official USD rate from bank.gov.ua."""
        try:
            r = requests.get("https://bank.gov.ua/ua/markets/exchangerates", timeout=10)
            soup = BeautifulSoup(r.content, "html.parser")
            rows = soup.find_all("tr")
            for row in rows:
                code = row.find("td", {"data-label": "Код літерний"})
                if code and code.get_text(strip=True) == "USD":
                    rate = row.find("td", {"data-label": "Офіційний курс"})
                    escaped_rate = escape_markdown_v2(rate.get_text(strip=True))
                    return f"\U0001F4B1 Офіційний курс USD від НБУ\\: {escaped_rate} UAH"
            return "N/A"
        except requests.exceptions.RequestException as e:
            logging.error(f"[{datetime.now()}] NewsWeather: Error getting USD rate: {e}", exc_info=True)
            return "N/A"
        except Exception as e:
            logging.error(f"[{datetime.now()}] NewsWeather: Unexpected error in get_official_usd_rate: {e}", exc_info=True)
            return "N/A"

    def get_bitcoin_price(self):
        """Fetches Bitcoin price from finance.ua."""
        try:
            r = requests.get("https://finance.ua/ua/crypto/btc", headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            soup = BeautifulSoup(r.content, "html.parser")
            container = soup.find("div", class_="MainInfostyles__Price-sc-1pcfgvi-16 gfcnFW")
            trend = soup.find("div", class_="MainInfostyles__Trend-sc-1pcfgvi-17 hwJIFp")
            if container:
                price = ''.join(container.stripped_strings)
                change = trend.get_text(strip=True) if trend else ''
                escaped_price = escape_markdown_v2(price)
                escaped_change = escape_markdown_v2(change)
                return f"\U0001FA99 Поточний курс бітка\\: 1 BTC \\= {escaped_price} \\({escaped_change}\\)"
            return "N/A"
        except requests.exceptions.RequestException as e:
            logging.error(f"[{datetime.now()}] NewsWeather: Error getting Bitcoin price: {e}", exc_info=True)
            return "N/A"
        except Exception as e:
            logging.error(f"[{datetime.now()}] NewsWeather: Unexpected error in get_bitcoin_price: {e}", exc_info=True)
            return "N/A"

# Instantiate NewsWeatherService
news_weather_service = NewsWeatherService()


# === Telegram Message Sender Class ===
class TelegramMessageSender:
    def __init__(self, bot_instance, db_manager_instance):
        self.bot = bot_instance
        self.db_manager = db_manager_instance

    def send_and_save_message(self, chat_id, text, parse_mode=None, bot_message_type=None, telegram_message_id_to_reply=None, media_type=None, media_file=None):
        """
        Sends a message (text or media) and saves its details to the database.
        The `text` parameter is expected to be correctly formatted for the given `parse_mode`.
        """
        try:
            reply_parameters = None
            if telegram_message_id_to_reply:
                reply_parameters = telebot.types.ReplyParameters(message_id=telegram_message_id_to_reply, chat_id=chat_id, allow_sending_without_reply=True)

            sent_message = None
            if media_type == 'video' and media_file:
                sent_message = self.bot.send_video(chat_id, media_file, caption=text, parse_mode=parse_mode, reply_parameters=reply_parameters)
            elif media_type == 'photo' and media_file:
                sent_message = self.bot.send_photo(chat_id, media_file, caption=text, parse_mode=parse_mode, reply_parameters=reply_parameters)
            else:
                sent_message = self.bot.send_message(chat_id, text, parse_mode=parse_mode, reply_parameters=reply_parameters)

            logging.info(f"[{datetime.now()}] Sender: Повідомлення надіслано до чату {chat_id}, message_id: {sent_message.message_id}, тип: {bot_message_type}")
            self.db_manager.save_message(
                telegram_message_id=sent_message.message_id,
                user_id=self.bot.get_me().id,
                username=self.bot.get_me().username,
                message_content=text, # Save the original text/caption for internal use
                message_date=datetime.utcnow(),
                chat_id_to_save=chat_id,
                is_bot_message=True,
                bot_message_type=bot_message_type
            )
            return sent_message.message_id
        except Exception as e:
            logging.error(f"[{datetime.now()}] Sender: Помилка при відправці повідомлення та збереженні ID: {e}", exc_info=True)
            return None

# Instantiate TelegramMessageSender
telegram_sender = TelegramMessageSender(bot, db_manager)


# === Report Generators ===

def generate_morning_report_text():
    """Generates the text content of the morning report."""
    now = datetime.now()
    weekday_ua = {
        "Monday": "Понеділок", "Tuesday": "Вівторок", "Wednesday": "Середа",
        "Thursday": "Четвер", "Friday": "П’ятниця", "Saturday": "Субота", "Sunday": "Неділя"
    }
    day = weekday_ua.get(now.strftime("%A"), now.strftime("%A"))
    date_str = now.strftime("%d.%m.%Y")

    report = (
        f"**Доброго ранку, шановні експерти\!**\n\n"
        f"**{escape_markdown_v2(day)} \\({escape_markdown_v2(date_str)}\\ року\\)**\n\n"
        f"{news_weather_service.get_daily_weather_report()}\n"
        f"{news_weather_service.get_official_usd_rate()}\n"
        f"{news_weather_service.get_bitcoin_price()}\n\n"
        f"{news_weather_service.get_top3_news_pravda()}\n\n"
        f"Всім гарного та мирного дня\! \U0000270C\ufe0f"
    )
    return report

def _send_morning_report_content(chat_id):
    """Generates and sends the morning report content."""
    logging.info(f"[{datetime.now()}] Report: Generating and sending morning report content.")
    try:
        report_text = generate_morning_report_text()
        telegram_sender.send_and_save_message(chat_id, report_text, parse_mode="MarkdownV2", bot_message_type='daily_report')
        logging.info(f"[{datetime.now()}] Report: Morning report content sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Report: Error generating/sending morning report content: {e}", exc_info=True)
        bot_response = f"Виникла помилка при створенні ранкового звіту\\: {escape_markdown_v2(str(e))}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='report_error')

def generate_wordcloud_image(texts):
    """Generates a word cloud image from a list of texts."""
    wordcloud = WordCloud(width=800, height=400, min_word_length=4, background_color="white").generate(" ".join(texts))
    img = io.BytesIO()
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.savefig(img, format="PNG")
    plt.close()
    img.seek(0)
    return img

def _send_daily_report_content(chat_id):
    """Generates and sends the daily activity report content."""
    logging.info(f"[{datetime.now()}] Report: Generating and sending daily report content.")
    try:
        today_utc = datetime.utcnow().date()
        total_messages, top_users, bot_messages_count = db_manager.get_daily_stats()
        texts_for_wordcloud = db_manager.get_all_texts_for_wordcloud()
        daily_swear_count = db_manager.get_swear_count(chat_id, today_utc)

        report = f"\U0001F4CA Звіт за **{escape_markdown_v2(today_utc.strftime('%d.%m.%Y'))}**\\:\n\n"
        report += f"Всього повідомлень у чаті\\: **{escape_markdown_v2(str(total_messages))}**\n\n"
        report += f"**Топ\\-5 найактивніших писаків\\-експертів**\\:\n\n"
        if top_users:
            for user, count in top_users:
                escaped_user = escape_markdown_v2(user if user else 'Невідомий користувач')
                report += f"\u2022 {escaped_user}\\: **{escape_markdown_v2(str(count))}** повідомлень\n"
        else:
            report += "Немає активних користувачів, крім бота\\.\n"

        bot_username = bot.get_me().username
        report += f"\nАктивність бота: **{escape_markdown_v2(str(bot_messages_count))}** повідомлень\n"
        report += f"\n\U0001F621 За сьогодні було виявлено **{escape_markdown_v2(str(daily_swear_count))}** матюків\\.\nСлідкуйте за мовою\\! 😉"

        telegram_sender.send_and_save_message(chat_id, report, parse_mode="MarkdownV2", bot_message_type='daily_report')

        if texts_for_wordcloud:
            wordcloud_caption = f"**\U0001F308 Хмара слів за добу**"
            telegram_sender.send_and_save_message(
                chat_id,
                wordcloud_caption,
                parse_mode="MarkdownV2",
                bot_message_type='wordcloud_image',
                media_type='photo',
                media_file=generate_wordcloud_image(texts_for_wordcloud)
            )
        else:
            bot_response = "⚠️ Недостатньо повідомлень для WordCloud\\."
            telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='wordcloud_no_data')
        logging.info(f"[{datetime.now()}] Report: Daily report content sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Report: Error generating/sending daily report content: {e}", exc_info=True)
        bot_response = f"Виникла помилка при створенні денного звіту\\: {escape_markdown_v2(str(e))}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='report_error')

def _send_random_fact_content(chat_id, telegram_message_id_to_reply=None):
    """Generates and sends a random interesting fact."""
    logging.info(f"[{datetime.now()}] Report: Generating and sending random fact content.")
    try:
        generated_fact = openai_service.generate_random_fact()
        bot_response = f"\U0001F9D0 **Цікавий факт\\:**\n\n{escape_markdown_v2(generated_fact)}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='random_fact', telegram_message_id_to_reply=telegram_message_id_to_reply)
        logging.info(f"[{datetime.now()}] Report: Sent generated random fact from OpenAI.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Report: Unexpected error sending random fact: {e}", exc_info=True)
        bot_response = f"Виникла несподівана помилка при отриманні факту\\.\\ {escape_markdown_v2(str(e))}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='fact_error', telegram_message_id_to_reply=telegram_message_id_to_reply)

def _send_ukrainian_history_fact_content(chat_id, telegram_message_id_to_reply=None):
    """Generates and sends a random Ukrainian historical fact."""
    logging.info(f"[{datetime.now()}] Report: Generating and sending Ukrainian historical fact content.")
    try:
        generated_fact = openai_service.generate_ukrainian_history_fact()
        bot_response = f"\U0001F4DA **Вчіть історію\\:**\n\n{escape_markdown_v2(generated_fact)}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='ukrainian_history_fact', telegram_message_id_to_reply=telegram_message_id_to_reply)
        logging.info(f"[{datetime.now()}] Report: Sent generated Ukrainian historical fact from OpenAI.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Report: Unexpected error sending Ukrainian historical fact: {e}", exc_info=True)
        bot_response = f"Виникла несподівана помилка при отриманні історичного факту\\.\\ {escape_markdown_v2(str(e))}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='fact_error', telegram_message_id_to_reply=telegram_message_id_to_reply)

def _send_ai_summary_content(chat_id, telegram_message_id_to_reply=None):
    """Generates and sends the AI summary content."""
    logging.info(f"[{datetime.now()}] Report: Generating and sending AI summary content.")
    try:
        messages_for_summary = db_manager.get_messages_for_summary()
        summary = openai_service.generate_summary(messages_for_summary)
        bot_response = f"\U0001F4AC **Стислий огляд дня\\:**\n\n{escape_markdown_v2(summary)}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='ai_summary', telegram_message_id_to_reply=telegram_message_id_to_reply)
        logging.info(f"[{datetime.now()}] Report: AI summary content sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Report: Error generating/sending AI summary content: {e}", exc_info=True)
        bot_response = f"Виникла помилка при створенні підсумку\\: {escape_markdown_v2(str(e))}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='summary_error', telegram_message_id_to_reply=telegram_message_id_to_reply)

def _send_cashback_reminder_content(chat_id):
    """Sends the cashback reminder message."""
    logging.info(f"[{datetime.now()}] Report: Sending cashback reminder.")
    reminder_text = (
        f"\U0001F4B8 **Кешбек\!**\n\n"
        f"Не забудьте увімкнути кешбек в улюблених категоріях цього місяця\\! 😉"
    )
    try:
        telegram_sender.send_and_save_message(chat_id, reminder_text, parse_mode="MarkdownV2", bot_message_type='cashback_reminder')
        logging.info(f"[{datetime.now()}] Report: Cashback reminder sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Report: Error sending cashback reminder: {e}", exc_info=True)
        bot_response = f"Виникла помилка при відправці нагадування про кешбек\\: {escape_markdown_v2(str(e))}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='reminder_error')

def _send_monthly_payments_reminder_content(chat_id):
    """Sends the monthly payments reminder message."""
    logging.info(f"[{datetime.now()}] Report: Sending monthly payments reminder.")
    reminder_text = (
        f"\U0001F4B3 **Останній день місяця\!**\n\n"
        f"Не забудьте оплатити інтернет та інші сервіси, які цього потребують\\."
    )
    try:
        telegram_sender.send_and_save_message(chat_id, reminder_text, parse_mode="MarkdownV2", bot_message_type='monthly_payments_reminder')
        logging.info(f"[{datetime.now()}] Report: Monthly payments reminder sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Report: Error sending monthly payments reminder: {e}", exc_info=True)
        bot_response = f"Виникла помилка при відправці нагадування про оплату сервісів\\: {escape_markdown_v2(str(e))}"
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='reminder_error')


# === Flask Web Server ===
app = Flask(__name__)

def set_webhook_with_retries(max_retries=5, retry_delay_seconds=5, initial_sleep_seconds=2):
    """Sets the Telegram webhook with retry logic."""
    full_url = f"{Config.WEBHOOK_BASE_URL}{Config.WEBHOOK_PATH}"
    logging.info(f"[{datetime.now()}] Webhook: Attempting to set webhook to URL: {full_url}")
    time.sleep(initial_sleep_seconds)

    for attempt in range(1, max_retries + 1):
        try:
            bot.remove_webhook()
            time.sleep(1)
            bot.set_webhook(url=full_url)
            logging.info(f"[{datetime.now()}] [+] Webhook explicitly set during startup: {full_url} on attempt {attempt}.")
            return True
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code == 429:
                retry_after = e.result_json.get('parameters', {}).get('retry_after', retry_delay_seconds)
                logging.warning(f"[{datetime.now()}] Webhook: Telegram API rate limit (429) hit on attempt {attempt}. Retrying in {retry_after} seconds.")
                time.sleep(retry_after)
            else:
                logging.error(f"[{datetime.now()}] [-] Error setting/initializing webhook on attempt {attempt}: {e}", exc_info=True)
                return False
        except Exception as e:
            logging.error(f"[{datetime.now()}] [-] Unexpected error setting/initializing webhook on attempt {attempt}: {e}", exc_info=True)
            if attempt < max_retries:
                logging.info(f"[{datetime.now()}] Webhook: Retrying webhook setup in {retry_delay_seconds} seconds...")
                time.sleep(retry_after)
            else:
                logging.error(f"[{datetime.now()}] Webhook: Failed to set webhook after {max_retries} attempts.")
                return False
    return False

def handle_swear_words(chat_id, effective_message_content, telegram_message_id):
    """Checks for swear words and updates count."""
    if effective_message_content:
        cleaned_message = re.sub(r'[^\w\s]', '', effective_message_content.lower())
        total_swears_in_message = sum(len(re.findall(pattern, cleaned_message)) for pattern in SWEAR_WORDS_REGEX_PATTERNS)
        if total_swears_in_message > 0:
            today_utc = datetime.utcnow().date()
            current_swear_count = db_manager.increment_swear_count(chat_id, today_utc, total_swears_in_message)
            bot_response_swear_count = f"\U0001F4A9 Лічильник матюків\\: **{escape_markdown_v2(str(current_swear_count))}**\\.\nСлідкуйте за мовою\\! 😉"
            telegram_sender.send_and_save_message(chat_id, bot_response_swear_count, parse_mode="MarkdownV2", bot_message_type='swear_counter', telegram_message_id_to_reply=telegram_message_id)

def handle_forwarded_message(message_data, chat_id, telegram_message_id, effective_message_content):
    """Handles forwarded messages for translation."""
    raw_forward_from_chat_name = 'невідомого джерела' # Default value
    raw_username = message_data.get('from', {}).get('username', 'невідомого користувача')
    original_message_link = "" # Initialize link

    if 'forward_from_chat' in message_data:
        forward_from_chat = message_data['forward_from_chat']
        raw_forward_from_chat_name = forward_from_chat.get('title', 'приватного каналу/групи')
        if forward_from_chat.get('type') == 'channel':
            channel_username = forward_from_chat.get('username')
            original_msg_id = message_data.get('forward_from_message_id')
            if channel_username and original_msg_id:
                original_message_link = f"https://t.me/{channel_username}/{original_msg_id}"
                logging.info(f"[{datetime.now()}] Webhook: Знайдено посилання на оригінальне повідомлення: {original_message_link}")
    elif 'forward_from' in message_data:
        forward_from_user = message_data['forward_from']
        user_name_parts = []
        if 'first_name' in forward_from_user:
            user_name_parts.append(forward_from_user['first_name'])
        if 'last_name' in forward_from_user:
            user_name_parts.append(forward_from_user['last_name'])
        raw_forward_from_chat_name = 'від користувача ' + ' '.join(user_name_parts) if user_name_parts else 'від невідомого користувача'

    translated_text_from_ai = ""
    if effective_message_content:
        bot.send_chat_action(chat_id, "typing")
        try:
            translated_text_from_ai = openai_service.translate_text(effective_message_content)
        except Exception as e:
            logging.error(f"[{datetime.now()}] Webhook: Помилка перекладу пересланого вмісту: {e}", exc_info=True)
            translated_text_from_ai = f"Помилка перекладу: {e}"

    escaped_forward_from_chat_name = escape_markdown_v2(raw_forward_from_chat_name)
    escaped_username_for_display = escape_markdown_v2(raw_username)
    escaped_translated_text_from_ai_for_display = escape_markdown_v2(translated_text_from_ai)

    # Нова логіка для формування назви джерела з посиланням або без
    source_name_display = escaped_forward_from_chat_name
    if original_message_link:
        # Якщо є оригінальне посилання, вбудовуємо його в назву чату
        source_name_display = f"[{escaped_forward_from_chat_name}]({original_message_link})" # Змінено: прибрано escape_markdown_v2 для URL


    base_caption_content = (
        "\U0001F4E4 Новина, переслана з "
        f"{source_name_display} " # Використовуємо нову змінну тут
        f"\\(від {escaped_username_for_display}\\):\n\n"
        f"{escaped_translated_text_from_ai_for_display}"
    )

    # Рядок, що додавав "Оригінал новини" окремо, тепер не потрібен
    # if original_message_link:
    #     base_caption_content += f"\n\n[Оригінал новини]({escape_markdown_v2(original_message_link)})"

    MAX_CAPTION_LENGTH = 1024
    final_caption = base_caption_content
    if len(base_caption_content) > MAX_CAPTION_LENGTH - 3:
        final_caption = base_caption_content[:MAX_CAPTION_LENGTH - 3] + "..."
        logging.warning(f"[{datetime.now()}] Webhook: Truncated combined raw content to {MAX_CAPTION_LENGTH - 3} chars and added '...'.")

    content_sent = False

    if 'video' in message_data and message_data['video']:
        video_file_id = message_data['video']['file_id']
        logging.info(f"[{datetime.now()}] Webhook: Переслане повідомлення містить відео. File ID: {video_file_id}")
        try:
            telegram_sender.send_and_save_message(
                Config.GROUP_REPORT_CHAT_ID, final_caption, parse_mode="MarkdownV2",
                bot_message_type='news_forward_video',
                media_type='video', media_file=video_file_id
            )
            bot_response_private = f"\U0001F504 Переклад новини \\(з відео\\) відправлено у групу\\. Дякую\\!"
            telegram_sender.send_and_save_message(chat_id, bot_response_private, parse_mode="MarkdownV2", bot_message_type='translation_confirmation', telegram_message_id_to_reply=telegram_message_id)
            content_sent = True
        except Exception as e:
            logging.error(f"[{datetime.now()}] Webhook: Помилка надсилання пересланого відео з підписом: {e}", exc_info=True)
            error_msg = f"Ой, щось пішло не так при пересилці зображення з перекладом\\: {escape_markdown_v2(str(e))}\\. Спробуйте ще раз\\."
            telegram_sender.send_and_save_message(chat_id, error_msg, parse_mode="MarkdownV2", bot_message_type='translation_error', telegram_message_id_to_reply=telegram_message_id)

    elif 'photo' in message_data and message_data['photo'] and not content_sent:
        photo_file_id = None
        if isinstance(message_data['photo'], list) and message_data['photo']:
            photo_file_id = message_data['photo'][-1]['file_id']

        if photo_file_id:
            logging.info(f"[{datetime.now()}] Webhook: Переслане повідомлення містить фото. File ID: {photo_file_id}")
            try:
                telegram_sender.send_and_save_message(
                    Config.GROUP_REPORT_CHAT_ID, final_caption, parse_mode="MarkdownV2",
                    bot_message_type='news_forward_photo',
                    media_type='photo', media_file=photo_file_id
                )
                bot_response_private = f"\U0001F504 Переклад новини \\(з зображенням\\) відправлено у групу\\. Дякую\\!"
                telegram_sender.send_and_save_message(chat_id, bot_response_private, parse_mode="MarkdownV2", bot_message_type='translation_confirmation', telegram_message_id_to_reply=telegram_message_id)
                content_sent = True
            except Exception as e:
                logging.error(f"[{datetime.now()}] Webhook: Помилка надсилання пересланого фото з підписом: {e}", exc_info=True)
                error_msg = f"Ой, щось пішло не так при пересилці зображення з перекладом\\: {escape_markdown_v2(str(e))}\\. Спробуйте ще раз\\."
                telegram_sender.send_and_save_message(chat_id, error_msg, parse_mode="MarkdownV2", bot_message_type='translation_error', telegram_message_id_to_reply=telegram_message_id)

    elif effective_message_content and not content_sent:
        try:
            telegram_sender.send_and_save_message(
                Config.GROUP_REPORT_CHAT_ID, final_caption, parse_mode="MarkdownV2",
                bot_message_type='news_forward_text'
            )
            bot_response_private = f"\U0001F504 Переклад новини відправлено у групу\\. Дякую\\!"
            telegram_sender.send_and_save_message(chat_id, bot_response_private, parse_mode="MarkdownV2", bot_message_type='translation_confirmation', telegram_message_id_to_reply=telegram_message_id)
            logging.info(f"[{datetime.now()}] Webhook: Переслане повідомлення лише з текстом відправлено до групи {Config.GROUP_REPORT_CHAT_ID}.")
            content_sent = True
        except Exception as e:
            logging.error(f"[{datetime.now()}] Webhook: Помилка надсилання пересланого повідомлення лише з текстом: {e}", exc_info=True)
            error_msg = f"Ой, щось пішло не так при перекладі або відправці новини\\: {escape_markdown_v2(str(e))}\\. Спробуйте ще раз\\."
            telegram_sender.send_and_save_message(chat_id, error_msg, parse_mode="MarkdownV2", bot_message_type='translation_error', telegram_message_id_to_reply=telegram_message_id)

    elif not content_sent:
        logging.warning(f"[{datetime.now()}] Webhook: Отримано переслане повідомлення без тексту/підпису та без медіа. Ігноруємо.")
        bot_response_private = "Отримано переслане повідомлення без тексту та медіа\\. Нічого перекладати або пересилати\\."
        telegram_sender.send_and_save_message(chat_id, bot_response_private, parse_mode="MarkdownV2", bot_message_type='no_content_forward', telegram_message_id_to_reply=telegram_message_id)

def handle_social_media_link(chat_id, user_id, effective_message_content, telegram_message_id, chat_type): # Додано chat_type
    """Handles social media links for video download."""
    # Дозволити завантаження, якщо це груповий чат АБО це приватний чат І користувач є власником
    if chat_type in ['group', 'supergroup'] or (chat_type == 'private' and user_id == Config.OWNER_TELEGRAM_USER_ID):
        bot.send_chat_action(chat_id, "upload_video")
        result = social_downloader.download_video(effective_message_content.strip())
        if result and result.startswith("http"):
            try:
                video_resp = requests.get(result, stream=True, timeout=60)
                video_bytes = io.BytesIO()
                for chunk in video_resp.iter_content(chunk_size=8192):
                    video_bytes.write(chunk)
                video_bytes.seek(0)

                file_size_mb = video_bytes.tell() / (1024 * 1024)
                if file_size_mb > 50:
                    bot_response = f"Відео занадто велике \\({escape_markdown_v2(f'{file_size_mb:.2f}')} МБ\\), не можу відправити\\. Макс\\. 50 МБ\\."
                    telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='video_too_large', telegram_message_id_to_reply=telegram_message_id)
                else:
                    video_bytes.name = "video.mp4"
                    # escaped_effective_message_content = escape_markdown_v2(effective_message_content) # Цей рядок більше не потрібен
                    telegram_sender.send_and_save_message(
                        chat_id, "", parse_mode="MarkdownV2", # Змінено: прибрано текст підпису
                        bot_message_type='video_upload', telegram_message_id_to_reply=telegram_message_id,
                        media_type='video', media_file=video_bytes
                    )

            except requests.exceptions.RequestException as e:
                bot_response = f"Не вдалося завантажити відео через помилку\\: {escape_markdown_v2(str(e))}\\. Перевірте посилання або спробуйте пізніше\\."
                telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='video_download_error', telegram_message_id_to_reply=telegram_message_id)
            except Exception as e:
                bot_response = "Виникла несподівана помилка при обробці відео\\."
                telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='video_processing_error', telegram_message_id_to_reply=telegram_message_id)
        else:
            bot_response = "Не вдалося обробити посилання на відео\\. Спробуйте інше\\."
            telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='video_link_error', telegram_message_id_to_reply=telegram_message_id)
    else:
        # Повідомлення про відмову, якщо це приватний чат і користувач не є власником
        bot_response = "Вибачте, завантаження відео доступне лише в групових чатах або у приватному чаті власника бота, щоб заощадити кошти\\."
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='permission_denied', telegram_message_id_to_reply=telegram_message_id)
        logging.info(f"[{datetime.now()}] Webhook: Video download rejected for non-owner in private chat.")

def handle_bot_mention_command(chat_id, user_id, command_or_query_part, telegram_message_id, chat_type): # Додано chat_type
    """Handles commands when the bot is explicitly mentioned."""
    # Дозволити експертну відповідь, якщо це груповий чат АБО це приватний чат І користувач є власником
    if chat_type in ['group', 'supergroup'] or (chat_type == 'private' and user_id == Config.OWNER_TELEGRAM_USER_ID):
        command_text_lower = command_or_query_part.lower()

        if command_text_lower == "стислийоглядвже":
            bot.send_chat_action(chat_id, "typing")
            _send_ai_summary_content(chat_id, telegram_message_id)
        elif command_text_lower.startswith("заплануй_анонс") or command_text_lower.startswith("заплануй анонс"):
            if command_text_lower.startswith("заплануй_анонс"):
                args_part = command_or_query_part[len("заплануй_анонс"):].strip()
            else:
                args_part = command_or_query_part[len("заплануй анонс"):].strip()

            parts = args_part.split(' ', 1)

            if len(parts) == 2:
                schedule_time_str = parts[0]
                announcement_text = parts[1]
                response_msg = db_manager.add_scheduled_announcement(Config.GROUP_REPORT_CHAT_ID, announcement_text, schedule_time_str)
                telegram_sender.send_and_save_message(chat_id, response_msg, parse_mode="MarkdownV2", telegram_message_id_to_reply=telegram_message_id, bot_message_type='announcement_scheduled')
            else:
                raw_error_message = "Некоректний формат команди\\.\nВикористовуйте\\: `@ваш_бот заплануй_анонс ГГ:ХХ Текст вашого анонсу`\nАбо\\: `@ваш_бот заплануй анонс ГГ:ХХ Текст вашого анонсу`\\.\nЦе ж елементарно, навіть ви мали б зрозуміти\\!"
                telegram_sender.send_and_save_message(chat_id, raw_error_message, parse_mode="MarkdownV2", bot_message_type='announcement_format_error', telegram_message_id_to_reply=telegram_message_id)
        else:
            bot.send_chat_action(chat_id, "typing")
            expert_answer_raw = openai_service.get_expert_answer(chat_id, command_or_query_part)
            escaped_expert_answer_full_message = f"\U0001F9D1\u200D\U0001F3EB **Ось експертна думка з цього питання\\:**\n\n{escape_markdown_v2(expert_answer_raw)}"
            telegram_sender.send_and_save_message(chat_id, escaped_expert_answer_full_message, parse_mode="MarkdownV2", telegram_message_id_to_reply=telegram_message_id, bot_message_type='expert_opinion')
    else:
        bot_response = "Вибачте, але я не відповідаю на запитання у приватних чатах від сторонніх користувачів, щоб заощадити кошти\\. Моя експертиза доступна лише для спеціальних запитів\\."
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", telegram_message_id_to_reply=telegram_message_id)
        logging.info(f"[{datetime.now()}] Webhook: AI expert response blocked for non-owner in private chat even with explicit mention.")

def handle_reply_to_bot_message(message_data, chat_id, user_id, effective_message_content, telegram_message_id, chat_type): # Додано chat_type
    """Handles replies to the bot's own messages."""
    reply_to_message = message_data['reply_to_message']
    if 'from' in reply_to_message and reply_to_message['from']['id'] == bot.get_me().id:
        replied_message_info = db_manager.get_message_by_id(reply_to_message['message_id'])
        bot_msg_type = replied_message_info.get('bot_message_type') if replied_message_info else None

        excluded_types_for_expert_opinion = [
            'news_forward_video', 'news_forward_photo', 'news_forward_text',
            'ai_summary', 'daily_report', 'random_fact', 'ukrainian_history_fact',
            'swear_counter', 'wordcloud_image', 'wordcloud_no_data',
            'translation_confirmation', 'translation_error', 'no_content_forward',
            'report_error', 'fact_error', 'anecdote_error', 'video_upload',
            'video_too_large', 'video_download_error', 'video_processing_error',
            'video_link_error', 'expert_no_query', 'announcement_scheduled',
            'announcement_format_error', 'welcome_message', 'permission_denied',
            'cashback_reminder', 'monthly_payments_reminder'
        ]

        if bot_msg_type in excluded_types_for_expert_opinion:
            logging.info(f"[{datetime.now()}] Webhook: Пропущено запит експертної думки через відповідь на повідомлення бота типу: {bot_msg_type} (без згадки)")
            return

        # Дозволити експертну відповідь, якщо це груповий чат АБО це приватний чат І користувач є власником
        if chat_type in ['group', 'supergroup'] or (chat_type == 'private' and user_id == Config.OWNER_TELEGRAM_USER_ID):
            logging.info(f"[{datetime.now()}] Webhook: Detected reply to bot's message (type: {bot_msg_type}). Activating expert conversation.")
            bot.send_chat_action(chat_id, "typing")
            expert_answer_raw = openai_service.get_expert_answer(chat_id, effective_message_content)
            escaped_expert_answer_full_message = f"\U0001F9D1\u200D\U0001F3EB **Ось експертна думка з цього питання\\:**\n\n{escape_markdown_v2(expert_answer_raw)}"
            telegram_sender.send_and_save_message(chat_id, escaped_expert_answer_full_message, parse_mode="MarkdownV2", bot_message_type='expert_opinion', telegram_message_id_to_reply=telegram_message_id)
        else:
            bot_response = "Вибачте, я можу відповідати експертною думкою в приватних чатах лише власнику\\. Це для економії ресурсів\\."
            telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='permission_denied', telegram_message_id_to_reply=telegram_message_id)
            logging.info(f"[{datetime.now()}] Webhook: AI expert response blocked for non-owner reply in private chat.")

def handle_private_chat_message(chat_id, user_id, effective_message_content, telegram_message_id):
    """Handles messages in private chats."""
    logging.info(f"[{datetime.now()}] Webhook: Detected message in private chat. Activating expert conversation.")
    if user_id == Config.OWNER_TELEGRAM_USER_ID:
        bot.send_chat_action(chat_id, "typing")
        expert_answer_raw = openai_service.get_expert_answer(chat_id, effective_message_content)
        escaped_expert_answer_full_message = f"\U0001F9D1\u200D\U0001F3EB **Ось експертна думка з цього питання\\:**\n\n{escape_markdown_v2(expert_answer_raw)}"
        telegram_sender.send_and_save_message(chat_id, escaped_expert_answer_full_message, parse_mode="MarkdownV2", telegram_message_id_to_reply=telegram_message_id, bot_message_type='expert_opinion')
    else:
        bot_response = "Вибачте, але я не відповідаю на запитання у приватних чатах від сторонніх користувачів, щоб заощадити кошти\\. Моя експертиза доступна лише для спеціальних запитів\\."
        telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", telegram_message_id_to_reply=telegram_message_id)
        logging.info(f"[{datetime.now()}] Webhook: AI expert response blocked for non-owner in private chat.")

def handle_new_chat_members(message_data, chat_id, telegram_message_id):
    """Handles new chat members, especially the bot itself."""
    for member in message_data['new_chat_members']:
        if member['id'] == bot.get_me().id:
            bot_username = bot.get_me().username
            raw_welcome_message = f"""
Привіт\! Я ваш особистий асистент у цьому чаті\. Мій функціонал\:

• **Ранковий звіт**: щоденна порція свіжих новин, погоди та курсів валют\.
• **Вечірній звіт**: підсумки дня, статистика активності та топ\-користувачі\.
• **Огляд повідомлень від ШІ**: ШІ зробить стислий переказ усіх обговорень за день\.
• **Скачування відео**: Завантажую відео з Facebook, Instagram, TikTok\. Просто надішліть посилання\!
• **Експертиза від ШІ**: Відповім на будь-яке запитання, якщо воно, звісно, не надто дурне\. Просто зверніться до мене, згадавши мій юзернейм @{bot_username}, і формулюйте своє питання\.
• **Переклад новин**: Перешліть мені повідомлення з будь-якого публічного каналу в приватний чат, і я перекладу його та опублікую у нашій групі\.
• **Лічильник матів**: Я знаю, хто тут тут найкультурніший, а хто\.\.\. ну, ви зрозуміли\.

Надішліть @{bot_username} допомога, щоб дізнатися більше\.
Або просто кидайте посилання на відео, і я спробую його скачати\.
            """
            telegram_sender.send_and_save_message(chat_id, raw_welcome_message, parse_mode="MarkdownV2", telegram_message_id_to_reply=telegram_message_id, bot_message_type='welcome_message')
            logging.info(f"[{datetime.now()}] Webhook: Sent welcome message to chat {chat_id}.")

@app.route(Config.WEBHOOK_PATH, methods=['POST'])
def webhook():
    """Main webhook endpoint for Telegram updates."""
    logging.info(f"[{datetime.now()}] Webhook: Entering webhook function.")
    if request.headers.get('content-type') != 'application/json':
        logging.warning(f"[{datetime.now()}] Webhook: Received POST request with incorrect content-type: {request.headers.get('content-type')}")
        abort(403)

    json_string = request.get_data().decode('utf-8')
    try:
        update_data = json.loads(json_string)
        logging.info(f"[{datetime.now()}] Webhook: Successfully parsed Telegram Update into dict.")

        update_id = update_data.get('update_id')
        if update_id:
            if update_id in processed_updates:
                logging.warning(f"[{datetime.now()}] Idempotency: Update with ID {update_id} already processed. Ignoring.")
                return 'ok', 200 # Return immediately if already processed
            else:
                processed_updates[update_id] = time.time()
                logging.info(f"[{datetime.now()}] Idempotency: Added update_id {update_id} to processed cache.")

        # Process the update in a separate thread to avoid webhook timeouts
        threading.Thread(target=process_telegram_update, args=(update_data,)).start()

    except json.JSONDecodeError as e:
        logging.error(f"[{datetime.now()}] Webhook: JSON decoding error: {e}. Raw data: {json_string[:200]}...", exc_info=True)
    except Exception as e:
        logging.error(f"[{datetime.now()}] Webhook: Error during direct update processing: {e}", exc_info=True)

    return 'ok', 200 # Always return 'ok' quickly

def process_telegram_update(update_data):
    """Processes a Telegram update in a separate thread."""
    try:
        if 'message' in update_data and update_data['message']:
            message_data = update_data['message']
            chat_id = message_data['chat']['id']
            message_text = message_data.get('text')
            chat_type = message_data['chat']['type']
            user_id = message_data.get('from', {}).get('id')
            username = message_data.get('from', {}).get('username')
            message_caption = message_data.get('caption')
            telegram_message_id = message_data.get('message_id')

            effective_message_content = message_text if message_text is not None else message_caption

            logging.info(f"[{datetime.now()}] Webhook: Message detected. Chat ID: {chat_id}, User ID: {user_id}, Content: '{effective_message_content[:50] if effective_message_content else 'No content'}'")

            db_manager.save_message(
                telegram_message_id=telegram_message_id,
                user_id=user_id,
                username=username,
                message_content=effective_message_content,
                message_date=datetime.utcfromtimestamp(message_data.get('date')),
                chat_id_to_save=chat_id,
                is_bot_message=False
            )

            handle_swear_words(chat_id, effective_message_content, telegram_message_id)

            bot_username = bot.get_me().username.lower() if bot.get_me() else ""
            bot_mention = f"@{bot_username}"

            is_bot_explicitly_mentioned_in_text = False
            mention_query_part = effective_message_content
            if effective_message_content and 'entities' in message_data:
                for entity in message_data['entities']:
                    if entity['type'] == 'mention' and effective_message_content[entity['offset']:entity['offset']+entity['length']].lower() == bot_mention:
                        is_bot_explicitly_mentioned_in_text = True
                        mention_query_part = effective_message_content[entity['offset'] + entity['length']:].strip()
                        break

            # --- MODIFIED FORWARD LOGIC ---
            # Handle forwarded messages from both channels/groups and individual users
            if chat_type == 'private' and ('forward_from_chat' in message_data or 'forward_from' in message_data):
                handle_forwarded_message(message_data, chat_id, telegram_message_id, effective_message_content)
            # --- END MODIFIED FORWARD LOGIC ---
            elif effective_message_content and any(x in effective_message_content.lower() for x in ["instagram.com/reel", "facebook.com/share/r", "vt.tiktok.com", "facebook.com/reel/", "facebook.com/share/v/"]):
                # Передача chat_type до handle_social_media_link
                handle_social_media_link(chat_id, user_id, effective_message_content, telegram_message_id, chat_type)
            elif is_bot_explicitly_mentioned_in_text:
                # Передача chat_type до handle_bot_mention_command
                handle_bot_mention_command(chat_id, user_id, mention_query_part, telegram_message_id, chat_type)
            elif 'reply_to_message' in message_data:
                # Передача chat_type до handle_reply_to_bot_message
                handle_reply_to_bot_message(message_data, chat_id, user_id, effective_message_content, telegram_message_id, chat_type)
            elif effective_message_content and chat_type == 'private':
                handle_private_chat_message(chat_id, user_id, effective_message_content, telegram_message_id)
            elif 'new_chat_members' in message_data:
                handle_new_chat_members(message_data, chat_id, telegram_message_id)
            else:
                logging.info(f"[{datetime.now()}] Webhook: Unhandled update or general text in GROUP chat. Full update: {update_data}")

        elif 'edited_message' in update_data:
            edited_message_data = update_data['edited_message']
            logging.info(f"[{datetime.now()}] Webhook Update Type: Edited Message. Chat ID: {edited_message_data['chat']['id']}, User ID: {edited_message_data['from']['id']}, Text: '{edited_message_data.get('text', 'No text')[:50]}'")
        elif 'callback_query' in update_data:
            callback_query_data = update_data['callback_query']
            logging.info(f"[{datetime.now()}] Webhook Update Type: Callback Query. Data: {callback_query_data.get('data')}")
        elif 'my_chat_member' in update_data:
            my_chat_member_data = update_data['my_chat_member']
            logging.info(f"[{datetime.now()}] Webhook Update Type: My Chat Member update. Old status: {my_chat_member_data['old_chat_member']['status']}, New status: {my_chat_member_data['new_chat_member']['status']}")
        elif 'chat_member' in update_data:
            chat_member_data = update_data['chat_member']
            logging.info(f"[{datetime.now()}] Webhook Update Type: Chat Member update for user {chat_member_data['new_chat_member']['user']['id']}. New status: {chat_member_data['new_chat_member']['status']}")
        else:
            logging.info(f"[{datetime.now()}] Webhook Update Type: Received other type of update. Keys: {update_data.keys()}. Full update: {update_data}")

        logging.info(f"[{datetime.now()}] Webhook: Finished direct processing of new updates. ")

    except Exception as e:
        logging.error(f"[{datetime.now()}] Webhook: Error during asynchronous update processing: {e}", exc_info=True)


@app.route("/", methods=['GET'])
def home():
    """Home endpoint for basic server health check."""
    logging.info(f"[{datetime.now()}] Home: Received GET request on /.")
    return "Bot is running. Database connection and scheduler should be active.", 200

@app.route("/daily", methods=['GET'])
def trigger_daily_report_endpoint():
    """Endpoint to manually trigger the daily report."""
    logging.info(f"[{datetime.now()}] Endpoint: Received request for /daily.")
    try:
        _send_daily_report_content(Config.GROUP_REPORT_CHAT_ID)
        logging.info(f"[{datetime.now()}] Endpoint: /daily - Report sent.")
        return "Report sent", 200
    except Exception as e:
        logging.error(f"[{datetime.now()}] Endpoint: Error in /daily endpoint: {e}", exc_info=True)
        return f"Error: {e}", 500

@app.route("/morning", methods=['GET'])
def trigger_morning_report_endpoint():
    """Endpoint to manually trigger the morning report."""
    logging.info(f"[{datetime.now()}] Endpoint: Received request for /morning.")
    try:
        _send_morning_report_content(Config.GROUP_REPORT_CHAT_ID)
        logging.info(f"[{datetime.now()}] Endpoint: /morning - Morning report sent.")
        return "Morning report sent", 200
    except Exception as e:
        logging.error(f"[{datetime.now()}] Endpoint: Error in /morning endpoint: {e}", exc_info=True)
        return f"Error: {e}", 500

@app.route("/summary", methods=['GET'])
def trigger_summary_endpoint():
    """Endpoint to manually trigger the AI summary."""
    logging.info(f"[{datetime.now()}] Endpoint: Received request for /summary.")
    try:
        _send_ai_summary_content(Config.GROUP_REPORT_CHAT_ID)
        logging.info(f"[{datetime.now()}] Endpoint: /summary - Summary sent.")
        return "Summary sent", 200
    except Exception as e:
        logging.error(f"[{datetime.now()}] Endpoint: Error in /summary endpoint: {e}", exc_info=True)
        return f"Error: {e}", 500

@app.route("/fact", methods=['GET'])
def trigger_fact_endpoint():
    """Endpoint to manually trigger a random fact (morning slot)."""
    logging.info(f"[{datetime.now()}] Endpoint: Received request for /fact.")
    try:
        _send_random_fact_content(Config.GROUP_REPORT_CHAT_ID)
        logging.info(f"[{datetime.now()}] Endpoint: /fact - Random fact sent.")
        return "Random fact sent", 200
    except Exception as e:
        logging.error(f"[{datetime.now()}] Endpoint: Error in /fact endpoint: {e}", exc_info=True)
        return f"Error: {e}", 500

@app.route("/ukraine_fact", methods=['GET'])
def trigger_ukraine_fact_endpoint():
    """Endpoint to manually trigger a random Ukrainian historical fact."""
    logging.info(f"[{datetime.now()}] Endpoint: Received request for /ukraine_fact.")
    try:
        _send_ukrainian_history_fact_content(Config.GROUP_REPORT_CHAT_ID)
        logging.info(f"[{datetime.now()}] Endpoint: /ukraine_fact - Random Ukrainian historical fact sent.")
        return "Random Ukrainian historical fact sent", 200
    except Exception as e:
        logging.error(f"[{datetime.now()}] Endpoint: Error in /ukraine_fact endpoint: {e}", exc_info=True)
        return f"Error: {e}", 500

@app.route("/trigger_cashback_reminder", methods=['GET'])
def trigger_cashback_reminder_endpoint():
    """Endpoint to manually trigger the cashback reminder."""
    logging.info(f"[{datetime.now()}] Endpoint: Received request for /trigger_cashback_reminder.")
    try:
        _send_cashback_reminder_content(Config.GROUP_REPORT_CHAT_ID)
        logging.info(f"[{datetime.now()}] Endpoint: /trigger_cashback_reminder - Cashback reminder sent directly.")
        return "Cashback reminder sent directly", 200
    except Exception as e:
        logging.error(f"[{datetime.now()}] Endpoint: Error in /trigger_cashback_reminder endpoint: {e}", exc_info=True)
        return f"Error: {e}", 500


# === Scheduler Jobs (Content Generation Functions) ===
# These functions are called by the scheduler_process.py,
# which handles idempotency and scheduling logic.
def job_morning(chat_id): # Modified to accept chat_id
    logging.info(f"[{datetime.now()}] Scheduler (main): Running job_morning content generation.")
    try:
        _send_morning_report_content(chat_id)
        logging.info(f"[{datetime.now()}] Scheduler (main): Morning report content sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Scheduler (main): Error in morning report content job: {e}", exc_info=True)

def job_daily(chat_id):
    logging.info(f"[{datetime.now()}] Scheduler (main): Running job_daily content generation.")
    try:
        _send_daily_report_content(chat_id)
        logging.info(f"[{datetime.now()}] Scheduler (main): Daily report content sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Scheduler (main): Error in daily report content job: {e}", exc_info=True)

def job_summary(chat_id):
    logging.info(f"[{datetime.now()}] Scheduler (main): Running job_summary content generation.")
    try:
        _send_ai_summary_content(chat_id)
        logging.info(f"[{datetime.now()}] Scheduler (main): GPT summary content sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Scheduler (main): Error in GPT summary content job: {e}", exc_info=True)

def job_send_scheduled_announcements():
    """Checks DB and sends scheduled announcements if due."""
    logging.info(f"[{datetime.now()}] Scheduler (main): Checking for scheduled announcements.")
    announcements_to_send = db_manager.get_scheduled_announcements_to_send()
    for ann_id, chat_id, message_text in announcements_to_send:
        try:
            # FIX: Removed double escaping. message_text is already escaped when saved to DB.
            bot_response = f"\U0001F4E2 **Анонс\\!**\n\n{message_text}"
            telegram_sender.send_and_save_message(chat_id, bot_response, parse_mode="MarkdownV2", bot_message_type='scheduled_announcement')
            db_manager.mark_announcement_sent(ann_id)
            logging.info(f"[{datetime.now()}] Scheduler (main): Announcement ID {ann_id} sent to chat {chat_id}.")
        except Exception as e:
            logging.error(f"[{datetime.now()}] Scheduler (main): Error sending announcement ID {ann_id}: {e}", exc_info=True)

def job_cashback_reminder():
    logging.info(f"[{datetime.now()}] Scheduler (main): Running job_cashback_reminder content generation.")
    try:
        _send_cashback_reminder_content(Config.GROUP_REPORT_CHAT_ID)
        logging.info(f"[{datetime.now()}] Scheduler (main): Cashback reminder content sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Scheduler (main): Error in cashback reminder content job: {e}", exc_info=True)

def job_monthly_payments_reminder():
    logging.info(f"[{datetime.now()}] Scheduler (main): Running job_monthly_payments_reminder content generation.")
    try:
        _send_monthly_payments_reminder_content(Config.GROUP_REPORT_CHAT_ID)
        logging.info(f"[{datetime.now()}] Scheduler (main): Monthly payments reminder content sent.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Scheduler (main): Error in monthly payments reminder content job: {e}", exc_info=True)


# === Main Application Entry Point ===
logging.info(f"[{datetime.now()}] Starting bot and initializing webhook...")

db_manager.create_tables()

set_webhook_with_retries()
logging.info(f"[{datetime.now()}] Flask app configured to handle Gunicorn.")

logging.info(f"[{datetime.now()}] Number of registered message_handlers: 0 (handled by direct webhook processing)")
logging.info(f"[{datetime.now()}] Кількість зареєстрованих callback_query_handlers: 0")
