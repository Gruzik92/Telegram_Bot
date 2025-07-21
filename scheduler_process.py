# scheduler_process.py
# Цей файл містить логіку запуску планувальника.

import schedule
import time
import logging
from datetime import datetime, timedelta
import os
import sys

# Налаштування логування для окремого процесу планувальника
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# !!! Важливо: Імпортуємо необхідні функції та об'єкти з main.py
# Це дозволить scheduler_process.py використовувати вже існуючі функції
# без дублювання коду.
try:
    from main import (
        db_manager, Config, # Import Config class
        job_morning, job_summary, job_daily, job_send_scheduled_announcements,
        _send_random_fact_content,
        _send_ukrainian_history_fact_content,
        _send_cashback_reminder_content,
        _send_monthly_payments_reminder_content
    )
except ImportError as e:
    logging.critical(f"[{datetime.now()}] CRITICAL: Failed to import necessary components from main.py: {e}")
    sys.exit(1)

# Переініціалізуємо db_manager, оскільки це окремий процес
# (хоча db_manager має внутрішній механізм перепідключення, це забезпечує,
# що з'єднання буде ініціалізовано для цього процесу).
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logging.critical(f"[{datetime.now()}] CRITICAL: DATABASE_URL environment variable is not set in scheduler_process. Cannot connect to DB. Exiting.")
    sys.exit(1)

db_manager.database_url = DATABASE_URL # Переконаємося, що URL встановлено коректно
db_manager.create_tables() # Створити таблиці, якщо їх немає (ідємпотентна операція)

def scheduled_job_wrapper(job_func, job_name_base, slot_name=None, check_condition=None):
    """
    Generic wrapper for scheduled jobs to handle idempotency and conditional execution.

    Args:
        job_func (callable): The actual function to execute (e.g., _send_random_fact_content).
        job_name_base (str): Base name for the job (used for DB idempotency).
        slot_name (str, optional): Specific slot for the job (e.g., "morning", "evening").
                                   Forms part of the unique job name in DB.
        check_condition (callable, optional): A function that returns True if the job should run,
                                              False otherwise. Takes no arguments.
    """
    logging.info(f"[{datetime.now()}] Scheduler: Running wrapper for '{job_name_base}' (slot: {slot_name}).")
    today_utc = datetime.utcnow().date()

    if check_condition and not check_condition():
        logging.info(f"[{datetime.now()}] Scheduler: Skipping '{job_name_base}' (slot: {slot_name}) due to check condition.")
        return

    if not db_manager.record_job_execution(job_name_base, today_utc, slot=slot_name):
        logging.info(f"[{datetime.now()}] Report: Skipping '{job_name_base}' (slot: {slot_name}) as it was already executed today.")
        return

    try:
        job_func(Config.GROUP_REPORT_CHAT_ID) # Pass GROUP_REPORT_CHAT_ID to the content function
        logging.info(f"[{datetime.now()}] Scheduler: Job '{job_name_base}' (slot: {slot_name}) executed successfully.")
    except Exception as e:
        logging.error(f"[{datetime.now()}] Scheduler: Error in job '{job_name_base}' (slot: {slot_name}): {e}", exc_info=True)


# NEW: Wrapper for job_cashback_reminder to handle idempotency
def job_cashback_reminder_wrapper():
    """Wrapper for cashback reminder, runs only on the first day of the month."""
    today_utc = datetime.utcnow().date()
    def is_first_day_of_month():
        return today_utc.day == 1
    scheduled_job_wrapper(_send_cashback_reminder_content, "job_cashback_reminder", check_condition=is_first_day_of_month)


# NEW: Wrapper for job_monthly_payments_reminder to handle idempotency and last day logic
def job_monthly_payments_reminder_wrapper():
    """Wrapper for monthly payments reminder, runs only on the last day of the month."""
    today_utc = datetime.utcnow().date()
    def is_last_day_of_month():
        if today_utc.month == 12:
            next_month_first_day = today_utc.replace(year=today_utc.year + 1, month=1, day=1)
        else:
            next_month_first_day = today_utc.replace(month=today_utc.month + 1, day=1)
        last_day_of_current_month = next_month_first_day - timedelta(days=1)
        return today_utc == last_day_of_current_month
    scheduled_job_wrapper(_send_monthly_payments_reminder_content, "job_monthly_payments_reminder", slot_name="last_day_of_month", check_condition=is_last_day_of_month)


def run_schedule():
    """Configures and runs the scheduler jobs."""
    # All times are in UTC
    schedule.every().day.at("05:45").do(scheduled_job_wrapper, job_morning, "job_morning")            # 08:45 Kyiv time
    schedule.every().day.at("20:00").do(scheduled_job_wrapper, job_summary, "job_summary")            # 23:00 Kyiv time
    schedule.every().day.at("20:20").do(scheduled_job_wrapper, job_daily, "job_daily")              # 23:20 Kyiv time

    # Schedule for random facts (morning and evening)
    schedule.every().day.at("07:30").do(scheduled_job_wrapper, _send_random_fact_content, "send_random_fact", slot_name="morning")       # 10:30 Kyiv time
    schedule.every().day.at("17:00").do(scheduled_job_wrapper, _send_random_fact_content, "send_random_fact", slot_name="evening")       # 20:00 Kyiv time

    # Schedule for Ukrainian historical facts (morning and afternoon)
    schedule.every().day.at("10:00").do(scheduled_job_wrapper, _send_ukrainian_history_fact_content, "send_ukrainian_history_fact", slot_name="morning_ukraine_fact") # 13:00 Kyiv time
    schedule.every().day.at("18:30").do(scheduled_job_wrapper, _send_ukrainian_history_fact_content, "send_ukrainian_history_fact", slot_name="afternoon_ukraine_fact") # 21:30 Kyiv time

    # Schedule for monthly cashback reminder (runs daily, but logic inside wrapper checks for 1st of month)
    schedule.every().day.at("05:00").do(job_cashback_reminder_wrapper) # 05:00 UTC (08:00 Kyiv time)

    # Schedule for monthly payments reminder (runs daily, but logic inside wrapper checks for last day of month)
    schedule.every().day.at("18:00").do(job_monthly_payments_reminder_wrapper) # 18:00 UTC (21:00 Kyiv time)

    schedule.every(5).minutes.do(job_send_scheduled_announcements) # Check announcements every 5 minutes
    logging.info(f"[{datetime.now()}] Scheduler: Scheduler started. Jobs configured.")
    while True:
        schedule.run_pending()
        time.sleep(30) # Check every 30 seconds for pending jobs

if __name__ == "__main__":
    logging.info(f"[{datetime.now()}] Dedicated scheduler process started.")
    run_schedule()
