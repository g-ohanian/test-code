import logging
from datetime import datetime

from celery import shared_task
from dateutil.relativedelta import relativedelta

from apps.filings.tasks import fetch_data
from apps.schedule.models import Schedule, ScheduleRunHistory

LOGGER = logging.getLogger(__name__)


@shared_task(bind=True, name="fetch_data_task")
def fetch_data_task(*args, **kwargs):
    """
    Periodic task to fetch data from Existing Domains.
    """
    LOGGER.info("Starting scrapping task")

    try:
        now = datetime.now()
        now_date = now.date()
        frequency_map = {
            Schedule.FrequencyChoices.DAILY.value: {"start_day": 0, "delta": {"days": 1}},
            Schedule.FrequencyChoices.WEEKLY.value: {"start_day": now.isoweekday(), "delta": {"weeks": 1}},
            Schedule.FrequencyChoices.MONTHLY.value: {"start_day": now.day, "delta": {"months": 1}},
        }
        for scheduler in Schedule.objects.filter(is_started=True):
            try:
                schedule_time = scheduler.start_time
                scheduler_frequency = scheduler.frequency
                options = frequency_map[scheduler_frequency]

                if options["start_day"] != scheduler.start_day:
                    LOGGER.info(f"Skipping {scheduler.name} as it is not scheduled for today.")
                    continue

                days_range = (now_date - (now_date - relativedelta(**options["delta"]))).days

                if schedule_time.strftime("%H:%M:%S") > now.strftime("%H:%M:%S"):
                    continue
                fetch_data.apply_async(
                    kwargs=dict(
                        name=scheduler.name,
                        days_range=days_range,
                        from_beat=True,
                    ),
                    link_error=error_callback_handler.s(name=scheduler.name, task_name=fetch_data.name),
                    link=success_callback_handler.s(name=scheduler.name, task_name=fetch_data.name),
                )
            except Exception as e:
                LOGGER.exception(f"An error occurred while fetching data: {e}")
                raise e
    except Exception as e:
        LOGGER.exception(f"An error occurred while fetching data: {e}")
        raise e


@shared_task
def success_callback_handler(result, **kwargs):
    LOGGER.info("Success callback handler")
    kwargs["result"] = result
    handler(**kwargs)


@shared_task
def error_callback_handler(request, exc, traceback, **kwargs):
    LOGGER.info("Failure callback handler")
    kwargs["exception"] = traceback
    handler(**kwargs)


def handler(*args, **kwargs):
    result = kwargs.get("result")
    scheduler = Schedule.objects.get(name=kwargs["name"])

    run_history_data = {
        "schedule": scheduler,
        "breaches_count": len(result) if result else 0,
        "status": ScheduleRunHistory.StatusChoices.SUCCESS,
    }
    log_message = f"Task with name {kwargs.get('task_name')} succeeded."

    exception_obj = kwargs.get("exception")
    if exception_obj:
        run_history_data["status"] = ScheduleRunHistory.StatusChoices.FAILED
        run_history_data["fail_reason"] = str(exception_obj)
        log_message = f"Task with name {kwargs.get('task_name')} failed with exception: {exception_obj}"

    ScheduleRunHistory.objects.create(**run_history_data)
    LOGGER.info(log_message)
