from datetime import datetime, time

from django.utils import timezone
from django.utils.timezone import get_current_timezone
from persiantools.jdatetime import JalaliDate


def convert_date_time_string_to_datetime(datetime_string):
    date_part, time_part = datetime_string.split()
    return get_current_timezone().localize(
        datetime.combine(convert_date_string_to_date(date_part), convert_time_string_to_time(time_part)))


def convert_date_string_to_date(date_string):
    date_parts = date_string.split('/') if '/' in date_string else (date_string[:-4], date_string[-4:-2], date_string[-2:])
    date_parts = list(map(int, date_parts))
    if 0 <= date_parts[0] <= 31 < date_parts[2]:
        date_parts.reverse()

    if date_parts[0] <= 100:
        date_parts[0] += 1400 if date_parts[0] <= 50 else 1300

    return JalaliDate(*date_parts).to_gregorian()


def convert_time_integer_to_datetime(dt, time_integer):
    return get_current_timezone().localize(
        datetime(dt.year, dt.month, dt.day, time_integer // 10000, time_integer % 10000 // 100, time_integer % 100))


def convert_time_string_to_time(string):
    hour, minute, second = string.split(":")
    return time(hour=int(hour), minute=int(minute), second=int(second))


def is_active_hour():
    now = timezone.localtime(timezone.now())
    return now.isoweekday() not in [4, 5] and 15 >= now.hour >= 8
