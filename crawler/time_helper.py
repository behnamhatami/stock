from datetime import datetime, time

from django.utils import timezone
from django.utils.timezone import make_aware
from persiantools.jdatetime import JalaliDate


def convert_date_time_string_to_datetime(datetime_string):
    date_part, time_part = datetime_string.split()
    return make_aware(datetime.combine(convert_date_string_to_date(date_part), convert_time_string_to_time(time_part)))


def convert_date_string_to_date(date_string):
    date_parts = date_string.split('/') if '/' in date_string else (
        date_string[:-4], date_string[-4:-2], date_string[-2:])
    date_parts = list(map(int, date_parts))
    if 0 <= date_parts[0] <= 31 < date_parts[2]:
        date_parts.reverse()

    if date_parts[0] <= 100:
        date_parts[0] += 1400 if date_parts[0] <= 50 else 1300

    return JalaliDate(*date_parts).to_gregorian()


def convert_integer_to_parts(int_format: int, parts: list[int] = (4, 2)) -> list[int]:
    parts = list(parts)
    parts.append(0)
    result: list[int] = [int_format // 10 ** parts[0]]
    for index in range(len(parts) - 1):
        result.append(int_format % 10 ** parts[index] // 10 ** parts[index + 1])
    return result


def convert_time_integer_to_time(time_integer: int) -> time:
    return time(*convert_integer_to_parts(time_integer))


def convert_time_string_to_time(string):
    hour, minute, second = string.split(":")
    return time(hour=int(hour), minute=int(minute), second=int(second))


def is_active_hour():
    now = timezone.localtime(timezone.now())
    return now.isoweekday() not in [4, 5] and 17 >= now.hour >= 8
