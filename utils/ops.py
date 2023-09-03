from datetime import datetime

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def date_from_string(date_str, date_format=DATE_FORMAT) -> datetime:
    return datetime.strptime(date_str, date_format)


def date_to_string(date_obj: datetime, date_format=DATE_FORMAT) -> str:
    return datetime.strftime(date_obj, date_format)
