from typing import Final
from pytz import timezone

KST: Final["tz"] = timezone("Asia/Seoul")

def str_to_datetime(date_str: str) -> "datetime":
    from dateutil.parser import parse

    return parse(date_str)

def utc_to_kst(
    utc_date_str: str,
) -> str:
    utc_date = str_to_datetime(utc_date_str)
    kst_date = utc_date.astimezone(KST).date()
    return str(kst_date)