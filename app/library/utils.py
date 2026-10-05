import re
from datetime import date
from typing import Optional, Tuple


def is_within_ndays(d1, d2, n=2):
    return abs((d1 - d2).days) <= n


def get_today_date():
    today = date.today()
    return today.strftime("%m-%d-%y")


def to_camel_case(text: str) -> str:
    """Convert text such as `Invoice Number` to `invoiceNumber`."""
    if not text:
        return ""
    words = text.split()
    if not words:
        return ""
    return words[0].lower() + "".join(word.capitalize() for word in words[1:])


def extract_mm_dd(text: str) -> Tuple[bool, Optional[str]]:
    """Extract MM/DD from text such as `Date: 01/04/2026`."""
    match = re.search(r"(\d{1,2}/\d{1,2})", text)
    if match:
        return True, match.group(1).replace("/", "-")
    return False, None


def extract_payment_id(text: str) -> Tuple[bool, Optional[str]]:
    """Extract a payment or check ID from text."""
    match = re.search(r"(\d+)", text)
    if match:
        return True, match.group(1)
    return False, None
