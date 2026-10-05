import json
import re
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Tuple

import pandas as pd


DEFAULT_STORE_MAPPING_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "costco-stores.json"
)
AUTHORITATIVE_STORE_OVERRIDES = {
    "0766": "C11#0766",
    "1994": "C991994",
    "1997": "C991997",
    "0000": "Unknown",
}


def is_within_ndays(d1, d2, n=2):
    return abs((d1 - d2).days) <= n


def get_today_date():
    today = date.today()
    return today.strftime("%m-%d-%y")


def to_camel_case(text: str) -> str:
    """Converts 'Invoice Number' to 'invoiceNumber'."""
    if not text:
        return ""
    words = text.split()
    if not words:
        return ""
    return words[0].lower() + "".join(word.capitalize() for word in words[1:])


def extract_mm_dd(text: str) -> Tuple[bool, Optional[str]]:
    """Extracts MM/DD from text like 'Date: 01/04/2026'."""
    match = re.search(r"(\d{1,2}/\d{1,2})", text)
    if match:
        return True, match.group(1).replace("/", "-")
    return False, None


def extract_payment_id(text: str) -> Tuple[bool, Optional[str]]:
    """Extracts payment/check ID from text."""
    # Look for patterns like #123456 or similar
    match = re.search(r"(\d+)", text)
    if match:
        return True, match.group(1)
    return False, None


def extract_key(invoice_num: str, store_names: Dict[str, str], n=-6) -> str:
    z = "0000"
    if not invoice_num:
        return z

    # Remove trailing chars if needed
    res_str = str(invoice_num)[:n]
    digits = re.findall(r"\d+", res_str)

    if not digits:
        return z

    res = digits[0].lstrip("0").zfill(4)

    if res not in store_names:
        lres = res.lstrip("0")
        if lres in store_names:
            return lres
        rres = res.rstrip("0").zfill(4)
        if rres in store_names:
            return rres
        return z

    return res


def get_store_names(
    csv_path: Optional[str] = None, df: Optional[pd.DataFrame] = None
) -> Dict[str, str]:
    def customer_formatter(value) -> str:
        customer = str(value).strip()
        return re.sub(
            r"#(\d+)$",
            lambda match: f"#{match.group(1).zfill(4)}",
            customer,
        )

    def key_formatter(s) -> str:
        s = str(s).strip()
        if not s or s.lower() == "nan":
            return "-1"

        if s.startswith("#"):
            s = s.lstrip("#")

        if s.isdigit():
            # Pad to 4 digits to match Costco's internal keys
            return s.zfill(4)

        return "-1"

    try:
        with DEFAULT_STORE_MAPPING_PATH.open(encoding="utf-8") as mapping_file:
            store_names = defaultdict(str, json.load(mapping_file))

        if df is None and csv_path:
            df = pd.read_csv(csv_path, header=None)

        if df is not None:
            # Costco store-nums.csv usually has Long Name in col 0 and #ID in col 2 or 1
            if len(df.columns) >= 3:
                long_col = df.iloc[:, 0]
                short_col = df.iloc[:, 2]
            elif len(df.columns) >= 2:
                long_col = df.iloc[:, 0]
                short_col = df.iloc[:, 1]
            else:
                return dict(store_names)

            for x, y in zip(long_col, short_col):
                formatted_key = key_formatter(y)
                if formatted_key != "-1":
                    store_names[formatted_key] = customer_formatter(x)

        # Preserve confirmed values when an update file contains conflicting rows.
        for x, y in AUTHORITATIVE_STORE_OVERRIDES.items():
            store_names[x] = y

        return dict(store_names)
    except Exception as e:
        print(f"Error reading stores: {e}")
        return dict(AUTHORITATIVE_STORE_OVERRIDES)
