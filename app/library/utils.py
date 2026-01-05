import pandas as pd
import re
from collections import defaultdict
from typing import Optional, Dict, Tuple


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
        return True, match.group(1)
    return False, None


def extract_payment_id(text: str) -> Tuple[bool, Optional[str]]:
    """Extracts payment/check ID from text."""
    # Look for patterns like #123456 or similar
    match = re.search(r"#(\d+)", text)
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
                return {"1997": "C991997", "0000": "Unknown"}

            store_names = defaultdict(str)
            for x, y in zip(long_col, short_col):
                formatted_key = key_formatter(y)
                if formatted_key != "-1":
                    store_names[formatted_key] = str(x).strip()
        else:
            store_names = defaultdict(str)

        # add missed out fields
        missed = {"1997": "C991997", "0000": "Unknown"}
        for x, y in missed.items():
            store_names[x] = y

        return dict(store_names)
    except Exception as e:
        print(f"Error reading stores: {e}")
        return {"1997": "C991997", "0000": "Unknown"}
