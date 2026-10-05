import json
import re
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Self

import pandas as pd

from app.library.costco_upload_policy import COSTCO_UPLOAD_POLICY


_BUNDLED_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "costco-stores.json"
)
_UNKNOWN_KEY = "0000"
_UNKNOWN_NAME = "Unknown"
_AUTHORITATIVE_OVERRIDES = {
    "0766": "C11#0766",
    "1994": "C991994",
    "1997": "C991997",
    _UNKNOWN_KEY: _UNKNOWN_NAME,
}


@dataclass(frozen=True)
class StoreCatalogUpdate:
    filename: str
    content: bytes


@dataclass(frozen=True)
class StoreResolution:
    store_key: str
    store_name: str
    known: bool


class StoreCatalogError(ValueError):
    """A catalog or store-update error safe to translate for the caller."""


@dataclass(frozen=True)
class CostcoStoreCatalog:
    _stores: Mapping[str, str]

    @classmethod
    def load(cls, update: StoreCatalogUpdate | None = None) -> Self:
        stores = _load_bundled_catalog()
        if update is not None:
            stores.update(_parse_update(update))
        stores.update(_AUTHORITATIVE_OVERRIDES)
        return cls(MappingProxyType(stores))

    def resolve_invoice(self, invoice_number: str) -> StoreResolution:
        invoice = str(invoice_number or "")
        trailer_widths = (6, 7) if len(invoice) >= 11 else (6,)

        for trailer_width in trailer_widths:
            store_key = _extract_store_key(invoice, self._stores, trailer_width)
            if store_key != _UNKNOWN_KEY:
                return StoreResolution(
                    store_key,
                    self._stores[store_key],
                    known=True,
                )

        return StoreResolution(_UNKNOWN_KEY, _UNKNOWN_NAME, known=False)


def _load_bundled_catalog() -> dict[str, str]:
    try:
        with _BUNDLED_CATALOG_PATH.open(encoding="utf-8") as catalog_file:
            raw_catalog = json.load(catalog_file)
    except (OSError, json.JSONDecodeError) as error:
        raise StoreCatalogError("Could not read the bundled Costco store catalog.") from error

    if not isinstance(raw_catalog, dict) or not raw_catalog:
        raise StoreCatalogError("The bundled Costco store catalog is invalid.")

    stores: dict[str, str] = {}
    for key, name in raw_catalog.items():
        if (
            not isinstance(key, str)
            or re.fullmatch(r"\d{4}", key) is None
            or not isinstance(name, str)
            or not name.strip()
        ):
            raise StoreCatalogError("The bundled Costco store catalog is invalid.")
        stores[key] = name.strip()
    return stores


def _parse_update(update: StoreCatalogUpdate) -> dict[str, str]:
    filename = update.filename.strip() if isinstance(update.filename, str) else ""
    suffix = Path(filename).suffix.lower()
    if suffix not in COSTCO_UPLOAD_POLICY.store_update_suffixes:
        raise StoreCatalogError("Store updates must be CSV, XLS, or XLSX files.")
    if not isinstance(update.content, bytes) or not update.content:
        raise StoreCatalogError("Could not read the store update. Upload a valid file.")

    try:
        if suffix == ".csv":
            frame = pd.read_csv(BytesIO(update.content), header=None, dtype=object)
        else:
            frame = pd.read_excel(BytesIO(update.content), header=None, dtype=object)
    except Exception as error:
        raise StoreCatalogError(
            "Could not read the store update. Upload a valid CSV, XLS, or XLSX file."
        ) from error

    if len(frame.columns) < 2:
        raise StoreCatalogError("Store updates must contain at least two columns.")

    customer_column = frame.iloc[:, 0]
    store_column = frame.iloc[:, 2] if len(frame.columns) >= 3 else frame.iloc[:, 1]
    stores: dict[str, str] = {}
    for customer_value, store_value in zip(customer_column, store_column):
        store_key = _normalize_store_key(store_value)
        customer_code = _normalize_customer_code(customer_value)
        if store_key is not None and customer_code is not None:
            stores[store_key] = customer_code
    return stores


def _normalize_store_key(value: object) -> str | None:
    if pd.isna(value):
        return None
    candidate = str(value).strip()
    if candidate.startswith("#"):
        candidate = candidate[1:]
    if not candidate.isdigit():
        return None
    return candidate.zfill(4)


def _normalize_customer_code(value: object) -> str | None:
    if pd.isna(value):
        return None
    customer_code = str(value).strip()
    if not customer_code:
        return None
    return re.sub(
        r"#(\d+)$",
        lambda match: f"#{match.group(1).zfill(4)}",
        customer_code,
    )


def _extract_store_key(
    invoice_number: str,
    stores: Mapping[str, str],
    trailer_width: int,
) -> str:
    if not invoice_number or len(invoice_number) <= trailer_width:
        return _UNKNOWN_KEY

    digits = re.findall(r"\d+", invoice_number[:-trailer_width])
    if not digits:
        return _UNKNOWN_KEY

    candidate = digits[0].lstrip("0").zfill(4)
    if candidate in stores and candidate != _UNKNOWN_KEY:
        return candidate

    without_leading_zeroes = candidate.lstrip("0")
    if without_leading_zeroes in stores:
        return without_leading_zeroes

    without_trailing_zeroes = candidate.rstrip("0").zfill(4)
    if without_trailing_zeroes in stores and without_trailing_zeroes != _UNKNOWN_KEY:
        return without_trailing_zeroes

    return _UNKNOWN_KEY
