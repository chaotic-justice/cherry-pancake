from collections.abc import Sequence
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import re

import numpy as np
import pandas as pd

from .utils import is_within_ndays


@dataclass(frozen=True)
class MonthFrames:
    month: str
    ap: pd.DataFrame
    vnb: pd.DataFrame


_AP_COLUMNS = {"checkAmount", "checkNumber", "checkDate", "name", "vendorNumber"}
_VNB_COLUMNS = {"postDate", "check", "description", "debit", "credit"}

_AR_COLUMNS = {
    "recordId",
    "sourceFile",
    "sheet",
    "row",
    "date",
    "customer",
    "amount",
    "depositTotal",
    "checkRef",
    "depositRef",
    "invoiceRef",
    "batchRef",
    "authRef",
}

_INTERNAL_TRANSFER = re.compile(
    r"PHONE/INTERNET TRNFR.*FUNDS TRANSFER (?:FRM|TO) DEP", re.IGNORECASE
)


def reconcile_receivables(
    months: Sequence[MonthFrames], receivables: pd.DataFrame
) -> list[MonthFrames]:
    """Match bank credits to normalized AR records in conservative rule order."""
    if not receivables.empty:
        _require_columns("AR", "records", receivables, _AR_COLUMNS)
    candidates = _ar_candidates(receivables)
    used: set[str] = set()
    results = []

    for month in months:
        vnb = month.vnb.copy()
        for column, default in {
            "arStatus": "not applicable",
            "arSource": "",
            "arReference": "",
            "arRule": "",
            "arNotes": "",
            "arDate": "",
            "arDateLag": "",
            "arWarning": "",
        }.items():
            vnb[column] = default

        for index, bank in vnb.iterrows():
            amount = _money(bank.get("credit"))
            description = str(bank.get("description") or "")
            if amount is None or amount <= 0:
                continue
            if _INTERNAL_TRANSFER.search(description):
                vnb.loc[index, "arStatus"] = "internal transfer"
                vnb.loc[index, "arRule"] = "Internal transfer exclusion"
                vnb.loc[index, "arNotes"] = (
                    "Excluded because the bank description identifies an internal transfer."
                )
                continue

            match = _match_bank_credit(bank, candidates, used)
            for column, value in match.items():
                if column != "memberIds":
                    vnb.loc[index, column] = value
            if match["arStatus"] == "matched":
                used.update(match.pop("memberIds", ()))

        results.append(MonthFrames(month.month, ap=month.ap, vnb=vnb))
    return results


def _ar_candidates(receivables: pd.DataFrame) -> list[dict]:
    if receivables.empty:
        return []
    details = []
    for _, row in receivables.iterrows():
        amount = _money(row["amount"])
        if amount is None or amount <= 0:
            continue
        details.append(_candidate([row], amount, "detail", row["sheet"]))

    groups = []
    for sheet, field, label in (
        ("CHECK", "depositRef", "deposit"),
        ("JGI CC", "batchRef", "batch"),
    ):
        rows = receivables[
            (receivables["sheet"] == sheet) & (receivables[field].astype(str) != "")
        ]
        for (_, reference), group in rows.groupby(["sourceFile", field], sort=False):
            amount = round(sum(filter(None, (_money(value) for value in group["amount"]))), 2)
            if amount > 0:
                groups.append(_candidate(group.to_dict("records"), amount, label, sheet, reference))

    for source_file, rows in receivables[receivables["sheet"] == "COSNEXT"].groupby(
        "sourceFile", sort=False
    ):
        pending = []
        for _, row in rows.sort_values("row").iterrows():
            pending.append(row)
            total = _money(row["depositTotal"])
            if total is not None and total > 0:
                groups.append(
                    _candidate(pending, total, "deposit total", "COSNEXT", row["depositRef"])
                )
                pending = []
    return details + groups


def _candidate(rows, amount: float, kind: str, sheet: str, reference="") -> dict:
    rows = list(rows)
    last = rows[-1]
    refs = []
    for row in rows:
        for field in ("checkRef", "depositRef", "invoiceRef", "batchRef", "authRef"):
            value = str(row[field] or "").strip()
            if value and value.lower() != "nan" and value not in refs:
                refs.append(value)
    if reference and str(reference) not in refs:
        refs.insert(0, str(reference))
    dates = [value for value in (row["date"] for row in rows) if isinstance(value, date)]
    return {
        "sheet": sheet,
        "sourceFile": last["sourceFile"],
        "row": int(last["row"]),
        "date": max(dates) if dates else None,
        "customer": str(last["customer"] or ""),
        "amount": amount,
        "refs": tuple(refs),
        "memberIds": tuple(str(row["recordId"]) for row in rows),
        "kind": kind,
    }


def _match_bank_credit(bank: pd.Series, candidates: list[dict], used: set[str]) -> dict:
    amount = _money(bank["credit"])
    bank_date = bank.get("postDate")
    description = str(bank.get("description") or "")
    available = [candidate for candidate in candidates if not used.intersection(candidate["memberIds"])]

    referenced = [candidate for candidate in candidates if _embedded_reference(description, candidate["refs"])]
    if referenced:
        same_amount = _distinct_candidates(
            [candidate for candidate in referenced if candidate["amount"] == amount]
        )
        unused = [
            candidate
            for candidate in same_amount
            if not used.intersection(candidate["memberIds"])
        ]
        if len(unused) == 1:
            return _matched(unused[0], "Embedded reference and exact amount", bank_date)
        if len(unused) > 1:
            return _review(
                "Duplicate reference and amount",
                "More than one unused AR record has both the referenced identifier and the bank-credit amount.",
                unused,
            )
        if same_amount:
            return _review(
                "Referenced record already used",
                "An AR record with this reference and amount was already matched to another bank credit.",
                same_amount,
            )
        return _review(
            "Referenced amount differs",
            "The bank description contains an AR reference, but the referenced AR amount does not equal the bank credit.",
            referenced,
        )

    expected = _expected_ar_source(description)
    grouped = [
        candidate
        for candidate in available
        if candidate["kind"] != "detail"
        and candidate["sheet"] == expected
        and candidate["amount"] == amount
        and _settled_within(bank_date, candidate["date"])
    ]
    if len(grouped) == 1:
        return _matched(grouped[0], "Batch/deposit total, source, amount, and date", bank_date)
    if len(grouped) > 1:
        return _review(
            "Multiple batch/deposit totals",
            "More than one unused AR batch or deposit matches the expected source, amount, and 0–4 day settlement window.",
            grouped,
        )

    settled = [
        candidate
        for candidate in available
        if candidate["kind"] == "detail"
        and candidate["sheet"] == expected
        and candidate["amount"] == amount
        and _settled_within(bank_date, candidate["date"])
    ]
    if len(settled) == 1:
        return _matched(settled[0], "Source, exact amount, and 0–4 day lag", bank_date)
    if len(settled) > 1:
        return _review(
            "Multiple source/amount/date matches",
            "More than one unused AR record matches the expected source, amount, and 0–4 day settlement window.",
            settled,
        )

    named = [
        candidate
        for candidate in available
        if candidate["kind"] == "detail"
        and candidate["sheet"] in {"CHECK", "INTERNET"}
        and candidate["amount"] == amount
        and candidate["date"] == bank_date
        and _payor_in_description(candidate["customer"], description)
    ]
    if len(named) == 1:
        return _matched(named[0], "Payor, exact amount, and date", bank_date)
    if len(named) > 1:
        return _review(
            "Multiple payor/amount/date matches",
            "More than one unused AR record matches the payor, amount, and bank date.",
            named,
        )

    if "AMAZON" in description.upper():
        return _review(
            "Missing Amazon settlement identifiers",
            "The uploaded AR workbooks do not include the Amazon settlement identifier or total needed for a safe match.",
        )
    if expected:
        note = f"No unused {expected} record has this amount within the 0–4 day settlement window."
    else:
        note = "No embedded reference, expected AR source, or same-day payor produced a safe match."
    return _review("No safe match", f"{note} Amount-only matching is disabled.")


def _matched(candidate: dict, rule: str, bank_date) -> dict:
    reference = next(iter(candidate["refs"]), "")
    ar_date = candidate["date"]
    lag = (bank_date - ar_date).days if isinstance(bank_date, date) and isinstance(ar_date, date) else None
    warning = ""
    if lag is None:
        warning = "AR date is missing or invalid."
    elif lag < 0:
        days = abs(lag)
        warning = f"AR date {ar_date.isoformat()} is {days} {'day' if days == 1 else 'days'} after bank date {bank_date.isoformat()}."
    elif lag > 4:
        warning = f"AR date {ar_date.isoformat()} is {lag} days before bank date {bank_date.isoformat()}; expected 0–4 days."
    return {
        "arStatus": "matched",
        "arSource": candidate["sheet"],
        "arReference": reference,
        "arRule": rule,
        "arNotes": f"Matched to {candidate['sourceFile']}, {candidate['sheet']} row {candidate['row']}.",
        "arDate": ar_date.isoformat() if isinstance(ar_date, date) else "",
        "arDateLag": lag if lag is not None else "",
        "arWarning": warning,
        "memberIds": candidate["memberIds"],
    }


def _review(rule: str, note: str, candidates=()) -> dict:
    candidates = _distinct_candidates(list(candidates))
    sources = ", ".join(dict.fromkeys(candidate["sheet"] for candidate in candidates))
    references = ", ".join(
        dict.fromkeys(
            reference
            for candidate in candidates
            for reference in candidate["refs"][:1]
            if reference
        )
    )
    dates = list(
        dict.fromkeys(
            candidate["date"] for candidate in candidates if isinstance(candidate["date"], date)
        )
    )
    if candidates:
        locations = "; ".join(
            f"{candidate['sourceFile']}, {candidate['sheet']} row {candidate['row']}"
            for candidate in candidates
        )
        note = f"{note} Candidate{'s' if len(candidates) != 1 else ''}: {locations}."
    return {
        "arStatus": "needs review",
        "arSource": sources,
        "arReference": references,
        "arRule": rule,
        "arNotes": note,
        "arDate": dates[0].isoformat() if len(dates) == 1 else "",
        "arDateLag": "",
        "arWarning": "",
    }


def _expected_ar_source(description: str) -> str | None:
    text = description.upper()
    if "8752044092" in text or ("MERCH" in text and "BNKCD DEPOSIT" in text):
        return "JGI CC"
    if "5911223280" in text or "COSTCO WHOLESALE" in text:
        return "CHECK"
    if "SHOPIFY COSTCO" in text:
        return "COSNEXT"
    if "SHOPIFYPMT" in text or "SHOPIFY TRANSFER" in text or "4270465600" in text:
        return "INTERNET"
    if text.strip() == "DEPOSIT":
        return "CHECK"
    return None


def _embedded_reference(description: str, references: tuple[str, ...]) -> bool:
    compact = re.sub(r"[^A-Z0-9]", "", description.upper())
    for reference in references:
        value = re.sub(r"[^A-Z0-9]", "", reference.upper())
        minimum = 7 if value.isdigit() else 4
        if len(value) >= minimum and value in compact:
            return True
    return False


def _payor_in_description(customer: str, description: str) -> bool:
    ignored = {
        "CASH", "CHECK", "COSTCO", "CUSTOMER", "INC", "INTERNET", "PAYMENT",
        "PHONE", "SHOP", "SHOPIFY",
    }
    tokens = [
        token
        for token in re.findall(r"[A-Z0-9]+", customer.upper())
        if len(token) >= 4 and token not in ignored
    ]
    text = set(re.findall(r"[A-Z0-9]+", description.upper()))
    return bool(tokens) and any(token in text for token in tokens)


def _distinct_candidates(candidates: list[dict]) -> list[dict]:
    unique = {}
    for candidate in candidates:
        key = (candidate["sheet"], candidate["amount"], candidate["memberIds"])
        unique.setdefault(key, candidate)
    return list(unique.values())


def _settled_within(bank_date, ar_date) -> bool:
    return isinstance(bank_date, date) and isinstance(ar_date, date) and 0 <= (bank_date - ar_date).days <= 4


def _money(value) -> float | None:
    try:
        if pd.isna(value):
            return None
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def reconcile_months(months: Sequence[MonthFrames]) -> list[MonthFrames]:
    """Reconcile normalized monthly frames in the supplied chronological order."""
    months = list(months)
    identifiers = [month.month for month in months]
    if any(not isinstance(identifier, str) or not identifier for identifier in identifiers):
        raise ValueError("month identifiers must be non-empty strings")
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("month identifiers must be unique")

    nodes = []
    for month in months:
        _require_columns(month.month, "AP", month.ap, _AP_COLUMNS)
        _require_columns(month.month, "VNB", month.vnb, _VNB_COLUMNS)
        nodes.append(_MonthMatcher(month.month, month.ap, month.vnb))

    results = []
    previous_split_txns = []
    for index, node in enumerate(nodes):
        next_vnb = nodes[index + 1].vnb if index + 1 < len(nodes) else None
        vnb, ap, previous_split_txns = node.find_matching_rows(
            previous_split_txns=previous_split_txns,
            next_vnb=next_vnb,
        )
        results.append(MonthFrames(node.month, ap=ap, vnb=vnb))
    return results


def _require_columns(
    month: str,
    sheet: str,
    frame: pd.DataFrame,
    required: set[str],
) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{month} {sheet} is missing columns: {', '.join(missing)}")


class _MonthMatcher:
    def __init__(self, month: str, ap: pd.DataFrame, vnb: pd.DataFrame):
        self.month = month
        self.ap = ap.copy(deep=True)
        self.vnb = vnb.copy(deep=True)
        self.statuses = {
            "no": "no match",
            "one": "matched",
            "split": "split match",
            "partial": "partial match",
        }
    def __ignored_rows(self):
            pattern = r"^PHONE/INTERNET TRNFR.*FUNDS TRANSFER TO DEP \d+3411"
            res = self.vnb[self.vnb["description"].str.contains(pattern, case=False)]
            return res.index
    
    def find_split_txns(
        self,
        ap_cell,
        next_vnb,
        unavailable_current=(),
        unavailable_next=(),
    ):
            res = {
            "notes": "No NET PR split was found in the current or following month's VNB transactions.",
            "row_idx": ap_cell.name,
            "ap_source_row": int(ap_cell["sourceRow"]),
            "month": self.month,
                "matched_indices": [],  # tuples of (index, boolean) where bool indicates whether txn comes from the next month
            }
    
            date_mask = self.vnb["postDate"].apply(
                lambda x: is_within_ndays(x, ap_cell["checkDate"])
            )
            str_mask = self.vnb["description"].str.contains(
                "ADP WAGE", case=False, regex=True
            )
    
            filtered_df = self.vnb[
                date_mask & str_mask & ~self.vnb.index.isin(unavailable_current)
            ]
            if filtered_df.empty:
                return res
            txn1 = filtered_df.iloc[0]
    
            diff = np.round(ap_cell["checkAmount"] - txn1["debit"], decimals=2)
            k = 2 if next_vnb is not None else 1
    
            # two loops to check for the diff amount in current/next month
            # will always exist in either one
            while k:
                if k > 1:
                    txn2_index = np.where(next_vnb["debit"] == diff)[0]
                    txn2_index = np.array(
                        [index for index in txn2_index if index not in unavailable_next]
                    )
                else:
                    txn2_index = np.where(self.vnb["debit"] == diff)[0]
                    txn2_index = np.array(
                        [
                            index
                            for index in txn2_index
                            if index not in unavailable_current and index != txn1.name
                        ]
                    )
    
                try:
                    txn2_index = txn2_index[0]
                    txn2 = (
                    next_vnb.iloc[txn2_index]
                        if k > 1
                        else self.vnb.iloc[txn2_index]
                    )
                except:
                    pass
                finally:
                    k -= 1
    
                if txn2_index.size > 0:
                    break
    
            if txn2_index.size == 0:
                return res
    
            res["matched_indices"].append((int(txn1.name), False))
            res["matched_indices"].append((txn2_index, k > 0))
            second_month = str(pd.Period(self.month, freq="M") + 1) if k > 0 else self.month
            res["notes"] = (
                f"Matched NET PR split: VNB {self.month} row {int(txn1['sourceRow'])} "
                f"(${float(txn1['debit']):,.2f}) and VNB {second_month} row "
                f"{int(txn2['sourceRow'])} (${float(txn2['debit']):,.2f})."
            )
            return res
    
    def find_matching_rows(self, previous_split_txns=(), next_vnb=None):
            print(f"\n***MATCHING RESULTS for month {self.month}***")
            credit_values = self.vnb["credit"].values
            debit_values = self.vnb["debit"].values
    
            ap_clone = self.ap.copy()
            ap_clone["status"] = self.statuses["no"]
            ap_clone["matchRule"] = "No safe match"
            ap_clone["notes"] = ""
    
            vnb_clone = self.vnb.copy()
            vnb_clone["status"] = self.statuses["no"]
            vnb_clone["notes"] = ""
    
            matched_vnb = defaultdict(int)
            for split_txn in previous_split_txns:
                for col_idx, is_next in split_txn["matched_indices"]:
                    if is_next:
                        matched_vnb[col_idx] = split_txn["row_idx"]
            cnt = 0
            ignorables = self.__ignored_rows()
    
            for i, row in self.ap.iterrows():
                check = row["checkAmount"]
                matched_indices = np.where(debit_values == check)[0]
                if check < 0:
                    matched_indices = np.where(credit_values == abs(check))[0]
                k = len(matched_indices)
    
                # if there's more than 1 row that matches
                #   compare the check number or postDate vs checkDate
                if len(matched_indices) > 1:
                    apCheckNum = row["checkNumber"]
                    apCheckDate = row["checkDate"]
                    rowName = row["name"]

                    for j in matched_indices:
                        if j in matched_vnb or j in ignorables:
                            continue
    
                        vnbCheckNum = self.vnb.loc[j, "check"]
                        vnbPostDate = self.vnb.iloc[j]["postDate"]
    
                        if vnbCheckNum == apCheckNum:
                            ap_clone.loc[i, "status"] = self.statuses["one"]
                            ap_clone.loc[i, "matchRule"] = "Amount and check number"
                            ap_clone.loc[i, "notes"] = (
                                f"Matched VNB row {int(self.vnb.loc[j, 'sourceRow'])} because the amount and check number match."
                            )
                            vnb_clone.loc[j, "status"] = self.statuses["one"]
                            vnb_clone.loc[j, "notes"] = (
                                f"Matched AP row {int(row['sourceRow'])} because the amount and check number match."
                            )
                            k -= 1
                            if j in matched_vnb:
                                raise AssertionError(
                                    f"index j already seen before: {j}, {matched_vnb[j]}"
                                )
                            matched_vnb[j] = i
                            cnt += 1
                            break
    
                        # Configured V&J/PayPal exception; dates need not match.
                        if (
                            rowName.lower().startswith("v & j")
                            and j not in matched_vnb
                            and "paypal" in vnb_clone.loc[j, "description"].lower()
                        ):
                            ap_clone.loc[i, "status"] = self.statuses["one"]
                            ap_clone.loc[i, "matchRule"] = "Configured V&J/PayPal exception"
                            ap_clone.loc[i, "notes"] = (
                            f"Matched VNB row {int(self.vnb.loc[j, 'sourceRow'])} because the configured V&J/PayPal exception applies."
                            )
                            vnb_clone.loc[j, "status"] = self.statuses["one"]
                            vnb_clone.loc[j, "notes"] = (
                            f"Matched AP row {int(row['sourceRow'])} because the configured V&J/PayPal exception applies."
                            )
                            matched_vnb[j] = i
                            cnt += 1
                            k -= 1
                            break
    
                        if vnbPostDate == apCheckDate:
                            if j in matched_vnb:
                                print(f'DEBUG: {row}')
                                print(f'dates: {vnbPostDate} vs {apCheckDate}')
                                print(f"DEBUG: {vnb_clone.loc[j, 'notes']}")
                                print(f'DEBUG: {ap_clone.loc[i, "notes"]}')
                                print(f'DEBUG: vnb row: {j+2}')
                                raise AssertionError(
                                    f"index {j} already seen before in sheet1: {row}\n see prev match: {matched_vnb[j]}"
                                )
                            ap_clone.loc[i, "status"] = self.statuses["one"]
                            ap_clone.loc[i, "matchRule"] = "Amount and date"
                            ap_clone.loc[i, "notes"] = (
                            f"Matched VNB row {int(self.vnb.loc[j, 'sourceRow'])} because the amount and date match."
                            )
                            vnb_clone.loc[j, "status"] = self.statuses["one"]
                            vnb_clone.loc[j, "notes"] = (
                            f"Matched AP row {int(row['sourceRow'])} because the amount and date match."
                            )
                            matched_vnb[j] = i
                            cnt += 1
                            k -= 1
                            break
    
                    if k == len(matched_indices):
                        ap_clone.loc[i, "status"] = self.statuses["no"]
                        ap_clone.loc[i, "matchRule"] = "Ambiguous amount"
                        matched_rows = ", ".join(
                            str(int(self.vnb.loc[index, "sourceRow"])) for index in matched_indices
                        )
                        ap_clone.loc[i, "notes"] = (
                            f"Found {len(matched_indices)} VNB rows with this amount ({matched_rows}), but none matched the AP check number or date."
                        )
                        print(
                            f"multiple amount matches but neither dates nor check nums matched; name: {rowName}, amt: {check}"
                        )
                else:  # only one match
                    for j in matched_indices:
                        if j in matched_vnb:
                            continue
                        # check if there exist multiple txns in AP that match the same amount in sheet1, if so, scope it down by comparing the check numbers
                        dupes = self.ap[self.ap["checkAmount"] == check]
                        if dupes.shape[0] > 1:
                            # print(
                            #     f"duplicates at rows {', '.join(str(di) for di in dupes.index.to_numpy())}: ${row['checkAmount']}"
                            # )
                            if all(
                                drow["vendorNumber"].lower().startswith("passaic")
                                for _, drow in dupes.iterrows()
                            ):
                                # ignoring passaic dupes...
                                continue
                            else:
                                vnbCheckNum = self.vnb.loc[j, "check"]
                                if row["checkNumber"] == vnbCheckNum:
                                    ap_clone.loc[i, "status"] = self.statuses["one"]
                                    ap_clone.loc[i, "matchRule"] = "Amount and check number"
                                    ap_clone.loc[i, "notes"] = (
                                    f"Matched VNB row {int(self.vnb.loc[j, 'sourceRow'])} because the amount and check number match."
                                    )
                                    vnb_clone.loc[j, "status"] = self.statuses["one"]
                                    vnb_clone.loc[j, "notes"] = (
                                    f"Matched AP row {int(row['sourceRow'])} because the amount and check number match."
                                    )
                                    matched_vnb[j] = i
                                    cnt += 1
                                else:
                                    ap_clone.loc[i, "matchRule"] = "Ambiguous amount"
                                    ap_clone.loc[i, "notes"] = (
                                    f"VNB row {int(self.vnb.loc[j, 'sourceRow'])} has the same amount, but multiple AP payments share it and the check number did not match."
                                    )
                                # if dupes exist and yet check numbers don't match, skip it
                                continue
    
                        ignored = j in ignorables
                        if ignored:
                            print(
                                f"check amount matched but row {j} should be ignored\n because phone/internet && ****3411"
                            )
                            continue
    
                        # exactly 1 to 1, no need to compare check number or date
                        ap_clone.loc[i, "status"] = self.statuses["one"]
                        ap_clone.loc[i, "matchRule"] = "Unique amount"
                        ap_clone.loc[i, "notes"] = (
                        f"Matched VNB row {int(self.vnb.loc[j, 'sourceRow'])} because it was the only available VNB transaction with the same amount."
                        )
                        vnb_clone.loc[j, "status"] = self.statuses["one"]
                        vnb_clone.loc[j, "notes"] = (
                        f"Matched AP row {int(row['sourceRow'])} because it was the only available AP payment with the same amount."
                        )
                        matched_vnb[j] = i
                        cnt += 1
    
            # attempt to match NET PR split-txns
            net_pr_rows = ap_clone[
                (ap_clone["vendorNumber"].str.contains("net pr", case=False))
                & (ap_clone["status"] == self.statuses["no"])
            ]
            unavailable_current = set(matched_vnb)
            unavailable_next = set()
            split_txns = []
            for _, net_pr_row in net_pr_rows.iterrows():
                split_txn = self.find_split_txns(
                    net_pr_row,
                    next_vnb,
                    unavailable_current,
                    unavailable_next,
                )
                split_txns.append(split_txn)
                for col_idx, is_next in split_txn["matched_indices"]:
                    (unavailable_next if is_next else unavailable_current).add(col_idx)
            for split_txn in split_txns:
                row_idx = split_txn["row_idx"]
                if len(split_txn["matched_indices"]) > 0:
                    cnt += 1
                    ap_clone.loc[row_idx, "status"] = self.statuses["split"]
                    ap_clone.loc[row_idx, "matchRule"] = "NET PR split across VNB transactions"
                    for col_idx, is_next in split_txn["matched_indices"]:
                        if not is_next:
                            vnb_clone.loc[col_idx, "status"] = self.statuses["partial"]
                            vnb_clone.loc[col_idx, "notes"] = (
                                f"Part of the NET PR split matched to AP {self.month} row {split_txn['ap_source_row']}."
                            )
                else:
                    ap_clone.loc[row_idx, "matchRule"] = "NET PR split not found"
                ap_clone.loc[row_idx, "notes"] = split_txn["notes"]
    
            # Apply split annotations carried from the previous month.
            if previous_split_txns:
                for split_txn in previous_split_txns:
                    row_idx = split_txn["row_idx"]
                    if len(split_txn["matched_indices"]) > 0:
                        for col_idx, is_next in split_txn["matched_indices"]:
                            if is_next:
                                vnb_clone.loc[col_idx, "status"] = self.statuses["partial"]
                                vnb_clone.loc[col_idx, "notes"] = (
                                    f"Part of the NET PR split matched to AP {split_txn['month']} row {split_txn['ap_source_row']}."
                                )
    
            amin_fee_mask = (self.ap["name"].str.contains("ascendant", case=False)) & (
                self.ap["checkAmount"] == 25
            )
            amin_name_mask = self.ap["name"].str.contains("amin", case=False)
            amino_rows = ap_clone[
                (amin_name_mask | amin_fee_mask)
                & (ap_clone["status"] == self.statuses["no"])
            ]
    
            # amin rows must be 2 for exact match
            # else if in feb, only 1 row be a partial match
            if amino_rows.shape[0] > 0:
                is_full_match = amino_rows.shape[0] == 2
                target_debit = (
                    np.sum(amino_rows["checkAmount"])
                    if amino_rows.shape[0] == 2
                    else amino_rows["checkAmount"].values[0] + 25
                )
                vnb_amino = self.vnb[
                    (self.vnb["debit"] == target_debit)
                    & ~self.vnb.index.isin(unavailable_current)
                ]
                if not vnb_amino.empty:
                    vnb_amino_idx = vnb_amino.index.to_numpy()[0]
                    unavailable_current.add(vnb_amino_idx)
                    for amino_idx, _ in amino_rows.iterrows():
                        ap_clone.loc[amino_idx, "status"] = self.statuses["partial"]
                        ap_clone.loc[amino_idx, "matchRule"] = "Configured AMIN/AFX grouping"
                        ap_clone.loc[amino_idx, "notes"] = (
                            f"Matched VNB row {int(self.vnb.loc[vnb_amino_idx, 'sourceRow'])} as part of the configured AMIN/AFX grouping."
                        )
                    amino_indices_str = " and ".join(
                        str(int(value)) for value in amino_rows["sourceRow"]
                    )
                    vnb_clone.loc[vnb_amino_idx, "status"] = (
                        self.statuses["split"]
                        if is_full_match
                        else self.statuses["partial"]
                    )
                    vnb_clone.loc[vnb_amino_idx, "notes"] = (
                        f"Matched AP rows {amino_indices_str} using the configured AMIN/AFX grouping."
                    )

            unmatched_ap = (ap_clone["status"] == self.statuses["no"]) & (ap_clone["notes"] == "")
            ap_clone.loc[unmatched_ap, "notes"] = "No VNB transaction safely matched this AP payment."
            unmatched_vnb = (vnb_clone["status"] == self.statuses["no"]) & (vnb_clone["notes"] == "")
            vnb_clone.loc[unmatched_vnb, "notes"] = "No AP payment safely matched this VNB transaction."
    
            print(
                f"{len(vnb_clone[vnb_clone['status'] != self.statuses['no']])} out of {len(vnb_clone)} matched for sheet1"
            )
            print(
                f"{len(ap_clone[ap_clone['status'] != self.statuses['no']])} out of {len(ap_clone)} matched for AP"
            )
            return vnb_clone, ap_clone, list(split_txns)
