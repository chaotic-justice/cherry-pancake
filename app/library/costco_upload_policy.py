from dataclasses import dataclass


@dataclass(frozen=True)
class CostcoUploadPolicy:
    max_report_files: int
    max_file_bytes: int
    report_suffixes: tuple[str, ...]
    store_update_suffixes: tuple[str, ...]

    @property
    def max_file_size_mb(self) -> int:
        return self.max_file_bytes // (1024 * 1024)

    def too_many_reports_message(self) -> str:
        return f"Choose no more than {self.max_report_files} Costco PDF files."

    def oversized_file_message(self, label: str) -> str:
        return f"{label} must be {self.max_file_size_mb} MB or smaller."


COSTCO_UPLOAD_POLICY = CostcoUploadPolicy(
    max_report_files=30,
    max_file_bytes=5 * 1024 * 1024,
    report_suffixes=(".pdf",),
    store_update_suffixes=(".csv", ".xls", ".xlsx"),
)
