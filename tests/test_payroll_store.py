import tempfile
import unittest
from pathlib import Path

from app.library.payroll_store import PayrollStore, StoredWorkbook


class PayrollStoreTest(unittest.TestCase):
    def test_keeps_revisions_and_reopens_latest_workbooks(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payroll.db"
            store = PayrollStore(path)
            original = StoredWorkbook("01.12.26.xlsx", b"original")
            revision = StoredWorkbook("01.12.26.xlsx", b"revision")

            store.save((original,))
            store.save((original,))
            store.save((revision,))
            store.save((original,))

            reopened = PayrollStore(path)
            self.assertEqual(reopened.current(), (original,))
            self.assertEqual(reopened.stats().current_workbooks, 1)
            self.assertEqual(reopened.stats().revisions, 2)


if __name__ == "__main__":
    unittest.main()
