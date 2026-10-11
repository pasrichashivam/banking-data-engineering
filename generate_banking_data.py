"""Generate realistic banking source CSVs for historical and incremental ETL testing.

The generator models source-system extracts, not a pre-resolved warehouse.  Consumers
can use the effective dates to build SCD-2 customer and account-ownership bridges,
then resolve the correct surrogate keys for transaction and balance facts.
"""

from __future__ import annotations

import argparse
import csv
import random
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


INITIAL_CUSTOMER_COUNT = 30
END_OF_TIME = date(9999, 12, 31)


@dataclass(frozen=True)
class CustomerVersion:
    customer_id: str
    full_name: str
    address_line_1: str
    city: str
    state: str
    postal_code: str
    customer_segment: str
    kyc_status: str
    effective_from_date: date
    source_modified_at: datetime
    source_system: str = "CORE_BANKING"


class BankingDataGenerator:
    """Creates deterministic source extracts with intentional production data conditions."""

    def __init__(self, output_dir: Path, seed: int) -> None:
        self.output_dir = output_dir
        self.random = random.Random(seed)
        self.customers = self._build_customer_history()
        self.branches = self._build_branches()
        self.accounts = self._build_accounts()
        self.ownership_history = self._build_ownership_history()
        self.products = self._build_products()

    def generate_initial(self, as_of_date: date) -> Path:
        """Write a historical backfill of all source tables through ``as_of_date``."""
        target = self.output_dir / "initial_load"
        transactions = self._transactions_for_period(date(2024, 1, 1), as_of_date)
        self._write_csv(target / "customer_history.csv", self._customer_rows(as_of_date))
        self._write_csv(target / "branch.csv", self.branches)
        self._write_csv(target / "account.csv", self.accounts)
        self._write_csv(target / "account_customer_history.csv", self._ownership_rows(as_of_date))
        self._write_csv(target / "product.csv", self.products)
        self._write_csv(target / "transaction.csv", transactions)
        self._write_csv(target / "transaction_line.csv", self._transaction_lines(transactions))
        self._write_csv(target / "daily_account_balance.csv", self._balance_snapshots(as_of_date))
        self._write_manifest(target, as_of_date, "INITIAL", transactions, late_arrivals=0)
        return target

    def generate_daily(self, processing_date: date) -> Path:
        """Write one daily CDC-style batch, including a predictable late-arrival pattern.

        The batch directory is determined solely by the processing date, so rerunning the
        same command recreates the same data and is safe for pipeline test reruns.
        """
        target = self.output_dir / "incremental" / f"processing_date={processing_date.isoformat()}"
        current_changes = self._daily_customer_changes(processing_date)
        ownership_changes = self._daily_ownership_changes(processing_date)
        current_transactions = self._transactions_for_period(processing_date, processing_date)

        late_transactions: list[dict[str, object]] = []
        late_customer_changes: list[dict[str, object]] = []
        if processing_date.weekday() == 1:  # Tuesday delivery of Monday source records.
            late_date = processing_date - timedelta(days=1)
            late_transactions = self._transactions_for_period(late_date, late_date, late_arrival=True)
            late_customer_changes = self._daily_customer_changes(late_date, late_arrival=True)

        all_transactions = current_transactions + late_transactions
        self._write_csv(target / "customer_cdc.csv", current_changes + late_customer_changes)
        self._write_csv(target / "account_customer_cdc.csv", ownership_changes)
        self._write_csv(target / "transaction_cdc.csv", all_transactions)
        self._write_csv(target / "transaction_line_cdc.csv", self._transaction_lines(all_transactions))
        self._write_csv(target / "daily_account_balance.csv", self._balance_snapshots(processing_date))
        self._write_manifest(target, processing_date, "INCREMENTAL", all_transactions, len(late_transactions))
        return target

    def _build_customer_history(self) -> list[CustomerVersion]:
        people = [
            ("Ava Patel", "Boston", "MA"), ("Noah Williams", "Chicago", "IL"),
            ("Mia Chen", "Seattle", "WA"), ("Liam Johnson", "Miami", "FL"),
            ("Emma Garcia", "Dallas", "TX"), ("Oliver Brown", "Denver", "CO"),
            ("Sophia Davis", "Phoenix", "AZ"), ("Ethan Wilson", "Austin", "TX"),
            ("Isabella Moore", "Orlando", "FL"), ("James Taylor", "Houston", "TX"),
        ]
        versions: list[CustomerVersion] = []
        for index in range(INITIAL_CUSTOMER_COUNT):
            name, city, state = people[index % len(people)]
            customer_id = f"C{index + 1:06d}"
            start = date(2023, 1, 1) + timedelta(days=(index * 17) % 300)
            versions.append(self._customer_version(customer_id, name, city, state, start, index))

            # Preserve source history for selected customers, including three versions.
            if index % 7 == 0:
                first_change = date(2024, 2, 1) + timedelta(days=index)
                versions.append(self._customer_version(customer_id, name, "Seattle", "WA", first_change, index + 40))
                second_change = first_change + timedelta(days=90)
                versions.append(self._customer_version(customer_id, name, "Austin", "TX", second_change, index + 80))
            elif index % 5 == 0:
                change_date = date(2024, 6, 1) + timedelta(days=index)
                versions.append(self._customer_version(customer_id, name, "Denver", "CO", change_date, index + 40))
        return versions

    def _customer_version(
        self, customer_id: str, name: str, city: str, state: str, effective_date: date, salt: int
    ) -> CustomerVersion:
        street_number = 100 + salt * 7
        return CustomerVersion(
            customer_id, name, f"{street_number} Market Street", city, state,
            f"{10000 + salt:05d}", ("RETAIL", "PREMIER", "SMALL_BUSINESS")[salt % 3],
            "VERIFIED" if salt % 11 else "PENDING_REVIEW", effective_date,
            datetime.combine(effective_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=8),
        )

    def _build_branches(self) -> list[dict[str, object]]:
        branch_locations = [
            ("B001", "Boston Central", "Boston", "MA"), ("B002", "Chicago West", "Chicago", "IL"),
            ("B003", "Seattle North", "Seattle", "WA"), ("B004", "Miami Downtown", "Miami", "FL"),
            ("B005", "Dallas Uptown", "Dallas", "TX"), ("B006", "Denver Central", "Denver", "CO"),
        ]
        return [dict(zip(("branch_id", "branch_name", "city", "state"), branch)) | {
            "region": "WEST" if branch[3] in {"WA", "CO", "AZ"} else "CENTRAL" if branch[3] in {"TX", "IL"} else "EAST",
            "source_modified_at": "2024-01-01T08:00:00+00:00",
        } for branch in branch_locations]

    def _build_accounts(self) -> list[dict[str, object]]:
        accounts = []
        for index in range(42):
            account_type = ("CHECKING", "SAVINGS", "MONEY_MARKET")[index % 3]
            accounts.append({
                "account_id": f"A{100000 + index}", "primary_customer_id": f"C{index % INITIAL_CUSTOMER_COUNT + 1:06d}",
                "branch_id": f"B{index % len(self.branches) + 1:03d}", "account_type": account_type,
                "account_status": "CLOSED" if index == 41 else "ACTIVE", "open_date": (date(2023, 1, 1) + timedelta(days=index * 5)).isoformat(),
                "close_date": "2024-11-15" if index == 41 else "", "currency_code": "USD",
                "source_modified_at": "2024-01-01T08:00:00+00:00",
            })
        return accounts

    def _build_ownership_history(self) -> list[dict[str, object]]:
        history = []
        for account in self.accounts:
            account_number = int(str(account["account_id"])[1:])
            primary = str(account["primary_customer_id"])
            if account_number % 6 == 0:
                secondary = f"C{(account_number + 3) % INITIAL_CUSTOMER_COUNT + 1:06d}"
                history.extend([
                    self._ownership_row(account["account_id"], primary, "PRIMARY", 50, date(2023, 1, 1)),
                    self._ownership_row(account["account_id"], secondary, "JOINT", 50, date(2023, 1, 1)),
                ])
                if account_number % 12 == 0:
                    history.extend([
                        self._ownership_row(account["account_id"], primary, "PRIMARY", 70, date(2024, 7, 1)),
                        self._ownership_row(account["account_id"], secondary, "JOINT", 30, date(2024, 7, 1)),
                    ])
            else:
                history.append(self._ownership_row(account["account_id"], primary, "PRIMARY", 100, date(2023, 1, 1)))
        return history

    @staticmethod
    def _ownership_row(account_id: object, customer_id: str, role: str, percentage: int, effective_date: date) -> dict[str, object]:
        return {
            "account_id": account_id, "customer_id": customer_id, "ownership_role": role,
            "ownership_percent": percentage, "effective_from_date": effective_date.isoformat(),
            "source_modified_at": f"{effective_date.isoformat()}T09:00:00+00:00", "source_system": "CORE_BANKING",
        }

    @staticmethod
    def _build_products() -> list[dict[str, object]]:
        return [
            {"product_id": "P001", "product_name": "Checking Account", "product_category": "DEPOSIT", "annual_fee": "0.00"},
            {"product_id": "P002", "product_name": "Savings Account", "product_category": "DEPOSIT", "annual_fee": "0.00"},
            {"product_id": "P003", "product_name": "Debit Card", "product_category": "CARD", "annual_fee": "0.00"},
            {"product_id": "P004", "product_name": "Overdraft Protection", "product_category": "SERVICE", "annual_fee": "35.00"},
        ]

    def _transactions_for_period(self, start_date: date, end_date: date, late_arrival: bool = False) -> list[dict[str, object]]:
        transactions = []
        current_date = start_date
        while current_date <= end_date:
            for sequence in range(8):
                account = self.accounts[(current_date.toordinal() + sequence * 5) % len(self.accounts)]
                transaction_type = ("CARD_PURCHASE", "DIRECT_DEPOSIT", "ACH_DEBIT", "ATM_WITHDRAWAL", "TRANSFER")[sequence % 5]
                amount = Decimal(20 + ((current_date.toordinal() * (sequence + 3)) % 1800)) / Decimal("10")
                if transaction_type in {"CARD_PURCHASE", "ACH_DEBIT", "ATM_WITHDRAWAL", "TRANSFER"}:
                    amount = -amount
                event_time = datetime.combine(current_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=8 + sequence, minutes=sequence * 7)
                arrival_time = event_time + timedelta(hours=2)
                if late_arrival:
                    arrival_time += timedelta(days=1)
                transaction_id = f"T{current_date:%Y%m%d}{sequence:03d}{'L' if late_arrival else ''}"
                transactions.append({
                    "transaction_id": transaction_id, "account_id": account["account_id"], "transaction_timestamp": event_time.isoformat(),
                    "transaction_date": current_date.isoformat(), "transaction_type": transaction_type,
                    "transaction_amount": f"{amount:.2f}", "currency_code": "USD",
                    "merchant_name": "PAYROLL EMPLOYER" if transaction_type == "DIRECT_DEPOSIT" else f"Merchant {sequence + 1}",
                    "merchant_category": "PAYROLL" if transaction_type == "DIRECT_DEPOSIT" else ("GROCERIES", "FUEL", "RESTAURANT")[sequence % 3],
                    "transaction_status": "REVERSED" if sequence == 7 and current_date.day % 13 == 0 else "POSTED",
                    "source_modified_at": arrival_time.isoformat(), "arrival_timestamp": arrival_time.isoformat(),
                    "is_late_arrival": str(late_arrival).lower(), "source_system": "PAYMENTS",
                })
            current_date += timedelta(days=1)
        return transactions

    def _transaction_lines(self, transactions: list[dict[str, object]]) -> list[dict[str, object]]:
        lines = []
        for transaction in transactions:
            product_id = "P003" if transaction["transaction_type"] == "CARD_PURCHASE" else "P001"
            lines.append({
                "transaction_id": transaction["transaction_id"], "line_number": 1, "product_id": product_id,
                "quantity": 1, "line_amount": transaction["transaction_amount"],
                "source_modified_at": transaction["source_modified_at"],
            })
            if transaction["transaction_type"] == "CARD_PURCHASE" and int(str(transaction["transaction_id"])[-4:-1]) % 2 == 0:
                lines.append({
                    "transaction_id": transaction["transaction_id"], "line_number": 2, "product_id": "P004",
                    "quantity": 1, "line_amount": "0.00",
                    "source_modified_at": transaction["source_modified_at"],
                })
        return lines

    def _balance_snapshots(self, balance_date: date) -> list[dict[str, object]]:
        snapshots = []
        for index, account in enumerate(self.accounts):
            opening = Decimal(500 + ((balance_date.toordinal() + index * 197) % 15000))
            snapshots.append({
                "balance_date": balance_date.isoformat(), "account_id": account["account_id"], "currency_code": "USD",
                "available_balance_amount": f"{opening - Decimal(index % 7) * Decimal('12.50'):.2f}",
                "ledger_balance_amount": f"{opening:.2f}", "overdraft_limit_amount": "500.00" if account["account_type"] == "CHECKING" else "0.00",
                "source_modified_at": f"{balance_date.isoformat()}T23:30:00+00:00", "source_system": "CORE_BANKING",
            })
        return snapshots

    def _customer_rows(self, as_of_date: date) -> list[dict[str, object]]:
        return [self._serialize_customer(version) for version in self.customers if version.effective_from_date <= as_of_date]

    def _ownership_rows(self, as_of_date: date) -> list[dict[str, object]]:
        return [row for row in self.ownership_history if date.fromisoformat(str(row["effective_from_date"])) <= as_of_date]

    def _daily_customer_changes(self, processing_date: date, late_arrival: bool = False) -> list[dict[str, object]]:
        if processing_date.day not in {5, 18}:
            return []
        customer_id = f"C{(processing_date.month * 3) % INITIAL_CUSTOMER_COUNT + 1:06d}"
        version = self._customer_version(customer_id, "Ava Patel", "Phoenix", "AZ", processing_date, processing_date.day)
        row = self._serialize_customer(version)
        if late_arrival:
            row["source_modified_at"] = f"{processing_date.isoformat()}T07:00:00+00:00"
            row["arrival_timestamp"] = f"{(processing_date + timedelta(days=1)).isoformat()}T06:30:00+00:00"
            row["is_late_arrival"] = "true"
        else:
            row["arrival_timestamp"] = f"{processing_date.isoformat()}T09:00:00+00:00"
            row["is_late_arrival"] = "false"
        return [row]

    def _daily_ownership_changes(self, processing_date: date) -> list[dict[str, object]]:
        if processing_date.day != 15:
            return []
        account_id = "A100012"
        return [
            self._ownership_row(account_id, "C000013", "PRIMARY", 60, processing_date),
            self._ownership_row(account_id, "C000016", "JOINT", 40, processing_date),
        ]

    @staticmethod
    def _serialize_customer(version: CustomerVersion) -> dict[str, object]:
        row = asdict(version)
        row["effective_from_date"] = version.effective_from_date.isoformat()
        row["source_modified_at"] = version.source_modified_at.isoformat()
        return row

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not rows:
            # Stable schemas let ingestion jobs distinguish an empty batch from a missing file.
            path.write_text("\n", encoding="utf-8")
            return
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _write_manifest(target: Path, processing_date: date, load_type: str, transactions: list[dict[str, object]], late_arrivals: int) -> None:
        target.mkdir(parents=True, exist_ok=True)
        manifest = [{
            "batch_id": f"{load_type.lower()}-{processing_date:%Y%m%d}", "processing_date": processing_date.isoformat(),
            "load_type": load_type, "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "transaction_record_count": len(transactions), "late_arrival_record_count": late_arrivals,
        }]
        BankingDataGenerator._write_csv(target / "batch_manifest.csv", manifest)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected ISO date format YYYY-MM-DD") from error


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate banking source CSV test data.")
    parser.add_argument("mode", choices=("initial", "daily"), help="initial historical load or a single daily incremental batch")
    parser.add_argument("--date", required=True, type=parse_date, help="As-of date for initial, or processing date for daily (YYYY-MM-DD)")
    parser.add_argument("--output-dir", default="data", type=Path, help="Root directory for generated data")
    parser.add_argument("--seed", default=20260926, type=int, help="Seed for deterministic output")
    arguments = parser.parse_args()

    generator = BankingDataGenerator(arguments.output_dir, arguments.seed)
    target = generator.generate_initial(arguments.date) if arguments.mode == "initial" else generator.generate_daily(arguments.date)
    print(f"Generated {arguments.mode} banking source data in {target}")


if __name__ == "__main__":
    main()