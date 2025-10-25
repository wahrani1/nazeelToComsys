#!/usr/bin/env python3
"""
Nazeel API to Comsys Database Integration Script - Guest Ledger System
Version: 2.0 - Production Ready
Author: Quality Point Team
Senior: Mahmoud Wahrani Software Engineer
Date: 2025-10-22

Description:
    Implements revenue recognition with a Guest Ledger clearing account.
    Processes invoices and receipts with multi-day matching and tolerance rules.

Usage:
    # Default 90-day lookback
    python nazeel_integration_v2.py

    # Custom date range
    python nazeel_integration_v2.py --start-date "2025-08-01 12:00:00" --end-date "2025-10-22 12:00:00"

    # Specific days back
    python nazeel_integration_v2.py --days 30
"""

import requests
import pyodbc
import hashlib
import uuid
import json
import logging
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

# ============================================================================
# Configuration
# ============================================================================
API_KEY = "QbLcg1QuqIoXL8vnpQ5SOAZeR0ZeR0"
SECRET_KEY = "981fccc0-819e-4aa8-87d4-343c3c42c44a"
BASE_URL = "https://eai.nazeel.net/api/odoo-TransactionsTransfer"
CONNECTION_STRING = "DRIVER={SQL Server};SERVER=COMSYS-API;DATABASE=AswarAlandalus;Trusted_Connection=yes;"
LOG_FILE = r"C:\Scripts\P03139\nazeel_log.txt"

# Table names
HED_TABLE = "FhglTxHed"
DED_TABLE = "FhglTxDed"

# Account codes
REVENUE_ACCOUNT = "101000020"
VAT_ACCOUNT = "021500010"
MUNICIPALITY_TAX_ACCOUNT = "021500090"
PENALTIES_ACCOUNT = "021100040"
GUEST_LEDGER_ACCOUNT = "011200010"
CASH_OVER_SHORT_ACCOUNT = "505000098"

# Payment matching thresholds
EXACT_MATCH_TOLERANCE = 0.01  # SAR
UNDERPAYMENT_TOLERANCE = 10.00  # SAR - shortage above this triggers rejection

# Payment method mapping
PAYMENT_METHOD_ACCOUNTS = {
    1: ("011500020", "Cash ( FO)"),
    2: ("011200065", "MADA"),
    3: ("-", "Payment Method 3"),
    4: ("011500001", "Bank Transfer"),
    5: ("-", "Aljazera Bank"),
    6: ("-", "American Express"),
    7: ("011200050", "Visa Card"),
    8: ("011200060", "Master Card"),
    9: ("-", " Payment Method 9"),
    10: ("-", "Other Electronic Payment")
}

# ============================================================================
# SQL Table Creation Scripts
# ============================================================================

CREATE_PROCESSED_INVOICES_TABLE = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Processed_Invoices' AND xtype='U')
BEGIN
    CREATE TABLE Processed_Invoices (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        InvoiceNumber NVARCHAR(50) NOT NULL,
        ReservationNumber NVARCHAR(50) NOT NULL,
        TotalAmount DECIMAL(18,6) NOT NULL,
        ProcessedDate DATETIME NOT NULL DEFAULT GETDATE(),
        RevenueDate DATE NOT NULL,
        RawInvoiceDate DATETIME NULL,
        Docu VARCHAR(5) NOT NULL,
        ComsysYear VARCHAR(4) NULL,
        ComsysMonth VARCHAR(2) NULL,
        ComsysSerial INT NULL,
        UNIQUE(InvoiceNumber)
    )
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Processed_Invoices' AND COLUMN_NAME = 'RevenueDate')
AND EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Processed_Invoices' AND COLUMN_NAME = 'InvoiceDate')
BEGIN
    EXEC sp_rename 'Processed_Invoices.InvoiceDate', 'RevenueDate', 'COLUMN'
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Processed_Invoices' AND COLUMN_NAME = 'ComsysYear')
BEGIN
    ALTER TABLE Processed_Invoices ADD ComsysYear VARCHAR(4) NULL
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Processed_Invoices' AND COLUMN_NAME = 'ComsysMonth')
BEGIN
    ALTER TABLE Processed_Invoices ADD ComsysMonth VARCHAR(2) NULL
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Processed_Invoices' AND COLUMN_NAME = 'ComsysSerial')
BEGIN
    ALTER TABLE Processed_Invoices ADD ComsysSerial INT NULL
END
"""

CREATE_PROCESSED_RECEIPTS_TABLE = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Processed_Receipts' AND xtype='U')
BEGIN
    CREATE TABLE Processed_Receipts (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        VoucherNumber NVARCHAR(50) NOT NULL,
        ReservationNumber NVARCHAR(50) NOT NULL,
        Amount DECIMAL(18,6) NOT NULL,
        PaymentMethodId INT NOT NULL,
        IssueDateTime DATETIME NOT NULL,
        RevenueDate DATE NOT NULL,
        ProcessedDate DATETIME NOT NULL DEFAULT GETDATE(),
        Docu VARCHAR(5) NOT NULL,
        ComsysYear VARCHAR(4) NULL,
        ComsysMonth VARCHAR(2) NULL,
        ComsysSerial INT NULL,
        UNIQUE(VoucherNumber)
    )
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Processed_Receipts' AND COLUMN_NAME = 'ComsysYear')
BEGIN
    ALTER TABLE Processed_Receipts ADD ComsysYear VARCHAR(4) NULL
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Processed_Receipts' AND COLUMN_NAME = 'ComsysMonth')
BEGIN
    ALTER TABLE Processed_Receipts ADD ComsysMonth VARCHAR(2) NULL
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Processed_Receipts' AND COLUMN_NAME = 'ComsysSerial')
BEGIN
    ALTER TABLE Processed_Receipts ADD ComsysSerial INT NULL
END
"""

# ============================================================================
# Setup logging
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)


# ============================================================================
# Main Integration Class
# ============================================================================

class NazeelComsysIntegrator:
    def __init__(self, start_date=None, end_date=None):
        """Initialize integrator with date range"""
        if start_date and end_date:
            self.start_date = start_date
            self.end_date = end_date
            self.api_fetch_start = start_date - timedelta(days=1)
            self.api_fetch_end = end_date
        else:
            now = datetime.now()
            self.current_run_time = now.replace(hour=12, minute=0, second=0, microsecond=0)
            self.end_date = self.current_run_time
            self.start_date = self.current_run_time - timedelta(days=90)
            self.api_fetch_start = self.start_date - timedelta(days=1)
            self.api_fetch_end = self.end_date

        self.current_date = date.today()
        self.auth_key = self._generate_auth_key()
        self._ensure_tracking_tables()

    def _generate_auth_key(self) -> str:
        """Generate MD5 hash for authKey"""
        date_str = self.current_date.strftime("%d/%m/%Y")
        combined = f"{SECRET_KEY}{date_str}"
        return hashlib.md5(combined.encode()).hexdigest()

    def _ensure_tracking_tables(self):
        """Ensure tracking tables exist and have all required columns"""
        try:
            with pyodbc.connect(CONNECTION_STRING) as conn:
                cursor = conn.cursor()

                for statement in CREATE_PROCESSED_INVOICES_TABLE.split('\nGO\n'):
                    if statement.strip():
                        try:
                            cursor.execute(statement)
                            conn.commit()
                        except Exception as e:
                            logging.debug(f"Statement execution note: {str(e)}")

                for statement in CREATE_PROCESSED_RECEIPTS_TABLE.split('\nGO\n'):
                    if statement.strip():
                        try:
                            cursor.execute(statement)
                            conn.commit()
                        except Exception as e:
                            logging.debug(f"Statement execution note: {str(e)}")

                logging.info("✓ Tracking tables verified/created successfully")

        except Exception as e:
            logging.error(f"Failed to ensure tracking tables: {str(e)}")
            raise

    def _validate_journal(self, conn, docu: str) -> bool:
        """Validate that the Docu value exists in FGnrJour table"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.FGnrJour WHERE Journal = ?", (docu,))
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logging.error(f"Failed to validate Docu {docu}: {str(e)}")
            return False

    def get_processed_invoices(self) -> set:
        """Get set of already processed invoice numbers"""
        try:
            with pyodbc.connect(CONNECTION_STRING) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT InvoiceNumber FROM Processed_Invoices")
                processed = {row[0] for row in cursor.fetchall()}
                logging.info(f"Found {len(processed)} previously processed invoices")
                return processed
        except Exception as e:
            logging.error(f"Failed to fetch processed invoices: {str(e)}")
            return set()

    def get_processed_receipts(self) -> set:
        """Get set of already processed receipt voucher numbers"""
        try:
            with pyodbc.connect(CONNECTION_STRING) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT VoucherNumber FROM Processed_Receipts")
                processed = {row[0] for row in cursor.fetchall()}
                logging.info(f"Found {len(processed)} previously processed receipts")
                return processed
        except Exception as e:
            logging.debug(f"Processed_Receipts table may not exist yet: {str(e)}")
            return set()

    def _make_api_request(self, endpoint: str) -> Optional[List]:
        """Make API request with proper headers and error handling"""
        url = f"{BASE_URL}/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "authKey": self.auth_key
        }

        if isinstance(self.api_fetch_start, datetime):
            start_str = self.api_fetch_start.strftime('%Y-%m-%d %H:%M')
            end_str = self.api_fetch_end.strftime('%Y-%m-%d %H:%M')
        else:
            start_str = f"{self.api_fetch_start} 12:00"
            end_str = f"{self.api_fetch_end} 12:00"

        payload = {
            "apiKey": API_KEY,
            "dateFrom": start_str,
            "dateTo": end_str
        }

        try:
            logging.info(f"Making API request to {endpoint} for date range: {start_str} to {end_str}")
            response = requests.post(url, json=payload, headers=headers, timeout=60)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get('status') == 200:
                return data.get('data', [])
            elif isinstance(data, list):
                return data
            else:
                logging.error(f"API returned unexpected response: {data}")
                return None
        except requests.RequestException as e:
            logging.error(f"API request failed for {endpoint}: {str(e)}")
            return None

    def assign_revenue_date(self, transaction_datetime: datetime) -> date:
        """Assign revenue date based on 12:00 PM cutoff"""
        noon = time(12, 0, 0)
        transaction_date = transaction_datetime.date()
        transaction_time = transaction_datetime.time()

        if transaction_time < noon:
            revenue_date = transaction_date - timedelta(days=1)
        else:
            revenue_date = transaction_date

        return revenue_date

    def fetch_invoices(self) -> List[Dict]:
        """Fetch invoices from API and filter out processed ones"""
        data = self._make_api_request("Getinvoices")
        if data is None:
            return []

        processed_invoices = self.get_processed_invoices()
        valid_invoices = []

        for inv in data:
            if not isinstance(inv, dict) or inv.get('isReversed', False):
                continue

            invoice_number = inv.get('invoiceNumber')
            if invoice_number in processed_invoices:
                continue

            creation_date_str = inv.get('creationDate', '')
            if creation_date_str:
                try:
                    creation_datetime = datetime.fromisoformat(creation_date_str.replace('Z', ''))
                    if hasattr(self, 'current_run_time') and creation_datetime > self.current_run_time:
                        continue

                    revenue_date = self.assign_revenue_date(creation_datetime)
                    inv['_raw_creation_datetime'] = creation_datetime
                    inv['_revenue_date'] = revenue_date

                except ValueError:
                    inv['_revenue_date'] = self.current_date
            else:
                inv['_revenue_date'] = self.current_date

            valid_invoices.append(inv)

        logging.info(f"✓ Fetched {len(valid_invoices)} new invoices")
        return valid_invoices

    def fetch_receipts(self) -> List[Dict]:
        """Fetch receipt vouchers from API and filter out processed ones"""
        data = self._make_api_request("GetReciptVouchers")
        if data is None:
            return []

        processed_receipts = self.get_processed_receipts()
        valid_receipts = []

        for rec in data:
            if not isinstance(rec, dict) or rec.get('isCanceled', False):
                continue

            voucher_number = rec.get('voucherNumber')
            if voucher_number in processed_receipts:
                continue

            issue_date_str = rec.get('issueDateTime', '')
            if issue_date_str:
                try:
                    issue_datetime = datetime.fromisoformat(issue_date_str.replace('Z', ''))
                    revenue_date = self.assign_revenue_date(issue_datetime)
                    rec['_raw_issue_datetime'] = issue_datetime
                    rec['_revenue_date'] = revenue_date
                except ValueError:
                    rec['_revenue_date'] = self.current_date
            else:
                rec['_revenue_date'] = self.current_date

            valid_receipts.append(rec)

        logging.info(f"✓ Fetched {len(valid_receipts)} new receipts")
        return valid_receipts

    def group_by_revenue_date(self, items: List[Dict], item_type: str) -> Dict[date, List[Dict]]:
        """Group items by revenue date"""
        grouped = defaultdict(list)
        for item in items:
            revenue_date = item.get('_revenue_date', self.current_date)
            grouped[revenue_date].append(item)

        sorted_groups = dict(sorted(grouped.items()))
        logging.info(f"Grouped {len(items)} {item_type} into {len(sorted_groups)} date groups")
        return sorted_groups

    def build_receipt_lookup(self, all_receipts: List[Dict]) -> Dict[str, List[Dict]]:
        """Build lookup dictionary of receipts by reservation number"""
        receipt_lookup = defaultdict(list)
        for receipt in all_receipts:
            reservation_num = receipt.get('reservationNumber')
            if reservation_num:
                receipt_lookup[reservation_num].append(receipt)
        return receipt_lookup

    def match_invoice_to_receipts(self, invoice: Dict, receipt_lookup: Dict[str, List[Dict]]) -> Tuple[
        str, float, float, str]:
        """Match invoice to receipts and determine processing status"""
        reservation_num = invoice.get('reservationNumber')
        invoice_amount = float(invoice.get('totalAmount', 0))

        receipts = receipt_lookup.get(reservation_num, [])

        if not receipts:
            return ('REJECT_NO_RECEIPT', invoice_amount, 0.0, 'No payment received')

        receipt_total = sum(float(r.get('amount', 0)) for r in receipts)
        difference = receipt_total - invoice_amount

        if abs(difference) <= EXACT_MATCH_TOLERANCE:
            return ('PROCESS_EXACT', invoice_amount, receipt_total, 'Exact match')
        elif difference > EXACT_MATCH_TOLERANCE:
            return ('PROCESS_OVERPAID', invoice_amount, receipt_total, f'Overpaid by {difference:.2f} SAR')
        elif abs(difference) <= UNDERPAYMENT_TOLERANCE:
            return ('PROCESS_UNDERPAID_TOLERABLE', invoice_amount, receipt_total,
                    f'Underpaid by {abs(difference):.2f} SAR (within tolerance)')
        else:
            return ('REJECT_UNDERPAID', invoice_amount, receipt_total,
                    f'Underpaid by {abs(difference):.2f} SAR (exceeds threshold)')

    def extract_invoice_components(self, invoice: Dict) -> Dict[str, float]:
        """Extract revenue components from invoice"""
        components = {
            'individual_rate': 0.0,
            'vat': 0.0,
            'municipality_tax': 0.0,
            'penalties': 0.0
        }

        components['vat'] = float(invoice.get('vatAmount', 0))

        for item in invoice.get('invoicesItemsDetalis', []):
            item_subtotal = float(item.get('subTotal', 0))
            item_type = item.get('itemType')
            item_type_str = item.get('type', '')

            if item_type == 4 or item_type_str.startswith('Fee--'):
                components['municipality_tax'] += item_subtotal
            elif item_type == 3:
                components['penalties'] += item_subtotal
            elif item_type == 1:
                components['individual_rate'] += item_subtotal

        return components

    def process_revenue_date(self, conn, revenue_date: date, date_receipts: List[Dict],
                             date_invoices: List[Dict], receipt_lookup: Dict[str, List[Dict]]) -> bool:
        """Process all transactions for a single revenue date"""
        try:
            logging.info(f"\n{'=' * 80}")
            logging.info(f"Processing Revenue Date: {revenue_date}")
            logging.info(f"Receipts: {len(date_receipts)} | Invoices: {len(date_invoices)}")

            processable_invoices = []
            rejected_invoices = []
            cash_over_short_entries = []

            for invoice in date_invoices:
                status, invoice_amt, receipt_amt, reason = self.match_invoice_to_receipts(invoice, receipt_lookup)

                invoice['_match_status'] = status
                invoice['_match_reason'] = reason
                invoice['_receipt_amount'] = receipt_amt

                if status.startswith('PROCESS'):
                    processable_invoices.append(invoice)
                    difference = receipt_amt - invoice_amt
                    if abs(difference) > EXACT_MATCH_TOLERANCE:
                        cash_over_short_entries.append({
                            'invoice_number': invoice.get('invoiceNumber'),
                            'reservation': invoice.get('reservationNumber'),
                            'amount': difference,
                            'type': 'overpayment' if difference > 0 else 'underpayment'
                        })
                else:
                    rejected_invoices.append(invoice)

            logging.info(f"Processable: {len(processable_invoices)} | Rejected: {len(rejected_invoices)}")

            if rejected_invoices:
                logging.warning(f"\n=== REJECTED INVOICES FOR {revenue_date} ===")
                for inv in rejected_invoices:
                    logging.warning(
                        f"  Invoice {inv.get('invoiceNumber')} - {inv.get('reservationNumber')}: "
                        f"{inv.get('_match_reason')}"
                    )

            payment_methods = defaultdict(float)
            for receipt in date_receipts:
                method_id = receipt.get('paymentMethodId')
                amount = float(receipt.get('amount', 0))
                payment_methods[method_id] += amount

            total_receipts = sum(payment_methods.values())

            revenue_components = {
                'individual_rate': 0.0,
                'vat': 0.0,
                'municipality_tax': 0.0,
                'penalties': 0.0
            }

            for invoice in processable_invoices:
                components = self.extract_invoice_components(invoice)
                for key in revenue_components:
                    revenue_components[key] += components[key]

            total_revenue = sum(revenue_components.values())
            cash_over_short_total = sum(entry['amount'] for entry in cash_over_short_entries)
            guest_ledger_amount = total_receipts - total_revenue - cash_over_short_total

            logging.info(
                f"Receipts: {total_receipts:.2f} | Revenue: {total_revenue:.2f} | Guest Ledger: {guest_ledger_amount:.2f}")

            conn.autocommit = False
            try:
                docu = self.generate_docu()

                if not self._validate_journal(conn, docu):
                    raise ValueError(f"Invalid Docu {docu}")

                year, month, serial = self.insert_fhgl_tx_hed(conn, docu, revenue_date)
                self.insert_fhgl_tx_ded(conn, docu, year, month, serial, revenue_date,
                                        payment_methods, revenue_components,
                                        cash_over_short_total, guest_ledger_amount)

                self.insert_processed_receipts(conn, docu, year, month, serial, date_receipts)
                self.insert_processed_invoices(conn, docu, year, month, serial, processable_invoices)

                conn.commit()
                logging.info(f"✓ Successfully committed transaction")
                return True

            except Exception as e:
                conn.rollback()
                logging.error(f"✗ Transaction failed: {str(e)}")
                return False

        except Exception as e:
            logging.error(f"✗ Processing failed: {str(e)}")
            return False

    def generate_docu(self) -> str:
        """Generate document number"""
        return "115"

    def get_next_serial(self, conn, docu: str, year: str, month: str) -> int:
        """Get the next available serial number"""
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT ISNULL(MAX(Serial), 0) + 1 FROM {HED_TABLE} "
                f"WHERE Docu = ? AND Year = ? AND Month = ?",
                (docu, year, month)
            )
            return cursor.fetchone()[0]
        except Exception as e:
            logging.error(f"Error getting next serial: {str(e)}")
            return 1

    def insert_fhgl_tx_hed(self, conn, docu: str, revenue_date: date) -> Tuple[str, str, int]:
        """Insert record into FhglTxHed table"""
        cursor = conn.cursor()
        year = str(revenue_date.year)
        month = f"{revenue_date.month:02d}"
        serial = self.get_next_serial(conn, docu, year, month)
        date_val = revenue_date.strftime('%Y-%m-%d')

        sql = f"""
        INSERT INTO {HED_TABLE} (Docu, Year, Month, Serial, Date, Currency, Rate, Posted, ReEvaluate, RepeatedSerial, Flag)
        VALUES ('{docu}', '{year}', '{month}', {serial}, '{date_val}', '001', 1.0, 0, 0, NULL, NULL)
        """
        cursor.execute(sql)
        return year, month, serial

    def insert_fhgl_tx_ded(self, conn, docu: str, year: str, month: str, serial: int,
                           revenue_date: date, payment_methods: Dict[int, float],
                           revenue_components: Dict[str, float],
                           cash_over_short: float, guest_ledger: float) -> None:
        """Insert records into FhglTxDed table"""
        cursor = conn.cursor()
        line = 1

        payment_methods = {k: round(v, 2) for k, v in payment_methods.items()}
        revenue_components = {k: round(v, 2) for k, v in revenue_components.items()}
        cash_over_short = round(cash_over_short, 2)
        guest_ledger = round(guest_ledger, 2)

        # Debit: Payment Methods
        for method_id, amount in payment_methods.items():
            if amount > 0 and method_id in PAYMENT_METHOD_ACCOUNTS:
                account, description = PAYMENT_METHOD_ACCOUNTS[method_id]
                self._insert_ded_line(
                    cursor, docu, year, month, serial, line,
                    account, amount, 0, amount, 0,
                    f"FOC Dep.: {description} for {revenue_date}"
                )
                line += 1

        # Credit: Revenue Components
        if revenue_components['individual_rate'] > 0:
            self._insert_ded_line(
                cursor, docu, year, month, serial, line,
                REVENUE_ACCOUNT, 0, revenue_components['individual_rate'],
                0, revenue_components['individual_rate'],
                f"FOC Dep.: Individual Rate for {revenue_date}"
            )
            line += 1

        if revenue_components['vat'] > 0:
            self._insert_ded_line(
                cursor, docu, year, month, serial, line,
                VAT_ACCOUNT, 0, revenue_components['vat'],
                0, revenue_components['vat'],
                f"FOC Dep.: VAT for {revenue_date}"
            )
            line += 1

        if revenue_components['municipality_tax'] > 0:
            self._insert_ded_line(
                cursor, docu, year, month, serial, line,
                MUNICIPALITY_TAX_ACCOUNT, 0, revenue_components['municipality_tax'],
                0, revenue_components['municipality_tax'],
                f"FOC Dep.: Municipality Tax for {revenue_date}"
            )
            line += 1

        if revenue_components['penalties'] > 0:
            self._insert_ded_line(
                cursor, docu, year, month, serial, line,
                PENALTIES_ACCOUNT, 0, revenue_components['penalties'],
                0, revenue_components['penalties'],
                f"FOC Dep.: Penalties for {revenue_date}"
            )
            line += 1

        # Cash Over/Short
        if abs(cash_over_short) > 0:
            if cash_over_short > 0:
                self._insert_ded_line(
                    cursor, docu, year, month, serial, line,
                    CASH_OVER_SHORT_ACCOUNT, 0, abs(cash_over_short),
                    0, abs(cash_over_short),
                    f"FOC Dep.: Cash Over/Short for {revenue_date}"
                )
            else:
                self._insert_ded_line(
                    cursor, docu, year, month, serial, line,
                    CASH_OVER_SHORT_ACCOUNT, abs(cash_over_short), 0,
                    abs(cash_over_short), 0,
                    f"FOC Dep.: Cash Over/Short for {revenue_date}"
                )
            line += 1

        # Guest Ledger
        if abs(guest_ledger) > 0:
            if guest_ledger > 0:
                self._insert_ded_line(
                    cursor, docu, year, month, serial, line,
                    GUEST_LEDGER_ACCOUNT, 0, abs(guest_ledger),
                    0, abs(guest_ledger),
                    f"FOC Dep.: Guest Ledger for {revenue_date}"
                )
            else:
                self._insert_ded_line(
                    cursor, docu, year, month, serial, line,
                    GUEST_LEDGER_ACCOUNT, abs(guest_ledger), 0,
                    abs(guest_ledger), 0,
                    f"FOC Dep.: Guest Ledger for {revenue_date}"
                )
            line += 1

    def _insert_ded_line(self, cursor, docu: str, year: str, month: str, serial: int,
                         line: int, account: str, valu_le_dr: float, valu_le_cr: float,
                         valu_fc_dr: float, valu_fc_cr: float, desc: str) -> None:
        """Insert a single line into FhglTxDed table"""
        desc_truncated = desc[:40] if len(desc) > 40 else desc.replace("'", "''")
        sql = f"""
        INSERT INTO {DED_TABLE} (Docu, Year, Month, Serial, Line, Account, ValuLeDr, ValuLeCr, ValuFcDr, ValuFcCr, [Desc])
        VALUES ('{docu}', '{year}', '{month}', {serial}, {line}, '{account}', 
                {valu_le_dr}, {valu_le_cr}, {valu_fc_dr}, {valu_fc_cr}, '{desc_truncated}')
        """
        cursor.execute(sql)

    def insert_processed_receipts(self, conn, docu: str, year: str, month: str,
                                  serial: int, receipts: List[Dict]) -> None:
        """Insert processed receipts into tracking table"""
        cursor = conn.cursor()

        for receipt in receipts:
            voucher_num = receipt.get('voucherNumber', '').replace("'", "''")
            reservation_num = receipt.get('reservationNumber', '').replace("'", "''")
            amount = float(receipt.get('amount', 0))
            payment_method_id = receipt.get('paymentMethodId', 0)
            issue_datetime = receipt.get('_raw_issue_datetime')
            revenue_date = receipt.get('_revenue_date')

            issue_dt_str = issue_datetime.strftime('%Y-%m-%d %H:%M:%S') if issue_datetime else datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')
            revenue_date_str = revenue_date.strftime('%Y-%m-%d') if revenue_date else date.today().strftime('%Y-%m-%d')

            try:
                sql = f"""
                INSERT INTO Processed_Receipts 
                (VoucherNumber, ReservationNumber, Amount, PaymentMethodId, IssueDateTime, RevenueDate, Docu, ComsysYear, ComsysMonth, ComsysSerial)
                VALUES ('{voucher_num}', '{reservation_num}', {amount}, {payment_method_id}, 
                        '{issue_dt_str}', '{revenue_date_str}', '{docu}', '{year}', '{month}', {serial})
                """
                cursor.execute(sql)
            except pyodbc.ProgrammingError:
                try:
                    sql = f"""
                    INSERT INTO Processed_Receipts 
                    (VoucherNumber, ReservationNumber, Amount, PaymentMethodId, IssueDateTime, RevenueDate, Docu)
                    VALUES ('{voucher_num}', '{reservation_num}', {amount}, {payment_method_id}, 
                            '{issue_dt_str}', '{revenue_date_str}', '{docu}')
                    """
                    cursor.execute(sql)
                except Exception as e:
                    logging.warning(f"Could not insert receipt {voucher_num}: {str(e)}")
            except pyodbc.IntegrityError:
                pass
            except Exception as e:
                logging.warning(f"Error inserting receipt {voucher_num}: {str(e)}")

    def insert_processed_invoices(self, conn, docu: str, year: str, month: str,
                                  serial: int, invoices: List[Dict]) -> None:
        """Insert processed invoices into tracking table"""
        cursor = conn.cursor()

        for invoice in invoices:
            invoice_num = invoice.get('invoiceNumber', '').replace("'", "''")
            reservation_num = invoice.get('reservationNumber', '').replace("'", "''")
            total_amount = float(invoice.get('totalAmount', 0))
            creation_datetime = invoice.get('_raw_creation_datetime')
            revenue_date = invoice.get('_revenue_date')

            creation_dt_str = creation_datetime.strftime('%Y-%m-%d %H:%M:%S') if creation_datetime else None
            revenue_date_str = revenue_date.strftime('%Y-%m-%d') if revenue_date else date.today().strftime('%Y-%m-%d')

            try:
                if creation_datetime:
                    sql = f"""
                    INSERT INTO Processed_Invoices 
                    (InvoiceNumber, ReservationNumber, TotalAmount, RevenueDate, RawInvoiceDate, Docu, ComsysYear, ComsysMonth, ComsysSerial)
                    VALUES ('{invoice_num}', '{reservation_num}', {total_amount}, '{revenue_date_str}', 
                            '{creation_dt_str}', '{docu}', '{year}', '{month}', {serial})
                    """
                else:
                    sql = f"""
                    INSERT INTO Processed_Invoices 
                    (InvoiceNumber, ReservationNumber, TotalAmount, RevenueDate, Docu, ComsysYear, ComsysMonth, ComsysSerial)
                    VALUES ('{invoice_num}', '{reservation_num}', {total_amount}, '{revenue_date_str}', 
                            '{docu}', '{year}', '{month}', {serial})
                    """
                cursor.execute(sql)

            except pyodbc.ProgrammingError:
                try:
                    if creation_datetime:
                        sql = f"""
                        INSERT INTO Processed_Invoices 
                        (InvoiceNumber, ReservationNumber, TotalAmount, InvoiceDate, RawInvoiceDate, Docu)
                        VALUES ('{invoice_num}', '{reservation_num}', {total_amount}, '{revenue_date_str}', 
                                '{creation_dt_str}', '{docu}')
                        """
                    else:
                        sql = f"""
                        INSERT INTO Processed_Invoices 
                        (InvoiceNumber, ReservationNumber, TotalAmount, InvoiceDate, Docu)
                        VALUES ('{invoice_num}', '{reservation_num}', {total_amount}, '{revenue_date_str}', '{docu}')
                        """
                    cursor.execute(sql)
                except Exception as e:
                    logging.warning(f"Could not insert invoice {invoice_num}: {str(e)}")

            except pyodbc.IntegrityError:
                pass
            except Exception as e:
                logging.warning(f"Error inserting invoice {invoice_num}: {str(e)}")

    def process_all_data(self) -> bool:
        """Main processing function"""
        try:
            logging.info(f"\n{'=' * 80}")
            logging.info(f"NAZEEL TO COMSYS INTEGRATION - GUEST LEDGER SYSTEM v2.0")
            logging.info(f"{'=' * 80}")
            logging.info(f"Script run time: {datetime.now()}")
            logging.info(f"Date range: {self.start_date} to {self.end_date}")

            all_invoices = self.fetch_invoices()
            all_receipts = self.fetch_receipts()

            if not all_invoices and not all_receipts:
                logging.warning("No new data to process")
                return False

            invoices_by_date = self.group_by_revenue_date(all_invoices, "invoices")
            receipts_by_date = self.group_by_revenue_date(all_receipts, "receipts")
            receipt_lookup = self.build_receipt_lookup(all_receipts)

            all_dates = sorted(set(list(invoices_by_date.keys()) + list(receipts_by_date.keys())))
            logging.info(f"\n✓ Processing {len(all_dates)} revenue dates\n")

            success_count = 0
            failed_count = 0
            all_rejected_invoices = []

            with pyodbc.connect(CONNECTION_STRING) as conn:
                for revenue_date in all_dates:
                    date_receipts = receipts_by_date.get(revenue_date, [])
                    date_invoices = invoices_by_date.get(revenue_date, [])

                    # Collect rejected invoices from this date
                    for invoice in date_invoices:
                        status, invoice_amt, receipt_amt, reason = self.match_invoice_to_receipts(invoice,
                                                                                                  receipt_lookup)
                        if not status.startswith('PROCESS'):
                            all_rejected_invoices.append({
                                'invoice_number': invoice.get('invoiceNumber'),
                                'reservation_number': invoice.get('reservationNumber'),
                                'invoice_amount': invoice_amt,
                                'receipt_amount': receipt_amt,
                                'reason': reason,
                                'status': status,
                                'revenue_date': revenue_date
                            })

                    success = self.process_revenue_date(
                        conn, revenue_date, date_receipts, date_invoices, receipt_lookup
                    )

                    if success:
                        success_count += 1
                    else:
                        failed_count += 1

            # Calculate processed invoices count
            processed_invoices_count = len(all_invoices) - len(all_rejected_invoices)
            logging.info(f"\n{'=' * 80}")
            logging.info(f"PROCESSING SUMMARY")
            logging.info(f"{'=' * 80}")
            logging.info(f"Total revenue dates: {len(all_dates)}")
            logging.info(f"✓ Successfully processed: {success_count}")
            if failed_count > 0:
                logging.info(f"✗ Failed: {failed_count}")
            logging.info(f"\nInvoice Statistics:")
            logging.info(f"  Total invoices fetched: {len(all_invoices)}")
            logging.info(f"  ✓ Processed invoices: {processed_invoices_count}")
            logging.info(f"  ✗ Rejected invoices: {len(all_rejected_invoices)}")
            logging.info(f"\nReceipt Statistics:")
            logging.info(f"  Total receipts fetched: {len(all_receipts)}")

            # Display rejected invoices summary
            if all_rejected_invoices:
                logging.info(f"\n{'=' * 80}")
                logging.info(f"REJECTED INVOICES DETAILS ({len(all_rejected_invoices)} total)")
                logging.info(f"{'=' * 80}")

                # Group by rejection reason
                rejected_by_reason = defaultdict(list)
                for rejected in all_rejected_invoices:
                    rejected_by_reason[rejected['status']].append(rejected)

                for status, invoices in rejected_by_reason.items():
                    if status == 'REJECT_NO_RECEIPT':
                        reason_label = "No Payment Received"
                    elif status == 'REJECT_UNDERPAID':
                        reason_label = f"Underpaid (> {UNDERPAYMENT_TOLERANCE} SAR)"
                    else:
                        reason_label = status

                    logging.info(f"\n{reason_label}: {len(invoices)} invoice(s)")
                    logging.info(f"{'-' * 80}")

                    for inv in invoices:
                        logging.info(
                            f"  Invoice: {inv['invoice_number']} | "
                            f"Reservation: {inv['reservation_number']} | "
                            f"Amount: {inv['invoice_amount']:.2f} SAR | "
                            f"Received: {inv['receipt_amount']:.2f} SAR | "
                            f"Revenue Date: {inv['revenue_date']}"
                        )

                logging.info(f"\n{'=' * 80}")
                logging.info(f"NOTE: Rejected invoices will be automatically retried on the next run")
                logging.info(f"      when additional payments are received.")

            logging.info(f"{'=' * 80}\n")

            return failed_count == 0

        except Exception as e:
            logging.error(f"✗ Processing failed: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Nazeel to Comsys Integration - Guest Ledger System v2.0',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python nazeel_integration_v2.py
  python nazeel_integration_v2.py --start-date "2025-08-01 12:00:00" --end-date "2025-10-22 12:00:00"
  python nazeel_integration_v2.py --days 30
        """
    )

    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--days', type=int, help='Days to look back')

    args = parser.parse_args()

    try:
        if args.start_date and args.end_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d %H:%M:%S')
        elif args.days:
            now = datetime.now()
            end_date = now.replace(hour=12, minute=0, second=0, microsecond=0)
            start_date = end_date - timedelta(days=args.days)
        else:
            now = datetime.now()
            end_date = now.replace(hour=12, minute=0, second=0, microsecond=0)
            start_date = end_date - timedelta(days=90)

        integrator = NazeelComsysIntegrator(start_date, end_date)
        success = integrator.process_all_data()

        if success:
            logging.info("✓ Processing completed successfully")
            exit(0)
        else:
            logging.error("✗ Processing completed with errors")
            exit(1)

    except Exception as e:
        logging.error(f"✗ Fatal error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        exit(1)


if __name__ == "__main__":
    main()