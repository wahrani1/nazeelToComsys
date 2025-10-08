#!/usr/bin/env python3

import requests
import pyodbc
import hashlib
import uuid
import json
import logging
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

# Configuration
API_KEY = "tgh1QayxXvoXL8vnpQ5SOAZeR0ZeR0"
SECRET_KEY = "981fccc0-819e-4aa8-87d4-343c3c42c44a"
BASE_URL = "https://eai.nazeel.net/api/odoo-TransactionsTransfer"
CONNECTION_STRING = "DRIVER={SQL Server};SERVER=COMSYS-API;DATABASE=AlndalusLuxery1;Trusted_Connection=yes;"
LOG_FILE = r"C:\Scripts\P03122\nazeel_log.txt"

# Table names
HED_TABLE = "FhglTxHed"
DED_TABLE = "FhglTxDed"

# Cash Over & Short account for handling small differences
CASH_OVER_SHORT_ACCOUNT = "505000098"
MAX_CASH_OVER_SHORT = 10.00  # Maximum difference allowed in SAR

# SQL to create processed invoices tracking table
CREATE_PROCESSED_INVOICES_TABLE = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Processed_Invoices' AND xtype='U')
CREATE TABLE Processed_Invoices (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceNumber NVARCHAR(50) NOT NULL,
    ReservationNumber NVARCHAR(50) NOT NULL,
    TotalAmount DECIMAL(18,6) NOT NULL,
    ProcessedDate DATETIME NOT NULL DEFAULT GETDATE(),
    Docu VARCHAR(5) NOT NULL,
    InvoiceDate DATE NOT NULL,
    RawInvoiceDate DATETIME NULL,
    UNIQUE(InvoiceNumber, ReservationNumber)
)
"""

# Payment method mapping
PAYMENT_METHOD_ACCOUNTS = {
    1: ("011500020", "Cash ( FO)"),
    2: ("011200065", "MADA"),
    3: ("011200060", "Payment Method 3"),
    4: ("011200050", "Payment Method 4"),
    5: ("011500001", "Aljazera Bank"),
    6: ("011200070", "American Express"),
    7: ("011200050", "Visa Card"),
    8: ("011200060", "Master Card 8"),
    9: ("011500010", "Bank Transfer"),
    10: ("011200100", "Other Electronic Payment")
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)


class NazeelComsysIntegrator:
    def __init__(self, start_date=None, end_date=None):
        """
        Initialize integrator with date range.
        Script should run daily at 12:00 PM on day D.
        Processes invoices from D-60 12:00:00 to D 12:00:00 for 60-day lookback.

        IMPORTANT: API fetch starts from (D-61) 12:00:00 to capture invoices created before 12:00 PM
        on D-60 that should be assigned to revenue date D-61.
        """
        if start_date and end_date:
            self.start_date = start_date
            self.end_date = end_date
            # Calculate API fetch range (fetch from 1 day earlier to catch before-noon invoices)
            self.api_fetch_start = start_date - timedelta(days=1)
            self.api_fetch_end = end_date
        else:
            # Default: 60-day lookback from current date at 12:00 PM
            now = datetime.now()
            self.current_run_time = now.replace(hour=12, minute=0, second=0, microsecond=0)
            self.end_date = self.current_run_time
            self.start_date = self.current_run_time - timedelta(days=60)
            # Fetch from one day earlier to capture all invoices for the 60-day revenue period
            self.api_fetch_start = self.start_date - timedelta(days=1)
            self.api_fetch_end = self.end_date

        self.current_date = date.today()
        self.auth_key = self._generate_auth_key()
        self._ensure_processed_invoices_table()

    def _generate_auth_key(self) -> str:
        """Generate MD5 hash for authKey using secret key and current date"""
        date_str = self.current_date.strftime("%d/%m/%Y")
        combined = f"{SECRET_KEY}{date_str}"
        return hashlib.md5(combined.encode()).hexdigest()

    def _ensure_processed_invoices_table(self):
        """Ensure the processed invoices tracking table exists with RawInvoiceDate column"""
        try:
            with pyodbc.connect(CONNECTION_STRING) as conn:
                cursor = conn.cursor()
                cursor.execute(CREATE_PROCESSED_INVOICES_TABLE)

                # Check if RawInvoiceDate column exists, add if not
                cursor.execute("""
                    IF NOT EXISTS (
                        SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = 'Processed_Invoices' 
                        AND COLUMN_NAME = 'RawInvoiceDate'
                    )
                    ALTER TABLE Processed_Invoices ADD RawInvoiceDate DATETIME NULL
                """)

                conn.commit()
                logging.info("Processed_Invoices table verified/created with RawInvoiceDate column")
        except Exception as e:
            logging.error(f"Failed to create/verify Processed_Invoices table: {str(e)}")
            raise

    def _validate_journal(self, conn, docu: str) -> bool:
        """Validate that the Docu value exists in FGnrJour table"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.FGnrJour WHERE Journal = ?", (docu,))
            count = cursor.fetchone()[0]
            if count == 0:
                logging.error(f"Docu {docu} not found in dbo.FGnrJour table")
                return False
            logging.info(f"Validated Docu {docu} in dbo.FGnrJour")
            return True
        except Exception as e:
            logging.error(f"Failed to validate Docu {docu} in dbo.FGnrJour: {str(e)}")
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

    def filter_new_invoices(self, invoices: List[Dict]) -> List[Dict]:
        """Filter out already processed invoices"""
        processed_invoices = self.get_processed_invoices()
        new_invoices = [
            inv for inv in invoices
            if inv.get('invoiceNumber') not in processed_invoices
        ]
        skipped_count = len(invoices) - len(new_invoices)
        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} already processed invoices")
        logging.info(f"Found {len(new_invoices)} new invoices to process")
        return new_invoices

    def _make_api_request(self, endpoint: str) -> Optional[Dict]:
        """Make API request with proper headers and error handling"""
        url = f"{BASE_URL}/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "authKey": self.auth_key
        }

        # Format dates for API request - use api_fetch_start to capture all needed invoices
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
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response: {str(e)}")
            return None

    def fetch_invoices(self) -> List[Dict]:
        """Fetch invoices from API"""
        data = self._make_api_request("Getinvoices")
        if data is None:
            return []
        if not isinstance(data, list):
            logging.error(f"Expected list from API, got {type(data)}")
            return []

        valid_invoices = []
        for inv in data:
            if not isinstance(inv, dict) or inv.get('isReversed', False):
                continue

            creation_date_str = inv.get('creationDate', '')
            if creation_date_str:
                try:
                    creation_datetime = datetime.fromisoformat(creation_date_str.replace('Z', ''))
                    # Exclude invoices created after the script's run time
                    if hasattr(self, 'current_run_time') and creation_datetime > self.current_run_time:
                        logging.debug(
                            f"Excluding invoice {inv.get('invoiceNumber')} created after run time: {creation_datetime}")
                        continue
                except ValueError:
                    pass

            valid_invoices.append(inv)

        logging.info(f"Fetched {len(valid_invoices)} valid invoices")
        return valid_invoices

    def fetch_receipts(self) -> List[Dict]:
        """Fetch receipt vouchers from API"""
        data = self._make_api_request("GetReciptVouchers")
        if data is None:
            return []
        if not isinstance(data, list):
            logging.error(f"Expected list from API, got {type(data)}")
            return []
        valid_receipts = [rec for rec in data if isinstance(rec, dict) and not rec.get('isCanceled', False)]
        logging.info(f"Fetched {len(valid_receipts)} valid receipts")
        return valid_receipts

    def assign_revenue_date(self, creation_datetime: datetime) -> date:
        """
        Assign revenue date based on 12:00 PM cutoff:
        - Before 12:00 PM on day D → Revenue date D-1
        - At/After 12:00 PM on day D → Revenue date D
        """
        noon = time(12, 0, 0)
        creation_date = creation_datetime.date()
        creation_time = creation_datetime.time()

        if creation_time < noon:
            # Before 12:00 PM → assign to previous day
            revenue_date = creation_date - timedelta(days=1)
        else:
            # At/After 12:00 PM → assign to same day
            revenue_date = creation_date

        return revenue_date

    def group_invoices_by_revenue_date(self, invoices: List[Dict]) -> Dict[date, List[Dict]]:
        """
        Group invoices by assigned revenue date based on creationDate.
        Eliminates before_12/after_12 period grouping.
        """
        grouped_invoices = defaultdict(list)

        for invoice in invoices:
            creation_date_str = invoice.get('creationDate', '')

            if creation_date_str:
                try:
                    # Parse the full timestamp
                    creation_datetime = datetime.fromisoformat(creation_date_str.replace('Z', ''))

                    # Assign revenue date
                    revenue_date = self.assign_revenue_date(creation_datetime)

                    # Store both raw and revenue dates in invoice for tracking
                    invoice['_raw_creation_datetime'] = creation_datetime
                    invoice['_revenue_date'] = revenue_date

                    grouped_invoices[revenue_date].append(invoice)

                    logging.debug(
                        f"Invoice {invoice.get('invoiceNumber')}: "
                        f"Created {creation_datetime}, Assigned to revenue date {revenue_date}"
                    )

                except ValueError as e:
                    logging.warning(
                        f"Could not parse creation date '{creation_date_str}' for invoice "
                        f"{invoice.get('invoiceNumber')}: {e}. Assigning to current date."
                    )
                    revenue_date = self.current_date
                    invoice['_revenue_date'] = revenue_date
                    grouped_invoices[revenue_date].append(invoice)
            else:
                logging.warning(
                    f"Invoice {invoice.get('invoiceNumber')} has no creation date, "
                    f"assigning to revenue date {self.current_date}"
                )
                revenue_date = self.current_date
                invoice['_revenue_date'] = revenue_date
                grouped_invoices[revenue_date].append(invoice)

        # Sort by revenue date for consistent processing
        sorted_groups = dict(sorted(grouped_invoices.items()))

        logging.info(f"Grouped {len(invoices)} invoices into {len(sorted_groups)} revenue date groups:")
        for revenue_date, date_invoices in sorted_groups.items():
            # Calculate the range of raw creation timestamps
            raw_dates = [inv.get('_raw_creation_datetime') for inv in date_invoices if '_raw_creation_datetime' in inv]
            if raw_dates:
                min_raw = min(raw_dates)
                max_raw = max(raw_dates)
                logging.info(
                    f"  Revenue date {revenue_date}: {len(date_invoices)} invoices "
                    f"(created from {min_raw} to {max_raw})"
                )
            else:
                logging.info(f"  Revenue date {revenue_date}: {len(date_invoices)} invoices")

        return sorted_groups

    def identify_paid_invoices(self, invoices: List[Dict], receipts: List[Dict]) -> List[Dict]:
        """Identify fully paid invoices by matching with receipts"""
        paid_invoices = []
        new_invoices = self.filter_new_invoices(invoices)
        receipts_by_reservation = {}

        for receipt in receipts:
            reservation_num = receipt.get('reservationNumber')
            if reservation_num:
                if reservation_num not in receipts_by_reservation:
                    receipts_by_reservation[reservation_num] = []
                receipts_by_reservation[reservation_num].append(receipt)

        for invoice in new_invoices:
            reservation_num = invoice.get('reservationNumber')
            total_amount = float(invoice.get('totalAmount', 0))
            if reservation_num in receipts_by_reservation:
                receipt_total = sum(
                    float(rec.get('amount', 0))
                    for rec in receipts_by_reservation[reservation_num]
                )
                if abs(receipt_total - total_amount) < 0.01:
                    invoice['matching_receipts'] = receipts_by_reservation[reservation_num]
                    paid_invoices.append(invoice)
                    revenue_date = invoice.get('_revenue_date', 'N/A')
                    raw_datetime = invoice.get('_raw_creation_datetime', 'N/A')
                    logging.debug(
                        f"Invoice {invoice.get('invoiceNumber')} is fully paid: "
                        f"{receipt_total:.2f}/{total_amount:.2f}, "
                        f"Created {raw_datetime}, Revenue date {revenue_date}"
                    )
                else:
                    logging.debug(
                        f"Invoice {invoice.get('invoiceNumber')} is partially paid: "
                        f"{receipt_total:.2f}/{total_amount:.2f}"
                    )

        logging.info(
            f"Identified {len(paid_invoices)} new fully paid invoices from "
            f"{len(new_invoices)} total new invoices"
        )
        return paid_invoices

    def aggregate_data(self, paid_invoices: List[Dict]) -> Dict:
        """Aggregate data for database insertion using exact API values"""
        aggregation = {
            'individual_rate': 0,
            'vat': 0,
            'municipality_tax': 0,
            'payment_methods': {}
        }

        for invoice in paid_invoices:
            invoice_vat = 0
            invoice_subtotal = 0

            # Use invoice item details if available
            if invoice.get('invoicesItemsDetalis'):
                for item in invoice.get('invoicesItemsDetalis', []):
                    item_subtotal = float(item.get('subTotal', 0))
                    item_vat = float(item.get('vatTaxCalculatedTotal', 0))

                    invoice_subtotal += item_subtotal
                    invoice_vat += item_vat
            else:
                # Fallback to invoice-level fields
                total_amount = float(invoice.get('totalAmount', 0))
                invoice_vat = float(invoice.get('vatAmount', 0))
                invoice_subtotal = total_amount - invoice_vat

            aggregation['individual_rate'] += invoice_subtotal
            aggregation['vat'] += invoice_vat

            # Process matching receipts for payment methods
            for receipt in invoice.get('matching_receipts', []):
                method_id = receipt.get('paymentMethodId')
                amount = float(receipt.get('amount', 0))
                if method_id not in aggregation['payment_methods']:
                    aggregation['payment_methods'][method_id] = 0
                aggregation['payment_methods'][method_id] += amount

        return aggregation

    def generate_docu(self) -> str:
        """Generate document number"""
        return "113"

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

    def insert_fhgl_tx_hed(self, conn, docu: str, revenue_date: date, aggregation: Dict) -> Tuple[str, str, int]:
        """Insert record into FhglTxHed table using revenue date"""
        if not self._validate_journal(conn, docu):
            raise ValueError(f"Invalid Docu {docu} for FhglTxHed insertion")

        cursor = conn.cursor()
        year = str(revenue_date.year)
        month = f"{revenue_date.month:02d}"
        serial = self.get_next_serial(conn, docu, year, month)
        date_val = revenue_date.strftime('%Y-%m-%d')
        row_guid = str(uuid.uuid4()).upper()

        sql = f"""
        INSERT INTO {HED_TABLE} (Docu, Year, Month, Serial, Date, Currency, Rate, Posted, ReEvaluate, RepeatedSerial, Flag)
        VALUES ('{docu}', '{year}', '{month}', {serial}, '{date_val}', '001', 1.0, 0, 0, NULL, NULL)
        """
        cursor.execute(sql)
        logging.info(
            f"Inserted {HED_TABLE} record: {docu}-{year}-{month}-{serial} "
            f"for revenue date {revenue_date}"
        )
        return year, month, serial

    def insert_fhgl_tx_ded(self, conn, docu: str, year: str, month: str, serial: int,
                           revenue_date: date, aggregation: Dict) -> None:
        """Insert records into FhglTxDed table with Cash Over & Short handling"""
        cursor = conn.cursor()
        line = 1

        # Round all amounts to 2 decimal places
        individual_rate = round(aggregation['individual_rate'], 2)
        vat = round(aggregation['vat'], 2)
        payment_methods = {k: round(v, 2) for k, v in aggregation['payment_methods'].items()}

        # Calculate totals for validation
        total_credits = individual_rate + vat
        total_debits = sum(payment_methods.values())

        # Calculate the difference
        difference = round(total_credits - total_debits, 2)

        # Log the balance check
        logging.info(
            f"Balance check for revenue date {revenue_date}: "
            f"Credits={total_credits:.2f}, Debits={total_debits:.2f}, Difference={difference:.2f}"
        )

        # Check if difference exceeds maximum allowed
        if abs(difference) > MAX_CASH_OVER_SHORT:
            logging.error(
                f"CRITICAL: Debit/Credit imbalance exceeds {MAX_CASH_OVER_SHORT} SAR "
                f"for revenue date {revenue_date}! Difference: {abs(difference):.2f}"
            )
            logging.error("This indicates partially paid invoices are being processed as fully paid!")
            raise ValueError(
                f"Accounting imbalance for revenue date {revenue_date}: "
                f"Credits={total_credits:.2f}, Debits={total_debits:.2f}, Difference={abs(difference):.2f}"
            )

        # Individual Rate (Credit)
        if individual_rate > 0:
            self._insert_fhgl_tx_ded_line(
                cursor, docu, year, month, serial, line,
                "101000020", 0, individual_rate, 0, individual_rate,
                f"FOC Dep.: Individual Rate for {revenue_date}"
            )
            line += 1

        # VAT (Credit)
        if vat > 0:
            self._insert_fhgl_tx_ded_line(
                cursor, docu, year, month, serial, line,
                "021500010", 0, vat, 0, vat,
                f"FOC Dep.: Value Added Tax for {revenue_date}"
            )
            line += 1

        # Municipality Tax (Credit - placeholder)
        self._insert_fhgl_tx_ded_line(
            cursor, docu, year, month, serial, line,
            "021500090", 0, 0, 0, 0,
            f"FOC Dep.: Municipality Tax for {revenue_date}"
        )
        line += 1

        # Payment Methods (Debits)
        for method_id, amount in payment_methods.items():
            if method_id in PAYMENT_METHOD_ACCOUNTS:
                account, description = PAYMENT_METHOD_ACCOUNTS[method_id]
                self._insert_fhgl_tx_ded_line(
                    cursor, docu, year, month, serial, line,
                    account, amount, 0, amount, 0,
                    f"FOC Dep.: {description} for {revenue_date}"
                )
                line += 1
            else:
                logging.warning(
                    f"Creating entry for unknown payment method ID: {method_id}, amount: {amount:.2f}"
                )
                self._insert_fhgl_tx_ded_line(
                    cursor, docu, year, month, serial, line,
                    "011200999", amount, 0, amount, 0,
                    f"FOC Dep.: Unknown Payment Method {method_id} for {revenue_date}"
                )
                line += 1

        # Handle Cash Over & Short if there's a difference
        if abs(difference) > 0:
            if difference > 0:
                # Credits > Debits: We need to debit Cash Over & Short
                self._insert_fhgl_tx_ded_line(
                    cursor, docu, year, month, serial, line,
                    CASH_OVER_SHORT_ACCOUNT, abs(difference), 0, abs(difference), 0,
                    f"FOC Dep.: Cash Over & Short for {revenue_date}"
                )
                logging.info(
                    f"Added Cash Over & Short DEBIT of {abs(difference):.2f} SAR "
                    f"for revenue date {revenue_date}"
                )
            else:
                # Debits > Credits: We need to credit Cash Over & Short
                self._insert_fhgl_tx_ded_line(
                    cursor, docu, year, month, serial, line,
                    CASH_OVER_SHORT_ACCOUNT, 0, abs(difference), 0, abs(difference),
                    f"FOC Dep.: Cash Over & Short for {revenue_date}"
                )
                logging.info(
                    f"Added Cash Over & Short CREDIT of {abs(difference):.2f} SAR "
                    f"for revenue date {revenue_date}"
                )
            line += 1

        logging.info(
            f"Inserted {line - 1} {DED_TABLE} records with balanced debits/credits "
            f"for revenue date {revenue_date}"
        )

    def _insert_fhgl_tx_ded_line(self, cursor, docu: str, year: str, month: str, serial: int,
                                 line: int, account: str, valu_le_dr: float, valu_le_cr: float,
                                 valu_fc_dr: float, valu_fc_cr: float, desc: str) -> None:
        """Insert a single line into FhglTxDed table"""
        row_guid = str(uuid.uuid4()).upper()
        desc_truncated = desc[:40] if len(desc) > 40 else desc.replace("'", "''")
        sql = f"""
        INSERT INTO {DED_TABLE} (Docu, Year, Month, Serial, Line, Account, ValuLeDr, ValuLeCr, ValuFcDr, ValuFcCr, [Desc])
        VALUES ('{docu}', '{year}', '{month}', {serial}, {line}, '{account}', {valu_le_dr}, {valu_le_cr}, {valu_fc_dr}, {valu_fc_cr}, '{desc_truncated}')
        """
        cursor.execute(sql)
        logging.debug(f"Inserted {DED_TABLE} line {line}: {account} - Dr:{valu_le_dr}, Cr:{valu_le_cr}")

    def insert_processed_invoices(self, conn, docu: str, paid_invoices: List[Dict]) -> None:
        """Insert processed invoices into tracking table with revenue date and raw creation date"""
        cursor = conn.cursor()
        for invoice in paid_invoices:
            invoice_number = invoice.get('invoiceNumber', '').replace("'", "''")
            reservation_number = invoice.get('reservationNumber', '').replace("'", "''")
            total_amount = float(invoice.get('totalAmount', 0))

            # Use revenue date for InvoiceDate field
            revenue_date = invoice.get('_revenue_date', self.current_date)
            revenue_date_str = revenue_date.strftime('%Y-%m-%d')

            # Store raw creation datetime for auditing
            raw_creation_datetime = invoice.get('_raw_creation_datetime')
            raw_datetime_str = raw_creation_datetime.strftime('%Y-%m-%d %H:%M:%S') if raw_creation_datetime else 'NULL'

            if raw_creation_datetime:
                sql = f"""
                INSERT INTO Processed_Invoices (InvoiceNumber, ReservationNumber, TotalAmount, Docu, InvoiceDate, RawInvoiceDate)
                VALUES ('{invoice_number}', '{reservation_number}', {total_amount}, '{docu}', '{revenue_date_str}', '{raw_datetime_str}')
                """
            else:
                sql = f"""
                INSERT INTO Processed_Invoices (InvoiceNumber, ReservationNumber, TotalAmount, Docu, InvoiceDate)
                VALUES ('{invoice_number}', '{reservation_number}', {total_amount}, '{docu}', '{revenue_date_str}')
                """

            try:
                cursor.execute(sql)
                logging.debug(
                    f"Tracked processed invoice: {invoice_number}, "
                    f"Revenue date: {revenue_date_str}, Raw creation date: {raw_datetime_str}"
                )
            except pyodbc.IntegrityError:
                logging.warning(f"Invoice {invoice_number} already exists in processed table")

        logging.info(f"Inserted {len(paid_invoices)} invoices into tracking table")

    def process_single_revenue_date(self, conn, revenue_date: date,
                                    date_invoices: List[Dict], all_receipts: List[Dict]) -> bool:
        """Process invoices for a single revenue date"""
        try:
            # Calculate raw creation period
            raw_dates = [inv.get('_raw_creation_datetime') for inv in date_invoices if '_raw_creation_datetime' in inv]
            if raw_dates:
                min_raw = min(raw_dates)
                max_raw = max(raw_dates)
                logging.info(
                    f"Processing {len(date_invoices)} invoices for revenue date {revenue_date} "
                    f"(created from {min_raw} to {max_raw})"
                )
            else:
                logging.info(f"Processing {len(date_invoices)} invoices for revenue date {revenue_date}")

            # Identify paid invoices
            paid_invoices = self.identify_paid_invoices(date_invoices, all_receipts)

            if not paid_invoices:
                logging.info(f"No fully paid invoices found for revenue date {revenue_date}")
                return True

            # Aggregate data
            aggregation = self.aggregate_data(paid_invoices)

            logging.info(
                f"Revenue date {revenue_date} aggregation: "
                f"Individual Rate: {aggregation['individual_rate']:.2f}, "
                f"VAT: {aggregation['vat']:.2f}, Payment Methods: {len(aggregation['payment_methods'])}"
            )

            # Log payment method breakdown
            for method_id, amount in aggregation['payment_methods'].items():
                method_name = PAYMENT_METHOD_ACCOUNTS.get(
                    method_id,
                    (f"Unknown-{method_id}", f"Unknown Method {method_id}")
                )[1]
                logging.info(f"  Payment Method {method_id} ({method_name}): {amount:.2f}")

            conn.autocommit = False
            try:
                docu = self.generate_docu()
                year, month, serial = self.insert_fhgl_tx_hed(conn, docu, revenue_date, aggregation)
                self.insert_fhgl_tx_ded(conn, docu, year, month, serial, revenue_date, aggregation)
                self.insert_processed_invoices(conn, docu, paid_invoices)
                conn.commit()

                logging.info(
                    f"Successfully processed {len(paid_invoices)} invoices for revenue date {revenue_date}"
                )
                logging.info(
                    f"  Total SubTotal: {aggregation['individual_rate']:.2f}, "
                    f"Total VAT: {aggregation['vat']:.2f}"
                )
                return True

            except Exception as e:
                conn.rollback()
                logging.error(f"Database transaction failed for revenue date {revenue_date}: {str(e)}")
                return False

        except Exception as e:
            logging.error(f"Processing failed for revenue date {revenue_date}: {str(e)}")
            return False

    def process_daily_data(self) -> bool:
        """Process data with revenue date assignment based on 12:00 PM cutoff using creationDate"""
        try:
            if isinstance(self.api_fetch_start, datetime):
                api_start_str = self.api_fetch_start.strftime('%Y-%m-%d %H:%M:%S')
                api_end_str = self.api_fetch_end.strftime('%Y-%m-%d %H:%M:%S')
            else:
                api_start_str = f"{self.api_fetch_start} 12:00:00"
                api_end_str = f"{self.api_fetch_end} 12:00:00"

            if isinstance(self.start_date, datetime):
                revenue_start_str = self.start_date.strftime('%Y-%m-%d %H:%M:%S')
                revenue_end_str = self.end_date.strftime('%Y-%m-%d %H:%M:%S')
            else:
                revenue_start_str = f"{self.start_date} 12:00:00"
                revenue_end_str = f"{self.end_date} 12:00:00"

            logging.info(f"API fetch range: {api_start_str} to {api_end_str}")
            logging.info(f"Revenue date processing range: {revenue_start_str} to {revenue_end_str}")
            logging.info("Using revenue date assignment: Before 12:00 PM → Previous day, At/After 12:00 PM → Same day")

            # Fetch all data for the date range
            invoices = self.fetch_invoices()
            receipts = self.fetch_receipts()

            if not invoices and not receipts:
                logging.warning("No data retrieved from API")
                return False

            # Group invoices by revenue date
            grouped_invoices = self.group_invoices_by_revenue_date(invoices)

            if not grouped_invoices:
                logging.info("No invoices to process after grouping")
                return True

            total_processed_dates = 0
            total_processed_invoices = 0
            failed_dates = 0

            with pyodbc.connect(CONNECTION_STRING) as conn:
                # Process each revenue date separately
                for revenue_date, date_invoices in grouped_invoices.items():
                    success = self.process_single_revenue_date(conn, revenue_date, date_invoices, receipts)

                    if success:
                        total_processed_dates += 1
                        # Count only the invoices that were actually processed (paid)
                        paid_count = len(self.identify_paid_invoices(date_invoices, receipts))
                        total_processed_invoices += paid_count
                    else:
                        failed_dates += 1
                        logging.error(f"Failed to process invoices for revenue date {revenue_date}")

            # Final summary
            logging.info(f"=== PROCESSING SUMMARY ===")
            logging.info(f"API fetch range: {api_start_str} to {api_end_str}")
            logging.info(f"Revenue date range: {revenue_start_str} to {revenue_end_str}")
            logging.info(f"Total revenue dates processed: {total_processed_dates}/{len(grouped_invoices)}")
            logging.info(f"Total invoices processed: {total_processed_invoices}")
            logging.info(f"Failed revenue dates: {failed_dates}")

            # Log revenue date coverage
            for revenue_date, date_invoices in grouped_invoices.items():
                raw_dates = [inv.get('_raw_creation_datetime') for inv in date_invoices if
                             '_raw_creation_datetime' in inv]
                if raw_dates:
                    min_raw = min(raw_dates)
                    max_raw = max(raw_dates)
                    paid_count = len(self.identify_paid_invoices(date_invoices, receipts))
                    logging.info(
                        f"Revenue date {revenue_date}: {paid_count} invoices processed "
                        f"(created from {min_raw} to {max_raw})"
                    )

            return failed_dates == 0

        except Exception as e:
            logging.error(f"Overall processing failed: {str(e)}")
            return False


def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser(
        description='Nazeel to Comsys Integration - Revenue Date Assignment with 12:00 PM Cutoff using creationDate'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date with time (YYYY-MM-DD HH:MM:SS), default: 60 days ago at 12:00 PM'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date with time (YYYY-MM-DD HH:MM:SS), default: today at 12:00 PM'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform a dry run without modifying the database'
    )
    args = parser.parse_args()

    if args.start_date and args.end_date:
        # Parse datetime strings
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d %H:%M:%S')
    else:
        # Default: Run as if it's 12:00 PM today, fetch last 60 days
        now = datetime.now()
        end_date = now.replace(hour=12, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=60)

    logging.info(f"Script run time: {datetime.now()}")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"Duration: {(end_date - start_date).days} days")

    if args.dry_run:
        logging.info("DRY RUN MODE: No database changes will be made")
        # TODO: Implement dry-run logic if needed

    integrator = NazeelComsysIntegrator(start_date, end_date)
    success = integrator.process_daily_data()

    if success:
        logging.info("Processing completed successfully")
        exit(0)
    else:
        logging.error("Processing failed")
        exit(1)


if __name__ == "__main__":
    main()