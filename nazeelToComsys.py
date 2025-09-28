#!/usr/bin/env python3
"""
Nazeel API to Comsys Database Integration Script
Fetches invoice and receipt voucher data from Nazeel API and inserts into Comsys database
Enhanced with 30-day lookback window for late payments
"""

import requests
import pyodbc
import hashlib
import uuid
import json
import logging
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from typing import Dict, List, Tuple, Optional

# Configuration
API_KEY = "tgh1QayxXvoXL8vnpQ5SOAZeR0ZeR0"
SECRET_KEY = "981fccc0-819e-4aa8-87d4-343c3c42c44a"
BASE_URL = "https://eai.nazeel.net/api/odoo-TransactionsTransfer"
CONNECTION_STRING = "DRIVER={SQL Server};SERVER=COMSYS-API;DATABASE=alshoribat;Trusted_Connection=yes;"
LOG_FILE = r"C:\Scripts\nazeel_log.txt"

# Table names
HED_TABLE = "FhglTxHed"
DED_TABLE = "FhglTxDed"

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
    UNIQUE(InvoiceNumber, ReservationNumber)
)
"""

# Payment method mapping
PAYMENT_METHOD_ACCOUNTS = {
    1: ("011500020", "Cash ( FO)"),
    2: ("011200065", "MADA"),
    3: ("011200060", "Master Card"),
    4: ("011200050", "Visa Card"),
    5: ("011500001", "Aljazera Bank"),
    6: ("011200070", "American Express"),
    7: ("011200080", "Payment Method 7"),
    8: ("011200090", "Payment Method 8"),
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
        if start_date and end_date:
            self.start_date = start_date
            self.end_date = end_date
        else:
            # Default to last 30 days through today at 23:00
            today = date.today()
            self.start_date = today - timedelta(days=60)
            self.end_date = today

        self.current_date = date.today()
        self.auth_key = self._generate_auth_key()
        self._ensure_processed_invoices_table()

    def _generate_auth_key(self) -> str:
        """Generate MD5 hash for authKey using secret key and current date"""
        date_str = self.current_date.strftime("%d/%m/%Y")
        combined = f"{SECRET_KEY}{date_str}"
        return hashlib.md5(combined.encode()).hexdigest()

    def _ensure_processed_invoices_table(self):
        """Ensure the processed invoices tracking table exists"""
        try:
            with pyodbc.connect(CONNECTION_STRING) as conn:
                cursor = conn.cursor()
                cursor.execute(CREATE_PROCESSED_INVOICES_TABLE)
                conn.commit()
                logging.info("Processed_Invoices table verified/created")
        except Exception as e:
            logging.error(f"Failed to create/verify Processed_Invoices table: {str(e)}")
            raise

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
        payload = {
            "apiKey": API_KEY,
            "dateFrom": f"{self.start_date} 00:00",
            "dateTo": f"{self.end_date} 23:59"
        }
        try:
            logging.info(
                f"Making API request to {endpoint} for date range: {self.start_date} 00:00 to {self.end_date} 23:59")
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
        valid_invoices = [inv for inv in data if isinstance(inv, dict) and not inv.get('isReversed', False)]
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
                    logging.info(
                        f"Invoice {invoice.get('invoiceNumber')} is fully paid: {receipt_total:.2f}/{total_amount:.2f}")
                else:
                    logging.debug(
                        f"Invoice {invoice.get('invoiceNumber')} is partially paid: {receipt_total:.2f}/{total_amount:.2f}")

        logging.info(
            f"Identified {len(paid_invoices)} new fully paid invoices from {len(new_invoices)} total new invoices")
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

        logging.info(f"Aggregated data: Individual Rate: {aggregation['individual_rate']:.2f}, "
                     f"VAT: {aggregation['vat']:.2f}, Payment Methods: {len(aggregation['payment_methods'])}")

        # Log payment method breakdown
        for method_id, amount in aggregation['payment_methods'].items():
            method_name = \
                PAYMENT_METHOD_ACCOUNTS.get(method_id, (f"Unknown-{method_id}", f"Unknown Method {method_id}"))[1]
            logging.info(f"  Payment Method {method_id} ({method_name}): {amount:.2f}")

        return aggregation

    def generate_docu(self) -> str:
        """Generate document number"""
        return "101"

    def check_duplicate_docu(self, conn, docu: str, year: str, month: str, serial: int) -> bool:
        """Check if document number already exists"""
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {HED_TABLE} WHERE Docu = ? AND Year = ? AND Month = ? AND Serial = ?",
                           (docu, year, month, serial))
            return cursor.fetchone()[0] > 0
        except Exception as e:
            logging.error(f"Error checking duplicate DOCU: {str(e)}")
            return True

    def get_next_serial(self, conn, docu: str, year: str, month: str) -> int:
        """Get the next available serial number"""
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT ISNULL(MAX(Serial), 0) + 1 FROM {HED_TABLE} WHERE Docu = ? AND Year = ? AND Month = ?",
                (docu, year, month))
            return cursor.fetchone()[0]
        except Exception as e:
            logging.error(f"Error getting next serial: {str(e)}")
            return 1

    def insert_fhgl_tx_hed(self, conn, docu: str, aggregation: Dict) -> Tuple[str, str, int]:
        """Insert record into FhglTxHed table"""
        cursor = conn.cursor()
        # Use current date for the transaction date, not the search date range
        transaction_date = date.today()
        year = str(transaction_date.year)
        month = f"{transaction_date.month:02d}"
        serial = self.get_next_serial(conn, docu, year, month)
        date_val = transaction_date.strftime('%Y-%m-%d')
        row_guid = str(uuid.uuid4()).upper()

        sql = f"""
        INSERT INTO {HED_TABLE} (Docu, Year, Month, Serial, Date, Currency, Rate, Posted, ReEvaluate, RepeatedSerial, Flag, rowguid)
        VALUES ('{docu}', '{year}', '{month}', {serial}, '{date_val}', '001', 1.0, 0, 0, NULL, NULL, '{row_guid}')
        """
        cursor.execute(sql)
        logging.info(
            f"Inserted {HED_TABLE} record: {docu}-{year}-{month}-{serial} for transaction date {transaction_date}")
        return year, month, serial

    def insert_fhgl_tx_ded(self, conn, docu: str, year: str, month: str, serial: int, aggregation: Dict) -> None:
        """Insert records into FhglTxDed table with proper debit/credit balance validation"""
        cursor = conn.cursor()
        line = 1
        transaction_date = date.today()

        # Calculate totals for validation
        total_credits = aggregation['individual_rate'] + aggregation['vat']
        total_debits = sum(aggregation['payment_methods'].values())

        # Log the balance check
        balance_diff = abs(total_credits - total_debits)
        logging.info(
            f"Balance check: Credits={total_credits:.2f}, Debits={total_debits:.2f}, Difference={balance_diff:.2f}")

        if balance_diff > 0.01:
            logging.error(f"CRITICAL: Debit/Credit imbalance detected! Difference: {balance_diff:.2f}")
            logging.error("This indicates partially paid invoices are being processed as fully paid!")
            raise ValueError(f"Accounting imbalance: Credits={total_credits:.2f}, Debits={total_debits:.2f}")

        # Individual Rate (Credit)
        if aggregation['individual_rate'] > 0:
            self._insert_fhgl_tx_ded_line(
                cursor, docu, year, month, serial, line,
                "101000020", 0, aggregation['individual_rate'], 0, aggregation['individual_rate'],
                f"FOC Dep.: Individual Rate for {transaction_date}"
            )
            line += 1

        # VAT (Credit)
        if aggregation['vat'] > 0:
            self._insert_fhgl_tx_ded_line(
                cursor, docu, year, month, serial, line,
                "021500010", 0, aggregation['vat'], 0, aggregation['vat'],
                f"FOC Dep.: Value Added Tax for {transaction_date}"
            )
            line += 1

        # Municipality Tax (Credit - placeholder)
        self._insert_fhgl_tx_ded_line(
            cursor, docu, year, month, serial, line,
            "021500090", 0, 0, 0, 0,
            f"FOC Dep.: Municipality Tax for {transaction_date}"
        )
        line += 1

        # Payment Methods (Debits)
        for method_id, amount in aggregation['payment_methods'].items():
            if method_id in PAYMENT_METHOD_ACCOUNTS:
                account, description = PAYMENT_METHOD_ACCOUNTS[method_id]
                self._insert_fhgl_tx_ded_line(
                    cursor, docu, year, month, serial, line,
                    account, amount, 0, amount, 0,
                    f"FOC Dep.: {description} for {transaction_date}"
                )
                line += 1
            else:
                # Create entry for unknown payment methods using a generic account
                logging.warning(f"Creating entry for unknown payment method ID: {method_id}, amount: {amount:.2f}")
                self._insert_fhgl_tx_ded_line(
                    cursor, docu, year, month, serial, line,
                    "011200999", amount, 0, amount, 0,  # Generic unknown payment method account
                    f"FOC Dep.: Unknown Payment Method {method_id} for {transaction_date}"
                )
                line += 1

        logging.info(f"Inserted {line - 1} {DED_TABLE} records with balanced debits/credits")

    def _insert_fhgl_tx_ded_line(self, cursor, docu: str, year: str, month: str, serial: int,
                                 line: int, account: str, valu_le_dr: float, valu_le_cr: float,
                                 valu_fc_dr: float, valu_fc_cr: float, desc: str) -> None:
        """Insert a single line into FhglTxDed table"""
        row_guid = str(uuid.uuid4()).upper()
        desc_truncated = desc[:40] if len(desc) > 40 else desc.replace("'", "''")
        sql = f"""
        INSERT INTO {DED_TABLE} (Docu, Year, Month, Serial, Line, Account, ValuLeDr, ValuLeCr, ValuFcDr, ValuFcCr, [Desc], rowguid)
        VALUES ('{docu}', '{year}', '{month}', {serial}, {line}, '{account}', {valu_le_dr}, {valu_le_cr}, {valu_fc_dr}, {valu_fc_cr}, '{desc_truncated}', '{row_guid}')
        """
        cursor.execute(sql)
        logging.debug(f"Inserted {DED_TABLE} line {line}: {account} - Dr:{valu_le_dr}, Cr:{valu_le_cr}")

    def insert_processed_invoices(self, conn, docu: str, paid_invoices: List[Dict]) -> None:
        """Insert processed invoices into tracking table"""
        cursor = conn.cursor()
        for invoice in paid_invoices:
            invoice_number = invoice.get('invoiceNumber', '').replace("'", "''")
            reservation_number = invoice.get('reservationNumber', '').replace("'", "''")
            total_amount = float(invoice.get('totalAmount', 0))
            invoice_date_str = invoice.get('invoiceDate', '')
            invoice_date = datetime.fromisoformat(
                invoice_date_str.replace('T00:00:00', '')).date() if invoice_date_str else date.today()
            invoice_date_str = invoice_date.strftime('%Y-%m-%d')
            sql = f"""
            INSERT INTO Processed_Invoices (InvoiceNumber, ReservationNumber, TotalAmount, Docu, InvoiceDate)
            VALUES ('{invoice_number}', '{reservation_number}', {total_amount}, '{docu}', '{invoice_date_str}')
            """
            try:
                cursor.execute(sql)
                logging.debug(f"Tracked processed invoice: {invoice_number}")
            except pyodbc.IntegrityError:
                logging.warning(f"Invoice {invoice_number} already exists in processed table")
        logging.info(f"Inserted {len(paid_invoices)} invoices into tracking table")

    def process_daily_data(self) -> bool:
        """Process data for the date range (30 days lookback)"""
        try:
            logging.info(f"Processing data for date range: {self.start_date} to {self.end_date}")
            logging.info(
                f"This covers the last {(self.end_date - self.start_date).days + 1} days to catch late payments")

            invoices = self.fetch_invoices()
            receipts = self.fetch_receipts()

            if not invoices and not receipts:
                logging.warning("No data retrieved from API")
                return False

            paid_invoices = self.identify_paid_invoices(invoices, receipts)

            if not paid_invoices:
                logging.info("No new fully paid invoices found")
                return True

            aggregation = self.aggregate_data(paid_invoices)

            with pyodbc.connect(CONNECTION_STRING) as conn:
                conn.autocommit = False
                try:
                    docu = self.generate_docu()
                    year, month, serial = self.insert_fhgl_tx_hed(conn, docu, aggregation)
                    self.insert_fhgl_tx_ded(conn, docu, year, month, serial, aggregation)
                    self.insert_processed_invoices(conn, docu, paid_invoices)
                    conn.commit()

                    logging.info(f"Successfully processed {len(paid_invoices)} invoices")
                    logging.info(
                        f"Total SubTotal: {aggregation['individual_rate']:.2f}, Total VAT: {aggregation['vat']:.2f}")
                    logging.info(f"Date range processed: {self.start_date} to {self.end_date}")
                    return True

                except Exception as e:
                    conn.rollback()
                    logging.error(f"Database transaction failed: {str(e)}")
                    return False

        except Exception as e:
            logging.error(f"Processing failed: {str(e)}")
            return False


def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser(description='Nazeel to Comsys Integration')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD), default: 30 days ago')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD), default: today')
    args = parser.parse_args()

    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        # Default: last 30 days through today
        today = date.today()
        start_date = today - timedelta(days=60)
        end_date = today

    logging.info(f"Date range: {start_date} to {end_date} ({(end_date - start_date).days + 1} days)")

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
