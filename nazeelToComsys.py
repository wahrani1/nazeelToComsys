#!/usr/bin/env python3
"""
Nazeel API to Comsys Database Integration Script
Fetches invoice and receipt voucher data from Nazeel API and inserts into Comsys database
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
SECRET_KEY = "981fccc0-819e-4aa8-87d4-343c3c42c44a"  # Replace with actual secret key
BASE_URL = "https://eai.nazeel.net/api/odoo-TransactionsTransfer"
CONNECTION_STRING = "DRIVER={SQL Server};SERVER=COMSYS-API;DATABASE=alshoribat;Trusted_Connection=yes;"
LOG_FILE = r"C:\Scripts\nazeel_log.txt"


# Table names (updated to match your actual tables)
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
    3: ("011200060", "Master Card"),  # Confirm ID
    4: ("011200050", "Visa Card"),    # Confirm ID
    5: ("011500001", "Aljazera Bank") # Confirm ID
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
            # Default to current month
            today = date.today()
            self.start_date = today.replace(day=1)  # First day of current month
            self.end_date = today  # Current date
        
        self.current_date = date.today()  # For auth key generation
        self.auth_key = self._generate_auth_key()
        self._ensure_processed_invoices_table()
        
    def _generate_auth_key(self) -> str:
        """Generate MD5 hash for authKey using secret key and current date"""
        date_str = self.current_date.strftime("%d/%m/%Y")  # dd/mm/yyyy format
        combined = f"{SECRET_KEY}{date_str}"  # Direct concatenation: secretkey + dd/mm/yyyy
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
            logging.info(f"Making API request to {endpoint} for date range: {self.start_date} to {self.end_date}")
            logging.debug(f"Request URL: {url}")
            logging.debug(f"Request headers: {headers}")
            logging.debug(f"Request payload: {payload}")
            
            response = requests.post(url, json=payload, headers=headers, timeout=60)  # Increased timeout for month data
            response.raise_for_status()
            
            logging.debug(f"Raw response: {response.text}")
            
            data = response.json()
            logging.debug(f"Parsed response type: {type(data)}")
            logging.debug(f"Response data: {data}")
            
            if isinstance(data, dict) and data.get('status') == 200:
                return data.get('data', [])
            elif isinstance(data, list):
                # Sometimes API returns data directly as a list
                return data
            else:
                logging.error(f"API returned unexpected response: {data}")
                return None
                
        except requests.RequestException as e:
            logging.error(f"API request failed for {endpoint}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response: {str(e)}")
            logging.error(f"Raw response text: {response.text if 'response' in locals() else 'No response'}")
            return None
    
    def fetch_invoices(self) -> List[Dict]:
        """Fetch invoices from API"""
        data = self._make_api_request("Getinvoices")
        if data is None:
            return []
        
        # Ensure data is a list
        if not isinstance(data, list):
            logging.error(f"Expected list from API, got {type(data)}: {data}")
            return []
        
        # Filter out reversed invoices and ensure each item is a dict
        valid_invoices = []
        for inv in data:
            if isinstance(inv, dict) and not inv.get('isReversed', False):
                valid_invoices.append(inv)
            elif not isinstance(inv, dict):
                logging.warning(f"Invalid invoice data type: {type(inv)}")
        
        logging.info(f"Fetched {len(valid_invoices)} valid invoices")
        return valid_invoices
    
    def fetch_receipts(self) -> List[Dict]:
        """Fetch receipt vouchers from API"""
        data = self._make_api_request("GetReciptVouchers")
        if data is None:
            return []
        
        # Ensure data is a list
        if not isinstance(data, list):
            logging.error(f"Expected list from API, got {type(data)}: {data}")
            return []
        
        # Filter out canceled receipts and ensure each item is a dict
        valid_receipts = []
        for rec in data:
            if isinstance(rec, dict) and not rec.get('isCanceled', False):
                valid_receipts.append(rec)
            elif not isinstance(rec, dict):
                logging.warning(f"Invalid receipt data type: {type(rec)}")
        
        logging.info(f"Fetched {len(valid_receipts)} valid receipts")
        return valid_receipts
    
    def identify_paid_invoices(self, invoices: List[Dict], receipts: List[Dict]) -> List[Dict]:
        """Identify fully paid invoices by matching with receipts"""
        paid_invoices = []
        
        # First filter out already processed invoices
        new_invoices = self.filter_new_invoices(invoices)
        
        # Group receipts by reservation number
        receipts_by_reservation = {}
        for receipt in receipts:
            reservation_num = receipt.get('reservationNumber')
            if reservation_num:
                if reservation_num not in receipts_by_reservation:
                    receipts_by_reservation[reservation_num] = []
                receipts_by_reservation[reservation_num].append(receipt)
        
        # Check each new invoice for payment
        for invoice in new_invoices:
            reservation_num = invoice.get('reservationNumber')
            total_amount = float(invoice.get('totalAmount', 0))
            
            if reservation_num in receipts_by_reservation:
                # Sum receipt amounts for this reservation
                receipt_total = sum(
                    float(rec.get('amount', 0)) 
                    for rec in receipts_by_reservation[reservation_num]
                )
                
                # Check if fully paid (allowing for small floating point differences)
                if abs(receipt_total - total_amount) < 0.01:
                    invoice['matching_receipts'] = receipts_by_reservation[reservation_num]
                    paid_invoices.append(invoice)
                    logging.debug(f"Invoice {invoice.get('invoiceNumber')} is fully paid")
                else:
                    logging.debug(f"Invoice {invoice.get('invoiceNumber')} is partially paid: {receipt_total}/{total_amount}")
        
        logging.info(f"Identified {len(paid_invoices)} new fully paid invoices")
        return paid_invoices
    
    def aggregate_data(self, paid_invoices: List[Dict]) -> Dict:
        """Aggregate data for database insertion"""
        aggregation = {
            'individual_rate': 0,
            'vat': 0,
            'municipality_tax': 0,  # Placeholder
            'payment_methods': {}
        }
        
        for invoice in paid_invoices:
            # Sum individual rates and VAT from invoice items
            for item in invoice.get('invoicesItemsDetalis', []):
                aggregation['individual_rate'] += float(item.get('subTotal', 0))
                aggregation['vat'] += float(item.get('vatTaxCalculatedTotal', 0))
            
            # If no detailed items, use invoice-level amounts
            if not invoice.get('invoicesItemsDetalis'):
                vat_amount = float(invoice.get('vatAmount', 0))
                total_amount = float(invoice.get('totalAmount', 0))
                aggregation['individual_rate'] += (total_amount - vat_amount)
                aggregation['vat'] += vat_amount
            
            # Aggregate payments by method
            for receipt in invoice.get('matching_receipts', []):
                method_id = receipt.get('paymentMethodId')
                amount = float(receipt.get('amount', 0))
                
                if method_id not in aggregation['payment_methods']:
                    aggregation['payment_methods'][method_id] = 0
                aggregation['payment_methods'][method_id] += amount
        
        logging.info(f"Aggregated data: Individual Rate: {aggregation['individual_rate']}, "
                    f"VAT: {aggregation['vat']}, Payment Methods: {len(aggregation['payment_methods'])}")
        
        return aggregation
    
    def generate_docu(self) -> str:
        """Generate document number in format NZAPI (5 characters to match varchar(5))"""
        return "NZAPI"
    
    def check_duplicate_docu(self, conn, docu: str, year: str, month: str, serial: int) -> bool:
        """Check if document number already exists for this year/month/serial"""
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {HED_TABLE} WHERE Docu = ? AND Year = ? AND Month = ? AND Serial = ?", 
                          (docu, year, month, serial))
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logging.error(f"Error checking duplicate DOCU: {str(e)}")
            return True  # Assume duplicate to be safe
    
    def get_next_serial(self, conn, docu: str, year: str, month: str) -> int:
        """Get the next available serial number for this docu/year/month"""
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT ISNULL(MAX(Serial), 0) + 1 FROM {HED_TABLE} WHERE Docu = ? AND Year = ? AND Month = ?", 
                          (docu, year, month))
            return cursor.fetchone()[0]
        except Exception as e:
            logging.error(f"Error getting next serial: {str(e)}")
            return 1
    
    def insert_fhgl_tx_hed(self, conn, docu: str, aggregation: Dict) -> Tuple[str, str, int]:
        """Insert record into FhglTxHed table"""
        cursor = conn.cursor()
        
        year = str(self.current_date.year)
        month = f"{self.current_date.month:02d}"
        serial = self.get_next_serial(conn, docu, year, month)
        
        date_val = self.current_date
        row_guid = uuid.uuid4()
        
        sql = f"""
        INSERT INTO {HED_TABLE} (Docu, Year, Month, Serial, Date, Currency, Rate, Posted, ReEvaluate, RepeatedSerial, Flag, rowguid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Convert boolean values to bit (0/1) and ensure proper data types
        values = (
            str(docu),           # varchar(5)
            str(year),           # varchar(4) 
            str(month),          # varchar(2)
            int(serial),         # int
            date_val,            # smalldatetime
            str("SAR"),          # varchar(5)
            float(1.0),          # float
            0,                   # bit (Posted) - use 0 instead of False
            0,                   # bit (ReEvaluate) - use 0 instead of False
            None,                # int (RepeatedSerial)
            None,                # tinyint (Flag)
            row_guid             # uniqueidentifier
        )
        
        cursor.execute(sql, values)
        logging.info(f"Inserted {HED_TABLE} record: {docu}-{year}-{month}-{serial}")
        
        return year, month, serial
    
    def insert_fhgl_tx_ded(self, conn, docu: str, year: str, month: str, serial: int, aggregation: Dict) -> None:
        """Insert records into FhglTxDed table"""
        cursor = conn.cursor()
        line = 1
        
        # Line 1: Individual Rate (Credit)
        if aggregation['individual_rate'] > 0:
            self._insert_fhgl_tx_ded_line(
                cursor, docu, year, month, serial, line,
                "101000020", 0, aggregation['individual_rate'], 0, aggregation['individual_rate'],
                "FOC Dep.: Individual Rate"
            )
            line += 1
        
        # Line 2: VAT (Credit)
        if aggregation['vat'] > 0:
            self._insert_fhgl_tx_ded_line(
                cursor, docu, year, month, serial, line,
                "021500010", 0, aggregation['vat'], 0, aggregation['vat'],
                "FOC Dep.: Value Added Tax"
            )
            line += 1
        
        # Line 3: Municipality Tax (Credit) - Placeholder
        self._insert_fhgl_tx_ded_line(
            cursor, docu, year, month, serial, line,
            "021500090", 0, 0, 0, 0,
            "FOC Dep.: Municipality Tax"
        )
        line += 1
        
        # Lines 4+: Payment Methods (Debit)
        for method_id, amount in aggregation['payment_methods'].items():
            if method_id in PAYMENT_METHOD_ACCOUNTS:
                account, description = PAYMENT_METHOD_ACCOUNTS[method_id]
                self._insert_fhgl_tx_ded_line(
                    cursor, docu, year, month, serial, line,
                    account, amount, 0, amount, 0,
                    f"FOC Dep.: {description}"
                )
                line += 1
            else:
                logging.warning(f"Unknown payment method ID: {method_id}")
        
        logging.info(f"Inserted {line-1} {DED_TABLE} records")
    
    def _insert_fhgl_tx_ded_line(self, cursor, docu: str, year: str, month: str, serial: int, 
                                line: int, account: str, valu_le_dr: float, valu_le_cr: float,
                                valu_fc_dr: float, valu_fc_cr: float, desc: str) -> None:
        """Insert a single line into FhglTxDed table"""
        row_guid = uuid.uuid4()
        
        sql = f"""
        INSERT INTO {DED_TABLE} (Docu, Year, Month, Serial, Line, Account, ValuLeDr, ValuLeCr, ValuFcDr, ValuFcCr, Desc, rowguid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Truncate description to fit varchar(40) limit
        desc_truncated = desc[:40] if len(desc) > 40 else desc
        
        values = (docu, year, month, serial, line, account, valu_le_dr, valu_le_cr, valu_fc_dr, valu_fc_cr, desc_truncated, row_guid)
        cursor.execute(sql, values)
        
        logging.debug(f"Inserted {DED_TABLE} line {line}: {account} - Dr:{valu_le_dr}, Cr:{valu_le_cr}")
    
    def insert_processed_invoices(self, conn, docu: str, paid_invoices: List[Dict]) -> None:
        """Insert processed invoices into tracking table"""
        cursor = conn.cursor()
        
        for invoice in paid_invoices:
            invoice_number = invoice.get('invoiceNumber')
            reservation_number = invoice.get('reservationNumber')
            total_amount = float(invoice.get('totalAmount', 0))
            invoice_date_str = invoice.get('invoiceDate', '')
            
            # Parse invoice date
            if invoice_date_str:
                try:
                    invoice_date = datetime.fromisoformat(invoice_date_str.replace('T00:00:00', '')).date()
                except:
                    invoice_date = self.current_date
            else:
                invoice_date = self.current_date
            
            sql = """
            INSERT INTO Processed_Invoices (InvoiceNumber, ReservationNumber, TotalAmount, Docu, InvoiceDate)
            VALUES (?, ?, ?, ?, ?)
            """
            
            try:
                cursor.execute(sql, (invoice_number, reservation_number, total_amount, docu, invoice_date))
                logging.debug(f"Tracked processed invoice: {invoice_number}")
            except pyodbc.IntegrityError:
                # Invoice already exists (shouldn't happen with our filtering, but just in case)
                logging.warning(f"Invoice {invoice_number} already exists in processed table")
        
        logging.info(f"Inserted {len(paid_invoices)} invoices into tracking table")
    
    def process_monthly_data(self) -> bool:
        """Main processing function for monthly data"""
        try:
            logging.info(f"Starting monthly processing for date range: {self.start_date} to {self.end_date}")
            
            # Fetch data from API
            invoices = self.fetch_invoices()
            receipts = self.fetch_receipts()
            
            if not invoices and not receipts:
                logging.warning("No data retrieved from API")
                return False
            
            # Identify paid invoices
            paid_invoices = self.identify_paid_invoices(invoices, receipts)
            
            if not paid_invoices:
                logging.info("No fully paid invoices found for the date range")
                return True  # Not an error, just no data to process
            
            # Group invoices by date and process each day separately
            invoices_by_date = {}
            for invoice in paid_invoices:
                invoice_date_str = invoice.get('invoiceDate', '')
                if invoice_date_str:
                    try:
                        invoice_date = datetime.fromisoformat(invoice_date_str.replace('T00:00:00', '')).date()
                    except:
                        invoice_date = self.current_date
                else:
                    invoice_date = self.current_date
                
                if invoice_date not in invoices_by_date:
                    invoices_by_date[invoice_date] = []
                invoices_by_date[invoice_date].append(invoice)
            
            # Process each date separately
            total_processed = 0
            for process_date in sorted(invoices_by_date.keys()):
                daily_invoices = invoices_by_date[process_date]
                logging.info(f"Processing {len(daily_invoices)} invoices for {process_date}")
                
                # Aggregate data for this date
                aggregation = self.aggregate_data(daily_invoices)
                
                # Database operations
                with pyodbc.connect(CONNECTION_STRING) as conn:
                    conn.autocommit = False  # Use transaction
                    
                    try:
                        docu = self.generate_docu()
                        year = str(process_date.year)
                        month = f"{process_date.month:02d}"
                        serial = self.get_next_serial(conn, docu, year, month)
                        
                        # Check for duplicates
                        if self.check_duplicate_docu(conn, docu, year, month, serial):
                            logging.warning(f"Duplicate entry found for {process_date}: {docu}-{year}-{month}-{serial}. Skipping this date.")
                            continue
                        
                        # Temporarily override current_date for this transaction
                        original_date = self.current_date
                        self.current_date = process_date
                        
                        # Insert records
                        year_str, month_str, actual_serial = self.insert_fhgl_tx_hed(conn, docu, aggregation)
                        self.insert_fhgl_tx_ded(conn, docu, year_str, month_str, actual_serial, aggregation)
                        self.insert_processed_invoices(conn, docu, daily_invoices)
                        
                        # Restore original date
                        self.current_date = original_date
                        
                        # Commit transaction
                        conn.commit()
                        total_processed += len(daily_invoices)
                        logging.info(f"Successfully processed {len(daily_invoices)} invoices for {process_date}")
                        
                    except Exception as e:
                        conn.rollback()
                        self.current_date = original_date  # Ensure we restore the date
                        logging.error(f"Database transaction failed for {process_date}: {str(e)}")
                        # Continue with next date instead of stopping
                        continue
            
            if total_processed > 0:
                logging.info(f"Monthly processing completed successfully. Total invoices processed: {total_processed}")
                return True
            else:
                logging.warning("No invoices were processed")
                return False
                    
        except Exception as e:
            logging.error(f"Monthly processing failed: {str(e)}")
            return False

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Nazeel to Comsys Integration')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--current-month', action='store_true', help='Process current month from start to today')
    parser.add_argument('--full-month', type=str, help='Process full month (YYYY-MM)')
    
    args = parser.parse_args()
    
    start_date = None
    end_date = None
    
    if args.start_date and args.end_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            exit(1)
    elif args.current_month:
        today = date.today()
        start_date = today.replace(day=1)
        end_date = today
    elif args.full_month:
        try:
            year, month = map(int, args.full_month.split('-'))
            start_date = date(year, month, 1)
            # Last day of the month
            if month == 12:
                end_date = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = date(year, month + 1, 1) - timedelta(days=1)
        except ValueError:
            print("Error: Invalid month format. Use YYYY-MM")
            exit(1)
    
    integrator = NazeelComsysIntegrator(start_date, end_date)
    
    if start_date and end_date:
        logging.info(f"Processing date range: {start_date} to {end_date}")
        success = integrator.process_monthly_data()
    else:
        logging.info("Processing current day only")
        success = integrator.process_monthly_data()  # Will use default dates
    
    if success:
        logging.info("Processing completed successfully")
        exit(0)
    else:
        logging.error("Processing failed")
        exit(1)

if __name__ == "__main__":
    main()