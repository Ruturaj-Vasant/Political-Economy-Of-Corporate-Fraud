from edgar import *
from datetime import datetime
import pandas as pd

# Set identity (required for SEC access)
set_identity('ruturaj@gmail.com')

# Get Apple company instance
apple = Company('AAPL')
print(f"Company Name: {apple.name}")
print(f"CIK: {apple.cik}")

# Get and display metadata for latest 10-K and DEF 14A
filings_10k = apple.get_filings(form='10-K')[-1]
filings_def14a = apple.get_filings(form='DEF 14A').latest()


# filings = apple.get_filings(form="10-K")
# latest_10k = filings.latest()
# tenk = latest_10k.obj()

print("\nLatest 10-K Filing:")

print(f"Filing Date: {filings_10k.filing_date}")
print(f"Report Period: {filings_10k.report_date}")
print(f"XBRL Available: {filings_10k.is_xbrl}")
# print(tenk.management_discussion)
# print(tenk.business)
# print(tenk.risk_factors)
# print(tenk.legal_proceedings)
# print("\nLatest DEF 14A Filing:")
# print(f"Filing: {filings_def14a.html()}")

# print(f"Filing: {filings_def14a.open()}")
# print(pd.DataFrame(filings_def14a.view()))
# print(pd.DataFrame(filings_10k.html()))