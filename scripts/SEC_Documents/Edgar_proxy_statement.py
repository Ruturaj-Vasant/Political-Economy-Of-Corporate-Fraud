# %%
import os
import pandas as pd
from edgar import *
# Set identity
set_identity('ruturaj@gmail.com')

# Choose the ticker
ticker = 'WELL'
company = Company(ticker)
print(f"Company Name: {company.name}, CIK: {company.cik}")

# print(f"Company Name: {getattr(company, 'name', 'N/A')}, CIK: {getattr(company, 'cik', 'N/A')}")

# # Fetch all DEF 14A filings
proxy_filings = company.get_filings(form='DEF 14A')
print(f"Total DEF 14A Filings Found: {len(proxy_filings)}")
print(f"Filings: {proxy_filings}")

# Create base directory for the company
base_path = f"data/{ticker.upper()}"
os.makedirs(base_path, exist_ok=True)
print(f"Directory created at: {base_path}")

for i, filing in enumerate(proxy_filings):
    print(f"\nProcessing filing {i + 1}/{len(proxy_filings)}")
    print(f"Filing Date: {filing.filing_date}, Accession No: {filing.accession_no}")

    primary = filing.primary_document
    print(f"Primary Document: {primary}")

    try:
        content = None
        ext = "txt"  # default extension

        # --- Case 1: .htm or .html file ---
        if primary and primary.lower().endswith((".htm", ".html")):
            try:
                content = filing.html()
                if content and "<html" in content.lower():
                    # Valid HTML — save as .html
                    ext = "html"
                    print("→ Valid HTML detected, saving as .html")
                else:
                    # HTML file but plain text
                    print("→ HTML tag missing, retrying as text and saving as .txt")
                    content = filing.text()
                    ext = "txt"
            except Exception:
                print("→ HTML fetch failed, retrying as text.")
                content = filing.text()
                ext = "txt"

        # --- Case 2: Non-HTML (plain text or unknown) ---
        else:
            content = filing.text()
            ext = "txt"

        # --- Save file if content exists ---
        if content:
            filing_date = getattr(filing, "filing_date", None) or f"filing_{i + 1}"
            filename = os.path.join(base_path, f"{filing_date}_DEF14A.{ext}")

            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Saved: {filename} ({len(content)} characters)")
        else:
            print(f"No content fetched for filing dated {filing.filing_date}")

    except Exception as e:
        print(f"Error retrieving filing {i + 1} ({filing.filing_date}): {e}")
    # attachments = filing.attachments
    # print(f"Number of attachments: {len(attachments)}")
#     print ("Attachments:", attachments)


# for i, filing in enumerate(proxy_filings):
#     print(f"\nProcessing filing {i+1}/{len(proxy_filings)}")
#     try:
#         # Use filing_date if available, else fallback to index-based name
#         filing_date = getattr(filing, "filing_date", None)
#         if not filing_date:
#             filing_date = f"filing_{i+1}"
#         filename = os.path.join(base_path, f"{filing_date}_DEF14A.txt")
#         # filename = os.path.join(base_path, f"{filing_date}_10K.txt")

#         # Save filing text
#         text = filing.text()
#         with open(filename, "w", encoding="utf-8") as f:
#             f.write(text)
#         print(f"Saved: {filename}")
#     except Exception as e:
#         print(f"Error saving filing {i+1}: {e}")






#     # for attachment in filing.attachments:
#     #     print(f"Document: {attachment.document}")
#     #     print(f"Description: {attachment.description}")
#     #     # print(f"Type: {attachment.type}")
#     #     print("---")

filing = proxy_filings[-1]

print(f"CIK: {filing.cik}")
print(f"Company: {filing.company}")
print(f"Form: {filing.form}")
print(f"Filing Date: {filing.filing_date}")
print(f"Report Date: {filing.report_date}")
print(f"Accession Number: {filing.accession_no}")


# Print core and extended filing properties
print("\n--- Filing Properties ---")
print(f"CIK: {filing.cik}")
print(f"Company: {filing.company}")
print(f"Form: {filing.form}")
print(f"Filing Date: {filing.filing_date}")
print(f"Report Date: {filing.report_date}")
print(f"Acceptance Datetime: {filing.acceptance_datetime}")
print(f"Accession Number: {filing.accession_no}")
print(f"File Number: {filing.file_number}")
print(f"Items: {filing.items}")
print(f"Size (bytes): {filing.size}")
print(f"Primary Document: {filing.primary_document}")
print(f"Primary Document Description: {filing.primary_doc_description}")
print(f"Contains XBRL: {filing.is_xbrl}")
print(f"Uses Inline XBRL: {filing.is_inline_xbrl}")
print("---\n")


attachments = filing.attachments
print(f"Number of attachments: {len(attachments)}")

for attachment in filing.attachments:
    print(f"Document: {attachment.document}")
    print(f"Description: {attachment.description}")
    # print(f"Type: {attachment.type}")
    print("---")

# # Fetch all 10-K filings
# # proxy_filings = company.get_filings(form='10-K')
# # print(f"Total 10-K Filings Found: {len(proxy_filings)}")

# xbrl = filing.xbrl()
# print(f"XBRL Instance Document: {xbrl}")  

# if xbrl:
#     print("\n--- XBRL Summary ---")
#     print(f"Number of facts: {len(xbrl.facts)}")
#     print(f"Number of contexts: {len(xbrl.contexts)}")
#     print(f"Statements: {xbrl.statements}")
#     # print(f"Statements available: {xbrl.statements.list_names()}")
# else:
#     print("No XBRL data found.")

# print("\n--- Sample XBRL statements ---")
# for stmt in xbrl.statements:
#     print(f"\nStatement: {stmt}")
#     if stmt == None:
#         break

# print(f"Sample XBRL facts:)")
# for fact in xbrl.facts:  # Print first 5 facts as a sample
#     print(fact)

# for stmt in xbrl.statements:
#     print(f"\nStatement: {stmt.name}")
#     print(stmt.to_dataframe().head())  # Shows first few rows

# print(proxy_filings)





# Loop through filings and save each as a text file
# for i, filing in enumerate(proxy_filings[1:]):
#     print(f"\nProcessing filing {i+1}/{len(proxy_filings)}")
#     try:
#         # Use filing_date if available, else fallback to index-based name
#         filing_date = getattr(filing, "filing_date", None)
#         if not filing_date:
#             filing_date = f"filing_{i+1}"
#         filename = os.path.join(base_path, f"{filing_date}_DEF14A.txt")
#         # filename = os.path.join(base_path, f"{filing_date}_10K.txt")

#         # Save filing text
#         text = filing.text()
#         with open(filename, "w", encoding="utf-8") as f:
#             f.write(text)
#         print(f"Saved: {filename}")
#     except Exception as e:
#         print(f"Error saving filing {i+1}: {e}")

