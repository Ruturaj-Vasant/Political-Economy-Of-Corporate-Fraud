from extract_sct import extract_summary_compensation_table

df = extract_summary_compensation_table("data/XRX/2020-04-08_DEF14A.html")
if df is not None:
    print(df)
else:
    print("Summary Compensation Table not found.")