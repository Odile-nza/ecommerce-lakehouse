"""
One-off helper: convert the instructor-provided sample data under Data/
(mixed CSV/XLSX) into plain CSVs under sample_data/, matching the CSV-only
raw-zone ingestion contract described in the project brief.

Named sample_data/ rather than data/ deliberately — this repo also has a
Data/ (capitalized) folder, and on case-insensitive filesystems (default on
Windows/WSL mounts and macOS) "data" and "Data" resolve to the same
directory, silently clobbering the instructor-provided originals.

Run locally once:
    python tools/convert_samples_to_csv.py

Output:
    sample_data/products.csv
    sample_data/orders.csv
    sample_data/order_items.csv
"""
import pathlib

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "Data"
DST = ROOT / "sample_data"


def main():
    DST.mkdir(exist_ok=True)

    products = pd.read_csv(SRC / "products.csv")
    products.to_csv(DST / "products.csv", index=False)
    print(f"wrote {len(products)} rows to {DST / 'products.csv'}")

    orders = pd.read_excel(SRC / "orders_apr_2025.xlsx")
    orders.to_csv(DST / "orders.csv", index=False)
    print(f"wrote {len(orders)} rows to {DST / 'orders.csv'}")

    order_items = pd.read_excel(SRC / "order_items_apr_2025.xlsx")
    order_items.to_csv(DST / "order_items.csv", index=False)
    print(f"wrote {len(order_items)} rows to {DST / 'order_items.csv'}")


if __name__ == "__main__":
    main()
