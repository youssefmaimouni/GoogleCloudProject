# main.py
import functions_framework
from google.cloud import bigquery, storage
import csv
import datetime
import io

@functions_framework.cloud_event
def load_csv_to_bigquery(cloud_event):
    bucket_name = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    if not file_name.endswith(".csv"):
        return

    client = bigquery.Client()
    table_id = "celestial-sum-459520-b1.globalshop_dataw.orders"
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_text()
    
    rows = []
    for row in csv.DictReader(io.StringIO(data)):
        try:
            # Validation des formats
            order_date = datetime.datetime.strptime(row["order_date"], "%Y-%m-%d").date()
            quantity = int(row["quantity"])
            unit_price = float(row["unit_price"])
            status = row["status"]
            if status not in ["PAID", "CANCELLED"]:
                continue
            rows.append({
                "order_id": row["order_id"],
                "client_id": row["client_id"],
                "product_id": row["product_id"],
                "country": row["country"],
                "order_date": order_date.isoformat(),
                "quantity": quantity,
                "unit_price": unit_price,
                "status": status
            })
        except Exception as e:
            print(f"Erreur dans une ligne: {e}")
            continue

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print("Erreurs lors de l'insertion :", errors)
    else:
        print(f"{len(rows)} lignes insérées avec succès.")

        
if __name__ == "__main__":
    from google.cloud import storage

    bucket_name = "globalshop_raw"
    prefix = ""  # You can use "2025-" to filter just by year

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    class DummyEvent:
        def __init__(self, name):
            self.data = {"bucket": bucket_name, "name": name}

    # Loop through all blobs (files) in the bucket
    for blob in blobs:
        # We only want CSV files in date folders
        if blob.name.endswith("_orders.csv") and blob.name.count("/") == 1:
            print(f"⏳ Processing: {blob.name}")
            try:
                load_csv_to_bigquery(DummyEvent(blob.name))
                print(f"✅ Successfully loaded: {blob.name}")
            except Exception as e:
                print(f"❌ Failed to load {blob.name}: {e}")
