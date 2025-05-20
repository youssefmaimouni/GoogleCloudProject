# main.py
from google.cloud import bigquery, storage
import csv
import datetime
import io

def process_file(bucket_name, file_name, max_rows=5):
    if not file_name.endswith(".csv"):
        return

    client = bigquery.Client()
    table_id = "celestial-sum-459520-b1.globalshop_dataw.orders"
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_text()
    
    rows = []
    row_count = 0
    
    for row in csv.DictReader(io.StringIO(data)):
        if row_count >= max_rows:
            break
            
        try:
            # Convert client_id and product_id to INT
            client_id = int(row["client_id"])
            product_id = int(row["product_id"])
            
            # Convert unit_price to INT (truncate decimals)
            unit_price = int(float(row["unit_price"]))
            
            # Validate other fields
            order_date = datetime.datetime.strptime(row["order_date"], "%Y-%m-%d").date()
            quantity = int(row["quantity"])
            status = row["status"]
            
            if status not in ["PAID", "CANCELLED"]:
                continue
                
            rows.append({
                "order_id": row["order_id"],
                "client_id": client_id,
                "product_id": product_id,
                "country": row["country"],
                "order_date": order_date.isoformat(),
                "quantity": quantity,
                "unit_price": unit_price,  # Now an integer
                "status": status
            })
            row_count += 1
            
        except Exception as e:
            print(f"Erreur dans une ligne: {e}")
            continue

    if rows:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            print("Erreurs lors de l'insertion :", errors)
        else:
            print(f"{len(rows)} lignes insérées avec succès.")
    else:
        print("Aucune ligne valide à insérer.")

if __name__ == "__main__":
    bucket_name = "globalshop_raw"
    file_to_process = "2025-04-01/MOB_orders.csv"
    
    print(f"⏳ Processing: {file_to_process} (first 5 rows only)")
    try:
        process_file(bucket_name, file_to_process, max_rows=5)
        print(f"✅ Successfully loaded first 5 rows from: {file_to_process}")
    except Exception as e:
        print(f"❌ Failed to load {file_to_process}: {e}")