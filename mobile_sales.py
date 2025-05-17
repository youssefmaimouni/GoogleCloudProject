from google.cloud import storage
import csv
import random
import uuid
from datetime import datetime, timedelta
import os

# Configuration initiale pour mobile_sales.py
SIMULATION_START_DATE = datetime(2025, 4, 1)
NUM_DAYS_TO_SIMULATE = 5
ORDERS_PER_DAY_MOBILE = 165000 # Ajuster pour atteindre ~500k total
BASE_OUTPUT_PATH = "local_data/globalshop-raw"
CHANNEL_PREFIX = "MOB" # Différent préfixe

# Données de référence pour la génération
COUNTRIES = ["USA", "Canada", "France", "Germany", "UK", "Japan", "Australia", "Brazil", "India", "China"]
STATUSES = ["PAID", "CANCELLED"]
# Pour plus de réalisme, vous pourriez avoir des listes de product_ids et client_ids
# ou générer des nombres dans des plages spécifiques.
# Pour la simplicité ici, nous allons générer des ID numériques aléatoires.
MAX_CLIENT_ID = 100000
MAX_PRODUCT_ID = 10000



def generate_sales_data(start_date, num_days, orders_per_day, output_path_base, channel_prefix):
    current_order_id_counter = 1  # Compteur simple pour les order_id

    for day_offset in range(num_days):
        current_simulation_date = start_date + timedelta(days=day_offset)
        date_str = current_simulation_date.strftime("%Y-%m-%d")

        # Créer le chemin du dossier de sortie
        # ex: local_data/globalshop-raw/2025-04-01/
        output_dir = os.path.join(output_path_base, date_str)
        os.makedirs(output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas

        output_file_path = os.path.join(output_dir, "orders.csv")

        print(f"Generating data for {channel_prefix} channel on {date_str} into {output_file_path}...")

        with open(output_file_path, 'w', newline='') as csvfile:
            fieldnames = ['order_id', 'client_id', 'product_id', 'country',
                          'order_date', 'quantity', 'unit_price', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(orders_per_day):
                order_id = f"{channel_prefix}-{date_str.replace('-', '')}-{current_order_id_counter + i}"
                client_id = random.randint(1, MAX_CLIENT_ID)
                product_id = random.randint(1, MAX_PRODUCT_ID)
                country = random.choice(COUNTRIES)
                order_date = date_str  # La date de la commande est la date du jour de simulation
                quantity = random.randint(1, 10)
                unit_price = round(random.uniform(5.0, 500.0), 2)
                # Pourrait avoir un biais, ex: 80% PAID, 20% CANCELLED
                status = random.choices(STATUSES, weights=[0.85, 0.15], k=1)[0]

                writer.writerow({
                    'order_id': order_id,
                    'client_id': client_id,
                    'product_id': product_id,
                    'country': country,
                    'order_date': order_date,
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'status': status
                })
            current_order_id_counter += orders_per_day  # Mettre à jour le compteur pour le prochain jour (ou globalement)
        print(f"Generated {orders_per_day} orders for {date_str}.")


# Main
if __name__ == "__main__":
    generate_sales_data(
        SIMULATION_START_DATE,
        NUM_DAYS_TO_SIMULATE,
        ORDERS_PER_DAY_MOBILE, # Utiliser la variable pour mobile
        BASE_OUTPUT_PATH,
        CHANNEL_PREFIX # Utiliser le préfixe pour mobile
    )
    print("Mobile sales data simulation complete.")