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

        # Détermine le nombre de commandes pour ce jour
        # PARTNER SALES - variation imposée
        daily_factors = [0.7, 1.3 ,1.5, 1.0, 1.6]  # Moyenne, Haut, Très haut, Bas, Moyen

        factor = daily_factors[day_offset]
        daily_orders = int(orders_per_day * factor)

        # Créer le chemin du dossier de sortie
        # ex: local_data/globalshop-raw/2025-04-01/
        output_dir = os.path.join(output_path_base, date_str)
        os.makedirs(output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas

        output_file_path = os.path.join(output_dir, f"{channel_prefix}_orders.csv")

        print(f"Generating data for mobile channel on {date_str} into {output_file_path}...")

        with open(output_file_path, 'w', newline='') as csvfile:
            fieldnames = ['order_id', 'client_id', 'product_id', 'country',
                          'order_date', 'quantity', 'unit_price', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(daily_orders):
                order_id = f"{channel_prefix}-{date_str.replace('-', '')}-{current_order_id_counter + i}"
                order_date = date_str  # La date de la commande est la date du jour de simulation
                country = random.choices(["Brazil", "India", "China", "USA", "France"], weights=[40, 25, 20, 10, 5])[0]
                unit_price = round(random.uniform(10.0, 150.0), 2)
                quantity = random.randint(1, 3)
                client_id = random.randint(70000, 100000)
                product_id = random.choices(
                    population=list(range(7000, 10000)),
                    weights=[3 if i % 111 == 0 else 1 for i in range(7000, 10000)],
                    k=1
                )[0]



                # Pourrait avoir un biais, ex: 80% PAID, 20% CANCELLED
                status = random.choices(STATUSES, weights=[0.75, 0.25], k=1)[0]


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
            current_order_id_counter += daily_orders  # Mettre à jour le compteur pour le prochain jour (ou globalement)
        print(f"Generated {daily_orders} orders for {date_str}.")


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