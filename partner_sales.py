import csv
import random
from datetime import datetime, timedelta
import os
import uuid # Pour des order_id plus uniques si besoin, sinon un compteur suffit

# Configuration initiale pour partner_sales.py
SIMULATION_START_DATE = datetime(2025, 4, 1)
NUM_DAYS_TO_SIMULATE = 5
ORDERS_PER_DAY_PARTNER = 165000 # Ajuster pour atteindre ~500k total
BASE_OUTPUT_PATH = "local_data/globalshop-raw"
CHANNEL_PREFIX = "PART" # Différent préfixe

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
    # Génère un facteur de variation pour chaque jour (ex. : [0.8, 1.1, 0.9, 1.3, 0.7])
    daily_factors = [round(random.uniform(0.7, 1.3), 2) for _ in range(num_days)]

    for day_offset in range(num_days):
        current_simulation_date = start_date + timedelta(days=day_offset)
        date_str = current_simulation_date.strftime("%Y-%m-%d")

        # Détermine le nombre de commandes pour ce jour
        # PARTNER SALES - variation imposée
        daily_factors = [1.0, 1.2, 1.5, 0.6, 1.3]  # Moyenne, Haut, Très haut, Bas, Moyen

        factor = daily_factors[day_offset]
        daily_orders = int(orders_per_day * factor)

        # Créer le chemin du dossier de sortie
        # ex: local_data/globalshop-raw/2025-04-01/
        output_dir = os.path.join(output_path_base, date_str)
        os.makedirs(output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas

        output_file_path = os.path.join(output_dir, f"{channel_prefix}_orders.csv")

        print(f"Generating data for partner channel on {date_str} into {output_file_path}...")

        with open(output_file_path, 'w', newline='') as csvfile:
            fieldnames = ['order_id', 'client_id', 'product_id', 'country',
                          'order_date', 'quantity', 'unit_price', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(daily_orders):
                order_id = f"{channel_prefix}-{date_str.replace('-', '')}-{current_order_id_counter + i}"
                order_date = date_str  # La date de la commande est la date du jour de simulation
                country = random.choices(["USA", "UK", "Germany", "Canada", "France"], weights=[40, 25, 20, 10, 5])[0]
                unit_price = round(random.uniform(150.0, 500.0), 2)
                quantity = random.randint(5, 20)
                client_id = random.randint(1, 30000)
                product_id = random.choices(
                    population=list(range(1000, 3000)),
                    weights=[5 if i % 100 == 0 else 1 for i in range(1000, 3000)],
                    k=1
                )[0]

                # Pourrait avoir un biais, ex: 80% PAID, 20% CANCELLED
                status = random.choices(STATUSES, weights=[0.95, 0.05], k=1)[0]

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


if __name__ == "__main__":
    generate_sales_data(
        SIMULATION_START_DATE,
        NUM_DAYS_TO_SIMULATE,
        ORDERS_PER_DAY_PARTNER, # Utiliser la variable pour partner
        BASE_OUTPUT_PATH,
        CHANNEL_PREFIX # Utiliser le préfixe pour partner
    )
    print("Partner sales data simulation complete.")

