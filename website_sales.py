import csv
import random
from datetime import datetime, timedelta
import os
import uuid # Pour des order_id plus uniques si besoin, sinon un compteur suffit

# Configuration initiale (vous pourrez ajuster ces valeurs)
SIMULATION_START_DATE = datetime(2025, 4, 3)
NUM_DAYS_TO_SIMULATE = 3 # Simuler pour 5 jours
ORDERS_PER_DAY_WEBSITE = 170000 # Viser environ 1/3 du total de 500k
BASE_OUTPUT_PATH = "local_data/globalshop-raw" # Simule le bucket GCS localement
CHANNEL_PREFIX = "WEB" # Pour aider à rendre les order_id uniques par canal

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
        daily_factors = [1.3, 1.5, 0.9]  # Moyenne, Haut, Très haut, Bas, Moyen

        factor = daily_factors[day_offset]
        daily_orders = int(orders_per_day * factor)
        print(daily_orders)
        # Créer le chemin du dossier de sortie
        # ex: local_data/globalshop-raw/2025-04-01/
        output_dir = os.path.join(output_path_base, date_str)
        os.makedirs(output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas

        output_file_path = os.path.join(output_dir, f"{channel_prefix}_orders.csv")

        print(f"Generating data for web site channel on {date_str} into {output_file_path}...")

        with open(output_file_path, 'w', newline='') as csvfile:
            fieldnames = ['order_id', 'client_id', 'product_id', 'country',
                          'order_date', 'quantity', 'unit_price', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(daily_orders):
                order_id = f"{channel_prefix}-{date_str.replace('-', '')}-{current_order_id_counter + i}"
                order_date = date_str  # La date de la commande est la date du jour de simulation
                country = random.choices(["France", "Canada", "Japan", "Germany", "UK"], weights=[35, 25, 20, 10, 10])[0]
                unit_price = round(random.uniform(50.0, 200.0), 2)
                quantity = int(random.expovariate(1 / 2.5)) + 1
                client_id = random.randint(30000, 70000)
                product_id = int(random.gauss(mu=5000, sigma=300))
                product_id = max(3000, min(product_id, 7000))




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
            current_order_id_counter += daily_orders  # Mettre à jour le compteur pour le prochain jour (ou globalement)
        print(f"Generated {daily_orders} orders for {date_str}.")


if __name__ == "__main__":
    generate_sales_data(
        SIMULATION_START_DATE,
        NUM_DAYS_TO_SIMULATE,
        ORDERS_PER_DAY_WEBSITE,
        BASE_OUTPUT_PATH,
        CHANNEL_PREFIX
    )
    print("Website sales data simulation complete.")

