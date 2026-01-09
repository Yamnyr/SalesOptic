import csv
import json
import random
from datetime import datetime, timedelta
import os

# Set seed for reproducibility
random.seed(42)

def generate_csv(filepath, num_rows=1000):
    products = [
        ("Laptop", 1200),
        ("Mouse", 25),
        ("Keyboard", 75),
        ("Monitor", 300),
        ("Headset", 100),
        ("Webcam", 60),
        ("USB Cable", 15)
    ]
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["transaction_id", "date", "client_id", "produit", "prix", "quantite"])
        
        start_date = datetime(2025, 1, 1)
        for i in range(1, num_rows + 1):
            # Problème aléatoire : 5% de chances d'avoir une donnée corrompue
            error_type = random.random()
            
            date = start_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            date_str = date.strftime("%Y-%m-%d")
            prod_name, base_price = random.choice(products)
            prix = base_price
            quantite = random.randint(1, 5)
            client_id = random.randint(1, 20)
            
            if error_type < 0.02: # 2% prix négatif
                prix = -random.randint(10, 100)
            elif error_type < 0.04: # 2% quantité nulle ou négative
                quantite = random.randint(-2, 0)
            elif error_type < 0.06: # 2% produit vide
                prod_name = ""
            elif error_type < 0.08: # 2% client_id vide
                client_id = ""
            elif error_type < 0.10: # 2% date invalide
                date_str = "invalid-date"

            writer.writerow([i, date_str, client_id, prod_name, prix, quantite])
    print(f"Généré {num_rows} lignes dans {filepath} (avec quelques erreurs)")

def generate_json(filepath, num_rows=500):
    events = ["click", "view", "purchase", "add_to_cart", "remove_from_cart"]
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        start_date = datetime(2025, 1, 1)
        for i in range(1, num_rows + 1):
            error_type = random.random()
            
            ts = start_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            user_id = random.randint(1, 200)
            event = random.choice(events)
            ts_str = ts.isoformat()
            
            if error_type < 0.03: # 3% user_id manquant
                user_id = None
            elif error_type < 0.06: # 3% event inconnu
                event = "unknown_action"
                
            data = {
                "user_id": user_id,
                "event": event,
                "timestamp": ts_str
            }
            f.write(json.dumps(data) + '\n')
    print(f"Généré {num_rows} événements dans {filepath} (avec quelques erreurs)")

def generate_logs(filepath, num_rows=800):
    levels = ["INFO", "ERROR", "WARNING", "DEBUG"]
    services = ["AuthService", "PaymentGateway", "InventoryManager", "FrontendAPI", "BackgroundJob"]
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        start_date = datetime(2025, 1, 1)
        for i in range(num_rows):
            error_type = random.random()
            
            ts = start_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            level = random.choice(levels)
            service = random.choice(services)
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
            
            if error_type < 0.05: # 5% format malformé
                f.write(f"MALFORMED_LOG_LINE_WITHOUT_COMMAS_{random.randint(100,999)}\n")
            else:
                f.write(f"{ts_str},{level},{service}\n")
    print(f"Généré {num_rows} lignes de log dans {filepath} (avec quelques erreurs)")

if __name__ == "__main__":
    generate_csv("source/ventes.csv", 2000)
    generate_json("source/events.json", 3000)
    generate_logs("source/logs.txt", 4000)
