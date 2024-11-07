# freightos_client.py
import requests
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

class FreightosClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.freightos.com/api/v1"
        self.headers = {
            "x-apikey": api_key,
            "Content-Type": "application/json"
        }
        self.setup_folders()
        self.setup_logging()

    def setup_folders(self):
        """Crée la structure de dossiers nécessaire"""
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def setup_logging(self):
        """Configure le système de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.data_dir / 'freight_rates.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_rates(self):
        """Récupère les taux de fret maritime"""
        payload = {
            "load": [{
                "quantity": 1,
                "unitType": "container40HC",
                "unitWeightKg": 17000,
                "unitVolumeCBM": 76.3
            }],
            "legs": [{
                "origin": {"unLocationCode": "CNSHA"},
                "destination": {"unLocationCode": "MACAS"}
            }]
        }

        try:
            self.logger.info("Récupération des taux de fret")
            response = requests.post(
                f"{self.base_url}/freightEstimates",
                headers=self.headers,
                json=payload,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                ocean_data = data.get('OCEAN', {})
                
                rate_data = {
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'route': 'Shanghai → Casablanca',
                    'container_type': 'Conteneur 40\'HC',
                    'weight_tons': 17,
                    'price_min_usd': ocean_data.get('priceEstimate', {}).get('min'),
                    'price_max_usd': ocean_data.get('priceEstimate', {}).get('max'),
                    'transit_min_days': ocean_data.get('transitTime', {}).get('min'),
                    'transit_max_days': ocean_data.get('transitTime', {}).get('max')
                }

                self.save_rate_data(rate_data)
                return rate_data
            else:
                self.logger.error(f"Erreur API: {response.text}")
                return None

        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des taux: {str(e)}")
            return None

    def save_rate_data(self, data):
        """Sauvegarde les données de taux"""
        try:
            # Sauvegarde des données brutes du jour
            current_date = datetime.now().strftime('%Y-%m-%d')
            raw_file = self.raw_dir / f'{current_date}_freight_rates.csv'
            
            df_new = pd.DataFrame([data])
            df_new.to_csv(raw_file, index=False)
            self.logger.info(f"Données brutes sauvegardées dans {raw_file}")

            # Mise à jour de l'historique
            historical_file = self.processed_dir / 'historical_freight_rates.csv'
            
            if historical_file.exists():
                df_historical = pd.read_csv(historical_file)
                df_combined = pd.concat([df_historical, df_new])
            else:
                df_combined = df_new
                
            df_combined.sort_values('date', inplace=True)
            df_combined.to_csv(historical_file, index=False)
            self.logger.info("Historique des taux mis à jour")

        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde des données: {str(e)}")

def main():
    api_key = "lhfzx3SOq7IPMGA25wQGUmSSEKlPZg8t"  # À stocker de manière sécurisée
    client = FreightosClient(api_key)
    client.get_rates()

if __name__ == "__main__":
    main()
