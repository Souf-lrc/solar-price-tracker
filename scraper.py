import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from pathlib import Path

class PVInsightsScraper:
    def __init__(self):
        self.url = "http://pvinsights.com/index.php"
        self.data_path = Path('data/solar_prices.csv')
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def fetch_data(self):
        """Récupère les données de la page PVInsights"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(self.url, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des données: {str(e)}")
            return None

    def parse_data(self, html_content):
        """Extrait les données du HTML"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            data = []
            
            # Trouve la table des prix
            tables = soup.find_all('table', {'class': 'price'})
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cols = row.find_all('td')
                    if len(cols) >= 2:  # Vérifie qu'il y a assez de colonnes
                        category = cols[0].get_text(strip=True)
                        price = cols[1].get_text(strip=True)
                        data.append({
                            'date': datetime.now().strftime('%Y-%m-%d'),
                            'category': category,
                            'price': price,
                            'currency': 'USD'
                        })
            
            return data
        except Exception as e:
            self.logger.error(f"Erreur lors du parsing des données: {str(e)}")
            return None

    def save_data(self, data):
        """Sauvegarde les données dans un CSV"""
        try:
            df_new = pd.DataFrame(data)
            
            # Crée le dossier data s'il n'existe pas
            self.data_path.parent.mkdir(exist_ok=True)
            
            # Ajoute aux données existantes ou crée un nouveau fichier
            if self.data_path.exists():
                df_existing = pd.read_csv(self.data_path)
                df = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df = df_new
                
            df.to_csv(self.data_path, index=False)
            self.logger.info(f"Données sauvegardées avec succès dans {self.data_path}")
        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde des données: {str(e)}")

    def run(self):
        """Exécute le processus complet de scraping"""
        self.logger.info("Début du scraping")
        html_content = self.fetch_data()
        if html_content:
            data = self.parse_data(html_content)
            if data:
                self.save_data(data)
                self.logger.info("Scraping terminé avec succès")
            else:
                self.logger.error("Échec du parsing des données")
        else:
            self.logger.error("Échec de la récupération des données")

if __name__ == "__main__":
    scraper = PVInsightsScraper()
    scraper.run()
