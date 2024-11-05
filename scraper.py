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
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def fetch_data(self):
        """Récupère les données de la page PVInsights"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            }
            self.logger.info(f"Tentative de connexion à {self.url}")
            response = requests.get(self.url, headers=headers, timeout=30)
            response.raise_for_status()
            self.logger.info("Données récupérées avec succès")
            return response.text
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des données: {str(e)}")
            return None

    def parse_data(self, html_content):
        """Extrait les données du HTML"""
        try:
            self.logger.info("Début du parsing des données")
            soup = BeautifulSoup(html_content, 'html.parser')
            data = []
            
            # Log du HTML pour debug
            self.logger.info("Contenu HTML reçu :")
            self.logger.info(html_content[:500])  # Premiers 500 caractères
            
            # Trouve toutes les tables
            tables = soup.find_all('table')
            self.logger.info(f"Nombre de tables trouvées : {len(tables)}")
            
            for table in tables:
                # Vérifie si c'est une table de prix
                if table.find('td'):  # Si la table contient au moins une cellule
                    rows = table.find_all('tr')
                    self.logger.info(f"Nombre de lignes dans la table : {len(rows)}")
                    
                    for row in rows[1:]:  # Skip header row
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            category = cols[0].get_text(strip=True)
                            price = cols[1].get_text(strip=True)
                            if category and price:  # Vérifie que les valeurs ne sont pas vides
                                data.append({
                                    'date': datetime.now().strftime('%Y-%m-%d'),
                                    'category': category,
                                    'price': price,
                                    'currency': 'USD'
                                })
                                self.logger.info(f"Données extraites : {category} - {price}")
            
            if not data:
                self.logger.error("Aucune donnée n'a été extraite")
                # Créer au moins une ligne de données pour éviter l'erreur de fichier manquant
                data.append({
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'category': 'No data',
                    'price': '0',
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
            return True
        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde des données: {str(e)}")
            return False

    def run(self):
        """Exécute le processus complet de scraping"""
        self.logger.info("Début du scraping")
        html_content = self.fetch_data()
        if html_content:
            data = self.parse_data(html_content)
            if data:
                if self.save_data(data):
                    self.logger.info("Scraping terminé avec succès")
                    return True
        return False

if __name__ == "__main__":
    scraper = PVInsightsScraper()
    scraper.run()
