import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

class EnergyTrendScraper:
    def __init__(self):
        self.url = "https://www.energytrend.com/solar-price.html"
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.data_dir / 'energytrend_scraping.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def fetch_data(self):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(self.url, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération: {str(e)}")
            return None

    def parse_data(self, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            data = []
            
            # Trouver toutes les tables
            tables = soup.find_all('table')
            
            # La table des modules est la 5ème table (index 4)
            if len(tables) >= 5:
                modules_table = tables[4]
                rows = modules_table.find_all('tr')
                
                for i, row in enumerate(rows):
                    cols = row.find_all(['td', 'th'])
                    
                    # Skip les lignes d'en-tête qui contiennent 'item', 'High', 'Low', etc.
                    if (len(cols) >= 4 and 
                        ('item' not in cols[0].get_text(strip=True).lower()) and
                        ('high' not in cols[1].get_text(strip=True).lower())):
                        
                        module_name = cols[0].get_text(strip=True)
                        if module_name:  # Ne prend que les lignes avec un nom de module
                            data.append({
                                'module_type': module_name,
                                'high': cols[1].get_text(strip=True),
                                'low': cols[2].get_text(strip=True),
                                'avg': cols[3].get_text(strip=True),
                                'change': cols[4].get_text(strip=True) if len(cols) > 4 else "",
                                'date': datetime.now().strftime('%Y-%m-%d')
                            })
                
            return data
        except Exception as e:
            self.logger.error(f"Erreur lors du parsing: {str(e)}")
            return None

    
    def save_data(self, data):
        try:
            if not data:
                self.logger.error("Aucune donnée à sauvegarder")
                return False
    
            self.logger.info(f"Tentative de sauvegarde de {len(data)} entrées")
            
            # Création du DataFrame avec les nouvelles données
            df_new = pd.DataFrame(data)
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # Sauvegarde dans le dossier raw
            raw_file = self.raw_dir / f'{current_date}_energytrend_prices.csv'
            df_new.to_csv(raw_file, index=False)
            self.logger.info(f"Données brutes sauvegardées dans {raw_file}")
            
            # Gestion du fichier historique
            historical_file = self.processed_dir / 'historical_energytrend_prices.csv'
            self.logger.info(f"Mise à jour du fichier historique {historical_file}")
            
            if historical_file.exists():
                self.logger.info("Lecture du fichier historique existant")
                df_historical = pd.read_csv(historical_file)
                self.logger.info(f"Nombre d'entrées historiques : {len(df_historical)}")
                
                # Fusion des données
                df_combined = pd.concat([df_historical, df_new])
                self.logger.info(f"Nombre d'entrées après fusion : {len(df_combined)}")
                
                # Suppression des doublons
                df_combined = df_combined.drop_duplicates(subset=['date', 'module_type'], keep='last')
                self.logger.info(f"Nombre d'entrées après dédoublonnage : {len(df_combined)}")
            else:
                self.logger.info("Pas de fichier historique existant, création d'un nouveau")
                df_combined = df_new
            
            # Sauvegarde finale
            df_combined.to_csv(historical_file, index=False)
            self.logger.info("Sauvegarde historique terminée avec succès")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde: {str(e)}")
            self.logger.exception("Détail de l'erreur:")
            return False

    
    def run(self):
        self.logger.info("Début du scraping EnergyTrend")
        html_content = self.fetch_data()
        if html_content:
            data = self.parse_data(html_content)
            if data:
                success = self.save_data(data)
                if success:
                    self.logger.info("Scraping EnergyTrend terminé avec succès")
                    return True
        return False

if __name__ == "__main__":
    scraper = EnergyTrendScraper()
    scraper.run()
