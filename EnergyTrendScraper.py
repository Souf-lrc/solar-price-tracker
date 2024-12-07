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
            
            current_category = ""
            for row in rows:
                cols = row.find_all(['td', 'th'])
                if len(cols) >= 4:
                    # Gestion des en-têtes de catégorie
                    if 'item' in cols[0].get_text(strip=True).lower():
                        continue
                        
                    # Extraction des données
                    data.append({
                        'module_type': cols[0].get_text(strip=True),
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

            df = pd.DataFrame(data)
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            raw_file = self.raw_dir / f'{current_date}_energytrend_prices.csv'
            df.to_csv(raw_file, index=False)
            
            historical_file = self.processed_dir / 'historical_energytrend_prices.csv'
            
            if historical_file.exists():
                df_historical = pd.read_csv(historical_file)
                df_combined = pd.concat([df_historical, df])
                df_combined.drop_duplicates(subset=['date', 'module_type'], keep='last', inplace=True)
            else:
                df_combined = df
                
            df_combined.sort_values(['date', 'module_type'], inplace=True)
            df_combined.to_csv(historical_file, index=False)
            
            return True
        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde: {str(e)}")
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
