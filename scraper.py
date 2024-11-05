import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from pathlib import Path

class PVInsightsScraper:
    def __init__(self):
        self.url = "http://pvinsights.com/index.php"
        self.setup_folders()
        self.setup_logging()
        
    def setup_folders(self):
        """Crée la structure de dossiers nécessaire"""
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        
        # Créer les dossiers s'ils n'existent pas
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def setup_logging(self):
        """Configure le système de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.data_dir / 'scraping.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def fetch_data(self):
        """Récupère les données de la page PVInsights"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
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
            soup = BeautifulSoup(html_content, 'html.parser')
            data = []
            
            for table in soup.find_all('table'):
                first_row = table.find('tr')
                if first_row and 'Solar PV Module Weekly Spot Price' in first_row.get_text():
                    rows = table.find_all('tr')
                    
                    for row in rows[1:]:  # Skip header row
                        cols = row.find_all('td')
                        if len(cols) >= 7:
                            try:
                                row_data = {
                                    'date': datetime.now().strftime('%Y-%m-%d'),
                                    'module_type': cols[0].get_text(strip=True),
                                    'high': float(cols[1].get_text(strip=True).replace(',', '')),
                                    'low': float(cols[2].get_text(strip=True).replace(',', '')),
                                    'average': float(cols[3].get_text(strip=True).replace(',', '')),
                                    'change': cols[4].get_text(strip=True),
                                    'change_percentage': cols[5].get_text(strip=True),
                                    'cny_price': cols[6].get_text(strip=True)
                                }
                                if not "Visit here" in row_data['module_type']:
                                    data.append(row_data)
                            except (ValueError, IndexError) as e:
                                self.logger.error(f"Erreur lors du parsing d'une ligne: {str(e)}")
                                continue
                    break
            
            return data
        except Exception as e:
            self.logger.error(f"Erreur lors du parsing des données: {str(e)}")
            return None

    def save_raw_data(self, data):
        """Sauvegarde les données brutes du jour"""
        try:
            if not data:
                self.logger.error("Aucune donnée à sauvegarder")
                return False
                
            df = pd.DataFrame(data)
            current_date = datetime.now().strftime('%Y-%m-%d')
            filename = self.raw_dir / f'{current_date}_prices.csv'
            
            df.to_csv(filename, index=False)
            self.logger.info(f"Données brutes sauvegardées dans {filename}")
            
            # Mettre à jour le fichier historique
            self.update_historical_data(df)
            
            return True
        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde des données: {str(e)}")
            return False
            
    def update_historical_data(self, new_data):
        """Met à jour le fichier historique avec les nouvelles données"""
        try:
            historical_file = self.processed_dir / 'historical_prices.csv'
            
            if historical_file.exists():
                historical_df = pd.read_csv(historical_file)
                # Concatenate and remove duplicates based on date and module_type
                combined_df = pd.concat([historical_df, new_data])
                combined_df = combined_df.drop_duplicates(subset=['date', 'module_type'], keep='last')
            else:
                combined_df = new_data
                
            combined_df.sort_values(['module_type', 'date'], inplace=True)
            combined_df.to_csv(historical_file, index=False)
            self.logger.info("Données historiques mises à jour avec succès")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la mise à jour des données historiques: {str(e)}")

    def run(self):
        """Exécute le processus complet de scraping"""
        self.logger.info("Début du scraping")
        html_content = self.fetch_data()
        
        if html_content:
            data = self.parse_data(html_content)
            if data:
                if self.save_raw_data(data):
                    self.logger.info("Scraping terminé avec succès")
                    return True
                
        self.logger.error("Échec du scraping")
        return False

if __name__ == "__main__":
    scraper = PVInsightsScraper()
    scraper.run()
