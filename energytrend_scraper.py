import sys
import re
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
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.data_dir / 'energytrend_scraping.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def fetch_data(self):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(self.url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.logger.error(f"Erreur lors de la recuperation: {str(e)}")
            return None

    def parse_data(self, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            data = []

            # Trouver toutes les tables
            tables = soup.find_all('table')

            self.logger.info(f"Nombre total de tables trouvees: {len(tables)}")

            # Parcourir toutes les tables (skip la première qui est une table de layout)
            for table_idx, table in enumerate(tables):
                # Skip la première table (index 0) qui est une grande table de layout
                if table_idx == 0:
                    continue

                rows = table.find_all('tr')
                if not rows:
                    continue

                # Verifier que c'est bien une table de prix (doit avoir les colonnes High, Low, etc.)
                header_row = rows[0]
                header_text = header_row.get_text(strip=True).lower()
                if 'high' not in header_text or 'low' not in header_text:
                    continue

                # Determiner la categorie en cherchant un titre avant la table
                category = None

                # Chercher un titre (h2, h3, strong, etc.) avant la table
                prev_element = table.find_previous(['h2', 'h3', 'h4', 'strong', 'b'])
                if prev_element:
                    category_text = prev_element.get_text(strip=True)
                    # Nettoyer le texte de la categorie (enlever la date et l'unite)
                    if category_text and len(category_text) < 100:
                        # Nettoyer: enlever les dates (pattern YYYY/MM/DD) et "update"
                        category_text = re.sub(r'\d{4}/\d{2}/\d{2}', '', category_text)
                        category_text = re.sub(r'\s*update\s*', '', category_text, flags=re.IGNORECASE)
                        category_text = category_text.strip()
                        category = category_text

                if not category:
                    continue  # Skip cette table si on ne peut pas identifier la categorie

                self.logger.info(f"Traitement de la table: {category}")

                # Extraire les données de cette table
                for row in rows:
                    cols = row.find_all(['td', 'th'])

                    # Les tables ont 6 colonnes: ['', 'item', 'High', 'Low', 'Avg', 'Chg']
                    # Skip les lignes d'en-tête qui contiennent 'item', 'High', 'Low', etc.
                    if (len(cols) >= 6 and
                        ('item' not in cols[1].get_text(strip=True).lower()) and
                        ('high' not in cols[2].get_text(strip=True).lower())):

                        item_name = cols[1].get_text(strip=True)
                        if item_name:  # Ne prend que les lignes avec un nom d'item
                            data.append({
                                'source': 'energytrend',
                                'category': category,
                                'item_name': item_name,
                                'high': cols[2].get_text(strip=True),
                                'low': cols[3].get_text(strip=True),
                                'avg': cols[4].get_text(strip=True),
                                'change': cols[5].get_text(strip=True) if len(cols) > 5 else "",
                                'date': datetime.now().strftime('%Y-%m-%d')
                            })

            self.logger.info(f"Total d'items extraits: {len(data)}")
            return data
        except Exception as e:
            self.logger.error(f"Erreur lors du parsing: {str(e)}")
            return None

    
    def save_data(self, data):
        try:
            if not data:
                self.logger.error("Aucune donnee a sauvegarder")
                return False
    
            self.logger.info(f"Tentative de sauvegarde de {len(data)} entrees")
            
            # Creation du DataFrame avec les nouvelles donnees
            df_new = pd.DataFrame(data)
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # Sauvegarde dans le dossier raw
            raw_file = self.raw_dir / f'{current_date}_energytrend_prices.csv'
            df_new.to_csv(raw_file, index=False)
            self.logger.info(f"Donnees brutes sauvegardees dans {raw_file}")
            
            # Gestion du fichier historique
            historical_file = self.processed_dir / 'historical_energytrend_prices.csv'
            self.logger.info(f"Mise a jour du fichier historique {historical_file}")
            
            if historical_file.exists():
                self.logger.info("Lecture du fichier historique existant")
                df_historical = pd.read_csv(historical_file)
                self.logger.info(f"Nombre d'entrees historiques : {len(df_historical)}")
                
                # Fusion des donnees
                df_combined = pd.concat([df_historical, df_new])
                self.logger.info(f"Nombre d'entrees apres fusion : {len(df_combined)}")
                
                # Suppression des doublons
                df_combined = df_combined.drop_duplicates(subset=['date', 'category', 'item_name'], keep='last')
                self.logger.info(f"Nombre d'entrees apres dedoublonnage : {len(df_combined)}")
            else:
                self.logger.info("Pas de fichier historique existant, creation d'un nouveau")
                df_combined = df_new
            
            # Sauvegarde finale
            df_combined.to_csv(historical_file, index=False)
            self.logger.info("Sauvegarde historique terminee avec succes")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde: {str(e)}")
            self.logger.exception("Detail de l'erreur:")
            return False

    
    def run(self):
        self.logger.info("Debut du scraping EnergyTrend")
        html_content = self.fetch_data()
        if html_content:
            data = self.parse_data(html_content)
            if data:
                success = self.save_data(data)
                if success:
                    self.logger.info("Scraping EnergyTrend termine avec succes")
                    return True
        return False

if __name__ == "__main__":
    scraper = EnergyTrendScraper()
    if not scraper.run():
        sys.exit(1)
