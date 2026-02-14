import sys
import requests
import warnings
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path


class PVInsightsScraper:
    def __init__(self):
        self.url = "http://pvinsights.com/"
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.setup_logging()

        # Titres des tables de prix à chercher dans le HTML
        self.price_table_markers = [
            'PV PolySilicon Weekly Spot Price',
            'Solar PV Wafer Weekly Spot Price',
            'Solar PV Cell Weekly Spot Price',
            'Solar PV Module Weekly Spot Price',
        ]

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.data_dir / 'pvinsights_scraping.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def fetch_data(self):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
            }
            warnings.filterwarnings('ignore')
            response = requests.get(self.url, headers=headers, verify=False, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.logger.error(f"Erreur lors de la recuperation: {str(e)}")
            return None

    def _find_price_tables(self, soup):
        """Trouve les tables de prix en cherchant les marqueurs de titre dans les cellules."""
        price_tables = {}
        all_tables = soup.find_all('table')

        for table in all_tables:
            rows = table.find_all('tr')
            if not rows:
                continue
            # Vérifier si la première cellule contient un marqueur de table de prix
            first_cell = rows[0].find(['td', 'th'])
            if not first_cell:
                continue
            cell_text = first_cell.get_text(strip=True)
            for marker in self.price_table_markers:
                if marker in cell_text:
                    price_tables[marker] = table
                    break

        return price_tables

    def parse_data(self, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            data = []

            price_tables = self._find_price_tables(soup)
            self.logger.info(f"Tables de prix trouvees: {len(price_tables)}")

            for category_marker, table in price_tables.items():
                # Simplifier le nom de la catégorie
                category = category_marker.replace(' Weekly Spot Price', '')
                self.logger.info(f"Traitement de la table: {category}")

                rows = table.find_all('tr')

                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 7:
                        continue

                    item_name = cols[0].get_text(strip=True)

                    # Ignorer les lignes d'en-tête
                    if not item_name or item_name.lower() in ('item', ''):
                        continue

                    # Ignorer les lignes de titre de la table
                    if any(marker in item_name for marker in self.price_table_markers):
                        continue

                    # Ignorer les lignes d'unité (ex: "Unit: USD/Kg")
                    if item_name.startswith('Unit:') or item_name.startswith('Last Update'):
                        continue

                    # Ignorer les lignes paywallées ("Visit here for more")
                    row_text = row.get_text(strip=True)
                    if 'Visit here' in row_text or 'for more' in row_text:
                        self.logger.info(f"  Ignore (paywall): {item_name}")
                        continue

                    high = cols[1].get_text(strip=True)
                    low = cols[2].get_text(strip=True)
                    avg = cols[3].get_text(strip=True)
                    avg_chg = cols[4].get_text(strip=True)
                    avg_chg_pct = cols[5].get_text(strip=True)
                    avg_cny = cols[6].get_text(strip=True)

                    # Ignorer les lignes sans données numériques
                    if not high or not low or not avg:
                        continue

                    data.append({
                        'source': 'pvinsights',
                        'category': category,
                        'item_name': item_name,
                        'high': high,
                        'low': low,
                        'avg': avg,
                        'avg_chg': avg_chg,
                        'avg_chg_pct': avg_chg_pct,
                        'avg_cny': avg_cny,
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

            df_new = pd.DataFrame(data)
            current_date = datetime.now().strftime('%Y-%m-%d')

            # Sauvegarde dans le dossier raw
            raw_file = self.raw_dir / f'{current_date}_pvinsights_prices.csv'
            df_new.to_csv(raw_file, index=False)
            self.logger.info(f"Donnees brutes sauvegardees dans {raw_file}")

            # Gestion du fichier historique
            historical_file = self.processed_dir / 'historical_pvinsights_prices.csv'
            self.logger.info(f"Mise a jour du fichier historique {historical_file}")

            if historical_file.exists():
                self.logger.info("Lecture du fichier historique existant")
                df_historical = pd.read_csv(historical_file)
                self.logger.info(f"Nombre d'entrees historiques : {len(df_historical)}")

                df_combined = pd.concat([df_historical, df_new])
                self.logger.info(f"Nombre d'entrees apres fusion : {len(df_combined)}")

                df_combined = df_combined.drop_duplicates(
                    subset=['date', 'category', 'item_name'], keep='last'
                )
                self.logger.info(f"Nombre d'entrees apres dedoublonnage : {len(df_combined)}")
            else:
                self.logger.info("Pas de fichier historique existant, creation d'un nouveau")
                df_combined = df_new

            df_combined.to_csv(historical_file, index=False)
            self.logger.info("Sauvegarde historique terminee avec succes")

            return True

        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde: {str(e)}")
            self.logger.exception("Detail de l'erreur:")
            return False

    def run(self):
        self.logger.info("Debut du scraping PVInsights")
        html_content = self.fetch_data()
        if html_content:
            data = self.parse_data(html_content)
            if data:
                success = self.save_data(data)
                if success:
                    self.logger.info("Scraping PVInsights termine avec succes")
                    return True
        return False


if __name__ == "__main__":
    scraper = PVInsightsScraper()
    if not scraper.run():
        sys.exit(1)
