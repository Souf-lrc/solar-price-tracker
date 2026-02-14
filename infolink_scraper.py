import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path


class InfoLinkScraper:
    def __init__(self):
        self.url = "https://www.infolink-group.com/spot-price/"
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
                logging.FileHandler(self.data_dir / 'infolink_scraping.log', encoding='utf-8'),
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

            # Les tables sont dans des div class="tb-wrap tb02"
            table_wrappers = soup.find_all('div', class_='tb-wrap')
            self.logger.info(f"Nombre de sections de tables trouvees: {len(table_wrappers)}")

            for wrapper in table_wrappers:
                # Extraire le nom de la catégorie depuis le texte du wrapper
                # Le titre est généralement dans un élément avant la table
                category = None
                title_el = wrapper.find(['h2', 'h3', 'h4', 'strong', 'b', 'p'])
                if title_el:
                    category = title_el.get_text(strip=True)

                if not category:
                    # Fallback: prendre le premier texte non-vide du wrapper
                    texts = wrapper.find_all(string=True, recursive=False)
                    for t in texts:
                        t = t.strip()
                        if t:
                            category = t
                            break

                if not category:
                    continue

                self.logger.info(f"Traitement de la table: {category}")

                table = wrapper.find('table')
                if not table:
                    continue

                rows = table.find_all('tr')
                if not rows:
                    continue

                # Parcourir les lignes de données (skip l'en-tête)
                for row in rows:
                    cols = row.find_all('td')
                    if not cols:
                        continue

                    # Colonnes: Item, High, Low, Average price, Change(%), Change($), Price prediction
                    item_name = cols[0].get_text(strip=True) if len(cols) > 0 else ""
                    if not item_name:
                        continue

                    # Nettoyer le emoji cadenas des noms d'items
                    item_name = item_name.replace('\U0001f512', '').strip()

                    high = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                    low = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                    avg = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                    change_pct = cols[4].get_text(strip=True) if len(cols) > 4 else ""
                    change_val = cols[5].get_text(strip=True) if len(cols) > 5 else ""

                    # Ignorer les lignes verrouillées (données premium avec --)
                    if high == "--" and low == "--" and avg == "--":
                        self.logger.info(f"  Ignore (donnees verrouillees): {item_name}")
                        continue

                    data.append({
                        'source': 'infolink',
                        'category': category,
                        'item_name': item_name,
                        'high': high,
                        'low': low,
                        'avg': avg,
                        'change_pct': change_pct,
                        'change_val': change_val,
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
            raw_file = self.raw_dir / f'{current_date}_infolink_prices.csv'
            df_new.to_csv(raw_file, index=False)
            self.logger.info(f"Donnees brutes sauvegardees dans {raw_file}")

            # Gestion du fichier historique
            historical_file = self.processed_dir / 'historical_infolink_prices.csv'
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
        self.logger.info("Debut du scraping InfoLink")
        html_content = self.fetch_data()
        if html_content:
            data = self.parse_data(html_content)
            if data:
                success = self.save_data(data)
                if success:
                    self.logger.info("Scraping InfoLink termine avec succes")
                    return True
        return False


if __name__ == "__main__":
    scraper = InfoLinkScraper()
    if not scraper.run():
        sys.exit(1)
