name: PVInsights Price Scraper

on:
  schedule:
    - cron: '0 8 * * 1'  # Tous les lundis à 8h00 UTC
  workflow_dispatch:  # Permet le déclenchement manuel

jobs:
  scrape:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.x'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run scraper
      run: python scraper.py
    
    - name: Commit changes
      run: |
        git config --local user.email "action@github.com"
        git config --local user.name "GitHub Action"
        git add data/solar_prices.csv
        git commit -m "Update solar panel prices" || exit 0
        git push
