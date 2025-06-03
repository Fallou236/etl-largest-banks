# ETL - Largest Banks by Market Capitalization

Ce projet effectue un processus ETL sur les données des plus grandes banques selon leur capitalisation boursière en USD, et les convertit en GBP, EUR et INR.

## Fichiers

- `etl_banks.py` : Script principal
- `exchange_rate.csv` : Fichier contenant les taux de change
- `Largest_banks_data.csv` : Résultat de la transformation
- `Banks.db` : Base de données SQLite contenant la table finale

## Étapes du pipeline
1. Extraction des données depuis Wikipedia
2. Transformation des montants en plusieurs devises
3. Chargement dans un fichier CSV et une base de données
