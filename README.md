# TP Spark - Analyse de Films

Ce projet utilise Apache Spark pour effectuer des analyses sur un ensemble de données de films. L'objectif est de pratiquer des opérations de tri, de filtrage, d'agrégation et de transformation des données.

## Structure du projet

- **input/** : Contient le fichier d'entrée `film.csv` avec les données de films.
- **output/** : Contient les fichiers CSV générés par les analyses Spark.
- **main.py** : Script Python contenant tout le code pour exécuter le TP Spark.

## Étapes du TP

1. **Chargement et Nettoyage des Données**
   - Chargement des données depuis `film.csv`.
   - Suppression des lignes inutiles exemple "type".

2. **Création de la Colonne `Credits`**
   - Génération d'une colonne décrivant chaque film avec son titre, acteur, actrice, et réalisateur.

3. **Requêtes d'Analyse**
   - Tri des films par année et par popularité.
   - Filtrage des films durant plus de 2 heures et par genre (`Drama` et `Comedy`).
   - Identification de l'acteur ayant tourné le plus de films et du directeur ayant remporté le plus de récompenses.
   - Classement des films par popularité pour chaque genre.
   - Détermination des genres de films sans récompenses.

4. **Création d'une UDF**
   - Création d'une fonction personnalisée pour convertir les titres en majuscules.

## Exécution

### Prérequis

- Apache Spark
- Python 3.x
- Docker (si vous utilisez un conteneur pour Spark)

### Lancer le script avec Docker

1. Lancer un conteneur Docker avec Spark et Jupyter Notebook :
   ```bash
   docker run -p 10000:8888 jupyter/pyspark-notebook
