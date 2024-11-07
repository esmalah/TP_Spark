# TP Spark - Analyse de Films
# Auteur : Asma LAHSI
# Description : Script d'analyse de données de films utilisant Apache Spark

# Imports nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, when, rank, dense_rank, udf
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Initialisation de la session Spark
spark = SparkSession.builder.appName("Movie Analysis").getOrCreate()

## Partie 1

# Question 1 - Chargement et prétraitement des données
df = spark.read.csv("/home/jovyan/work/film.csv", header=True, inferSchema=True, sep=";")

# Question 2-1 - Suppression de la colonne *Image
df = df.drop("*Image")

# Question 2-2 - Suppression de la ligne des types
rdd = df.rdd
header = rdd.first()  # Récupérer la première ligne
rdd = rdd.filter(lambda row: row != header)  # Supprimer la première ligne
df = spark.createDataFrame(rdd, df.schema)

# Question 2-3 - Création de la colonne 'Credits'
df = df.withColumn(
    "Credits",
    concat(col("Title"), lit(": a "), col("Actor"), lit(" and "), col("Actress"), lit(" film’s, directed by "), col("Director"))
)

# Question 2-4
# Trier les films du plus ancien au plus récent
sorted_by_date = df.orderBy("Year")
sorted_by_date.write.csv("output/sorted_by_year.csv", header=True)

# Trier les films par popularité
df = df.withColumn("Popularity", col("Popularity").cast("int"))
sorted_by_popularity = df.orderBy(col("Popularity").desc())
sorted_by_popularity.show(truncate=False)
sorted_by_popularity.write.csv("output/sorted_by_popularity.csv", header=True)

# Les films durant plus de 2 heures
long_movies = df.filter(col("Length") > 120)
long_movies.write.csv("output/long_movies.csv", header=True)

# Filtrer les films selon deux genres
drama_movies = df.filter(col("Subject") == "Drama")
drama_movies.write.csv("output/drama_movies.csv", header=True)

comedy_movies = df.filter(col("Subject") == "Comedy")
comedy_movies.write.csv("output/comedy_movies.csv", header=True)

# L'acteur ayant tourné le plus de films
actor_count = df.groupBy("Actor").count().orderBy(col("count").desc())
most_frequent_actor = actor_count.limit(1)
most_frequent_actor.write.csv("output/most_frequent_actor.csv", header=True)

# Trouver le directeur ayant remporté le plus de récompenses
df = df.withColumn("Awards_Num", when(col("Awards") == "Yes", 1).otherwise(0))
director_awards = df.groupBy("Director").sum("Awards_Num").orderBy(col("sum(Awards_Num)").desc())
top_director = director_awards.limit(1)
top_director.write.csv("output/top_director.csv", header=True)

# Le film le plus populaire ayant remporté un prix
popular_awarded_movie = df.filter(col("Awards") == "Yes").orderBy(col("Popularity").desc()).limit(1)
popular_awarded_movie.write.csv("output/popular_awarded_movie.csv", header=True)

# Les genres de films sans récompenses
genre_without_awards = df.filter(col("Awards") == "No").select("Subject").distinct()
genre_without_awards.write.csv("output/genre_without_awards.csv", header=True)

## Partie 1

# Question 1
# Classement des films par popularité pour chaque genre
df = df.withColumn("Popularity", col("Popularity").cast("int"))
window_spec = Window.partitionBy("Subject").orderBy(desc("Popularity"))
df_ranked = df.withColumn("rank", dense_rank().over(window_spec))
df_ranked.write.csv("output/films_ranked_by_subject.csv", header=True)

# Nombre de films de chaque directeur
director_movie_count = df.groupBy("Director").count()
director_movie_count.write.csv("output/director_movie_count.csv", header=True)

# Convertir les titres en majuscules avec une UDF
uppercase_title = udf(lambda title: title.upper() if title else None, StringType())
df_with_uppercase_title = df.withColumn("Title_Uppercase", uppercase_title(col("Title")))
df_with_uppercase_title.write.csv("output/title_uppercase.csv", header=True)
