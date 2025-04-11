# Importamos librerías 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('SpotifyGlobalAnalysis').getOrCreate()

# Ruta del archivo
file_path = 'hdfs://localhost:9000/Tarea3/Spotify_2024_Global_Streaming_Data.csv'

# Lee el archivo .csv 
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Verificación tipo de datos
print("Esquema del DataFrame:")
df.printSchema()

# Mostrar las primeras filas del DataFrame
print("\nPrimeras 20 filas del dataset:")
df.show(20, truncate=False)

# Estadísticas básicas de las columnas numéricas
print("\nEstadísticas descriptivas básicas:")
numeric_cols = ['Monthly Listeners (Millions)', 'Total Streams (Millions)', 
                'Total Hours Streamed (Millions)', 'Avg Stream Duration (Min)', 
                'Streams Last 30 Days (Millions)', 'Skip Rate (%)']
df.select(numeric_cols).summary().show()

# Top 10 países con más streams
print("\nTop 10 países con más streams totales:")
top_countries = df.groupBy('Country').agg(
    F.sum('Total Streams (Millions)').alias('Total_Streams_Millions'),
    F.count('*').alias('Count_Records')
).orderBy(F.col('Total_Streams_Millions').desc()).limit(10)
top_countries.show()

# Top 10 artistas más populares
print("\nTop 10 artistas con más oyentes mensuales:")
top_artists = df.groupBy('Artist').agg(
    F.max('Monthly Listeners (Millions)').alias('Max_Monthly_Listeners_Millions'),
    F.sum('Total Streams (Millions)').alias('Total_Streams_Millions')
).orderBy(F.col('Max_Monthly_Listeners_Millions').desc()).limit(10)
top_artists.show()

# Distribución de streams por género
print("\nDistribución de streams por género musical:")
genre_stats = df.groupBy('Genre').agg(
    F.sum('Total Streams (Millions)').alias('Total_Streams_Millions'),
    F.avg('Skip Rate (%)').alias('Avg_Skip_Rate'),
    F.count('*').alias('Count_Records')
).orderBy(F.col('Total_Streams_Millions').desc())
genre_stats.show()

# Evolución por año de lanzamiento
print("\nEvolución de streams por año de lanzamiento:")
year_stats = df.groupBy('Release Year').agg(
    F.sum('Total Streams (Millions)').alias('Total_Streams_Millions'),
    F.avg('Monthly Listeners (Millions)').alias('Avg_Monthly_Listeners'),
    F.count('*').alias('Count_Records')
).orderBy('Release Year')
year_stats.show()

# Comparación entre tipos de plataforma (Free vs Premium)
print("\nComparación entre plataformas Free y Premium:")
platform_stats = df.groupBy('Platform Type').agg(
    F.sum('Total Streams (Millions)').alias('Total_Streams_Millions'),
    F.avg('Skip Rate (%)').alias('Avg_Skip_Rate'),
    F.avg('Avg Stream Duration (Min)').alias('Avg_Stream_Duration')
).orderBy(F.col('Total_Streams_Millions').desc())
platform_stats.show()

# Canciones/álbumes con mayor engagement
print("\nTop 10 álbumes con mejor engagement (bajo skip rate y alta duración):")
engagement_stats = df.groupBy('Artist', 'Album').agg(
    F.avg('Skip Rate (%)').alias('Avg_Skip_Rate'),
    F.avg('Avg Stream Duration (Min)').alias('Avg_Stream_Duration'),
    F.sum('Total Streams (Millions)').alias('Total_Streams_Millions')
).orderBy(F.col('Avg_Skip_Rate').asc(), F.col('Avg_Stream_Duration').desc()).limit(10)
engagement_stats.show()

# Correlación entre variables
print("\nCorrelación entre métricas clave:")
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

# Seleccionar columnas numéricas para correlación
numeric_data = df.select(numeric_cols).na.drop()

# Convertir a vector para cálculo de correlación
assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
vector_data = assembler.transform(numeric_data).select("features")

# Calcular matriz de correlación
matrix = Correlation.corr(vector_data, "features").collect()[0][0]
corr_matrix = matrix.toArray().tolist()

# Mostrar matriz de correlación
for i, row in enumerate(corr_matrix):
    print(f"{numeric_cols[i]}: {row}")

# Detener la sesión de Spark
spark.stop()
