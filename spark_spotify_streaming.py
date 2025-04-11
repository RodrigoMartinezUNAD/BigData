from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, sum, max, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# Configuración de Spark Session
spark = SparkSession.builder \
    .appName("SpotifyStreamingAnalysis") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema para los datos de Spotify
spotify_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("artist", StringType()),
    StructField("track", StringType()),
    StructField("genre", StringType()),
    StructField("country", StringType()),
    StructField("streams", IntegerType()),
    StructField("duration_ms", IntegerType())
])

# Leer datos de Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "spotify_streaming") \
    .option("startingOffsets", "latest") \
    .load()

# Parsear los datos JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), spotify_schema).alias("data")
).select(
    col("data.timestamp"),
    col("data.artist"),
    col("data.genre"),
    col("data.country"),
    col("data.streams"),
    col("data.duration_ms")
).filter(
    col("artist").isNotNull() & 
    col("genre").isNotNull() & 
    col("country").isNotNull()
)

# Estadísticas por artista
artist_stats = parsed_df.groupBy(
    window(col("timestamp"), "5 minutes", "1 minute"),
    "artist"
).agg(
    count("*").alias("total_plays"),
    sum("streams").alias("total_streams"),
    avg("streams").alias("avg_streams"),
    min("streams").alias("min_streams"),
    max("streams").alias("max_streams"),
    avg("duration_ms").alias("avg_duration_ms")
).orderBy("window", col("total_streams").desc())

# Estadísticas por género y país
genre_country_stats = parsed_df.groupBy(
    window(col("timestamp"), "5 minutes", "1 minute"),
    "genre",
    "country"
).agg(
    count("*").alias("play_count"),
    sum("streams").alias("total_streams"),
    avg("streams").alias("avg_streams")
).orderBy("window", col("total_streams").desc())

# Función para mostrar estadísticas de artistas
def write_artist_stats(df, epoch_id):
    if not df.isEmpty():
        print("\n" + "="*60)
        print("TOP ARTISTAS (últimos 5 minutos)")
        print("="*60)
        df.select(
            "window.start", "artist", "total_plays", 
            "total_streams", "avg_streams", "avg_duration_ms"
        ).show(10, truncate=False)

# Función para mostrar estadísticas por género y país
def write_genre_stats(df, epoch_id):
    if not df.isEmpty():
        print("\n" + "="*60)
        print("TENDENCIAS POR GÉNERO Y PAÍS")
        print("="*60)
        df.select(
            "window.start", "genre", "country", 
            "play_count", "total_streams"
        ).show(10, truncate=False)

# Stream artistas
query_artist = artist_stats \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_artist_stats) \
    .start()

# Stream géneros y países
query_genre = genre_country_stats \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_genre_stats) \
    .start()

# Mantener las consultas activas
spark.streams.awaitAnyTermination()
