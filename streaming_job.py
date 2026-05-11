from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructType, TimestampType

INCOMING_PATH = "data/incoming"

ARTICLE_SCHEMA = (
    StructType()
    .add("source", StringType())
    .add("title", StringType())
    .add("url", StringType())
    .add("ts", TimestampType())
)

STOPWORDS = {
    "the", "and", "for", "with", "from", "this", "that", "have", "has", "had",
    "are", "was", "were", "but", "not", "you", "your", "they", "their", "them",
    "will", "would", "could", "should", "into", "over", "after", "before",
    "about", "what", "when", "where", "which", "while", "than", "then", "says",
    "said", "new", "one", "two", "more", "most", "some", "amid", "any", "all",
    "out", "off", "its", "been", "being", "just", "also", "still", "only",
    "even", "much", "many", "such", "how", "why", "who", "whom", "here",
    "there", "these", "those", "because", "between", "during", "through",
    "under", "again", "very", "each", "both", "other", "against", "another",
}


def build_spark(app_name: str = "headline_pulse") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def headline_stream(spark: SparkSession, path: str = INCOMING_PATH) -> DataFrame:
    return (
        spark.readStream
        .schema(ARTICLE_SCHEMA)
        .option("maxFilesPerTrigger", 4)
        .json(path)
    )


def _memory(df: DataFrame, name: str) -> StreamingQuery:
    return (
        df.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName(name)
        .start()
    )


def _source_counts(stream: DataFrame) -> DataFrame:
    return stream.groupBy("source").count()


def _hourly_volume(stream: DataFrame) -> DataFrame:
    return (
        stream.withWatermark("ts", "3 hours")
        .groupBy(F.window(F.col("ts"), "1 hour"))
        .count()
    )


def _word_counts(stream: DataFrame, spark: SparkSession) -> DataFrame:
    stop_df = spark.createDataFrame([(w,) for w in STOPWORDS], ["word"])
    tokens = (
        stream.select(
            F.explode(F.split(F.lower(F.col("title")), r"[^a-z]+")).alias("word")
        )
        .filter(F.length("word") > 3)
    )
    return (
        tokens.join(F.broadcast(stop_df), "word", "left_anti")
        .groupBy("word")
        .count()
    )


def start_streams(spark: SparkSession) -> List[StreamingQuery]:
    stream = headline_stream(spark)
    return [
        _memory(_source_counts(stream), "source_counts"),
        _memory(_hourly_volume(stream), "hourly_volume"),
        _memory(_word_counts(stream, spark), "word_counts"),
    ]


def main() -> None:
    spark = build_spark()
    queries = start_streams(spark)
    print("[stream] active queries:", [q.name for q in queries])
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
