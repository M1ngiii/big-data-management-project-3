import os
import sys
import base64
from decimal import Decimal

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType as SparkDoubleType


if "PROJECT_ROOT" in os.environ:
    PROJECT_ROOT = os.environ["PROJECT_ROOT"]
elif os.path.exists("/home/jovyan/project"):
    PROJECT_ROOT = "/home/jovyan/project"
else:
    PROJECT_ROOT = "/opt/project"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "sourcedb")
PG_USER = os.environ.get("PG_USER", "cdc_user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "cdc_pass")

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123")

CDC_DB = "lakehouse.cdc"
TAXI_DB = "lakehouse.taxi"

BRONZE_CDC = f"{CDC_DB}.bronze_cdc"
SILVER_CUSTOMERS = f"{CDC_DB}.silver_customers"
SILVER_DRIVERS = f"{CDC_DB}.silver_drivers"

BRONZE_TAXI = f"{TAXI_DB}.bronze_taxi"
SILVER_TAXI = f"{TAXI_DB}.silver_taxi"
GOLD_TAXI = f"{TAXI_DB}.gold_taxi_hourly_zone"
GOLD_DRIVER_SCORECARD = f"{TAXI_DB}.gold_driver_scorecard"

SILVER_TAXI_STAGE = f"{TAXI_DB}.silver_taxi_stage"


def create_spark(app_name):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")
    return spark


def pg_execute(sql, fetch=True):
    import psycopg2

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall() if fetch else None
    cur.close()
    conn.close()
    return rows


def parse_debezium_ts(json_col, field_name):
    raw = f"get_json_object({json_col}, '$.{field_name}')"
    return F.expr(f"""
        CASE
            WHEN {raw} IS NULL THEN NULL
            WHEN {raw} RLIKE '^[0-9]+$' AND length({raw}) >= 16
                THEN timestamp_micros(CAST({raw} AS BIGINT))
            WHEN {raw} RLIKE '^[0-9]+$' AND length({raw}) >= 13
                THEN timestamp_millis(CAST({raw} AS BIGINT))
            WHEN {raw} RLIKE '^[0-9]+$'
                THEN to_timestamp(from_unixtime(CAST({raw} AS BIGINT)))
            ELSE try_to_timestamp({raw})
        END
    """)


def decode_debezium_decimal(value, scale=2):
    if value is None:
        return None

    try:
        return float(value)
    except Exception:
        pass

    try:
        raw = base64.b64decode(value)
        integer_value = int.from_bytes(raw, byteorder="big", signed=True)
        return float(Decimal(integer_value) / (Decimal(10) ** scale))
    except Exception:
        return None


decode_driver_rating = udf(lambda x: decode_debezium_decimal(x, 2), SparkDoubleType())


def parse_taxi_ts(col_name):
    return F.coalesce(
        F.to_timestamp(F.col(col_name)),
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss")
    )


def taxi_schema():
    return StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
    ])


def create_tables(spark):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_CDC} (
        source_table STRING,
        record_id INT,
        op STRING,
        before_json STRING,
        after_json STRING,
        source_lsn BIGINT,
        source_snapshot STRING,
        ts_ms BIGINT,
        kafka_key STRING,
        raw_value STRING,
        topic STRING,
        kafka_partition INT,
        kafka_offset BIGINT,
        kafka_timestamp TIMESTAMP,
        is_tombstone BOOLEAN,
        bronze_ingested_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (source_table)
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_CUSTOMERS} (
        id INT,
        name STRING,
        email STRING,
        country STRING,
        created_at TIMESTAMP,
        last_updated_ms BIGINT
    ) USING iceberg
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_DRIVERS} (
        id INT,
        name STRING,
        license_number STRING,
        rating DOUBLE,
        city STRING,
        active BOOLEAN,
        created_at TIMESTAMP,
        last_updated_ms BIGINT
    ) USING iceberg
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TAXI} (
        kafka_key STRING,
        raw_json STRING,
        topic STRING,
        kafka_partition INT,
        kafka_offset BIGINT,
        kafka_timestamp TIMESTAMP,
        bronze_ingested_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(kafka_timestamp))
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TAXI} (
        trip_id STRING,
        vendor_id INT,
        pickup_ts TIMESTAMP,
        dropoff_ts TIMESTAMP,
        pickup_date DATE,
        pickup_hour INT,
        trip_duration_minutes DOUBLE,
        passenger_count INT,
        trip_distance DOUBLE,
        pu_location_id INT,
        pickup_zone STRING,
        pickup_borough STRING,
        do_location_id INT,
        dropoff_zone STRING,
        dropoff_borough STRING,
        fare_amount DOUBLE,
        tip_amount DOUBLE,
        total_amount DOUBLE,
        payment_type INT,
        congestion_surcharge DOUBLE,
        raw_json STRING
    ) USING iceberg
    PARTITIONED BY (pickup_date)
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TAXI} (
        pickup_date DATE,
        pickup_hour INT,
        pickup_zone STRING,
        trip_count BIGINT,
        avg_fare_amount DOUBLE,
        avg_total_amount DOUBLE,
        avg_trip_distance DOUBLE,
        avg_trip_duration_minutes DOUBLE
    ) USING iceberg
    PARTITIONED BY (pickup_date)
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_DRIVER_SCORECARD} (
        report_date DATE,
        driver_id INT,
        driver_name STRING,
        license_number STRING,
        city STRING,
        active BOOLEAN,
        current_rating DOUBLE,
        previous_rating DOUBLE,
        rating_trend STRING,
        total_trips BIGINT,
        avg_fare_per_trip DOUBLE,
        avg_trip_distance DOUBLE,
        total_revenue DOUBLE
    ) USING iceberg
    PARTITIONED BY (report_date)
    """)


def read_kafka_batch(spark, subscribe=None, subscribe_pattern=None):
    reader = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
    )

    if subscribe is not None:
        reader = reader.option("subscribe", subscribe)

    if subscribe_pattern is not None:
        reader = reader.option("subscribePattern", subscribe_pattern)

    return reader.load()


def append_new_rows(df, target_table, key_cols):
    spark = df.sparkSession

    if spark.catalog.tableExists(target_table):
        existing = spark.table(target_table).select(*key_cols)
        df = df.join(existing, key_cols, "left_anti")

    count = df.count()
    if count == 0:
        print(f"{target_table}: 0 new rows")
        return 0

    df.writeTo(target_table).append()
    print(f"{target_table}: appended {count} rows")
    return count


def latest_cdc_rows(spark, source_table):
    bronze = (
        spark.table(BRONZE_CDC)
        .filter(F.col("source_table") == source_table)
        .filter(~F.col("is_tombstone"))
        .filter(F.col("record_id").isNotNull())
        .filter(F.col("op").isNotNull())
    )

    w = Window.partitionBy("record_id").orderBy(
        F.col("ts_ms").desc(),
        F.col("kafka_offset").desc()
    )

    return (
        bronze
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def run_bronze_cdc():
    spark = create_spark("project3-bronze-cdc")
    create_tables(spark)

    kafka_df = read_kafka_batch(
        spark,
        subscribe_pattern=r"dbserver1\.public\.(customers|drivers)"
    )

    bronze_df = (
        kafka_df
        .select(
            F.regexp_extract(F.col("topic"), r"dbserver1\.public\.(.+)", 1).alias("source_table"),
            F.get_json_object(F.col("key").cast("string"), "$.payload.id").cast("int").alias("key_id"),
            F.get_json_object(F.col("value").cast("string"), "$.payload.after.id").cast("int").alias("after_id"),
            F.get_json_object(F.col("value").cast("string"), "$.payload.before.id").cast("int").alias("before_id"),
            F.get_json_object(F.col("value").cast("string"), "$.payload.op").alias("op"),
            F.get_json_object(F.col("value").cast("string"), "$.payload.before").alias("before_json"),
            F.get_json_object(F.col("value").cast("string"), "$.payload.after").alias("after_json"),
            F.get_json_object(F.col("value").cast("string"), "$.payload.source.lsn").cast("bigint").alias("source_lsn"),
            F.get_json_object(F.col("value").cast("string"), "$.payload.source.snapshot").alias("source_snapshot"),
            F.get_json_object(F.col("value").cast("string"), "$.payload.ts_ms").cast("bigint").alias("ts_ms"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").isNull().alias("is_tombstone"),
            F.current_timestamp().alias("bronze_ingested_at")
        )
        .withColumn("record_id", F.coalesce("after_id", "before_id", "key_id"))
        .drop("key_id", "after_id", "before_id")
    )

    append_new_rows(
        bronze_df,
        BRONZE_CDC,
        ["topic", "kafka_partition", "kafka_offset"]
    )

    spark.stop()


def run_silver_cdc():
    spark = create_spark("project3-silver-cdc")
    create_tables(spark)

    customer_stage = f"{CDC_DB}.silver_customers_stage"
    driver_stage = f"{CDC_DB}.silver_drivers_stage"

    customers_latest = latest_cdc_rows(spark, "customers").select(
        F.col("record_id").alias("id"),
        "op",
        F.get_json_object("after_json", "$.name").alias("name"),
        F.get_json_object("after_json", "$.email").alias("email"),
        F.get_json_object("after_json", "$.country").alias("country"),
        parse_debezium_ts("after_json", "created_at").alias("created_at"),
        F.col("ts_ms").alias("last_updated_ms")
    )

    spark.sql(f"DROP TABLE IF EXISTS {customer_stage}")
    customers_latest.writeTo(customer_stage).using("iceberg").create()

    spark.sql(f"""
    MERGE INTO {SILVER_CUSTOMERS} AS t
    USING {customer_stage} AS s
    ON t.id = s.id

    WHEN MATCHED AND s.op = 'd' THEN DELETE

    WHEN MATCHED AND s.op IN ('c', 'u', 'r') THEN UPDATE SET
      t.name = s.name,
      t.email = s.email,
      t.country = s.country,
      t.created_at = s.created_at,
      t.last_updated_ms = s.last_updated_ms

    WHEN NOT MATCHED AND s.op IN ('c', 'u', 'r') THEN INSERT (
      id, name, email, country, created_at, last_updated_ms
    ) VALUES (
      s.id, s.name, s.email, s.country, s.created_at, s.last_updated_ms
    )
    """)

    drivers_latest = latest_cdc_rows(spark, "drivers").select(
        F.col("record_id").alias("id"),
        "op",
        F.get_json_object("after_json", "$.name").alias("name"),
        F.get_json_object("after_json", "$.license_number").alias("license_number"),
        decode_driver_rating(F.get_json_object("after_json", "$.rating")).alias("rating"),
        F.get_json_object("after_json", "$.city").alias("city"),
        F.get_json_object("after_json", "$.active").cast("boolean").alias("active"),
        parse_debezium_ts("after_json", "created_at").alias("created_at"),
        F.col("ts_ms").alias("last_updated_ms")
    )

    spark.sql(f"DROP TABLE IF EXISTS {driver_stage}")
    drivers_latest.writeTo(driver_stage).using("iceberg").create()

    spark.sql(f"""
    MERGE INTO {SILVER_DRIVERS} AS t
    USING {driver_stage} AS s
    ON t.id = s.id

    WHEN MATCHED AND s.op = 'd' THEN DELETE

    WHEN MATCHED AND s.op IN ('c', 'u', 'r') THEN UPDATE SET
      t.name = s.name,
      t.license_number = s.license_number,
      t.rating = s.rating,
      t.city = s.city,
      t.active = s.active,
      t.created_at = s.created_at,
      t.last_updated_ms = s.last_updated_ms

    WHEN NOT MATCHED AND s.op IN ('c', 'u', 'r') THEN INSERT (
      id, name, license_number, rating, city, active, created_at, last_updated_ms
    ) VALUES (
      s.id, s.name, s.license_number, s.rating, s.city, s.active, s.created_at, s.last_updated_ms
    )
    """)

    print("silver_customers:", spark.table(SILVER_CUSTOMERS).count())
    print("silver_drivers:", spark.table(SILVER_DRIVERS).count())

    spark.stop()


def run_bronze_taxi():
    spark = create_spark("project3-bronze-taxi")
    create_tables(spark)

    kafka_df = read_kafka_batch(spark, subscribe="taxi-trips")

    bronze_df = (
        kafka_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("bronze_ingested_at")
        )
    )

    append_new_rows(
        bronze_df,
        BRONZE_TAXI,
        ["topic", "kafka_partition", "kafka_offset"]
    )

    spark.stop()


def run_silver_taxi():
    spark = create_spark("project3-silver-taxi")
    create_tables(spark)

    zones_path = os.path.join(PROJECT_ROOT, "data", "taxi_zone_lookup.parquet")
    zones = (
        spark.read.parquet(zones_path)
        .select(
            F.col("LocationID").cast("int").alias("LocationID"),
            F.col("Zone").alias("Zone"),
            F.col("Borough").alias("Borough")
        )
    )

    pickup_zones = zones.select(
        F.col("LocationID").alias("pu_location_id"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("pickup_borough")
    )

    dropoff_zones = zones.select(
        F.col("LocationID").alias("do_location_id"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough")
    )

    parsed = (
        spark.table(BRONZE_TAXI)
        .select(
            F.from_json("raw_json", taxi_schema()).alias("j"),
            "raw_json",
            "kafka_timestamp",
            "kafka_offset"
        )
        .select("j.*", "raw_json", "kafka_timestamp", "kafka_offset")
    )

    source_df = (
        parsed
        .withColumn("pickup_ts", parse_taxi_ts("tpep_pickup_datetime"))
        .withColumn("dropoff_ts", parse_taxi_ts("tpep_dropoff_datetime"))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("pu_location_id", F.col("PULocationID").cast("int"))
        .withColumn("do_location_id", F.col("DOLocationID").cast("int"))
        .withColumn(
            "trip_duration_minutes",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
        )
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("VendorID").cast("string"), F.lit("")),
                    F.coalesce(F.col("tpep_pickup_datetime"), F.lit("")),
                    F.coalesce(F.col("tpep_dropoff_datetime"), F.lit("")),
                    F.coalesce(F.col("PULocationID").cast("string"), F.lit("")),
                    F.coalesce(F.col("DOLocationID").cast("string"), F.lit("")),
                    F.coalesce(F.col("trip_distance").cast("string"), F.lit("")),
                    F.coalesce(F.col("fare_amount").cast("string"), F.lit("")),
                    F.coalesce(F.col("tip_amount").cast("string"), F.lit("")),
                    F.coalesce(F.col("total_amount").cast("string"), F.lit(""))
                ),
                256
            )
        )
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.col("pu_location_id").isNotNull())
        .filter(F.col("do_location_id").isNotNull())
        .filter(F.col("dropoff_ts") >= F.col("pickup_ts"))
        .filter(F.col("trip_duration_minutes") > 0)
        .filter(F.col("trip_duration_minutes") <= 720)
        .filter(F.col("trip_distance") >= 0)
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("tip_amount") >= 0)
        .filter(F.col("total_amount") >= 0)
        .join(pickup_zones, on="pu_location_id", how="left")
        .join(dropoff_zones, on="do_location_id", how="left")
        .select(
            "trip_id",
            F.col("VendorID").alias("vendor_id"),
            "pickup_ts",
            "dropoff_ts",
            "pickup_date",
            "pickup_hour",
            "trip_duration_minutes",
            "passenger_count",
            "trip_distance",
            "pu_location_id",
            "pickup_zone",
            "pickup_borough",
            "do_location_id",
            "dropoff_zone",
            "dropoff_borough",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "payment_type",
            "congestion_surcharge",
            "raw_json",
            "kafka_timestamp",
            "kafka_offset"
        )
    )

    w = Window.partitionBy("trip_id").orderBy(
        F.col("kafka_timestamp").desc(),
        F.col("kafka_offset").desc()
    )

    latest_df = (
        source_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "kafka_timestamp", "kafka_offset")
    )

    spark.sql(f"DROP TABLE IF EXISTS {SILVER_TAXI_STAGE}")
    latest_df.writeTo(SILVER_TAXI_STAGE).using("iceberg").create()

    spark.sql(f"""
    MERGE INTO {SILVER_TAXI} AS t
    USING {SILVER_TAXI_STAGE} AS s
    ON t.trip_id = s.trip_id

    WHEN MATCHED THEN UPDATE SET
      t.vendor_id = s.vendor_id,
      t.pickup_ts = s.pickup_ts,
      t.dropoff_ts = s.dropoff_ts,
      t.pickup_date = s.pickup_date,
      t.pickup_hour = s.pickup_hour,
      t.trip_duration_minutes = s.trip_duration_minutes,
      t.passenger_count = s.passenger_count,
      t.trip_distance = s.trip_distance,
      t.pu_location_id = s.pu_location_id,
      t.pickup_zone = s.pickup_zone,
      t.pickup_borough = s.pickup_borough,
      t.do_location_id = s.do_location_id,
      t.dropoff_zone = s.dropoff_zone,
      t.dropoff_borough = s.dropoff_borough,
      t.fare_amount = s.fare_amount,
      t.tip_amount = s.tip_amount,
      t.total_amount = s.total_amount,
      t.payment_type = s.payment_type,
      t.congestion_surcharge = s.congestion_surcharge,
      t.raw_json = s.raw_json

    WHEN NOT MATCHED THEN INSERT (
      trip_id, vendor_id, pickup_ts, dropoff_ts, pickup_date, pickup_hour,
      trip_duration_minutes, passenger_count, trip_distance, pu_location_id,
      pickup_zone, pickup_borough, do_location_id, dropoff_zone,
      dropoff_borough, fare_amount, tip_amount, total_amount,
      payment_type, congestion_surcharge, raw_json
    ) VALUES (
      s.trip_id, s.vendor_id, s.pickup_ts, s.dropoff_ts, s.pickup_date, s.pickup_hour,
      s.trip_duration_minutes, s.passenger_count, s.trip_distance, s.pu_location_id,
      s.pickup_zone, s.pickup_borough, s.do_location_id, s.dropoff_zone,
      s.dropoff_borough, s.fare_amount, s.tip_amount, s.total_amount,
      s.payment_type, s.congestion_surcharge, s.raw_json
    )
    """)

    print("silver_taxi:", spark.table(SILVER_TAXI).count())
    spark.stop()


def run_gold_taxi():
    spark = create_spark("project3-gold-taxi")
    create_tables(spark)

    gold_df = (
        spark.table(SILVER_TAXI)
        .filter(F.col("pickup_date").isNotNull())
        .filter(F.col("pickup_hour").isNotNull())
        .filter(F.col("pickup_zone").isNotNull())
        .groupBy("pickup_date", "pickup_hour", "pickup_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
        )
    )

    gold_df.writeTo(GOLD_TAXI).overwritePartitions()
    print("gold_taxi:", spark.table(GOLD_TAXI).count())

    spark.stop()


def run_gold_driver_scorecard():
    spark = create_spark("project3-gold-driver-scorecard")
    create_tables(spark)

    drivers_df = spark.table(SILVER_DRIVERS)
    silver_taxi_df = spark.table(SILVER_TAXI)

    driver_count = drivers_df.count()
    if driver_count == 0:
        raise RuntimeError("silver_drivers is empty, cannot build driver scorecard")

    driver_slot_window = Window.orderBy("id")

    current_drivers = (
        drivers_df
        .select("id", "name", "license_number", "city", "active", "rating")
        .withColumn("driver_slot", F.row_number().over(driver_slot_window) - F.lit(1))
        .withColumnRenamed("id", "driver_id")
        .withColumnRenamed("name", "driver_name")
        .withColumnRenamed("rating", "current_rating")
    )

    report_dates = (
        silver_taxi_df
        .select(F.col("pickup_date").alias("report_date"))
        .distinct()
    )

    assigned_trips = (
        silver_taxi_df
        .select("trip_id", "pickup_date", "fare_amount", "trip_distance", "total_amount")
        .withColumn("report_date", F.col("pickup_date"))
        .withColumn(
            "driver_slot",
            F.pmod(F.abs(F.xxhash64("trip_id")), F.lit(driver_count))
        )
    )

    trip_metrics = (
        assigned_trips
        .groupBy("report_date", "driver_slot")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_per_trip"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue")
        )
    )

    driver_rating_history = (
        spark.table(BRONZE_CDC)
        .filter(F.col("source_table") == "drivers")
        .filter(~F.col("is_tombstone"))
        .filter(F.col("op").isin("c", "u", "r"))
        .select(
            F.col("record_id").alias("driver_id"),
            decode_driver_rating(F.get_json_object("after_json", "$.rating")).alias("rating"),
            "ts_ms",
            "kafka_offset"
        )
        .filter(F.col("rating").isNotNull())
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("driver_id").orderBy(
                    F.col("ts_ms").desc(),
                    F.col("kafka_offset").desc()
                )
            )
        )
    )

    previous_rating = (
        driver_rating_history
        .filter(F.col("rn") == 2)
        .select(
            "driver_id",
            F.col("rating").alias("previous_rating")
        )
    )

    driver_calendar = (
        report_dates
        .crossJoin(
            current_drivers.select(
                "driver_slot",
                "driver_id",
                "driver_name",
                "license_number",
                "city",
                "active",
                "current_rating"
            )
        )
    )

    scorecard = (
        driver_calendar
        .join(trip_metrics, on=["report_date", "driver_slot"], how="left")
        .join(previous_rating, on="driver_id", how="left")
        .fillna({
            "total_trips": 0,
            "avg_fare_per_trip": 0.0,
            "avg_trip_distance": 0.0,
            "total_revenue": 0.0
        })
        .withColumn(
            "rating_trend",
            F.when(F.col("previous_rating").isNull(), F.lit("no_prior_rating"))
             .when(F.col("current_rating") > F.col("previous_rating"), F.lit("up"))
             .when(F.col("current_rating") < F.col("previous_rating"), F.lit("down"))
             .otherwise(F.lit("same"))
        )
        .select(
            "report_date",
            "driver_id",
            "driver_name",
            "license_number",
            "city",
            "active",
            "current_rating",
            "previous_rating",
            "rating_trend",
            "total_trips",
            "avg_fare_per_trip",
            "avg_trip_distance",
            "total_revenue"
        )
    )

    scorecard.writeTo(GOLD_DRIVER_SCORECARD).overwritePartitions()

    print("gold_driver_scorecard:", spark.table(GOLD_DRIVER_SCORECARD).count())
    spark.table(GOLD_DRIVER_SCORECARD).orderBy(
        F.col("report_date").desc(),
        F.col("total_revenue").desc()
    ).show(20, truncate=False)

    spark.stop()


def run_validate():
    spark = create_spark("project3-validate")
    create_tables(spark)

    for source_table, silver_table in [
        ("customers", SILVER_CUSTOMERS),
        ("drivers", SILVER_DRIVERS),
    ]:
        pg_count = pg_execute(f"SELECT COUNT(*) FROM {source_table};")[0][0]
        silver_count = spark.table(silver_table).count()
        print(f"{source_table}: postgres={pg_count}, silver={silver_count}")

        if pg_count != silver_count:
            raise RuntimeError(f"Row count mismatch for {source_table}: postgres={pg_count}, silver={silver_count}")

        print(f"PostgreSQL sample: {source_table}")
        for row in pg_execute(f"SELECT * FROM {source_table} ORDER BY id LIMIT 3;"):
            print(row)

        print(f"Iceberg sample: {silver_table}")
        spark.table(silver_table).orderBy("id").show(3, truncate=False)
        print("-" * 80)

    print("Taxi counts")
    print("bronze_taxi:", spark.table(BRONZE_TAXI).count())
    print("silver_taxi:", spark.table(SILVER_TAXI).count())
    print("gold_taxi:", spark.table(GOLD_TAXI).count())
    print("gold_driver_scorecard:", spark.table(GOLD_DRIVER_SCORECARD).count())

    spark.stop()


def main():
    if len(sys.argv) != 2:
        print("Usage: python jobs/project3_jobs.py [bronze_cdc|silver_cdc|bronze_taxi|silver_taxi|gold_taxi|gold_driver_scorecard|validate]")
        sys.exit(1)

    job = sys.argv[1]

    if job == "bronze_cdc":
        run_bronze_cdc()
    elif job == "silver_cdc":
        run_silver_cdc()
    elif job == "bronze_taxi":
        run_bronze_taxi()
    elif job == "silver_taxi":
        run_silver_taxi()
    elif job == "gold_taxi":
        run_gold_taxi()
    elif job == "gold_driver_scorecard":
        run_gold_driver_scorecard()
    elif job == "validate":
        run_validate()
    else:
        print(f"Unknown job: {job}")
        sys.exit(1)


if __name__ == "__main__":
    main()
