from pyspark.sql.session import SparkSession

class LocalSparkSession:
    def __init__(self) -> None:
        import findspark
        findspark.init()

    def spark_session(self) -> SparkSession:
        from pyspark.sql import SparkSession

        jars_avro = ["https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar"]

        spark = (SparkSession.builder
                 .appName("FileFormatBenchmark")
                 .config("spark.driver.memory", "4g")
                 .config("spark.executor.memory", "4g")
                 .config("spark.jars", ",".join(jars_avro))
                 .master('local[*]').getOrCreate())

        return spark