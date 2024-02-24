from pyspark.sql.session import SparkSession
import pandas as pd
import os

fileFormats = ['csv', 'parquet', 'orc', 'json', 'avro']


def list_root_files():
    dirPath = "datasets\\"
    allFiles = [os.path.join(dirPath, file) for file in os.listdir(dirPath)]
    mainFiles = [file for file in allFiles if file.endswith(f".csv")]

    return mainFiles


def list_files(ext: str) -> list[str]:
    dirPath = f"datasets\\{ext}"
    allFiles = [os.path.join(dirPath, file) for file in os.listdir(dirPath)]
    filteredFiles: list[str] = [file for file in allFiles if file.endswith(f".{ext}")]

    return filteredFiles


def save_result(operation: str, format: str, time: float, file_name: str):
    file = "results.csv"

    if os.path.exists(file):
        df = pd.read_csv(file)
    else:
        df = pd.DataFrame(columns=['operation', 'format', 'time', 'file'])

    newRow = pd.DataFrame({'operation': [operation], 'format': [format], 'time': [time], 'file': [file_name]})
    df = pd.concat([df, newRow], ignore_index=True)

    df.to_csv(file, index=False)


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
