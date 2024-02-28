from configs import LocalSparkSession, save_result, fileFormats, get_root_file
import time
from pyspark.sql.functions import col, lower


def main() -> None:
    ss = LocalSparkSession().spark_session()
    rootFile, fileName = get_root_file()

    dfMain = ss.read.option("header", "true").csv(rootFile)
    for fileFormat in fileFormats:
        print(f"Writing {fileFormat} file.")
        start = time.time()
        newDf = dfMain.select([col(column_name).alias(column_name.replace(" ", "_").lower()) for column_name in dfMain.columns])
        newDf.repartition(1).write.mode("overwrite").option("header", "true").format(fileFormat).save(
            f'datasets/{fileFormat}')
        end = time.time()
        save_result('write', fileFormat, 'sec', float(f"{end - start:.2f}"), fileName)

    ss.stop()
