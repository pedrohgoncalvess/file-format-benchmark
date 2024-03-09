from utils.file_manage import save_result, fileFormats, get_root_file, list_files
from utils.spark_local import LocalSparkSession
import time
from pyspark.sql.functions import col, lower


def main(query:str) -> None:
    ss = LocalSparkSession().spark_session()
    _, fileName = get_root_file()
    
    for fileFormat in fileFormats:
        allFiles = list_files(fileFormat)
        for file in allFiles:
            start = time.time()
            dfMain = ss.read.option("header", "true").format(fileFormat).load(file)
            dfMain.createOrReplaceTempView(f"file_{fileFormat}")
            newDf = dfMain.select([col(column_name).alias(column_name.replace(" ", "_").lower()) for column_name in dfMain.columns])
            resultQuery = ss.sql(query.replace("file",f"file_{fileFormat}"))
            resultQuery.show(5)
            newDf.repartition(1).write.mode("overwrite").option("header", "true").format(fileFormat).save(f'datasets/{fileFormat}')
            end = time.time()
            save_result('all', fileFormat, 'sec', float(f"{end - start:.2f}"), fileName)

    ss.stop()
