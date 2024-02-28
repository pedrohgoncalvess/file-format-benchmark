from configs import LocalSparkSession, save_result, fileFormats, list_files, get_root_file
import time


def main(query: str) -> None:
    ss = LocalSparkSession().spark_session()
    _, fileName = get_root_file()

    for fileFormat in fileFormats:
        allFiles = list_files(fileFormat)
        for file in allFiles:
            ss.read.option("header", "true").format(fileFormat).load(file).createOrReplaceTempView(f"file_{fileFormat}")
            print(f"Querying {file.split('.')[-1]}.")
            start = time.time()
            resultQuery = ss.sql(query.replace("file",f"file_{fileFormat}"))
            resultQuery.show(5)
            end = time.time()
            save_result('query', fileFormat, 'sec', float(f"{end - start:.2f}"), fileName)

    ss.stop()
