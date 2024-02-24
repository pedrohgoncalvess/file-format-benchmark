from main import LocalSparkSession, save_result, fileFormats, list_files, get_root_file
import time


def main() -> None:
    ss = LocalSparkSession().spark_session()
    _, fileName = get_root_file()

    for fileFormat in fileFormats:
        allFiles = list_files(fileFormat)
        for file in allFiles:
            ss.read.option("header", "true").format(fileFormat).load(file).createOrReplaceTempView(f"file_{fileFormat}")
            start = time.time()
            resultQuery = ss.sql(f"select * from file_{fileFormat} where brand = 'apple'") #you can change the query for any other
            resultQuery.show(5)
            end = time.time()
            save_result('query', fileFormat, 'sec', float(f"{end - start:.2f}"), fileName)


if __name__ == "__main__":
    main()
