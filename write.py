from main import LocalSparkSession, save_result, fileFormats, get_root_file
import time


def main() -> None:
    ss = LocalSparkSession().spark_session()
    rootFile, fileName = get_root_file()

    dfMain = ss.read.option("header", "true").csv(rootFile)
    for fileFormat in fileFormats:
        start = time.time()
        dfMain.repartition(1).write.mode("overwrite").option("header", "true").format(fileFormat).save(
            f'datasets/{fileFormat}')
        end = time.time()
        save_result('write', fileFormat, 'sec', float(f"{end - start:.2f}"), fileName)


if __name__ == "__main__":
    main()