from main import LocalSparkSession, save_result, fileFormats, list_root_files
import time


def main() -> None:
    ss = LocalSparkSession().spark_session()
    rootFiles = list_root_files()

    for rootFile in rootFiles:
        dfMain = ss.read.option("header", "true").csv(rootFile)
        fileName = rootFile.split("\\")[1].split(".")[0]
        for fileFormat in fileFormats:
            start = time.time()
            dfMain.repartition(1).write.mode("overwrite").option("header", "true").format(fileFormat).save(
                f'datasets/{fileFormat}')
            end = time.time()
            save_result('write', fileFormat, float(f"{end - start:.2f}"), fileName)


if __name__ == "__main__":
    main()