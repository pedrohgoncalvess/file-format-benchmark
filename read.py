from main import LocalSparkSession, fileFormats, list_files, save_result
import time


def main() -> None:
    ss = LocalSparkSession().spark_session()

    for fileFormat in fileFormats:
        allFiles = list_files(fileFormat)
        for file in allFiles:
            fileName = file.split("\\")[1].split(".")[0]
            start = time.time()
            dfMain = ss.read.option("header", "true").format(fileFormat).load(file)
            dfMain.show(5)
            end = time.time()
            save_result('read', fileFormat, float(f"{end - start:.2f}"), fileName)


if __name__ == "__main__":
    main()