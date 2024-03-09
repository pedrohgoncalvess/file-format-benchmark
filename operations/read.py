from utils.file_manage import fileFormats, list_files, save_result, get_root_file
from utils.spark_local import LocalSparkSession
import time


def main() -> None:
    ss = LocalSparkSession().spark_session()
    _, fileName = get_root_file()

    for fileFormat in fileFormats:
        allFiles = list_files(fileFormat)
        for file in allFiles:
            print(f"Reading {file.split('.')[-1]}.")
            start = time.time()
            dfMain = ss.read.option("header", "true").format(fileFormat).load(file)
            dfMain.show(5)
            end = time.time()
            save_result('read', fileFormat, 'sec', float(f"{end - start:.2f}"), fileName)

    ss.stop()
