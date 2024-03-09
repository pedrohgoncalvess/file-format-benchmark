import pandas as pd
import os
from utils.parser import read_configs

fileFormats = read_configs().get("formats")


def get_root_file() -> (str, str):
    dirPath = "datasets\\"
    allFiles = [os.path.join(dirPath, file) for file in os.listdir(dirPath)]
    mainFiles = [file for file in allFiles if file.endswith(f".csv")]
    mainFile = mainFiles[0]
    fileName = mainFile.split("\\")[1].split('.')[0]

    return mainFile, fileName


def list_files(ext: str) -> list[str]:
    dirPath = f"datasets\\{ext}"
    allFiles = [os.path.join(dirPath, file) for file in os.listdir(dirPath)]
    filteredFiles: list[str] = [file for file in allFiles if file.endswith(f".{ext}")]

    return filteredFiles


def save_result(operation: str, file_format: str, measure:str, value: float, file_name: str):
    file = "results.csv"

    if os.path.exists(file):
        df = pd.read_csv(file)
    else:
        df = pd.DataFrame(columns=['operation', 'format', 'measure', 'value', 'file'])

    newRow = pd.DataFrame({'operation': [operation], 'format': [file_format], 'measure': [measure], 'value': [value], 'file': [file_name]})
    df = pd.concat([df, newRow], ignore_index=True)

    df.to_csv(file, index=False)
