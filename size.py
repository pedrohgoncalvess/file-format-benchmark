import os

from main import fileFormats, save_result, get_root_file


def main():
    for fileFormat in fileFormats:
        dirPath = f"datasets\\{fileFormat}"
        _, fileName = get_root_file()

        file = [f for f in os.listdir(dirPath) if f.endswith(f".{fileFormat}")][0]

        filePath = os.path.join(dirPath, file)
        fileSize = os.path.getsize(filePath) / (1024 ** 2)  # mb
        save_result('size', fileFormat, 'mb', float(f"{fileSize:.2f}"), fileName)


if __name__ == "__main__":
    main()
