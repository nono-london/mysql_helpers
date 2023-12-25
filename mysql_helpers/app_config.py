import logging
import platform
from pathlib import Path


def logging_config():
    if platform.system() == "Linux":
        logging_folder = Path("/var", "log", "my_apps", "python", "mysql_helpers")
        logging_folder.mkdir(parents=True, exist_ok=True)
    else:
        logging_folder = Path(get_project_root_path(), "mysql_helpers", "downloads")
        logging_folder.mkdir(parents=True, exist_ok=True)

    logging_file_path = Path(logging_folder, "mysql_helpers.log")
    # Configure the root logger
    logging.basicConfig(
        filename=logging_file_path,  # Global log file name
        level=logging.DEBUG,  # Global log level
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def get_project_root_path():
    root_path: Path = Path(__file__).parent.parent
    return root_path


if __name__ == "__main__":
    logging_config()
    print(get_project_root_path)
