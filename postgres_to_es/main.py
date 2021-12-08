from dotenv import load_dotenv
import logging
import os

from config import Config
from etl_process import ETLProcess

if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    config = Config.parse_file("config.json")
    config.dsn.password = os.environ.get('DB_PASSWORD')
    etl_process = ETLProcess(config)
    etl_process.process()
