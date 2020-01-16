    # -*- coding: utf-8 -*-
"""Wait for the CSV file updates and import data to GOOGLE Spreadsheets."""
# Local import
from dim_reader import Dim_reader
from gsheets_saver import GSheets_saver
# ---------------
import time
import configparser
import logging
import os

logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

# ************* SETTINGS ****************
def createConfig(path):
    """
    Create a config file
    """
    config = configparser.ConfigParser()
    
    # GENERAL
    config.add_section("General")
    config.set("General", "log_level", str(logging.DEBUG))
    
    # DIM_Reader
    config.add_section("Dim_reader")
    config.set("Dim_reader", "DIM_FILE_PATH", "./input/dims.csv")
    #config.set("Dim_reader", "DIM_FILE_PATH", "./input/data_2019-10-19.sqlite")
    config.set("Dim_reader", "DIM_FILE_CHECK_INTERVAL", "10")
    config.set("Dim_reader", "DIM_FILE_DELIMITER", ",")
    config.set("Dim_reader", "DIM_FILE_DECIMALS", ".")
    config.set("Dim_reader", "DIM_TIMESTAMP_COLUMN", "Date-Time")
    config.set("Dim_reader", "DIM_FILE_ENCODING", "cp1251") # "utf-8" or https://docs.python.org/3/library/codecs.html#standard-encodings
    
    # GSheets_saver
    config.add_section("GSheets_saver")
    config.set("GSheets_saver", "CRED_FILENAME", './dim2GSheets-aa30775bc217.json')
    config.set("GSheets_saver", "SPREADSHEET_ID", '1Oui0gLgglQy8-1fxd-oKtQv9VPavqSIFOdgplNWi5Zg')    
    config.set("GSheets_saver", "RETRIES_NUMBER", '3') # number or retries, =0 for infinite
    config.set("GSheets_saver", "RETRY_INTERVAL", '2')
    
    with open(path, "w") as config_file:
        config.write(config_file)

# ---------- Global variables -----------
config_path = "settings.ini"        

if __name__ == "__main__":
    if not os.path.exists(config_path):
        createConfig(config_path)
    
    config = configparser.ConfigParser()
    config.read(config_path)

    filename, file_extension = os.path.splitext(config.get("Dim_reader", "DIM_FILE_PATH"))

    log_level = int(config.get("General", "log_level"))
    if (log_level):
        logging.basicConfig(level=log_level)
    
    saver = GSheets_saver(config)
    dim_reader = Dim_reader.constructor(file_extension, config, saver)
    dim_reader.wait_for_updates() # infinite cycle
    
# ---------------------------------------------------