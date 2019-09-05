# -*- coding: utf-8 -*-
""" Wait for the CSV file updates and save data to SOMEWHERE """

import pandas
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
# Reduce the amount of logs:
import logging
import os
import sys
import datetime

class CSVFileHandler(FileSystemEventHandler):
    
    def __init__(self, _dim_reader):
        super(FileSystemEventHandler, self).__init__()
        self.dim_reader = _dim_reader

    def on_modified(self, event):
        if not event.is_directory and not 'thumbnail' in event.src_path:
            logging.debug("Updated: " + event.src_path)
            self.dim_reader.get_new_line()
            
class Dim_reader():
    
    def __init__(self, _config, _saver):
        self.saver = _saver
        
        
        try:
            self.DIM_FILE_PATH           = _config.get("Dim_reader", "DIM_FILE_PATH")
            self.DIM_FILE_CHECK_INTERVAL = int(_config.get("Dim_reader", "DIM_FILE_CHECK_INTERVAL"))
            self.DIM_FILE_DELIMITER      = _config.get("Dim_reader", "DIM_FILE_DELIMITER")
            self.DIM_TIMESTAMP_COLUMN    = _config.get("Dim_reader", "DIM_TIMESTAMP_COLUMN")
            self.DIM_FILE_ENCODING       = _config.get("Dim_reader", "DIM_FILE_ENCODING")
            self.DIM_FILE_DECIMALS       = _config.get("Dim_reader", "DIM_FILE_DECIMALS")
        except Exception as e:
            logging.error("Error while getting settings: {error}".format(error=e))
            sys.exit()
            
        self.last_line_time = self.saver.get_last_timestamp(self.DIM_TIMESTAMP_COLUMN)
        self.get_new_line()
        
    def wait_for_updates(self):
        event_handler = CSVFileHandler(self)
        observer = Observer()
        observer.schedule(event_handler, os.path.dirname(self.DIM_FILE_PATH), recursive=False)
        observer.start()
        logging.debug("Watchdog observer started")
        try:
            while True:
                time.sleep(self.DIM_FILE_CHECK_INTERVAL)
                logging.debug("Check for the updates...")
        except KeyboardInterrupt:
            observer.stop()
            sys.exit()
        observer.join()

    def get_new_line(self):
        try:
            data_src = pandas.read_csv(self.DIM_FILE_PATH, 
                                       delimiter=self.DIM_FILE_DELIMITER, 
                                       decimal=self.DIM_FILE_DECIMALS, 
                                       encoding=self.DIM_FILE_ENCODING,
                                       warn_bad_lines=True, 
                                       error_bad_lines=False,
                                       dayfirst=True)
        except Exception as e:
            logging.error('Error while reading CSV file: {name}...:\n{error}\n'.format(
                name=self.DIM_FILE_PATH, error=e))
            raise e
        
        if not data_src.empty:
            #data_src['Datetime'] = pandas.to_datetime(data_src.iloc[self.DIM_TIMESTAMP_COLUMN],
            data_src['Datetime'] = pandas.to_datetime(data_src[self.DIM_TIMESTAMP_COLUMN],
                                                     errors='coerce',
                                                     dayfirst=True)
            #logging.debug(data_src[['Item Number', 'Date-Time','Datetime']].head(10))
            new_lines = data_src[data_src['Datetime'] > self.last_line_time]
            new_lines.fillna('', inplace=True)
            
            logging.debug('Found {num} new lines, saving to the spreadsheet'.format(num=new_lines.shape[0]))
            if (new_lines.shape[0] > 1):
                #logging.debug("bulk upd >>> timestamp = {ts}\n line >>>> {line}".format(ts=new_lines.tail(1)['Datetime'], line=new_lines.tail(1)))
                if (self.saver.add_rows_bulk(new_lines.drop(columns=['Datetime']))):
                    self.last_line_time = self.saver.get_last_timestamp(self.DIM_TIMESTAMP_COLUMN)
                    return
            #else:
            for idx, row in new_lines.iterrows():
                # drop - because gspread cannot save Timestamp object to JSON
                if (self.saver.add_row(row.drop('Datetime'))): 
                    self.last_line_time = row['Datetime'] # Timestamp from the last saved line
        else:
            logging.debug("Datasource is empty after reading CSV")
            return 