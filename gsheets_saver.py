    # -*- coding: utf-8 -*-
""" Wait for the CSV file updates and save data to SOMEWHERE """
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import pandas
import time
import datetime
import logging
import os
import sys

class GSheets_saver():
    def __init__(self, _config):
        self.worksheet = None
        self.next_row = None
        self.gc = None
        self.credentials = None
        
        try:
            self.CRED_FILENAME          = _config.get("GSheets_saver", "CRED_FILENAME")
            self.SPREADSHEET_ID         = _config.get("GSheets_saver", "SPREADSHEET_ID")
            self.RETRIES_NUMBER         = int(_config.get("GSheets_saver", "RETRIES_NUMBER"))
            self.RETRY_INTERVAL         = int(_config.get("GSheets_saver", "RETRY_INTERVAL"))
        except Exception as e:
            logging.error("Error while getting settings: {error}".format(error=e))
            sys.exit()
            
        self.open()

    def open(self):
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        
        try:
            self.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.CRED_FILENAME, SCOPES)

            self.gc = gspread.authorize(self.credentials)
            self.worksheet = self.gc.open_by_key(self.SPREADSHEET_ID).sheet1
            self.next_row = self.next_available_row()
            
        except Exception as e:
            logging.error('Cannot load Google spreadsheet with id: {id}...:\n{error}\n'.format(
                id=self.SPREADSHEET_ID, error=e))
            sys.exit()
        
    def next_available_row(self):
        str_list = list(filter(None, self.worksheet.col_values(1)))
        return len(str_list)+1

    def add_row(self, _new_line):
        #logging.debug('Adding the row with EAN={ean}'.format(ean=_new_line[0]))
        #logging.debug(_new_line)
        
        iter = 1 # first try to save the line
        while ((self.RETRIES_NUMBER == 0) or (iter <= self.RETRIES_NUMBER)):
            try:
                self.next_row = self.next_available_row()
                self._add_row2gsheet(_new_line)
                return True
            except Exception as e:
                if self.credentials.access_token_expired:   # every hour
                    self.gc.login()                         # refreshes the token
                    continue
                logging.warning('Iter {i} of {total} - Cannot save line with EAN={ean}:\n{error}\n'.format(
                    i=iter, total=self.RETRIES_NUMBER, ean=_new_line[0], error=e))
            iter += 1
        return False
            
    def _add_row2gsheet(self, _new_line):        
        cell_list = self.worksheet.range(self.next_row, 1, self.next_row, len(_new_line))
        i=0
        for cell in cell_list:
            #if type(_new_line[i]) is pandas.Timestamp:
            #    cell.value = str(_new_line[i])
            #else:
            cell.value = _new_line[i]
            i += 1
        # Update in batch
        self.worksheet.update_cells(cell_list)   

    def get_last_timestamp(self, _col_name):
        try:
            col_num = self.worksheet.find(_col_name).col
            val = self.worksheet.cell(self.next_available_row()-1, col_num).value
            last_timestamp = datetime.datetime.strptime(val, "%d.%m.%Y %H:%M:%S")
            
            logging.debug('Last timestamp = {ts} found.'.format(ts=last_timestamp))
        except Exception as e:
            logging.warning('Couldn\'t find timestamp in cell ({r_num}, {c_num}). Set 01-01-1970.\n{err}'
                            .format(r_num=self.next_available_row()-1, c_num=col_num, err=e))
            last_timestamp = datetime.datetime(1970, 1, 1)
        if not last_timestamp:
            last_timestamp = datetime.datetime(1970, 1, 1)
            
        return last_timestamp
            