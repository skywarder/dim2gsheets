# Integrate QBit-DB by CubiScan with Google Spreadsheets

Get measurement data from QBit-DB *.csv file and put it to Google Spreadsheets record by record

## Deployment

Microsoft Windows
1. Install python3 and pip https://pypi.org/project/pip/ (Anaconda or miniConda package is the simpliest way)
2. Install git
3. Run git and download project using git from github
```
git clone https://github.com/skywarder/dim2gsheets
```
4. Install packages required from command line (or Anaconda terminal)
```
pip3 install -r requirements.txt
```
5. After making changes in the project, update project from github
```
git pull origin master
```
## Settings
1. Run QBit-DB and set auto export to CSV after each measurement and format for exported file:
 - delimiter = ';'
 - decimals = ','  (QBit-DB use only comma, cannot switch to dot)
 - date time format = dd.mm.yyyy H:M:S
2. Run python app first time to create 'Settings.ini' file
3. Set up settings in settings.ini file:
 - path to CSV file
 - Google Spreadsheets ID
 - Google API credentials (https://gspread.readthedocs.io/en/latest/oauth2.html)
 - Column name in CSV file for Date-Time
 - CSV format (';','.')
 - Encoding
 - etc
 
## Running the application
```
python dim2gsheets.py
```

## Check if the application has run successfully 
by reading the logs on the screen
1. Connected to Google API 
2. Found last row time stamp (if not, all rows from CSV file is being dumped to Google Spreadsheets again!)
3. Watchdog started

## Monitoring during measurement
1. Check the spreadsheet if it contains last records by the timestamp (Date-Time column)
2. Switch to the terminal/command line window with application and check that no errors occured

## Known issues
1. Sometimes application reports "No column to read from CSV" and terminal the CSV-monitoring thread - no updates send to spreadsheet.
Solution: just restart the application 
