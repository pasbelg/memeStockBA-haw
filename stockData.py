from databaseFunctions import writeRecord
import yfinance as yf
import time

# Funktion um die Daten von Yahoo Finance herunterzuladen, zu verarbeiten und dann in die Datenbank zu schreiben
def processStockData(stock, datasets):
    # Um die aufgrund der relativen Berechnung auf 0 stehenden Volumendaten entfernen zu können müssen die zu beschaffenden Datensets immer 2 Minuten mehr umfassen
    # (insgesamt 4 Minuten, da angebrochene Minute nicht dazu zählt)
    df = yf.download(tickers=stock, period=str(datasets+3)+'m', interval='1m', progress = False)
    df = df[:-1]
    df = df[1:]
    df['Ticker'] = stock
    df['Time'] = df.index.strftime("%Y-%m-%d %H:%M:%S")
    recordsList = df.to_dict('records')
    if df.empty:
        return False
    else:
        # Abfangen des Fehlercodes bei Problemen mit der Datenbank UND beim schreiben in die Failover-Datei
        try:
            for minuteData in recordsList:
                writeRecord(minuteData, 'stockData')
            print(time.strftime("%Y-%m-%d %H:%M:%S") + '>>', datasets, 'Kursdatensätze für "'+stock+'" wurden erfolgreich gespiechert')
            return True
        except Exception as e:
            print(time.strftime("%Y-%m-%d %H:%M:%S") + '>>', stock+': Fehler beim schreiben der Kursdaten. Fehlercode:', e)
            return False

def stockMain(stockList, backupCounter):
    for stock in stockList:
        written = processStockData(stock, backupCounter[stock])
        # backupCounter zählt bei Download/Schreibfehlern den Counter innerhalb des Dictionarys hoch um beim nächsten Schleifendurchlauf den alten Datesatz mitzunehmen
        if written:
            backupCounter[stock] = 1
        else:
            print(time.strftime("%Y-%m-%d %H:%M:%S") + '>>', stock+': Es konnten keine Einträge gespeichert werden. Aktie wird für Backuplauf vorgemerkt.')
            backupCounter[stock] += 1
