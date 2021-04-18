# Raw Package
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import datetime
import time
from databaseFunctions import writeRecord
from tweetData import tweetMain
import yfinance as yf
import pytz
from tzlocal import get_localzone
import threading

researchSchedule = pd.read_excel('input/test.xlsx')
# Funktion die überprüft ob eine übergebene Zeit zwischen zwei übergebenen Zeitpunkten liegt
def timeBetween(now, start, end):
    if start <= end:
        return start <= now < end
    else: # over midnight e.g., 23:30-04:15
        return start <= now or now < end

# Funktion, welche die übergebene Zeit in UTC zeit umwandelt
def getUTC(time):
    local = pytz.timezone(str(get_localzone()))
    naive = time
    localDT = local.localize(naive, is_dst=None)
    return localDT.astimezone(pytz.utc)

# Funktion, welche die Sekunden bis zur nächsten vollen Stunde wiedergibt
def secondsTillNext(duration):
    now = datetime.datetime.now()
    if duration == 'minute':
        delta = datetime.timedelta(minutes=1)
        nextTime = (now + delta).replace(microsecond=0, second=1)
    elif duration == 'hour':
        delta = datetime.timedelta(hours=1)
        nextTime = (now + delta).replace(microsecond=0, second=0, minute=1)
    elif duration == 'day':
        delta = datetime.timedelta(days=1)
        nextTime = (now + delta).replace(microsecond=0, second=0, minute=0, hour=0)
    secondsTillNextTime = (nextTime - now).seconds  
    return secondsTillNextTime

# Funktion, die über einen als Pandas Dataframe übergebenen Zeiplan die zu untersuchenden Aktien für den Tag ermittelt
def stocksBySchedule(schedule):
    tickers = []
    for stock in schedule:
        for date in schedule[stock]:
            if date == today:
                tickers.append(stock)
    return tickers

def processStockData(stock, datasets):
    # Um die aufgrund der relativen Berechnung auf 0 stehenden Volumendaten entfernen zu können müssen die zu beschaffenden Datensetzt immer 2 Minuten mehr umfassen
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
        try:
            for minuteData in recordsList:
                writeRecord(minuteData, 'stockData')
            print(time.strftime("%Y-%m-%d %H:%M:%S"), '>>', datasets, 'Kursdatensätze für "'+stock+'" wurden erfolgreich gespiechert')
            return True
        except Exception as e:
            print(time.strftime("%Y-%m-%d %H:%M:%S"), '>>', stock+': Fehler beim schreiben der Kursdaten. Fehlercode:', e)
            return False

while True:
    #Test Zeiten
    now = datetime.datetime(2021,4,16,16,0,0)
    today = datetime.date(2021,4,16)
    # Live Zeiten
    #now = datetime.datetime.today()
    #today = datetime.date.today()
    todaysStocks = stocksBySchedule(researchSchedule)
    if todaysStocks:
        # Setup und Info für den Twitter Stream
        twitterStream = threading.Thread(name='twitterStream', target=tweetMain, args=[todaysStocks[0]])
        if len(todaysStocks) > 1:
            print(time.strftime("%Y-%m-%d %H:%M:%S"), '>> Mehr als eine Aktie eingeplant. Twitterstream nur für', todaysStocks[0], 'aufgebaut')

        # Setup für den Zeitplan der NYSE
        nyse = mcal.get_calendar('NYSE')
        nyseScheduleToday = nyse.schedule(start_date=today, end_date=today)
        backupCounter = dict((stock,1) for stock in todaysStocks)
        while timeBetween(getUTC(now), nyseScheduleToday['market_open'][0], nyseScheduleToday['market_close'][0]):
            print(time.strftime("%Y-%m-%d %H:%M:%S"), 'Der Markt ist geöffnet Datenerhebung gestartet')
            if twitterStream.is_alive() == 0:
                twitterStream.start()
            for stock in todaysStocks:
                written = processStockData(stock, backupCounter[stock])
                if written:
                    backupCounter[stock] = 1
                else:
                    print(time.strftime("%Y-%m-%d %H:%M:%S"), '>>', stock+': Es konnten keine Einträge gespeichert werden. Aktie wird für Backuplauf vorgemerkt.')
                    backupCounter[stock] += 1
            print(time.strftime("%Y-%m-%d %H:%M:%S"), '>> Nächster Schreibvorgang in der nächsten vollen Minute')
            time.sleep(secondsTillNext('minute'))
            #time.sleep(3)
            
        else:
            print(time.strftime("%Y-%m-%d %H:%M:%S"), '>> Keine neuen Daten verfügbar weil die NYSE geschlossen ist. Es wird bis zur nächsten vollen Minute gewartet')
            print(time.strftime("%Y-%m-%d %H:%M:%S"), '>> UTC Zeit:', getUTC(now), 'Heutige Öffnungszeiten (NYSE):', nyseScheduleToday)
            time.sleep(secondsTillNext('minute'))
    else:
        print(time.strftime("%Y-%m-%d %H:%M:%S"), '>> Keine Untersuchung für heute eingeplant es wird bis zum nächsten Tag gewartet')
        time.sleep(secondsTillNext('day'))