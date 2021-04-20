import pandas as pd
import pandas_market_calendars as mcal
import datetime, time
from tweetData import tweetMain
from stockData import stockMain
import pytz
from tzlocal import get_localzone
import dateutil.parser
import multiprocessing

# Funktion, die überprüft ob eine übergebene Zeit zwischen zwei übergebenen Zeitpunkten liegt
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
            try:
                date = dateutil.parser.parse(str(date))
            except:
                pass
            if date.date() == today:
                tickers.append(stock)
    return tickers

while True:
    # Test Zeiten
    #now = datetime.datetime(2021,4,19,16,0,0)
    #today = datetime.date(2021,4,19)
    # Live Zeiten
    now = datetime.datetime.today()
    today = datetime.date.today()
    
    # Der Zeitplan wird in jedem Durchlauf außerhalb der Marktzeiten neu eingelesen (Änderungen unterbrechen nicht den Datensammlung)
    researchSchedule = pd.read_csv('input/researchSchedule.csv', sep='[,; ]', engine='python')
    #researchSchedule = pd.read_excel('input/researchSchedule.xlsx') # Auch Excel import möglich
    todaysStocks = stocksBySchedule(researchSchedule)
    
    # Wenn Aktien im für den Tag eingeplant sind
    if todaysStocks:
        # Setup und Info für den Twitter Stream
        twitterStream = multiprocessing.Process(name='twitterStream', target=tweetMain, args=[todaysStocks[0]])
        if len(todaysStocks) > 1:
            print(time.strftime("%Y-%m-%d %H:%M:%S") + '>> Mehr als eine Aktie eingeplant. Twitterstream nur für', todaysStocks[0], 'aufgebaut')

        # Setup für den tagesaktuellen Zeitplan der NYSE
        nyse = mcal.get_calendar('NYSE')
        nyseScheduleToday = nyse.schedule(start_date=today, end_date=today)
        backupCounter = dict((stock,1) for stock in todaysStocks)
        
        # Now wird bei jedem Durchgang in der while True Loop erhöht (Zeit verläuft now wird am Anfang neu gesetzt)
        # Ist der Markt eröffnet gibt es eine Ausgabe bevor die Schleife für die Datenerhebung startet
        if timeBetween(getUTC(now), nyseScheduleToday['market_open'][0], nyseScheduleToday['market_close'][0]):
            print(time.strftime("%Y-%m-%d %H:%M:%S") + '>> Der Markt ist geöffnet Datenerhebung gestartet')

        # Schleife die so lange ausgeführt wird wie die aktuelle UTC Zeit des Systems innerhalb der UTC Zeit der NYSE Öffnungszeiten des Tages liegt
        while timeBetween(getUTC(now), nyseScheduleToday['market_open'][0], nyseScheduleToday['market_close'][0]):
            # Abfrage ob der Thread schon läuft damit er nicht doppelt gestartet wird
            if twitterStream.is_alive() == 0:
                twitterStream.start()
            
            stockMain(todaysStocks, backupCounter)
            
            print(time.strftime("%Y-%m-%d %H:%M:%S") + '>> Nächster Schreibvorgang in der nächsten vollen Minute')
            time.sleep(secondsTillNext('minute'))
            #time.sleep(3) # Wird zum schnelleren Testen der Logik benötigt
            
            # now wird neu gesetzt damit die While Loop nicht unendlich läuft
            now = datetime.datetime.today()
        else:
            print(time.strftime("%Y-%m-%d %H:%M:%S") + '>> Keine neuen Daten verfügbar weil die NYSE geschlossen ist. Es wird bis zur nächsten vollen Minute gewartet')
            print(time.strftime("%Y-%m-%d %H:%M:%S") + '>> UTC Zeit:', getUTC(now), 'Heutige Öffnungszeiten (NYSE in UTC):', nyseScheduleToday['market_open'][0], 'bis',  nyseScheduleToday['market_close'][0])
            if twitterStream.is_alive() == 1:
                twitterStream.terminate()
            time.sleep(secondsTillNext('minute'))
    else:
        print(time.strftime("%Y-%m-%d %H:%M:%S") + '>> Keine Untersuchung für heute eingeplant es wird bis zum nächsten Tag gewartet')
        time.sleep(secondsTillNext('day'))
