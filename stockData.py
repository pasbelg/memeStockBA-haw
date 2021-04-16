# Raw Package
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import datetime
import time
from setupFunctions import mongoConnect
import yfinance as yf
import pytz
from tzlocal import get_localzone

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

def writeStockData(stock, datesets):
    print(stock, 'wird heruntergeladen. Einträge zu Erwarten:', backupCounter)
    # Um die aufgrund der relativen Berechnung auf 0 stehenden Volumendaten entfernen zu können müssen die zu beschaffenden Datensetzt immer 2 Minuten mehr umfassen
    # (insgesamt 4 Minuten, da angebrochene Minute nicht dazu zählt)
    df = yf.download(tickers=stock, period=str(datesets+3)+'m', interval='1m')
    df = df[:-1]
    df = df[1:]
    df['Ticker'] = stock
    df.index = df.index.strftime("%m/%d/%Y, %H:%M:%S")
    if df.empty:
        return False
    else:
        try:
            db = mongoConnect()
            # Erstellung einer Collection wenn sie noch nicht existiert
            stockData = db['stockData']
            print(stock, 'wurde erfolgreich in die Datenbank geschrieben')
            for minuteData in df.iterrows():
                #print(df.index)
                stockData.insert_one(df.to_dict())
            return True
        except Exception as e:
            print(e)
            print(stock+': Fehler beim schreiben in die Datenbank')
            return False


while True:
    now = datetime.datetime.today()
    today = datetime.date.today()
    todaysStocks = stocksBySchedule(researchSchedule)
    if todaysStocks:
        nyse = mcal.get_calendar('NYSE')
        nyseScheduleToday = nyse.schedule(start_date=today, end_date=today)
        backupCounter = dict((stock,1) for stock in todaysStocks)
        print(backupCounter)
        while timeBetween(getUTC(now), nyseScheduleToday['market_open'][0], nyseScheduleToday['market_close'][0]):
            print('Der Markt ist geöffnet schreibe Kursdaten in die Datenbank')
            for stock in todaysStocks:
                written = writeStockData(stock, backupCounter[stock])
                if written:
                    print('Daten wurden gespeichert neuer Schreibvorgang in einer Minute')
                    backupCounter[stock] = 1
                    print(backupCounter)
                else:
                    print('Es gab einen Fehler beim Speichern der Daten Backup wird vorgemerkt')
                    backupCounter[stock] += 1
            time.sleep(secondsTillNext('minute'))
            #time.sleep(3)
            
        else:
            print('Keine neuen Daten verfügbar weil die NYSE geschlossen ist. Es wird bis zur nächsten vollen Minute gewartet')
            print('Das sind', secondsTillNext('minute'), 'Sekunden')
            time.sleep(secondsTillNext('minute'))
    else:
        print('Keine Untersuchung für heute eingeplant es wird bis zur nächsten vollen Stunde gewartet')
        print(secondsTillNext('hour'))
        time.sleep(secondsTillNext('hour'))
#Data Source
db = mongoConnect()
db['stockData']


#print(nyse.schedule(start_date=nowUTC, end_date=nowUTC))
researchStart = '15'
nyseScheduleToday = nyse.schedule(start_date=nowUTC, end_date=nowUTC)
nyseTodayOpen = nyseScheduleToday['market_open'][0]
nyseTodayClose = nyseScheduleToday['market_close'][0]
#print('nyseOpen:', type(nyseTodayOpen))
#print('nowUTC:', type(nowUTC))
#Interval required 1 minute

dataGME = yf.Ticker("GME")
#data = yf.download(tickers='GME AMC UBER AAPL', period='2d', interval='5m')
# Einlesen des Zeitplans für die Aktien damit das Programm die datenerhebung am richtigen Tag starten kann
# So aufgebaut

firstStock = yf.download(tickers='GME', period='2m', interval='1m')
secondStock = yf.download(tickers='AMC', period='2m', interval='1m')
##print(firstStock.to_json())
#print(secondStock.to_json())
#Weniger Daten die abgefragt werden aber keine Volumeninfo?
'''
while True:
    data = yf.download(tickers='GME', period='2m', interval='1m')
    print(data)
    time.sleep(1)
'''
#nyseClose = datetime.datetime(now())
#nyseOpen = 
#while 

'''
#Data viz
#import plotly.graph_objs as go

#Interval required 1 minute
data = yf.download(tickers='GME AMC NOK BB GOOG AAPL', start="2021-01-14", end="2021-02-05", interval='1d')
dataGME = yf.Ticker("GME")
#data = yf.download(tickers='GME AMC UBER AAPL', period='2d', interval='5m')

with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    print(dataGME.info['beta'])
    #print(data)
    #print(data['Volume'].idxmax(axis=1))
    #print(data['Volume'])


#declare figure
fig = go.Figure()

#Candlestick
fig.add_trace(go.Candlestick(x=data.index,
                open=data['Open'],
                high=data['High'],
                low=data['Low'],
                close=data['Close'], name = 'market data'))

# Add titles
fig.update_layout(
    title='Uber live share price evolution',
    yaxis_title='Stock Price (USD per Shares)')

# X-Axes
fig.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=15, label="15m", step="minute", stepmode="backward"),
            dict(count=45, label="45m", step="minute", stepmode="backward"),
            dict(count=1, label="HTD", step="hour", stepmode="todate"),
            dict(count=3, label="3h", step="hour", stepmode="backward"),
            dict(step="all")
        ])
    )
)

#Show
fig.show()
'''