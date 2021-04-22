# MemeStocksBA

MemeStocksBA ist ein Datamining Tool zum Sammeln von Kurs- und Twitterdaten zu bestimmten Aktien. Es sammelt die Daten automatisch innerhalb der Öffnungszeiten der NYSE anhand eines als Tabelle zu Verfügung gestellten Zeitplans. Es ist Teil meiner Bachelorarbeit mit dem Thema "Einfluss von viraler Twitter-Kommunikation auf die Kursaktivitäten von Wertpapieren" an der HAW-Hamburg.

## Vor dem Start
    
Vor dem Starten sollte ein Zeitplan in Form einer CSV oder Excel Datei im Verzeichnis input/ mit dem Namen researchSchedule abgelegt werden.
Die Datei sollte wie folgt aufgebaut sein:

| GME        | AAPL       | TSLA       |
| -----------|:----------:| ----------:|
| 01.01.2021 | 20.04.2021 | 15.10.2022 |
| 02.01.2021 | 10.05.2021 | 27.10.2022 |

Zugangs- und Serverdaten zur Datenbank, sowie die nötigen API Keys für Twitter sollten in der Datei "secrets" gespeichert werden. Hierfür können die Informationen in der Datei secrets.template ergänzt und diese dann umbenannt werden. Alternativ können die Informationen auch als String im Code in tweetData.py und databaseFunctions.py eingefügt werden.
    
Folgende Bibliotheken sollten installiert sein.
- [yfinance](https://pypi.org/project/yfinance/)
- [pandas](https://pypi.org/project/pandas/)
- [pandas_market_calendars](https://pypi.org/project/pandas-market-calendars/)
- [pytz](https://pypi.org/project/pytz/)
- [tzlocal](https://pypi.org/project/tzlocal/)
- [python-dateutil](https://pypi.org/project/python-dateutil/)


## Nutzung
Zum Nutzen des Tools sollte das Repository heruntergeladen und die Datei main.py mit Pyhton 3 ausgeführt werden.

Das Tool ist darauf ausgelegt auf einem Linux System Hintergrund zu laufen. Zum starten eines Hintergrundservices kann der Befehl ```nohup python3 -u ./main.py > run.log &``` genutzt werden. Jegliche Ausgaben werden damit in die Datei run.log geschrieben. Die Funktionalität auf anderen System wurde nicht getestet.

Einmal gestartet überprüft das Tool den Zeitplan und fängt an den definierten Tagen automatisch mit der Datensammlung an.

Für die komplette Funktionalität ist MongoDB Datenbank mit Schreibzugriff nötig. Ist diese nicht vorhanden kann der automatische Failover zum primären Speichern der Daten genutzt werden. Dieser speichert die Dateien in den Dateien tweetDataFailover.txt und tweetDataFailover.txt im Verzeihis output/

## Lizenz
[GPLv3](https://choosealicense.com/licenses/gpl-3.0/)
