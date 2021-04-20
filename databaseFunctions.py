import pymongo
from setupFunctions import getSecret

# Funktion zum Verbindungaufbau mit der Datenbank (Serverinformationen befinden sich in der Datei "secrets")
def mongoConnect():
    mongoHost = getSecret('mongodb', 'host')
    mongoPort = int(getSecret('mongodb', 'port'))
    con = pymongo.MongoClient(mongoHost, mongoPort)
    return con

# Funktion, zur Authentifizierung mit der Dantenbank auf übergebenen Connection-Objekts (Datenbank- und Nutzerinformationen befinden sich in der Datei "secrets")
def dbAuth(con):
    db = getSecret('mongodb', 'database')
    user = getSecret('mongodb', 'user')
    password = getSecret('mongodb', 'password')
    db = con[db]
    db.authenticate(user, password)
    return db

#Funktion, die eine Verbindung zur Datenbank aufbaut, und die als dict übergebenen Daten in die übergebene collection oder im Falle eines Fehlers in eine Datei schreibt
def writeRecord(record, dataOrigin):
    try:
        con = mongoConnect()
        db = dbAuth(con)
        collection = db[dataOrigin]
        collection.insert_one(record)
        con.close()
        return True
    except Exception as e:
        #Tweet wird in einer Datei ausgegeben falls es Probleme beim schreiben in die Datenbank gab
        with open('output/'+dataOrigin+'Failover.txt', 'a') as file:
            file.write(str(record) + '\n')
        print(e)
        return False

