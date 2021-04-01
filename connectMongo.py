import pymongo

MONGO_HOST = "192.168.178.25"
MONGO_PORT = 49155
MONGO_DB = "baData"
MONGO_USER = "pythonScript"
MONGO_PASS = "python123"

con = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
db = con[MONGO_DB]
db.authenticate(MONGO_USER, MONGO_PASS)

print(db)