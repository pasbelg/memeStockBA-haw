
from setupFunctions import getSecret
from databaseFunctions import writeRecord
import tweepy, time, json

searchTerm = input('Bitte den Begriff eingeben auf den die Tweets gefiltert werden sollen')
# Authentifizierung mittels Zugangsdaten, die unter 
consumer_key = getSecret("twitter", "consumerKey")
consumer_secret = getSecret("twitter", "consumerSecret")
access_key = getSecret("twitter", "accessKey")
access_secret = getSecret("twitter", "accessSecret")
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)
api.me().screen_name

class tweepylistener(tweepy.StreamListener):
    def __init__(self, api = None):
        self.api = api or API()
        self.dbError = open('output/tweetBackup.txt', 'a')
    def on_data(self, data):
        if 'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
            elif 'limit' in data:
                if self.on_limit(json.loads(data)['limit']['track']) is False:
                    return False
            elif 'warning' in data:
                warning = json.loads(data)['warnings']
                print(warning['message'])
                return False
    # Bestimmte Vorgehen in unterschiedlichen Situationen
    # Fall 1: neuer Status Tweet
    def on_status(self, status):
        writeRecord(json.loads(status), 'tweetData')
        #print(status)
        return True
    # Fall 2: User löscht Tweet nach gewisser Zeit
    def on_delete(self, status_id, user_id):
        writeRecord(json.loads(status), 'deletedTweetData')
        return
    # Fall 3: Streaming API Rate Limit
    def on_limit(self, track):
        #writeRecord(json.loads(track), 'twitterLimitInfo')
        sys.stderr.write(time.strftime("%Y%m%d-%H%M%S") + '>> Fehler: ' + str(status_code) + + '\n')
        time.sleep(60)
        return False
    def on_timeout(self):
        #writeRecord(json.loads(track), 'twitterTimeoutInfo')
        sys.stderr.write(time.strftime("%Y%m%d-%H%M%S") + '>> Timeout, ware für 120 Sekunden\n')
        time.sleep(120)
        return
def main():
    list_terms = [searchTerm]
    listener = tweepylistener(api)
    stream = tweepy.Stream(auth, listener, timeout=600.0)
    while True:
        print(time.strftime("%Y%m%d-%H%M%S") + ">> Streaming gestartet... beobachte und sammle" )
        print(time.strftime("%Y%m%d-%H%M%S") + ">> Suche Twitter nach: " + str(list_terms)[1:-1])
        try:
            stream.filter(track=list_terms, is_async=False)
            break
        except Exception as e:
            print('Fehler')
            time.sleep(60)

main() 
