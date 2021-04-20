# Die Funktion getSecret wird nur benötigt, falls die Secrets über eine secrets Datei eingespielt werden soll
from setupFunctions import getSecret
import tweepy, time 
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
        self.deleted = open('deletedTweet.txt', 'a')
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
        print(status)
        return True
    # Fall 2: User löscht Tweet nach gewisser Zeit
    def on_delete(self, status_id, user_id):
        self.deleted.write(str(status_id) + '\n')
        return
    # Fall 3: Streaming API Rate Limit
    def on_limit(self, track):
        sys.stderr.write(time.strftime("%Y%m%d-%H%M%S") + '>> Fehler: ' + str(status_code) + + '\n')
        time.sleep(60)
        return False
    def on_timeout(self):
        sys.stderr.write(time.strftime("%Y%m%d-%H%M%S") + '>> Timeout, ware für 120 Sekunden\n')
        time.sleep(120)
        return
def main():
    list_terms = ["#corona"]
    listener = tweepylistener(api)
    stream = tweepy.Stream(auth, listener, timeout=600.0)
    while True:
        print(time.strftime("%Y%m%d-%H%M%S") + ">> Streaming gestartet... beobachte und sammle" )
        print(time.strftime("%Y%m%d-%H%M%S") + ">> Suche Twitter nach: " + str(list_terms)[1:-1])
        try:
            stream.filter(track=list_terms, is_async=False)
            break
        except Exception as e:
            time.sleep(60)
if __name__ == "__main__":
    main() 
