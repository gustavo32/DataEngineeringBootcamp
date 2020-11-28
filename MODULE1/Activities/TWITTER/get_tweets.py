import json
from datetime import datetime
from tweepy import OAuthHandler, Stream, StreamListener

consumer_key = 'pTkq9BQtW8OkgbvWUcWb3VrMK'
consumer_secret = 'TbLk3K0Voibm6am4BSk5G0Zb732fEpSXp8LEfnIUA4BewcZA7A'

access_token = '293510563-Hlk0m9B581PZuhgVHhUpstMdLYZakewKY2E7lhK4'
access_token_secret = 'aO0G46Cd0ZfrkNqt7Gpgg51EvL6DrDmODw8H9wPeaFSh0'

out = open('collected_tweets_{}.txt'.format(
    datetime.now().strftime('%Y-%m-%d-%H-%M-%S')), 'w')


class MyListener(StreamListener):

    def on_data(self, data):
        itemString = json.dumps(data)
        out.write(itemString + '\n')
        return True

    def on_error(self, status):
        print(status)


if __name__ == "__main__":
    listener = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, listener)
    stream.filter(track=["Santos"])
