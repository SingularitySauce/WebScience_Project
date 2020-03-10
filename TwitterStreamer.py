from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
import credentials
from pymongo import MongoClient
import json
import operator
from tweepy import TweepError
from http.client import IncompleteRead as http_incompleteRead
from urllib3.exceptions import IncompleteRead as urllib3_incompleteRead
from urllib3.exceptions import ProtocolError


class TwitterClient():
    def __init__(self, twitter_user=None):
        self.authenticator = TwitterAuthenticator().authenticate_app()
        self.twitter_client = API(self.authenticator)

        self.twitter_user = twitter_user

    def get_user_tweets_and_insert(self, num_tweets, collection):
        try:
            most_recent_tweets = self.twitter_client.user_timeline(id=self.twitter_user, count=num_tweets)
            for tweet in most_recent_tweets:
                collection.insert_one(tweet._json)
        except TweepError as e:
            pass


class TwitterAuthenticator():
    def authenticate_app(self):
        auth = OAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_KEY_SECRET)
        auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
        return auth


class TwitterStreamer():
    def __init__(self, limit):
        self.authenticator = TwitterAuthenticator()
        self.listener = StdOutListener(limit)

    def stream_tweets_topic(self, hashtag_list, limit):
        self.listener = StdOutListener(limit)
        auth = self.authenticator.authenticate_app()
        stream = Stream(auth, self.listener)
        print("Now adding tweets from most popular hashtags")
        stream.filter(track=hashtag_list)

    def stream_tweets_sample(self):
        auth = self.authenticator.authenticate_app()
        stream = Stream(auth, self.listener)

        try:
            stream.sample(languages=['en'], stall_warnings=True)
        except (ProtocolError, AttributeError):
            pass

    def store_sample(self):
        # Create a sample of data - 2000 out of 20,000 tweets
        tweets = collection.find({}).limit(2000)

        # Save sample data to a file
        with open("tweets.json", 'w') as f:
            for tweet in tweets:
                tweet.pop('_id')
                t = json.dumps(tweet)
                f.write(t)

    def find_powerusers_and_topics(self, data, number_users, number_hashtags):
        users = {}
        hashtags = {}

        for tweet in data:

            # Evaluate user data
            username = tweet['user']['screen_name']
            if username in users.keys():
                users[username] += 1
            else:
                users[username] = 1

            # Evaluate hashtags
            tweet_hashtags = tweet["entities"]["hashtags"]
            if len(tweet_hashtags) != 0:
                for hashtag in tweet_hashtags:
                    value = hashtag['text']
                    if value in hashtags.keys():
                        hashtags[value] += 1
                    else:
                        hashtags[value] = 1

        sorted_hashtags = sorted(hashtags.items(), key=operator.itemgetter(1), reverse=True)[:number_hashtags]
        sorted_users = sorted(users.items(), key=operator.itemgetter(1), reverse=True)[:number_users]

        print(sorted_users, sorted_hashtags)
        return sorted_users, sorted_hashtags


class StdOutListener(StreamListener):
    def __init__(self, limit):
        self.count = 0
        self.limit = limit

    def on_data(self, data):
        try:
            t = json.loads(data)
            collection.insert_one(t)
            self.count += 1
            print(self.count)
            if self.count < self.limit:
                return True
            else:
                self.count = 0
                self.limit = 100000
                return False
        except BaseException as e:
            print("Error on data: %s" % str(e))
            return True
        except http_incompleteRead as e:
            print("http.client Incomplete Read error: %s" % str(e))
            return True
        except urllib3_incompleteRead as e:
            print("urllib3 Incomplete Read error: %s" % str(e))
            return True

    def on_error(self, status_code):
        if status_code == 420:
            print(status_code)
            return False
        else:
            print(status_code)
            pass


def data_collection(number_of_sample_tweets, number_of_power_users, tweets_per_user, number_of_hashtags,
                    hashtag_related_tweets):

    # Stream 1% of sample tweets until there's number_of_sample_tweets tweets
    streamer = TwitterStreamer(number_of_sample_tweets)
    streamer.stream_tweets_sample()

    # Store sample data as .json
    streamer.store_sample()

    #Fetch all tweets
    tweets = collection.find({})

    # Find a desired number of power users and popular hashtags to follow
    power_users, hashtags = streamer.find_powerusers_and_topics(tweets, number_users=number_of_power_users,
                                                                number_hashtags=number_of_hashtags)

    # Enrich the data by fetching tweets by power users and tweets relating to the identified topics
    for user in power_users:
        username = user[0]
        client = TwitterClient(twitter_user=username)
        client.get_user_tweets_and_insert(num_tweets=tweets_per_user, collection=collection)
        print("Added %s tweets" % username)

    hashtag_list = []
    for hashtag in hashtags:
        hashtag_list.append(hashtag[0])

    print("Adding tweets from most popular hashtags")
    streamer.stream_tweets_topic(hashtag_list=hashtag_list, limit=hashtag_related_tweets)
    print("Completed data collection")


if __name__ == "__main__":
    cluster = MongoClient("mongodb+srv://user:1234@cluster0-qe3mx.mongodb.net/test?retryWrites=true&w=majority")
    db = cluster["tweets"]

    # Drop the collection and starts it up again fresh with every run
    db.drop_collection("tweets")
    db.create_collection("tweets")
    collection = db["tweets"]

    data_collection(
        number_of_sample_tweets=100,
        number_of_power_users=5,
        tweets_per_user=20,
        number_of_hashtags=5,
        hashtag_related_tweets=100
    )


