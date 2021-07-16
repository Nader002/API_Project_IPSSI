# set chdir to current dir
import os
import sys
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__)))
os.chdir(os.path.realpath(os.path.dirname(__file__)))

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from unidecode import unidecode
import time
from threading import Lock, Timer
import pandas as pd
from config import stop_words
import regex as re
from collections import Counter
import string
import pickle
import itertools
from textblob import TextBlob
import re

analyzer = SentimentIntensityAnalyzer()

#consumer key, consumer secret, access token, access secret.
ckey="Ft8omrPUeGfeJ1QNW49GJpz4p"
csecret="0By9KDs2UWxBaFijeegUH1Wc7v4bCI7k5d2a1JiIGQOKzSchtn"
atoken="1058024247937900545-2150Qoh0X6vQ0QvzzsBM3WrrTFqyZQ"
asecret="K1HE8CUrhLr6TZEMrDZcEKhAKqqnFOZBcqyp5OTYGbbPu"
# isolation lever disables automatic transactions,
# we are disabling thread check as we are creating connection here, but we'll be inserting from a separate thread 
conn = sqlite3.connect('twitter.db', isolation_level=None, check_same_thread=False)
c = conn.cursor()

def create_table():
    try:

        # allows concurrent write and reads
        c.execute("PRAGMA journal_mode=wal")
        c.execute("PRAGMA wal_checkpoint=TRUNCATE")

        # changed unix to INTEGER (it is integer, sqlite can use up to 8-byte long integers)
        c.execute("CREATE TABLE IF NOT EXISTS sentiment(id INTEGER PRIMARY KEY AUTOINCREMENT, unix INTEGER, tweet TEXT, sentiment REAL)")
        # key-value table for random stuff
        c.execute("CREATE TABLE IF NOT EXISTS misc(key TEXT PRIMARY KEY, value TEXT)")
        # id on index, both as DESC 
        c.execute("CREATE INDEX id_unix ON sentiment (id DESC, unix DESC)")
        # out full-text search table
        c.execute("CREATE VIRTUAL TABLE sentiment_fts USING fts5(tweet, content=sentiment, content_rowid=id, prefix=1, prefix=2, prefix=3)")
        # Create trigger will automagically update out table when row is interted
        c.execute("""
            CREATE TRIGGER sentiment_insert AFTER INSERT ON sentiment BEGIN
                INSERT INTO sentiment_fts(rowid, tweet) VALUES (new.id, new.tweet);
            END
        """)
    except Exception as e:
        print(str(e))
create_table()

# create lock
lock = Lock()

class listener(StreamListener):

    data = []
    lock = None

    def __init__(self, lock):

        # create lock
        self.lock = lock

        # init timer for database save
        self.save_in_database()
        super().__init__()

    def save_in_database(self):

        # set a timer (1 second)
        Timer(0.5, self.save_in_database).start()

        # with lock, if there's data, save in transaction using one bulk query
        with self.lock:
            if len(self.data):
                c.execute('BEGIN TRANSACTION')
                try:
                    c.executemany("INSERT INTO sentiment (unix, tweet, sentiment) VALUES (?, ?, ?)", self.data)
                except:
                    pass
                c.execute('COMMIT')

                self.data = []

    def on_data(self, data):
        try:
            #print('------------------------------------------------------------------------- NEW TWEET ---------------------------------------------------------------------------------------------------------')
            data = json.loads(data)
            #print(data)
            if 'truncated' not in data:
                return True
            if data['truncated']:
                tweet = unidecode(data['extended_tweet']['full_text'])
            else:
                tweet = unidecode(data['text'])
            time_ms = data['timestamp_ms']
            print('tweet')
            print(tweet)
            vs = analyzer.polarity_scores(tweet) # -1 bad feeling +1 good feeling
            print(vs)

            print('-------')
            tweet = re.sub(r'@[A-Za-z0-9]+','',tweet)
            tweet = re.sub('https?://[A-Za-z0-9./]+','',tweet)
            tweet = re.sub("[^a-zA-Z]", " ", tweet)

            def remove_pattern(input_txt, pattern):
                r = re.findall(pattern, input_txt)
                for i in r:
                    input_txt = re.sub(i, '', input_txt)
                    
                return input_txt  

            tweet = remove_pattern(tweet, 'RT ')
            print('clean')
            print(tweet)

            vs = analyzer.polarity_scores(tweet)
            print(vs)
            sentiment = vs['compound']
            #print(time_ms, tweet, sentiment)

            # append to data list (to be saved every 1 second)
            with self.lock:
                self.data.append((time_ms, tweet, sentiment))

        except KeyError as e:
            print(str(e))
        return True

    def on_error(self, status):
        print(status)


# make a counter with blacklist words and empty word with some big value - we'll use it later to filter counter
stop_words.append('')
blacklist_counter = Counter(dict(zip(stop_words, [1000000] * len(stop_words))))

# complie a regex for split operations (punctuation list, plus space and new line)
punctuation = [str(i) for i in string.punctuation]
split_regex = re.compile("[ \n" + re.escape("".join(punctuation)) + ']')

def map_nouns(col):
    return [word[0] for word in TextBlob(col).tags if word[1] == u'NNP']

# generate "trending"
def generate_trending():

    try:
        # select last 10k tweets
        df = pd.read_sql("SELECT * FROM sentiment ORDER BY id DESC, unix DESC LIMIT 10000", conn)
        df['nouns'] = list(map(map_nouns,df['tweet']))

        # make tokens
        tokens = split_regex.split(' '.join(list(itertools.chain.from_iterable(df['nouns'].values.tolist()))).lower())
        # clean and get top 10 on all twitter !!
        trending = (Counter(tokens) - blacklist_counter).most_common(10)

        # get sentiments
        trending_with_sentiment = {}
        for term, count in trending:
            df = pd.read_sql("SELECT sentiment.* FROM  sentiment_fts fts LEFT JOIN sentiment ON fts.rowid = sentiment.id WHERE fts.sentiment_fts MATCH ? ORDER BY fts.rowid DESC LIMIT 1000", conn, params=(term,))
            trending_with_sentiment[term] = [df['sentiment'].mean(), count]
            print(trending_with_sentiment)

        # save in a database
        with lock:
            c.execute('BEGIN TRANSACTION')
            try:
                c.execute("REPLACE INTO misc (key, value) VALUES ('trending', ?)", (pickle.dumps(trending_with_sentiment),))
            except:
                pass
            c.execute('COMMIT')


    except Exception as e:
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')
    finally:
        Timer(0.5, generate_trending).start()

Timer(0.5, generate_trending).start()

while True:

    try:
        auth = OAuthHandler(ckey, csecret)
        auth.set_access_token(atoken, asecret)
        twitterStream = Stream(auth, listener(lock))
        twitterStream.filter(track=["tesla","google","apple","elon","musk","iphone","stock","stocks","think"],languages=["en"])
    except Exception as e:
        print(str(e))
        time.sleep(5)
 