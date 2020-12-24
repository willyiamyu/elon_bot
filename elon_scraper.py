
import tweepy
import pandas as pd
import time
from pathlib import Path


def authenticate(consumer_key, consumer_secret, access_key, access_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True) 
    return api

def initialize(filename):
    if Path(filename).exists():
        tweet_df = pd.read_csv(filename)
        since_id = tweet_df['id'][0]
    else:
        most_recent = api.user_timeline(screen_name='ElonMusk', count=1)
        tweet_list = [tweet for tweet in most_recent]
        max_id = tweet_list[0].id
        tweet_df = pd.DataFrame(columns = ['username', 'id', 'text', 'createdtime', 
        'num_of_retweets', 'hashtags'])

    try: since_id
    except NameError: since_id = None 
        
    try: max_id
    except NameError: max_id = None 

    return(tweet_df, since_id, max_id)

def unroll_tweets(tweet_list):
    users, tweet_ids, texts, createdtimes, num_of_retweets, hashtag_list = [], [], [], [], [], [] 
    unrolled = pd.DataFrame(columns = ['username', 'id', 'text', 'createdtime', 
        'num_of_retweets', 'hashtags'])
    for tweet in tweet_list:
        # if tweet.in_reply_to_screen_name is None or tweet.in_reply_to_screen_name == 'elonmusk':
        user = tweet.user.screen_name
        tweet_id = tweet.id
        text = tweet.text
        createdtime = tweet.created_at
        num_of_retweet = tweet.retweet_count
        hashtags = tweet.entities['hashtags']
        users.append(user)
        tweet_ids.append(tweet_id)
        texts.append(text)
        createdtimes.append(createdtime)
        num_of_retweets.append(num_of_retweet)
        hashtag_list.append(hashtags)

    unrolled = pd.DataFrame({'username': users, 'id': tweet_ids, 'text': texts,
        'createdtime': createdtimes, 'num_of_retweets': num_of_retweets, 'hashtags': hashtag_list})

    return unrolled

def retrieve_tweets(tweet_df, tweet_id, since_id, max_id, api):
    
    if since_id is not None:
        tweets = api.user_timeline(screen_name=tweet_id, since_id=since_id, include_rts= False, count=300)
        tweet_list_since = [tweet for tweet in tweets]
        since_df = unroll_tweets(tweet_list_since)
        tweet_df = pd.concat([since_df, tweet_df], ignore_index=True)
        
    if max_id is not None:
        for i in range(600):
            print('next call')
            try:
                tweets = api.user_timeline(screen_name=tweet_id, max_id=max_id, include_rts= False, count=300)
                tweet_list_max = [tweet for tweet in tweets]

            except tweepy.error.TweepError:
                time.sleep(60)
                continue

            tweet_df = pd.concat([unroll_tweets(tweet_list_max), tweet_df], ignore_index=True)
            if not tweet_list_max:
                max_id = tweet_list_max[-1].id
                print('beep boop done')
                return tweet_df

            print('Finished one call: proceeding to sleep for 2 min')
            time.sleep(60)

    return tweet_df

if __name__ == "__main__":
    consumer_key = 'Tu86Kk5Fic2gd2uYCrzlyisVm'
    consumer_secret='wITCru0oGhxH4kuljo96sh19PMtnrbQ0Kxa6n7Wmim0L0ZN3aN'
    access_key = '332945015-XnSTOY7yEARlnmO7dIUnDrQH5UhzRQauq5L84ehv'
    access_secret = 'Ogm0pADQiJdN9mlmHaeKP7ZpK8Zb4a0aAiy6XYMw9vKyh'
    tweet_id = 'ElonMusk'
    filename = 'elon_tweets.csv'
    api = authenticate(consumer_key, consumer_secret, access_key, access_secret)
    tweet_df, since_id, max_id = initialize(filename)
    final_df = retrieve_tweets(tweet_df, tweet_id, since_id, max_id, api)
    final_df = final_df.loc[final_df.astype(str).drop_duplicates(['id']).index]
    final_df.to_csv(filename, index=False)
