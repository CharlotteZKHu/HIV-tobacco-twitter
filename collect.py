import tweepy
from typing import Dict
from tweepy import User, Place, Media, Poll, Tweet, ReferencedTweet
import json
from joblib import Parallel, delayed
from dateutil import parser
import gzip
from datetime import date, datetime, timedelta
import os
from time import sleep

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: json_serial(v) for k,v in obj.items()}
    if isinstance(obj, list):
        return [json_serial(v) for v in obj]
    if isinstance(obj, ReferencedTweet):
        return None
    else:
        return obj

def get_weeks(start_time, end_time):
    week = timedelta(weeks=1)
    prev = parser.parse(start_time)
    while True:
        new_time = (prev - week)
        yield new_time.isoformat(), prev.isoformat()
        if str(new_time) < end_time:
            break
        prev = new_time

OUTPUT = "data/"

with open("keywords.json") as f:
    keys = json.load(f)


tobacco_keywords = keys['tobacco']
#bidi,bidis,cig,cigar,cigars,cigarette,cigarettes,cigarillos,cigarillo,cigars,cigs,,durrie,durries,e cig,e cigs,e-cigarette,e-cigarettes,e-cig,ecigs,ecig,ecigs,ecigarette,ecigarettes,hookah,kretek,kreteks,nicorette,nicotine,smoking,snuff,snus,tobacco,vape,vaper,vaping,iqos]

tobacco_keywords_string = ' OR '.join(tobacco_keywords)


hiv_testing = 'hiv (test OR tests OR tested  OR testing OR oraquick)'
#Analysis:Keywords_testing: true/false indicating if the tweet contains one of these words: HIV & 'test', 'tests', 'tested', 'testing', or 'oraquick'

hiv_prep = 'hiv (prep OR prophylaxis OR truvada OR descovy OR apretude)'

# ecig_keywords_string = 'e (cig OR cigs) OR e-cigarette OR e-cigarettes OR e-cig OR ecigs OR ecig OR ecigs OR ecigarette'
ecig_keywords_string = 'e (cig OR cigs) OR e-cigarette OR e-cigarettes OR e-cig OR ecigs OR ecig'
vape_keywords_string = 'vape OR vaper OR vaping'
# vape_keywords_string = 'vape OR vaping'

query_dict = {
    #"hiv_testing": hiv_testing,
    #"hiv_prep": hiv_prep,
    # "tobacco": tobacco_keywords_string,
    "ecig": ecig_keywords_string,
    "vape": vape_keywords_string
}

bearer_token = "AAAAAAAAAAAAAAAAAAAAAHSxlgEAAAAALMoJifag4Mf7%2Fxlp2qXewPwe4xI%3D7p4xP2cMHsI6BqCoStfTBqfCyajH913GtD5bBQuWWwDcpWHMtR"


# Constants that are used to define data that we want to retrieve from the Twitter API.
expansions = [
    # these we need for sure
    "geo.place_id",
    "author_id",
    # these might be optional
    "attachments.poll_ids",
    "attachments.media_keys",
    "edit_history_tweet_ids",
    "entities.mentions.username",
    "in_reply_to_user_id",
    "referenced_tweets.id",
    "referenced_tweets.id.author_id",
]

tweet_fields = [
    "id",
    "text",
    "edit_history_tweet_ids",
    "attachments",
    "author_id",
    "context_annotations",
    "conversation_id",
    "created_at",
    "edit_controls",
    "entities",
    "in_reply_to_user_id",
    "lang",
    #non_public_metrics,
    #"organic_metrics",
    "possibly_sensitive",
    #"promoted_metrics",
    "public_metrics",
    "referenced_tweets",
    "reply_settings",
    "source",
    "withheld",
]

user_fields = [
    "id",
    "name",
    "username",
    "created_at",
    "description",
    "entities",
    "location",
    "pinned_tweet_id",
    "profile_image_url",
    "protected",
    "public_metrics",
    "url",
    "verified",
    "withheld",
]

place_fields = [
    "full_name",
    "id",
    "contained_within",
    "country",
    "country_code",
    "geo",
    "name",
    "place_type",
]

media_fields = [
    "media_key",
    "type",
    "url",
    "duration_ms",
    "height",
    #non_public_metrics,
    #organic_metrics,
    "preview_image_url",
    #"promoted_metrics",
    "public_metrics",
    "width",
    "alt_text",
    "variants",
]

poll_fields = ["id", "options", "duration_minutes", "end_datetime", "voting_status"]

# change it

client = tweepy.Client(bearer_token=bearer_token)


def get_data_objects(response):
    tweets = []
    # if response.data contains 10 tweets, then we should expect references user object at response.includes.users
    # I make this map because I am not sure if tweets in response.data list are in the sam order as authors in response.includes["users"] - authors.  However, it is plausible...
    tweets_users: Dict[str, tweepy.User] = {user.id: user for user in response.includes.get("users", [])}
    tweet_places: Dict[str, tweepy.Place] = {place.id: place for place in response.includes.get("places", [])}

    tweet: tweepy.Tweet
    if response.data is None:
        print(f"no tweets")
        return [], None, 0
    for tweet in response.data:
        output_tweet = {}
        tweet_user: tweepy.User = tweets_users.get(tweet.author_id)
        output_tweet['author'] = {k:json_serial(v) for k,v in tweet_user.items()}
        output_tweet['tweet'] = {k:json_serial(v) for k,v in tweet.items()}
        # get the data of Tweet's place if exists
        if tweet.geo:
            tweet_place: tweepy.Place = tweet_places.get(tweet.geo.get("place_id"))
            if tweet_place is None:
                output_tweet['place'] = {k:json_serial(v) for k,v in tweet.geo.items()}
            else:
                output_tweet['place'] = {k:json_serial(v) for k,v in tweet_place.items()}
        tweets.append(output_tweet)
    next_token = None
    if 'next_token' in response.meta:
        next_token = response.meta['next_token']
    return tweets, next_token, response.meta['result_count']

def save_tweets(tweets, start_time, query_name):
    sdate = parser.parse(start_time)
    path = os.path.join(OUTPUT, str(sdate.year), str(sdate.month))
    if not os.path.exists(path):
        os.makedirs(path)
    with gzip.open(os.path.join(path, f"{start_time}_{query_name}.json.gz"), "wb") as f:
        for tweet in tweets:
            f.write(json.dumps(tweet).encode('utf-8'))

global_count = 0
def update_count(new_count):
    global global_count
    global_count += new_count
    print(f"Number of tweets: {global_count}\r", end="")


def search_wrapper(query, next_token, start_time, end_time):
    response = None
    count = 0
    while response is None:
        try:
            if next_token:
                response = client.search_all_tweets(
                query=query,
                start_time=start_time,
                end_time=end_time,
                max_results=100, # This needs to be increased
                ### IMPORTANT ###
                # https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
                # If you use the expansions, the references objects will be available in the response.includes
                # expansions=referenced_tweets.id -> response.includes.users -> list of tweepy.User
                # expansions=geo.place_id -> response.includes.places -> list of tweepy.Place
                # expansions=attachments.media_keys -> response.includes.media -> list of tweepy.Media
                #    expansions=attachments.poll_ids -> response.includes.polls -> list of tweepy.Pool
                # expansions=referenced_tweets.id -> includes.tweets -> list of tweepy.Tweet
                expansions=expansions,
                tweet_fields=tweet_fields,
                user_fields=user_fields,
                place_fields=place_fields,
                media_fields=media_fields,
                poll_fields=poll_fields,
                next_token=next_token
                )
            else:
                response = client.search_all_tweets(
                query=query,
                start_time=start_time,
                end_time=end_time,
                max_results=100, # This needs to be increased
                ### IMPORTANT ###
                # https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
                # If you use the expansions, the references objects will be available in the response.includes
                # expansions=referenced_tweets.id -> response.includes.users -> list of tweepy.User
                # expansions=geo.place_id -> response.includes.places -> list of tweepy.Place
                # expansions=attachments.media_keys -> response.includes.media -> list of tweepy.Media
                # expansions=attachments.poll_ids -> response.includes.polls -> list of tweepy.Pool
                # expansions=referenced_tweets.id -> includes.tweets -> list of tweepy.Tweet
                expansions=expansions,
                tweet_fields=tweet_fields,
                user_fields=user_fields,
                place_fields=place_fields,
                media_fields=media_fields,
                poll_fields=poll_fields,
                )
        except Exception as e:
            count += 1
            if str(e) == "429 Too Many Requests product cap":
                print("reached monthly cap")
                break
            print(f"Error {e} : waiting to retry {count}\r", end="")
            sleep(32)
    if response.data:
        print(f"getting {len(response.data)} tweets\r", end="")
    return response

def make_query(start_time, end_time, next_token, query_string):
    query_name = query_string[0]
    query_string = query_string[1]
    query = '{} -is:retweet lang:en'.format(query_string)
    sleep(1)
    response = search_wrapper(query, None, start_time, end_time)
    tweets, next_token, count = get_data_objects(response)
    while next_token is not None:
        sleep(1)
        response = search_wrapper(query, next_token, start_time, end_time)
        new_tweets, next_token, new_count = get_data_objects(response)
        count += new_count
        tweets += new_tweets
        if count > 2500000:
            print("reached 2.5M this week, skipping to next week")
            break
    save_tweets(tweets, start_time, query_name)
    update_count(count)
    
    

if __name__ == "__main__":

    # queries = [hiv_testing, hiv_prep, tobacco_keywords_string]
    queries = [vape_keywords_string, ecig_keywords_string]
    # was at 145k
    # start_time = '2023-02-11T00:00:00Z'
    # end_time = '2021-02-01T00:00:00z'
    start_time = '2023-06-03T00:00:00Z'
    end_time = '2010-01-01T00:00:00z'


    next_token = None

    Parallel(n_jobs=1, require='sharedmem')(delayed(make_query)(start, end, None, q)  for start, end in get_weeks(start_time, end_time) for q in query_dict.items())
