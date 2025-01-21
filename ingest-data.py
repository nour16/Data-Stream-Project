import tweepy
import json
from kafka import KafkaProducer

# Set up the Twitter API client
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAOcVyQEAAAAA2530edog2a0oqEjkEqtqsCgkJYg%3DUJI2eIFDiQoVH3nW1L7SdgjsAGf2LOrrmP8mwXSe7yXq1fXjkM'
client = tweepy.Client(bearer_token=bearer_token)

# Define the search query
query = 'France -is:retweet lang:en'

# Configure the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)

topic_name = 'raw-tweets' 

# Fetch, print, and send tweets to Kafka
size = 100  
try:
    for tweet in tweepy.Paginator(
        client.search_recent_tweets,
        query=query,
        tweet_fields=['created_at', 'lang'],
        max_results=size
    ).flatten(limit=500):  
        if 'lang' in tweet and 'text' in tweet:
            data = json.dumps({
                "language": tweet['lang'],
                "text": tweet['text'],
                "created_at": str(tweet['created_at'])
            })
            print(json.dumps(json.loads(data), indent=2)) 
            producer.send(topic_name, data.encode('utf-8'))

except tweepy.TooManyRequests:
    print("Rate limit reached. Please wait before retrying.")
except Exception as e:
    print(f"Error fetching tweets: {e}")
finally:
    # Close Kafka producer 
    producer.close()

