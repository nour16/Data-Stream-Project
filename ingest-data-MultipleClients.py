import tweepy
import json
from kafka import KafkaProducer
import time

# Set up multiple Twitter API clients
clients = [
    tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAADDwyAEAAAAAX20TtR0axK7Vol%2F2Gskqi6ujcls%3DIaLRaFDBqd7rSrDVrhPmLy2ZZig5yIilUkqnffdGfKdIZJWx9F'),
    tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAPfvyAEAAAAAu4K5Z6X7RCr6qyALhf1jTwtsbvE%3Djdy6iBUUKiunUCUMVL7uON7sBcP1vq6J0L4FDtkNUpCVB3UdKO'),
    tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAANEQyQEAAAAAtlRYwJQw9sMoaSLctiK%2B3gzB5Zw%3DqROxzcN2pRiSzyOvgjF20o1qdfrJxiVmMZjmwZWpdCbmi8tRnt'),
    tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAADkSyQEAAAAAuDHwMScuTtD9IcfmbH02YaQCF6M%3DN4qZrWtLxRBmwK29zovNvsyQd6lUMwHVVA5UOglTKp5MXBBC8h'),
    tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAIAVyQEAAAAAHw3%2FQjaRbriqNSt1XPzZlJJVVzk%3DESM5CVURN41t9bxWhflHW8nInA0BNqpWMKFVGE84U4SGlnqGI1'),
    tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAM0VyQEAAAAA49v%2FIGSBHvae53JegYIF0cjcEkQ%3DmEPLhrzosK05LDWUrddXlrq6MpIkumCOZCuL3d4OtktUjpvxCm'),
    tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAOcVyQEAAAAA2530edog2a0oqEjkEqtqsCgkJYg%3DUJI2eIFDiQoVH3nW1L7SdgjsAGf2LOrrmP8mwXSe7yXq1fXjkM')
]

# Define the search query
query = 'France -is:retweet lang:en'

# Configure the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)

topic_name = 'raw-tweets'

# Function to handle pagination for a single client
def fetch_tweets(client, query, limit):
    for tweet in tweepy.Paginator(
        client.search_recent_tweets,
        query=query,
        tweet_fields=['created_at', 'lang'],
        max_results=10
    ).flatten(limit=limit):
        if 'lang' in tweet and 'text' in tweet:
            data = json.dumps({
                "language": tweet['lang'],
                "text": tweet['text'],
                "created_at": str(tweet['created_at'])
            })
            print(json.dumps(json.loads(data), indent=2))
            producer.send(topic_name, data.encode('utf-8'))

try:
    for idx, client in enumerate(clients):
        try:
            print(f"Starting with client {idx + 1}...")
            fetch_tweets(client, query, limit=20)
            print(f"Client {idx + 1} is done.")
        except tweepy.TooManyRequests as e:
            print("Rate limit reached. Switching to next client or waiting.")
            reset_time = int(e.response.headers.get("x-rate-limit-reset", time.time() + 60))
            wait_time = max(reset_time - time.time(), 0)
            print(f"Sleeping for {wait_time} seconds.")
            time.sleep(wait_time)
        except Exception as e:
            print(f"Error with client {idx + 1}: {e}")
finally:
    # Close Kafka producer 
    producer.close()

