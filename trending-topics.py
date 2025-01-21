from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
from collections import Counter
import re
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk


# Preprocessing Function for Hashtags
def extract_hashtags(tweet_text):
    # Regex for hashtags
    hashtag_regex = r"#\w+"

    # Extract hashtags
    hashtags = re.findall(hashtag_regex, tweet_text.lower())
    return [hashtag[1:] for hashtag in hashtags]  


# Kafka Consumer Configuration
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
input_topic = 'raw-tweets'

# Assign the consumer to the input topic partition
tp = TopicPartition(input_topic, 0)
consumer.assign([tp])

# Get the size of the topic
consumer.seek_to_end(tp)
last_offset = consumer.position(tp)
consumer.seek_to_beginning(tp)

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
output_topic = 'trending-hashtags'

# Initialize hashtag counter
hashtag_counter = Counter()

# Process tweets
for message in consumer:
    tweet = json.loads(message.value.decode('utf-8')).get('text', '')  
    print("Processing Tweet:", tweet)

    # Extract hashtags
    hashtags = extract_hashtags(tweet)
    print("Extracted Hashtags:", hashtags)

    # Update counters
    hashtag_counter.update(hashtags)

    # Stop processing when reaching the last offset
    if message.offset == last_offset - 1:
        break

# Display the top trending hashtags
top_hashtags = hashtag_counter.most_common(10)
print("Top Hashtags:")
print(top_hashtags)

# Send trending hashtags to the 'trending-hashtags' topic
trending_data = {"top_hashtags": top_hashtags}
producer.send(output_topic, trending_data)
print(f"Sent trending hashtags to topic '{output_topic}':", trending_data)

# Visualize with WordCloud
hashtag_wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(hashtag_counter)
plt.figure(figsize=(10, 5))
plt.imshow(hashtag_wordcloud, interpolation='bilinear')
plt.axis('off')
plt.savefig("hashtag_wordcloud.png")
