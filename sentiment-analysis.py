from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from transformers import pipeline
import json
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter

# Initialize the BERTweet sentiment analysis pipeline
pipe = pipeline(model="finiteautomata/bertweet-base-sentiment-analysis", max_length=128, truncation=True)

# Initialize Kafka consumer and producer
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Input topic to consume from
input_topic = "raw-tweets"

# Output topics for labeled tweets
labeled_topic = "labeled-tweets"
positive_topic = "positive-tweets"
negative_topic = "negative-tweets"
neutral_topic = "neutral-tweets"

print("Starting sentiment labeling...")

# Assign the consumer to the input topic partition
tp = TopicPartition(input_topic, 0)
consumer.assign([tp])

# Get the size of the topic
consumer.seek_to_end(tp)
last_offset = consumer.position(tp)
consumer.seek_to_beginning(tp)

# Lists to store results for visualization
sentiments = []
confidence_scores = []
sentiments_high_conf = []
confidence_scores_high_conf = []


# Process each message
for msg in consumer:
    try:
        # Decode the tweet
        tweet_text = msg.value.decode()

        # Predictions
        sentiment = pipe(tweet_text)[0]
        label = sentiment['label'] 
        confidence = sentiment['score'] 

        if label == "POS":
            output_topic = positive_topic
        elif label == "NEG":
            output_topic = negative_topic
        elif label == "NEU":
            output_topic = neutral_topic

        
        sentiments.append(label)
        confidence_scores.append(confidence)

        # Create the labeled message
        labeled_message = {
            "tweet": tweet_text,
            "label": label,
            "confidence": confidence
        }

        labeled_json = json.dumps(labeled_message).encode('utf-8')

        # Send the labeled message to the labeled topic
        producer.send(labeled_topic, labeled_json)

        # Send to the corresponding topics only if confidence > 0.7
        if confidence > 0.7:
            sentiments_high_conf.append(label)
            confidence_scores_high_conf.append(confidence)
            producer.send(output_topic, labeled_json)

        print(f"Tweet sent to {labeled_topic} and {output_topic if confidence > 0.7 else 'not published to specific topic'}: {labeled_message}")

        # Break the loop if the last offset is reached
        if msg.offset == last_offset - 1:
            break
    except Exception as e:
        print(f"Error processing message: {e}")

# Visualizations
# Sentiment Distribution
sentiment_counts = Counter(sentiments)
plt.bar(sentiment_counts.keys(), sentiment_counts.values())
plt.title("Sentiment Distribution")
plt.xlabel("Sentiment")
plt.ylabel("Number of Tweets")
plt.savefig("sentiment_distribution.png")
plt.close()

# Confidence Scores by Sentiment - All Labeled Tweets
df = pd.DataFrame({"sentiment": sentiments, "confidence": confidence_scores})
plt.boxplot([df[df['sentiment'] == label]['confidence'] for label in sentiment_counts.keys()],
            tick_labels=sentiment_counts.keys())
plt.title("Confidence Scores by Sentiment (All Labeled Tweets)")
plt.xlabel("Sentiment")
plt.ylabel("Confidence Score")
plt.savefig("confidence_scores_all.png")
plt.close()

# Confidence Scores by Sentiment - High Confidence Tweets
df_highconf = pd.DataFrame({"sentiment": sentiments_high_conf, "confidence": confidence_scores_high_conf})
if not df_highconf.empty:
    sentiment_counts_high = Counter(sentiments_high_conf)
    plt.boxplot([df_highconf[df_highconf['sentiment'] == label]['confidence'] for label in sentiment_counts_high.keys()],
                tick_labels=sentiment_counts_high.keys())
    plt.title("Confidence Scores by Sentiment (High Confidence Tweets)")
    plt.xlabel("Sentiment")
    plt.ylabel("Confidence Score")
    plt.savefig("confidence_scores_high.png")
    plt.close()
