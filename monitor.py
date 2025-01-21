import time
from kafka import KafkaConsumer, TopicPartition
from collections import Counter

# Kafka configuration
bootstrap_servers = 'localhost:9092'
input_topic = 'raw_tweets'

# Initialize counters
processed_tweets = Counter()
start_time = time.time()

def monitor_raw_tweets():
    """Monitor Kafka 'raw-tweets' topic and display metrics."""
    global processed_tweets, start_time

    try:
        print("Starting monitoring of 'raw-tweets' topic...")

        # Input topic
        input_consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        tp_input = TopicPartition(input_topic, 0)
        input_consumer.assign([tp_input])
        input_consumer.seek_to_end(tp_input)
        last_offset_input = input_consumer.position(tp_input)
        input_consumer.seek_to_beginning(tp_input)

        for msg in input_consumer:
            processed_tweets['raw-tweets'] += 1
            if msg.offset == last_offset_input - 1:
                break

        # Calculate metrics
        elapsed_time = time.time() - start_time
        throughput = sum(processed_tweets.values()) / elapsed_time if elapsed_time > 0 else 0

        # Print metrics
        print(f"Total Tweets Processed: {processed_tweets['raw-tweets']}")
        print(f"Throughput: {throughput:.2f} tweets/second")

    except KeyboardInterrupt:
        print("Monitoring stopped by user.")
    except Exception as e:
        print(f"Error in monitoring: {e}")
    finally:
        # Close consumer on exit
        input_consumer.close()

if __name__ == "__main__":
    monitor_raw_tweets()
