import json
from kafka import KafkaProducer

import streaming


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

    stream = streaming.SampledStream()
    response = stream.connect_to_endpoint()

    while True:
        for tweet in stream.flush_tweets(response=response):
            print(json.dumps(json.loads(tweet), indent=4,
                  sort_keys=True, ensure_ascii=False))
            producer.send("sample-twitter", tweet)
