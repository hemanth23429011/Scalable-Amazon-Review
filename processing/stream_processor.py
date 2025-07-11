import boto3
import json
import time
from collections import deque, Counter
from datetime import datetime, timedelta

STREAM_TITLE = "ScalableKenesis"
AWS_REGION = "us-east-1"

live_pipe = boto3.client("kinesis", region_name=AWS_REGION)
def judge_sentiment(score_value):
    try:
        rating = float(score_value)
        if rating <= 2.0:
            return "negative"
        elif rating == 3.0:
            return "neutral"
        elif rating >= 4.0:
            return "positive"
        else:
            return "unknown"
    except:
        return "unknown"
def stream_and_classify_with_window(live_limit=50, window_seconds=60):
    try:
        shard_details = live_pipe.describe_stream(StreamName=STREAM_TITLE)
        shard_id = shard_details['StreamDescription']['Shards'][0]['ShardId']

        iterator = live_pipe.get_shard_iterator(
            StreamName=STREAM_TITLE,
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )['ShardIterator']

        processed = 0
        sliding_window = deque()

        print(f"Starting stream with {window_seconds}s sliding window...\n")

        while processed < live_limit:
            records_response = live_pipe.get_records(ShardIterator=iterator, Limit=5)
            iterator = records_response['NextShardIterator']
            reviews = records_response.get('Records', [])

            for note in reviews:
                try:
                    review_obj = json.loads(note['Data'])
                    feeling = judge_sentiment(review_obj.get("score", 3))
                    now = datetime.utcnow()
                    sliding_window.append((now, feeling))
                    processed += 1

                    print(f"\n{review_obj['review_text'][:80]}...")
                    print(f"Score: {review_obj['score']} {feeling}")
                    while sliding_window and (now - sliding_window[0][0]).total_seconds() > window_seconds:
                        sliding_window.popleft()
                    mood_counts = Counter(label for _, label in sliding_window)
                    print(f"Last {window_seconds}s: {dict(mood_counts)}")

                except Exception as oops:
                    print(f"[WARN] Failed to process a review: {oops}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[EXIT] Stopped by user.")
    except Exception as trouble:
        print(f"[ERROR] Problem with stream: {trouble}")

if __name__ == "__main__":
    stream_and_classify_with_window(live_limit=50, window_seconds=60)



