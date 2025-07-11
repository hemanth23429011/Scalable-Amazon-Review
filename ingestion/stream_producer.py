import csv
import json
import time
import uuid
from datetime import datetime
import boto3

STREAM_TITLE = "amazon-review-stream"
AWS_REGION = "us-east-1"

data_pipe = boto3.client("kinesis", region_name=AWS_REGION)
def convert_unix_to_date(timestamp_value):
    try:
        return datetime.utcfromtimestamp(int(timestamp_value)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return "unknown-time"
def throw_into_stream(review_bundle):
    try:
        data_pipe.put_record(
            StreamName=STREAM_TITLE,
            Data=json.dumps(review_bundle),
            PartitionKey=review_bundle["review_id"]
        )
        print(f"[STREAMED] Review dropped: {review_bundle['review_id']}")
    except Exception as oops:
        print(f"[FAIL] Couldn’t stream review: {oops}")

def pour_reviews_from_csv(file_path, total_to_send=50):
    try:
        with open(file_path, mode="r", encoding="utf-8") as opened_file:
            all_reviews = csv.DictReader(opened_file)
            poured_count = 0

            for review_line in all_reviews:
                if poured_count >= total_to_send:
                    break

                if not review_line.get("Text") or not review_line.get("Score"):
                    continue 

                review_bundle = {
                    "review_id": f"rev_{uuid.uuid4().hex[:6]}",
                    "review_text": review_line["Text"].strip(),
                    "score": float(review_line["Score"]),
                    "summary": review_line.get("Summary", "").strip(),
                    "streamed_at": convert_unix_to_date(review_line.get("Time", time.time()))
                }

                throw_into_stream(review_bundle)
                time.sleep(0.5)
                poured_count += 1

    except FileNotFoundError:
        print(f"[ERROR] File not found: {file_path}")
    except Exception as surprise:
        print(f"[ERROR] Problem during streaming: {surprise}")
if __name__ == "__main__":

    file_location = "dataset/Reviews.csv"
    pour_reviews_from_csv(file_location, total_to_send=30)
