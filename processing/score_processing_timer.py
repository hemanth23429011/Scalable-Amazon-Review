import os
import csv
import json
import time
from multiprocessing import Pool, Process, cpu_count, Manager
from collections import Counter

REVIEW_SOURCE = "dataset/Reviews.csv"
STATS_OUTPUT = "benchmarks/load_mode_comparison.csv"
os.makedirs("benchmarks", exist_ok=True)

def tag_emotion(score_str):
    try:
        numeric_score = float(score_str)
        if numeric_score <= 2.0:
            return "negative"
        elif numeric_score == 3.0:
            return "neutral"
        else:
            return "positive"
    except:
        return "unknown"
def walk_one_by_one(score_group):
    begin = time.time()
    tag_list = [tag_emotion(score) for score in score_group]
    finish = time.time()
    return Counter(tag_list), finish - begin

def sprint_with_cores(score_group):
    begin = time.time()
    with Pool(cpu_count()) as helper_pool:
        tag_list = helper_pool.map(tag_emotion, score_group)
    finish = time.time()
    return Counter(tag_list), finish - begin

def hybrid_mapper(chunk_scores, result_box, box_key):
    with Pool(cpu_count()) as helper_pool:
        tag_list = helper_pool.map(tag_emotion, chunk_scores)
    result_box[box_key] = dict(Counter(tag_list))

def run_combo_logic(score_group):
    task_center = Manager()
    memory_box = task_center.dict()

    split_at = len(score_group) // 2
    first_half = score_group[:split_at]
    second_half = score_group[split_at:]

    branch1 = Process(target=hybrid_mapper, args=(first_half, memory_box, "first"))
    branch2 = Process(target=hybrid_mapper, args=(second_half, memory_box, "second"))

    begin = time.time()
    branch1.start()
    branch2.start()
    branch1.join()
    branch2.join()
    finish = time.time()

    final_bag = Counter()
    for record in memory_box.values():
        final_bag.update(record)

    return final_bag, finish - begin

def grab_all_scores():
    review_scores = []
    with open(REVIEW_SOURCE, encoding='utf-8') as doc:
        sheet = csv.DictReader(doc)
        for row in sheet:
            if 'Score' in row:
                review_scores.append(row['Score'])
    return review_scores
def execute_benchmark_series():
    score_data = grab_all_scores()
    test_sizes = [5000, 10000, 25000, 50000, len(score_data)]
    techniques = ['Sequential', 'Parallel', 'Hybrid']
    summary_log = []

    for chunk_size in test_sizes:
        small_batch = score_data[:chunk_size]
        print(f"\n🔹 Processing {chunk_size} records")

        for method in techniques:
            if method == 'Sequential':
                label_counts, time_taken = walk_one_by_one(small_batch)
            elif method == 'Parallel':
                label_counts, time_taken = sprint_with_cores(small_batch)
            else:
                label_counts, time_taken = run_combo_logic(small_batch)

            records_per_sec = chunk_size / time_taken
            time_per_record = time_taken / chunk_size
            print(f"\n--- {method} Mode ---")
            print("Sentiment counts:", dict(label_counts))
            print(f"Time Taken: {round(time_taken, 4)} sec")
            print(f"Latency (per record): {round(time_per_record, 6)} sec")

            summary_log.append({
                "Load": chunk_size,
                "Mode": method,
                "TimeSec": round(time_taken, 4),
                "Throughput": round(records_per_sec, 2),
                "Latency": round(time_per_record, 6)
            })

    return summary_log

def stash_summary_csv(data_rows, target_file):
    with open(target_file, "w", newline='') as csv_doc:
        writer = csv.DictWriter(csv_doc, fieldnames=["Load", "Mode", "TimeSec", "Throughput", "Latency"])
        writer.writeheader()
        for row in data_rows:
            writer.writerow(row)

if __name__ == "__main__":
    final_report = execute_benchmark_series()
    stash_summary_csv(final_report, STATS_OUTPUT)
    print("Benchmarking done! Data stored in:", STATS_OUTPUT)
