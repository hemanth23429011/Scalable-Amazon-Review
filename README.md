# Scalable Cloud-Based Text Processing System using AWS

This project demonstrates a scalable text analysis pipeline using parallel and stream processing techniques on the AWS cloud. It processes Amazon product reviews and performs sentiment analysis using three different processing modes: Sequential, Parallel, and Hybrid.

---

## Project Features

- Real-time ingestion of review data using **AWS Kinesis**
- Batch and stream processing with **Python multiprocessing**
- Three processing modes: **Sequential**, **Parallel**, and **Hybrid**
- Benchmarking with metrics: **Execution Time, Throughput, Latency**
- Auto-scaling using **AWS EC2 Auto Scaling Groups**
- Visual performance analysis via **Matplotlib**
- Full support for AWS environment deployment

---

# How to Run

## 1. Setup EC2 Environment

### Create virtual environment and activate it
- python3 -m venv env
- source env/bin/activate

### Install required packages
- pip install -r requirements.txt

# 2. Run each python file
- python3 final_name.py
