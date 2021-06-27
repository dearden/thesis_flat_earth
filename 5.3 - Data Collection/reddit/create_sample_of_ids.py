import sqlite3
import json
import os
import sys
import pandas as pd
from datetime import datetime

sys.path.insert(1, "C:/Users/Eddie/Documents/language-change-methods")
from utility_functions import get_time_windows


convert_to_date = lambda x: datetime.strptime(x, "%Y/%m/%d %H:%M:%S")


def sample_ids(db_fp, sample_size, table="comments"):
    conn = sqlite3.connect(db_fp)
    comment_times = pd.read_sql_query(f"SELECT uid, time, substr(body, 1, 10) AS body FROM {table}", conn)
    conn.close()
    del(conn)
    comment_times.set_index("uid", inplace=True)
    comment_times["time"] = comment_times["time"].apply(convert_to_date)
    comment_times = comment_times[comment_times["time"] > datetime(2012, 1, 1)]
    comment_times = comment_times[comment_times["time"] < datetime(2021, 1, 1)]
    comment_times.sort_values("time", inplace=True)

    print(f"Read in db - Time: {datetime.now()}")

    check_body = lambda x: False if x is not None and (x.strip() == "[removed]" or  x.strip() == "[deleted]") else True
    comment_times = comment_times[comment_times['body'].apply(check_body)]

    print(f"Filtered bodies - Time: {datetime.now()}")

    curr_idxs = []

    # We will sample per time window to ensure consistency, however it may also not be true of the data
    # Alternatively, could just take a sample of 800,000 comments from the entire range.
    for date, window in get_time_windows(comment_times, 365, 365):
        print(f"Curr date {date} - Time started: {datetime.now()}")
        curr_window_sample = window.sample(int(sample_size), random_state=123).index
        curr_idxs += list(curr_window_sample)

    return curr_idxs


if __name__ == "__main__":
    DB_FP = input("Insert filepath of database:\n")
    OUT_FP = input("Enter filepath to write the sample to:\n")
    SAMPLE_SIZE = input("Enter the desired sample size:\n")
    TABLE = input("Enter the DB Table to get entries from:\n")

    start_time = datetime.now()
    print(start_time)

    sample = sample_ids(DB_FP, SAMPLE_SIZE, table=TABLE)

    with open(OUT_FP, "w", encoding="utf-8") as out_file:
        json.dump(sample, out_file)

    print(datetime.now())
    print("Time Taken: ", datetime.now() - start_time)