import sqlite3
import json
import os
import sys

sys.path.insert(1, "C:/Users/Eddie/Documents/language-change-methods")
from utility_functions import basic_preprocessing


def get_submission_chunks(db_fp, chunk_size):
    conn = sqlite3.connect(db_fp)
    cur = conn.cursor()
    i = 0
    curr_chunk = dict()
    for submission in cur.execute("SELECT uid, title, body FROM submissions"):
        uid = submission[0]
        title = submission[1]
        body = submission[2]

        if title.strip() == "[removed]" or  title.strip() == "[deleted]":
            pass
        else:
            text = title
        
        if body is not None:
            if body.strip() == "[removed]" or  body.strip() == "[deleted]":
                pass
            else:
                text = text + "\n" + body
        
        text = basic_preprocessing(text)

        if len(text) > 0:
            curr_chunk[uid] = text

            i += 1
            if i == chunk_size:
                yield curr_chunk
                i = 0
                curr_chunk = dict()
                continue

    yield curr_chunk



if __name__ == "__main__":
    directory = input("Insert subreddit master directory:\n")
    OUT_DIR = input("Enter directory to dump chunks into:\n")

    for subdir, dirs, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(subdir, filename)

            if filename.endswith(".db"):
                sub_name = filename[:-3]

                chunk_num = 1
                for chunk in get_submission_chunks(filepath, 50000):
                    with open(os.path.join(OUT_DIR, f"{sub_name}-submissions-chunk-{chunk_num}.json"), "w", encoding="utf-8") as out_file:
                        json.dump(chunk, out_file)
                    chunk_num += 1
