import sqlite3
import json
import os
from language_change_methods.utility_functions import basic_preprocessing


def get_comment_chunks(db_fp, chunk_size):
    conn = sqlite3.connect(db_fp)
    cur = conn.cursor()
    i = 0
    curr_chunk = dict()
    for comment in cur.execute("SELECT uid, body FROM posts"):
        uid = comment[0]
        body = comment[1]

        body = basic_preprocessing(body)

        if len(body) > 0:
            curr_chunk[uid] = body
            i += 1
            if i == chunk_size:
                yield curr_chunk
                i = 0
                curr_chunk = dict()
                continue

    yield curr_chunk



if __name__ == "__main__":
    db_name = input("Enter forum name:\n")
    filepath = input("Insert db path:\n")
    OUT_DIR = input("Enter directory to dump chunks into:\n")

    chunk_num = 1
    for chunk in get_comment_chunks(filepath, 50000):
        with open(os.path.join(OUT_DIR, f"{db_name}-posts-chunk-{chunk_num}.json"), "w", encoding="utf-8") as out_file:
            json.dump(chunk, out_file)
        chunk_num += 1
