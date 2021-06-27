import sqlite3
import json
import os
from datetime import datetime
from language_change_methods.utility_functions import basic_preprocessing


def get_comment_chunks(db_fp, chunk_size, idx_list=None):
    conn = sqlite3.connect(db_fp)
    cur = conn.cursor()
    i = 0
    curr_chunk = dict()
    chunk_num = 0
    for comment in cur.execute("SELECT uid, body FROM comments"):
        uid = comment[0]
        body = comment[1]

        if idx_list is not None and uid not in idx_list:
            continue
        
        if body.strip() == "[removed]" or  body.strip() == "[deleted]":
            continue

        body = basic_preprocessing(body)

        if len(body) > 0:
            curr_chunk[uid] = body

            i += 1
            if i == chunk_size:
                yield curr_chunk
                i = 0
                curr_chunk = dict()
                chunk_num += 1
                continue
        print(f"{chunk_num}.{i} - Adding comment {uid}")

    yield curr_chunk


def get_comment_chunks_direct(db_fp, chunk_size, idx_list):
    conn = sqlite3.connect(db_fp)
    cur = conn.cursor()

    for i in range(0, len(idx_list), 50000):
        curr_chunk_idx = idx_list[i:i+50000]
        idx_string = ",".join([f"'{x}'" for x in curr_chunk_idx])
        query = f"SELECT uid, body FROM comments WHERE uid IN ({idx_string});"
        curr_chunk = dict()

        for comment in cur.execute(query):
            uid = comment[0]
            body = comment[1]
            
            if body.strip() == "[removed]" or  body.strip() == "[deleted]":
                continue

            body = basic_preprocessing(body)
            if len(body) > 0:
                curr_chunk[uid] = body

        yield curr_chunk


def get_submission_chunks(db_fp, chunk_size, idx_list=None):
    conn = sqlite3.connect(db_fp)
    cur = conn.cursor()
    i = 0
    curr_chunk = dict()
    for submission in cur.execute("SELECT uid, title, body FROM submissions"):
        uid = submission[0]
        title = submission[1]
        body = submission[2]

        if idx_list is not None and uid not in idx_list:
            continue

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

        print(f"{i} - Adding comment {uid}")

    yield curr_chunk


if __name__ == "__main__":
    directory = input("Insert subreddit master directory:\n")
    OUT_DIR = input("Enter directory to dump chunks into:\n")
    TYPE = input("Enter 'comments' or 'submissions':\n")
    IDX_LIST = input("Include only specified indices in 'sample-*.json', y/n:\n")

    if TYPE == "comments" and IDX_LIST == "n":
        curr_method = get_comment_chunks
    elif TYPE == "submissions" and IDX_LIST == "n":
        curr_method = get_submission_chunks
    elif TYPE == "comments" and IDX_LIST == "y":
        curr_method = get_comment_chunks_direct
    elif TYPE == "submissions" and IDX_LIST == "y":
        curr_method = get_submission_chunks
    else:
        raise Exception("Type must be 'comments' or 'submissions'")

    for subdir, dirs, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(subdir, filename)

            if filename.endswith(".db"):
                sub_name = filename[:-3]
                print(f"Looking at {sub_name}")

                if IDX_LIST == "y":
                    with open(os.path.join(subdir, f"sample-{TYPE}.json")) as idx_file:
                        curr_idxs = json.load(idx_file)
                else:
                    curr_idxs = None

                chunk_num = 1
                for chunk in curr_method(filepath, 50000, curr_idxs):
                    print(f"Dumping chunk {chunk_num}.")
                    start = datetime.now()
                    with open(os.path.join(OUT_DIR, f"{sub_name}-{TYPE}-chunk-{chunk_num}.json"), "w", encoding="utf-8") as out_file:
                        json.dump(chunk, out_file)

                    print(f"Time Taken: {datetime.now()-start}")
                    chunk_num += 1
