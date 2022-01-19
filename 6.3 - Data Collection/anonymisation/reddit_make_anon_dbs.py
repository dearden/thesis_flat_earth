import sqlite3
import json
from tqdm import tqdm
import os
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict


sql_create_comments = """ 
    CREATE TABLE IF NOT EXISTS comments (
        uid int PRIMARY KEY,
        submission_id int,
        parent_id int,
        poster_id int,
        poster_name text,
        poster_flair_text text,
        body text NOT NULL,
        time integer NOT NULL,
        score integer NOT NULL,
        FOREIGN KEY (submission_id) REFERENCES submissions (uid),
        FOREIGN KEY (parent_id) REFERENCES comments (uid)
    );"""

sql_create_submissions = """ 
CREATE TABLE IF NOT EXISTS submissions (
    uid int PRIMARY KEY,
    poster_name text,
    poster_id int,
    poster_flair_text text,
    title text NOT NULL,
    body text,
    time integer,
    score integer,
    upvote_ratio real,
    url text,
    FOREIGN KEY (poster_id) REFERENCES users (uid)
);"""


def read_db(db_fp, query):
    conn = sqlite3.connect(db_fp)
    comments = pd.read_sql_query(query, conn)
    conn.close()
    return comments


def update_one_sub(db_fp, new_fp, com_map, sub_map, usr_map, nam_map):
    ##########################################################
    # Load in the DB etc
    ##########################################################
    conn = sqlite3.connect(db_fp)

    curr_coms = read_db(db_fp, "SELECT * FROM comments")
    curr_subs = read_db(db_fp, "SELECT * FROM submissions")

    conn.close()

    startTime = datetime.now()

    ##########################################################
    # Update comments table
    ##########################################################
    # UID
    curr_coms["uid"] = curr_coms["uid"].apply(lambda x: com_map[x])

    # Parent IDs
    curr_coms["parent_id"] = curr_coms["parent_id"].apply(lambda x: com_map[x] if x is not None else x)

    # Submissions
    curr_coms["submission_id"] = curr_coms["submission_id"].apply(lambda x: sub_map[x])

    # Users
    curr_coms["poster_id"] = curr_coms["poster_id"].apply(lambda x: usr_map[x] if x is not None else x)

    # Usernames
    # curr_coms["poster_name"] = [None] * len(curr_coms)
    curr_coms["poster_name"] = curr_coms["poster_name"].apply(lambda x: nam_map[x] if x is not None else x)

    print("updated comments table")

    ##########################################################
    # Update submissions table
    ##########################################################
    
    # UID
    curr_subs["uid"] = curr_subs["uid"].apply(lambda x: sub_map[x])

    # Users
    curr_subs["poster_id"] = curr_subs["poster_id"].apply(lambda x: usr_map[x] if x is not None else x)

    # Usernames
    # curr_subs["poster_name"] = [None] * len(curr_subs)
    curr_subs["poster_name"] = curr_subs["poster_name"].apply(lambda x: nam_map[x] if x is not None else x)

    # URL
    curr_subs["url"] = [None] * len(curr_subs)

    print("updated submissions table")

    ##########################################################
    # Build the new database
    ##########################################################

    conn = sqlite3.connect(new_fp)
    curs = conn.cursor()

    curs.execute(sql_create_comments)
    curs.execute(sql_create_submissions)

    # Fill the comments table
    for i, comment in curr_coms.iterrows():
        command = '''INSERT INTO comments(uid , submission_id, parent_id, poster_id, poster_name, poster_flair_text, body, time, score)
                                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);'''
        # curr_entry = list(comment.values)
        curr_entry = (comment["uid"],
                        int(comment["submission_id"]) if not np.isnan(comment["submission_id"]) else None, 
                        int(comment["parent_id"]) if not np.isnan(comment["parent_id"]) else None, 
                        int(comment["poster_id"]) if not np.isnan(comment["poster_id"]) else None, 
                        int(comment["poster_name"]) if not np.isnan(comment["poster_name"]) else None,
                        comment["poster_flair_text"], comment["body"], comment["time"], comment["score"])
        curs.execute(command, curr_entry)

    print("added comments to database")

    # Fill the submissions table
    for i, submission in curr_subs.iterrows():
        command = '''INSERT INTO submissions(uid ,poster_name, poster_id, poster_flair_text, title, body, time, score, upvote_ratio, url)
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
        # curr_entry = list(submission.values)
        curr_entry = (submission["uid"], 
                        int(submission["poster_name"]) if not np.isnan(submission["poster_name"]) else None,
                        int(submission["poster_id"]) if not np.isnan(submission["poster_id"]) else None,
                        submission["poster_flair_text"], submission["title"], submission["body"],
                        submission["time"], submission["score"], submission["upvote_ratio"], submission["url"])
        curs.execute(command, curr_entry)

    print("added submissions to database")
    print("Time taken:", datetime.now() - startTime)

    conn.commit()
    conn.close()



if __name__ == "__main__":
    red_dir = input("Enter directory containing Reddit databases:\n")
    map_dir = input("Enter directory to put maps:\n")

    ##########################################################
    # Load in the maps
    ##########################################################

    # Posts
    with open(os.path.join(map_dir, "reddit_com_map.json")) as f:
        com_map = json.load(f)
        com_map = defaultdict(lambda: None, com_map)

    # Topics
    with open(os.path.join(map_dir, "reddit_sub_map.json")) as f:
        sub_map = json.load(f)
        sub_map = defaultdict(lambda: None, sub_map)

    # Boards
    with open(os.path.join(map_dir, "reddit_usr_map.json")) as f:
        usr_map = json.load(f)
        usr_map = defaultdict(lambda: None, usr_map)

    # Names
    with open(os.path.join(map_dir, "reddit_nam_map.json")) as f:
        nam_map = json.load(f)
        nam_map = defaultdict(lambda: None, nam_map)

    for subdir, dirs, files in os.walk(red_dir):
        for filename in files:
            if filename.endswith(".db") and "ANON" not in subdir:
                print("---------------------------------------------")
                print(filename)
                print("---------------------------------------------")

                curr_fp = os.path.join(subdir, filename)
                new_fp = os.path.join(subdir, "ANON", filename)
                update_one_sub(curr_fp, new_fp, com_map, sub_map, usr_map, nam_map)