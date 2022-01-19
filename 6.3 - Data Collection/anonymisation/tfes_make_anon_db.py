import sqlite3
import json
from tqdm import tqdm
import os
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict


sql_create_posts = """ 
    CREATE TABLE IF NOT EXISTS posts (
        uid integer PRIMARY KEY,
        topic integer NOT NULL,
        user integer,
        poster_name text,
        body text NOT NULL,
        time text NOT NULL,
        FOREIGN KEY (topic) REFERENCES topics (uid)
        FOREIGN KEY (user) REFERENCES users (uid)
    );"""

sql_create_users = """ 
CREATE TABLE IF NOT EXISTS users (
    uid integer PRIMARY KEY,
    name text,
    position text,
    personal_text text,
    custom_title text,
    signature text,
    location text,
    age integer,
    gender text,
    num_posts integer,
    num_posts_per_day integer,
    date_registered text,
    days_since_last_active text,
    language text
);"""

sql_create_topics = """ 
CREATE TABLE IF NOT EXISTS topics (
    uid integer PRIMARY KEY,
    board integer,
    name text NOT NULL,
    user integer,
    FOREIGN KEY (board) REFERENCES boards (uid)
    FOREIGN KEY (user) REFERENCES users (uid)
);"""

sql_create_boards = """ 
CREATE TABLE IF NOT EXISTS boards (
    uid integer PRIMARY KEY,
    name text NOT NULL
);"""

sql_create_quotes = """ 
CREATE TABLE IF NOT EXISTS quotes (
    uid integer PRIMARY KEY,
    post integer NOT NULL,
    body text NOT NULL,
    quote_origin integer,
    FOREIGN KEY (post) REFERENCES posts (uid)
    FOREIGN KEY (quote_origin) REFERENCES posts (uid)
);"""


def read_db(db_fp, query):
    conn = sqlite3.connect(db_fp)
    comments = pd.read_sql_query(query, conn)
    conn.close()
    return comments


def update_one_sub(db_fp, new_fp, pst_map, top_map, brd_map, usr_map):
    ##########################################################
    # Load in the DB etc
    ##########################################################
    conn = sqlite3.connect(db_fp)

    curr_psts = read_db(db_fp, "SELECT * FROM posts")
    curr_tops = read_db(db_fp, "SELECT * FROM topics")
    curr_brds = read_db(db_fp, "SELECT * FROM boards")
    curr_usrs = read_db(db_fp, "SELECT * FROM users")
    curr_quos = read_db(db_fp, "SELECT * FROM quotes")

    conn.close()

    startTime = datetime.now()

    ##########################################################
    # Update posts table
    ##########################################################
    # UID
    curr_psts["uid"] = curr_psts["uid"].apply(lambda x: pst_map[x])
    # Topic
    curr_psts["topic"] = curr_psts["topic"].apply(lambda x: top_map[x])
    # User
    curr_psts["user"] = curr_psts["user"].apply(lambda x: usr_map[x])# if not np.isnan(x) else np.nan)
    # Poster name
    curr_psts["poster_name"] = [None] * len(curr_psts)

    print("updated posts table")

    ##########################################################
    # Update topic table
    ##########################################################
    # UID
    curr_tops["uid"] = curr_tops["uid"].apply(lambda x: top_map[x])
    # Topic
    curr_tops["board"] = curr_tops["board"].apply(lambda x: brd_map[x])
    # User
    curr_tops["user"] = curr_tops["user"].apply(lambda x: usr_map[x])

    print("updated topic table")

    ##########################################################
    # Update Board table
    ##########################################################
    # UID
    curr_brds["uid"] = curr_brds["uid"].apply(lambda x: brd_map[x])

    print("updated board table")

    ##########################################################
    # Update User table
    ##########################################################
    # UID
    curr_usrs["uid"] = curr_usrs["uid"].apply(lambda x: usr_map[x])
    # Poster name
    curr_usrs["name"] = [None] * len(curr_usrs)

    print("updated users table")

    ##########################################################
    # Update Quote table
    ##########################################################
    # Post
    curr_quos["post"] = curr_quos["post"].apply(lambda x: pst_map[x])
    # Origin
    curr_quos["quote_origin"] = curr_quos["quote_origin"].apply(lambda x: pst_map[x] if x is not None else None)

    print("updated quotes table")

    ##########################################################
    # Build the new database
    ##########################################################

    conn = sqlite3.connect(new_fp)
    curs = conn.cursor()

    curs.execute(sql_create_boards)
    curs.execute(sql_create_users)
    curs.execute(sql_create_topics)
    curs.execute(sql_create_posts)
    curs.execute(sql_create_quotes)

    # Fill the posts table
    print("POSTS")
    for i, row in tqdm(curr_psts.iterrows()):
        command = '''INSERT INTO posts(uid, topic, user, poster_name, body, time)
                            VALUES(?, ?, ?, ?, ?, ?);'''
        curr_entry = list(row.values)
        curs.execute(command, curr_entry)

    print("added comments to database")

    # Fill the topics table
    print("TOPICS")
    for i, row in tqdm(curr_tops.iterrows()):
        command = '''INSERT INTO topics(uid, board, name, user)
                        VALUES(?, ?, ?, ?);'''
        curr_entry = list(row.values)
        curs.execute(command, curr_entry)

    # Fill the boards table
    print("BOARDS")
    for i, row in tqdm(curr_brds.iterrows()):
        command = '''INSERT INTO boards(uid, name)
                        VALUES(?, ?);'''
        curr_entry = list(row.values)
        curs.execute(command, curr_entry)

    # Fill the users table
    print("USERS")
    for i, row in tqdm(curr_usrs.iterrows()):
        command = '''INSERT INTO users(uid, name, position, personal_text, custom_title, signature, 
                    location, age, gender, num_posts, num_posts_per_day, date_registered, 
                    days_since_last_active, language)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
        curr_entry = list(row.values)
        curs.execute(command, curr_entry)

    # Fill the quotes table
    print("QUOTES")
    for i, row in tqdm(curr_quos.iterrows()):
        command = '''INSERT INTO quotes(uid, post, body, quote_origin)
                                VALUES(?, ?, ?, ?);'''
        curr_entry = (row["uid"], row["post"], row["body"], 
                        int(row["quote_origin"]) if not np.isnan(row["quote_origin"]) else None) 
        curs.execute(command, curr_entry)

    print("Time taken:", datetime.now() - startTime)

    conn.commit()
    conn.close()



if __name__ == "__main__":
    ##########################################################
    # Load in the maps
    ##########################################################

    # Posts
    with open("MAPS/tfes_post_map.json") as f:
        post_map = json.load(f)
        post_map = {int(i):v for i, v in post_map.items()}
        post_map = defaultdict(lambda: None, post_map)

    # Topics
    with open("MAPS/tfes_topi_map.json") as f:
        topi_map = json.load(f)
        topi_map = {int(i):v for i, v in topi_map.items()}
        topi_map = defaultdict(lambda: None, topi_map)

    # Boards
    with open("MAPS/tfes_boar_map.json") as f:
        boar_map = json.load(f)
        boar_map = {int(i):v for i, v in boar_map.items()}
        boar_map = defaultdict(lambda: None, boar_map)

    # Users
    with open("MAPS/tfes_user_map.json") as f:
        user_map = json.load(f)
        user_map = {int(i):v for i, v in user_map.items()}
        user_map = defaultdict(lambda: None, user_map)

    update_one_sub("tfes_forum_orig.db", 
                    "tfes_forum_anon.db", 
                    post_map, topi_map, boar_map, user_map)