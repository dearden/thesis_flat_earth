import pickle
import sys
import os
import re
import json
	
import sqlite3
from sqlite3 import Error as SQLError
from datetime import datetime


# Some useful lambdas
get_datetime = lambda x: datetime.utcfromtimestamp(x)           # Converts unix timestamp to datetime
convert_date = lambda x: x.strftime("%Y/%m/%d %H:%M:%S")        # Converts datetime to formatted string
int_or_none = lambda x: int(x) if x is not None else None       # Returns either the int of a string or None

sql_create_comments = """ 
    CREATE TABLE IF NOT EXISTS comments (
        uid text PRIMARY KEY,
        submission_id text NOT NULL,
        parent_id text,
        poster_id text,
        poster_name text NOT NULL,
        poster_flair_text text,
        body text NOT NULL,
        time integer NOT NULL,
        score integer NOT NULL,
        FOREIGN KEY (submission_id) REFERENCES submissions (uid),
        FOREIGN KEY (parent_id) REFERENCES comments (uid)
    );"""

sql_create_users = """ 
CREATE TABLE IF NOT EXISTS users (
    uid text PRIMARY KEY,
    name text NOT NULL
);"""

sql_create_submissions = """ 
CREATE TABLE IF NOT EXISTS submissions (
    uid text PRIMARY KEY,
    poster_name text NOT NULL,
    poster_id text,
    poster_flair_text text,
    title text NOT NULL,
    body text,
    time integer,
    score integer,
    upvote_ratio real,
    url text,
    FOREIGN KEY (poster_id) REFERENCES users (uid)
);"""


def create_connection(filename):
    """ create a database connection to a database that resides
        in the memory
    """
    connection = None
    try:
        connection = sqlite3.connect(filename)
        print(sqlite3.version)
    except SQLError as e:
        print(e)
        if connection:
            connection.close()
            print("Error occurred creating connection")
            connection = None
    
    return connection


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except SQLError as e:
        print(e)


def make_submissions_table(submissions, connection):
    cursor = connection.cursor()

    for submission in submissions:
        try:
            time = convert_date(get_datetime(submission["created"]))

            command = '''INSERT INTO submissions(uid ,poster_name, poster_id, poster_flair_text, title, body, time, score, upvote_ratio, url)
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            curr_entry = (submission["id"], submission["author"], submission["author_fullname"], submission["author_flair_text"], 
                            submission["title"], submission["selftext"], time, submission["score"], 
                            submission["upvote_ratio"], submission["url"])
            cursor.execute(command, curr_entry)

            print("Added submission {}".format(submission["id"]))
        except sqlite3.IntegrityError:
            print(f"Submission {submission['id']} could not be added")


def make_comments_table(comments, connection):
    cursor = connection.cursor()

    for comment in comments:
        try:
            if re.match(r"t1\_\w+", comment["parent_id"]):
                parent_id = comment["parent_id"].split("_")[-1].strip()
            else:
                parent_id = None

            submission_id = comment["link_id"].split("_")[-1].strip()

            time = convert_date(get_datetime(comment["created_utc"]))

            if comment["author"] is None:
                print(comment)

            command = '''INSERT INTO comments(uid , submission_id, parent_id, poster_id, poster_name, poster_flair_text, body, time, score)
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            curr_entry = (comment["id"], submission_id, parent_id, 
                            comment["author_fullname"], comment["author"], comment["author_flair_text"], 
                            comment["body"], time, comment["score"])
            cursor.execute(command, curr_entry)

            print("Added comment {}".format(comment["id"]))

        except sqlite3.IntegrityError:
            print(f"Comment {comment['id']} could not be added")


def read_reddit_data(fp):
    with open(fp, 'r') as curr_file:
        for line in curr_file.readlines():
            curr = json.loads(line.strip())
            yield curr


def make_subreddit_db(submission_file, comments_file, db_path):
    connection = create_connection(db_path)

    if connection is not None:
        # Create submissions table
        create_table(connection, sql_create_submissions)
        print("Created submissions table.")

        # Create comments table
        create_table(connection, sql_create_comments)
        print("Created comments table.")

        # Add entries to submissions table
        startTime = datetime.now()
        make_submissions_table(read_reddit_data(submission_file), connection)
        subTime = datetime.now() - startTime

        # Add entrieds to comments table
        startTime = datetime.now()
        make_comments_table(read_reddit_data(comments_file), connection)
        comTime = datetime.now() - startTime

        print("\n-------------------------------------------")
        print(f"Time taken to make submissions: {subTime}")
        print(f"Time taken to make comments: {comTime}")
    else:
        print("Cannot connect to Database.")

    connection.commit()
    connection.close()
            

if __name__ == '__main__':
    if len(sys.argv) > 3:
        submission_file = sys.argv[1]
        comments_file = sys.argv[2]
        db_path = sys.argv[3]
    else:
        submission_file = input("Enter submissions file path:\n")
        comments_file = input("Enter comments file path:\n")
        db_path = input("Enter path for db:\n")

    make_subreddit_db(submission_file, comments_file, db_path)

    
