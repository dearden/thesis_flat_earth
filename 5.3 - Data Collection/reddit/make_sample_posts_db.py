import json
import sqlite3
from sqlite3 import Error as SQLError
from datetime import datetime


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


if __name__ == '__main__':
    TOKS_COM_FP = input("Insert Filepath of the comment samples:\n")
    TOKS_SUB_FP = input("Insert Filepath of the submission samples:\n")
    O_DB_FP = input("Insert Filepath of Original Database:\n")
    N_DB_FP = input("Insert Filepath of New Database:\n")

    with open(TOKS_COM_FP, "r", encoding="utf-8") as sample_file:
        com_idxs = json.load(sample_file)

    with open(TOKS_SUB_FP, "r", encoding="utf-8") as sample_file:
        sub_idxs = json.load(sample_file)

    print("Got indices.")

    com_idx_string = ",".join([f"'{x}'" for x in com_idxs])
    sub_idx_string = ",".join([f"'{x}'" for x in sub_idxs])

    # Create new database
    connection = create_connection(N_DB_FP)
    curs = connection.cursor()

    # Create submissions table
    create_table(connection, sql_create_submissions)
    print("Created submissions table.")

    # Create comments table
    create_table(connection, sql_create_comments)
    print("Created comments table.")

    # Attach old database
    curs.execute("ATTACH DATABASE ? AS big_db", (O_DB_FP,))

    transfer_comments_sql = f"""
    INSERT INTO comments
    SELECT * FROM big_db.comments WHERE uid IN ({com_idx_string});"""

    transfer_submissions_sql = f"""
    INSERT INTO submissions
    SELECT * FROM big_db.submissions WHERE uid IN ({sub_idx_string});"""

    curs.execute(transfer_comments_sql)
    connection.commit()
    print("Transferred sample comments.")
    curs.execute(transfer_submissions_sql)
    connection.commit()
    print("Transferred sample submissions.")

    curs.execute("DETACH DATABASE big_db")
    connection.commit()
    connection.close()

