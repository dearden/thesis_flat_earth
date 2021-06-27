import pickle
import sys
import os
import re
import json
	
import sqlite3
from sqlite3 import Error as SQLError
from datetime import datetime


# Some useful lambdas
convert_date = lambda x: x.strftime("%Y/%m/%d %H:%M:%S")        # Converts datetime to formatted string
int_or_none = lambda x: int(x) if x is not None else None       # Returns either the int of a string or None


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


def make_boards_table(boards, connection):
    cursor = connection.cursor()

    for board in boards.values():
        command = '''INSERT INTO boards(uid, name)
                        VALUES(?, ?);'''
        curr_entry = (int(board.id), board.name)
        cursor.execute(command, curr_entry)

        print("Added board {}".format(board.id))

def add_user(user, cursor):
    curr_age = int(user.age) if user.age.isdigit() else None

    time_since_active = (user.local_time - user.last_active).days

    float_or_zero = lambda x: 0 if x == "N/A" else float(x)

    command = '''INSERT INTO users(uid, name, position, personal_text, custom_title, signature, 
                    location, age, gender, num_posts, num_posts_per_day, date_registered, 
                    days_since_last_active, language)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
    curr_entry = (user.id, user.name, user.position, user.personal_text, user.custom_title, 
                    user.signature, user.location, curr_age, user.gender, 
                    int(user.num_posts), float_or_zero(user.num_posts_day),
                    convert_date(user.date_registered), time_since_active, user.language)
    cursor.execute(command, curr_entry)

    print("Added user {}".format(user.id))

def make_users_table(users, connection):
    cursor = connection.cursor()

    for user in users.values():
        add_user(user, cursor)

def make_topics_table(topics, connection):
    cursor = connection.cursor()
    quote_counter = 0

    # Loop through topics and make a row for each in topics table
    for topic in topics.values():
        command = '''INSERT INTO topics(uid, board, name, user)
                        VALUES(?, ?, ?, ?);'''
        curr_entry = (int(topic.id), int_or_none(topic.board), topic.topic_name, None)
        cursor.execute(command, curr_entry)

        # Loop through each post in this topic and insert into posts table
        for post in topic.posts.values():
            # If the post was written "today" we don't know the correct date, so skip it.
            if post.time is None:
                continue

            command = '''INSERT INTO posts(uid, topic, user, poster_name, body, time)
                            VALUES(?, ?, ?, ?, ?, ?);'''
            curr_entry = (int(post.id), int(post.topic), int_or_none(post.user_id), post.poster, post.text, convert_date(post.time))
            cursor.execute(command, curr_entry)

            # Go through each quote and add it to the quotes table
            for quote in post.quotes:
                command = '''INSERT INTO quotes(uid, post, quote_origin, body)
                                VALUES(?, ?, ?, ?);'''
                # Change quote["q_num"] to quote counter if you don't have globally id'ed quotes.
                curr_entry = (quote["q_num"], int(post.id), quote["quote_origin"], quote["quote_text"])
                cursor.execute(command, curr_entry)
                quote_counter += 1

        print("Added topic {}".format(topic.id))
            

if __name__ == '__main__':
    with open("C:/Users/Eddie/Documents/Datasets/Flat Earth/big_forum.pk1", 'rb') as out_file:
        forum = pickle.load(out_file)

    filename = "C:/Users/Eddie/Documents/Datasets/Flat Earth/big_forum.db"
    connection = create_connection(filename)

    sql_create_posts = """ 
    CREATE TABLE IF NOT EXISTS posts (
        uid integer PRIMARY KEY,
        topic integer NOT NULL,
        user integer,
        poster_name text NOT NULL,
        body text NOT NULL,
        time text NOT NULL,
        FOREIGN KEY (topic) REFERENCES topics (uid)
        FOREIGN KEY (user) REFERENCES users (uid)
    );"""

    sql_create_users = """ 
    CREATE TABLE IF NOT EXISTS users (
        uid integer PRIMARY KEY,
        name text NOT NULL,
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

    if connection is not None:
        # Create Debates table
        create_table(connection, sql_create_posts)
        print("Created debates table.")

        # Create Members table
        create_table(connection, sql_create_users)
        print("Created members table.")

        # Create Contribution table
        create_table(connection, sql_create_topics)
        print("Created contributions table.")

        # Create Contribution table
        create_table(connection, sql_create_boards)
        print("Created contributions table.")

        # Create Contribution table
        create_table(connection, sql_create_quotes)
        print("Created contributions table.")

        make_boards_table(forum.boards, connection)
        make_users_table(forum.users, connection)
        make_topics_table(forum.topics, connection)
    else:
        print("Cannot connect to Database.")

    connection.commit()
    connection.close()
