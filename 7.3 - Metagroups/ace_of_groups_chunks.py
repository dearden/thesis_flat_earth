import pandas as pd
import matplotlib.pyplot as plt
import os
import json
from datetime import datetime
import numpy as np
import seaborn as sns
import sqlite3
import sys
sys.path.insert(1, "../utilities")

from language_change_methods.utility_functions import tokenise, get_time_windows, get_data_windows
from language_change_methods.cross_entropy import single_CE_run
from helpers import load_toks, make_tok_chunks

sys.path.insert(1, "../")
from settings import TFES_FP, TFES_TOK_FP, DATA_DIR


def get_db(DB_FP):
    conn = sqlite3.connect(DB_FP)
    curs = conn.cursor()

    # Gets all the contributions and creates a nice dataframe
    all_posts = pd.read_sql_query(sql_get_all_posts, conn)
    all_posts.columns = ['uid', 'poster_name', 'body', 'time', 'topic', 'board', 'poster_id', 'position', 'personal_text', 'custom_title', 'location', 'age', 'gender']
    all_posts.set_index("uid", inplace=True)
    convert_to_date = lambda x: datetime.strptime(x, "%Y/%m/%d %H:%M:%S")
    all_posts['time'] = all_posts['time'].apply(convert_to_date)
    all_posts.sort_values("time", inplace=True)
    return all_posts


if __name__ == "__main__":
    if len(sys.argv) > 3:
        groups_fp = sys.argv[1]
        db_file = sys.argv[2]
        tok_file = sys.argv[3]
        out_fp = sys.argv[4]
    else:
        # groups_fp = input("Enter filepath of groups json:\n")
        # db_file = input("Enter filepath of database:\n")
        # tok_file = input("Enter filepath of tokens:\n")
        groups_fp = "../data/metagroups.json"
        db_file = TFES_FP
        tok_file = TFES_TOK_FP
        out_fp = input("Enter output FP (json):\n")

    sql_get_all_posts ="""
    SELECT p.uid, p.poster_name, p.body, p.time, t.name, b.name, u.uid, u.position, u.personal_text, u.custom_title, u.location, u.age, u.gender
    FROM posts as p
    LEFT JOIN users as u
    ON u.uid = p.user
    INNER JOIN topics as t
    ON t.uid = p.topic
    INNER JOIN boards as b
    ON b.uid= t.board;""".strip()

    # Gets all the posts
    posts = get_db(db_file)

    toks = {x[0]: x[1] for x in load_toks(tok_file)}
    toks = pd.Series(toks)

    with open(groups_fp) as groups_file:
        groups = json.load(groups_file)

    # Setting the hyperparameters for all runs.
    win_size = 15000
    win_step = 15000
    n_runs = 5
    balanced=False
    w_limit = False
    contrib_limit = None
    token_limit = 30

    # Convert to chunks
    toks =  make_tok_chunks(toks, token_limit)
    posts = posts.loc[toks["idx"]].set_index(toks.index)

    group_posts = {gname: posts.loc[toks["idx"].isin(group)] for gname, group in groups.items()}
    group_toks = {gname: toks.loc[toks["idx"].isin(group)]["chunk"] for gname, group in groups.items()}

    single_CE_run(list(group_posts.keys()), group_posts, group_toks, None, None,
                    win_size, win_step, n_runs, balanced, w_limit,
                    token_limit, contrib_limit, ["NA"], out_fp, member_field="poster_id")