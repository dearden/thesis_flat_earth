import json
import sqlite3
from sqlite3 import Error as SQLError
from datetime import datetime
import pandas as pd


def read_db(db_fp, query):
    conn = sqlite3.connect(db_fp)
    comments = pd.read_sql_query(query, conn)
    comments.set_index("uid", inplace=True)
    conn.close()
    return comments


if __name__ == '__main__':
    TOKS_COM_FP = input("Insert Filepath of the comment samples:\n")
    TOKS_SUB_FP = input("Insert Filepath of the submission samples:\n")
    O_DB_FP = input("Insert Filepath of Original Database:\n")
    NC_FP = input("Insert Filepath of New Comments JSON File:\n")
    NS_FP = input("Insert Filepath of New Submissions JSON File:\n")

    with open(TOKS_COM_FP, "r", encoding="utf-8") as sample_file:
        com_idxs = json.load(sample_file)

    with open(TOKS_SUB_FP, "r", encoding="utf-8") as sample_file:
        sub_idxs = json.load(sample_file)

    print("Got indices.")

    com_idx_string = ",".join([f"'{x}'" for x in com_idxs])
    sub_idx_string = ",".join([f"'{x}'" for x in sub_idxs])

    query = f"SELECT uid, time, poster_id FROM comments WHERE uid IN ({com_idx_string});"
    comments = read_db(O_DB_FP, query)
    comments.to_json(NC_FP)

    query = f"SELECT uid, time, poster_id FROM submissions WHERE uid IN ({sub_idx_string});"
    submissions = read_db(O_DB_FP, query)
    submissions.to_json(NS_FP)

