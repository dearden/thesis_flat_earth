import regex as re
import pandas as pd
import json
from datetime import datetime
import sqlite3

from language_change_methods.utility_functions import tokenise, get_time_windows, get_data_windows
from collections import Counter
import itertools

from language_change_methods.features import function_words
from nltk.corpus import stopwords
stops = set(stopwords.words('english'))
func_and_stops = stops.union(function_words)
func_and_stops.add("n't")

pd.plotting.register_matplotlib_converters()

sql_get_all_posts ="""
SELECT p.uid, p.poster_name, p.body, p.time, t.name, t.uid, b.name, b.uid, u.uid, u.position, u.personal_text, u.custom_title, u.location, u.age, u.gender
FROM posts as p
LEFT JOIN users as u
ON u.uid = p.user
INNER JOIN topics as t
ON t.uid = p.topic
INNER JOIN boards as b
ON b.uid= t.board;""".strip()

flat_earth_boards = [3, 4, 5, 10, 13]
off_topic_boards = [6,7,8,9]
misc_boards = [1,2,11,12]

convert_to_date = lambda x: datetime.strptime(x, "%Y/%m/%d %H:%M:%S")


def load_posts(DB_FP):
    conn = sqlite3.connect(DB_FP)
    curs = conn.cursor()

    # Gets all the contributions and creates a nice dataframe
    all_posts = pd.read_sql_query(sql_get_all_posts, conn)
    all_posts.columns = ['uid', 'poster_name', 'body', 'time', 'topic_name', 'topic_id', 'board_name', 'board_id', 'poster_id', 'position', 'personal_text', 'custom_title', 'location', 'age', 'gender']
    all_posts.set_index("uid", inplace=True)
    convert_to_date = lambda x: datetime.strptime(x, "%Y/%m/%d %H:%M:%S")
    all_posts['time'] = all_posts['time'].apply(convert_to_date)
    all_posts.sort_values("time", inplace=True)
    return all_posts


def tok_okay(tok):
    okay = True
    okay = okay and not re.match(r"[\p{P}|\p{S}|\p{N}]+", tok)
    okay = okay and tok.lower() not in func_and_stops
    return okay


def read_tok_file(toks_fp):
    with open(toks_fp, encoding="utf-8") as tok_file:
        for line in tok_file.readlines():
            curr = json.loads(line.strip())
            yield curr

    
def load_toks(toks_fp):
    for curr in read_tok_file(toks_fp):
        yield curr[0], [w["tok"].lower() for w in curr[1]["words"]]


def load_pos(toks_fp):
    for curr in read_tok_file(toks_fp):
        yield curr[0], [w["pos"] for w in curr[1]["words"]]


def load_ent(toks_fp):
    for curr in read_tok_file(toks_fp):
        yield curr[0], [w for w in curr[1]["entities"]]


def get_top_n_toks(toks, n):
    counts = Counter()
    for row in toks:
        counts.update(row)
    return [x[0] for x in counts.most_common(n)]


merge_lists = lambda x: list(itertools.chain.from_iterable(x))


def get_chunks(idx, tokens, chunk_size):
    for i in range(0, len(tokens)-chunk_size, chunk_size):
        yield idx, tokens[i:i+chunk_size]
        

def make_tok_chunks(tokens, chunk_size):
    chunks = [[[idx, chunk] for idx, chunk in get_chunks(idx, curr_toks, chunk_size)] for idx, curr_toks in tokens.items()]
    chunks = merge_lists(chunks)
    chunks = pd.DataFrame(chunks, columns=["idx", "chunk"])
    return chunks