import json
import sqlite3
import os

if __name__ == "__main__":
    db_fp = input("Enter filepath to unanonymous database:\n")
    map_dir = input("Enter directory to put maps:\n")

    # Connect to the database
    conn = sqlite3.connect(db_fp)
    curs = conn.cursor()

    # Get list of post ids
    post_ids = curs.execute("SELECT uid FROM posts").fetchall()
    post_ids = [p[0] for p in post_ids]
    post_map = {id: i+1 for i, id in enumerate(post_ids)}
    with open(os.path.join(map_dir, "tfes_post_map.json"), "w") as out_file:
        json.dump(post_map, out_file)

    # Get list of topic ids
    topi_ids = curs.execute("SELECT uid FROM topics").fetchall()
    topi_ids = [p[0] for p in topi_ids]
    topi_map = {id: i+1 for i, id in enumerate(topi_ids)}
    with open(os.path.join(map_dir, "tfes_topi_map.json"), "w") as out_file:
        json.dump(topi_map, out_file)

    # Get list of board ids
    boar_ids = curs.execute("SELECT uid FROM boards").fetchall()
    boar_ids = [p[0] for p in boar_ids]
    boar_map = {id: i+1 for i, id in enumerate(boar_ids)}
    with open(os.path.join(map_dir, "tfes_boar_map.json"), "w") as out_file:
        json.dump(boar_map, out_file)

    # Get list of user ids
    user_ids = curs.execute("SELECT uid FROM users").fetchall()
    user_ids = [p[0] for p in user_ids]
    user_map = {id: i+1 for i, id in enumerate(user_ids)}
    with open(os.path.join(map_dir, "tfes_user_map.json"), "w") as out_file:
        json.dump(user_map, out_file)