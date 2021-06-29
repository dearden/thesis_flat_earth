import json
import sqlite3
import os

if __name__ == "__main__":
    red_dir = input("Enter directory containing Reddit databases:\n")
    map_dir = input("Enter directory to put maps:\n")

    # initialise id lists
    com_ids = set()
    sub_ids = set()
    usr_ids = set()
    names = set()

    db_dic = dict()

    for subdir, dirs, files in os.walk(red_dir):
        for filename in files:
            if filename.endswith(".db"):
                db_dic[filename[:-3]] = os.path.join(subdir, filename)


    # Loop through all databases to get full list
    for db_name, db_fp in db_dic.items():
        print(db_name, db_fp)

        # Connect to the database
        conn = sqlite3.connect(db_fp)
        curs = conn.cursor()

        # Update list of comment ids
        curr = curs.execute("SELECT uid FROM comments").fetchall()
        curr = [p[0] for p in curr]
        com_ids.update(curr)

        # Update list of submission ids
        curr = curs.execute("SELECT uid FROM submissions").fetchall()
        curr = [p[0] for p in curr]
        sub_ids.update(curr)

        # Update list of user ids
        curr = curs.execute("SELECT poster_id FROM comments").fetchall()
        curr = [p[0] for p in curr]
        usr_ids.update(curr)

        curr = curs.execute("SELECT poster_id FROM submissions").fetchall()
        curr = [p[0] for p in curr]
        usr_ids.update(curr)

        # Update list of usernames
        curr = curs.execute("SELECT poster_name FROM comments").fetchall()
        curr = [p[0] for p in curr]
        names.update(curr)

        curr = curs.execute("SELECT poster_name FROM submissions").fetchall()
        curr = [p[0] for p in curr]
        names.update(curr)
        
        
    com_ids = list(com_ids)
    com_map = {id: i+1 for i, id in enumerate(com_ids)}
    with open(os.path.join(map_dir, "reddit_com_map.json"), "w") as out_file:
        json.dump(com_map, out_file)

    sub_ids = list(sub_ids)
    sub_map = {id: i+1 for i, id in enumerate(sub_ids)}
    with open(os.path.join(map_dir, "reddit_sub_map.json"), "w") as out_file:
        json.dump(sub_map, out_file)

    usr_ids = list(usr_ids)
    usr_ids = [i for i in usr_ids if i != "[removed]" and i is not None]
    usr_map = {id: i+1 for i, id in enumerate(usr_ids)}
    with open(os.path.join(map_dir, "reddit_usr_map.json"), "w") as out_file:
        json.dump(usr_map, out_file)

    names = list(names)
    names = [i for i in names if i != "[deleted]" and i is not None]
    nam_map = {id: i+1 for i, id in enumerate(names)}
    with open(os.path.join(map_dir, "reddit_nam_map.json"), "w") as out_file:
        json.dump(nam_map, out_file)