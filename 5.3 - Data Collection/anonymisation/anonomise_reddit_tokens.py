import json
import os


def read_tok_file(toks_fp):
    with open(toks_fp, encoding="utf-8") as tok_file:
        for line in tok_file.readlines():
            curr = json.loads(line.strip())
            yield curr


def make_toks_anon(old_fp, new_fp, curr_map):
    with open(new_fp, "w", encoding="utf-8") as f:
            for post in read_tok_file(old_fp):
                curr_entry = "{}\n".format(json.dumps([curr_map[post[0]], post[1]]))
                f.write(curr_entry)


def anonymise_json_files(curr_dir, curr_map):
    for filename in os.listdir(curr_dir):
        if filename.endswith("json"):
            old_fp = os.path.join(curr_dir, filename)
            new_fp = os.path.join(curr_dir, "ANON", filename)
            make_toks_anon(old_fp, new_fp, curr_map)
            print(f"Completed {filename}")

if __name__ == "__main__":
    dir_to_anonymise = input("Enter directory containing comments/submissions to anonymise:\n")
    map_dir = input("Enter directory to put maps:\n")

    # Posts
    with open(os.path.join(map_dir, "reddit_com_map.json")) as f:
        com_map = json.load(f)

    # Topics
    with open(os.path.join(map_dir, "reddit_sub_map.json")) as f:
        sub_map = json.load(f)

    # Need to do this for each Comment/Submission directory individually.
    anonymise_json_files(dir_to_anonymise, com_map)

    print("Finished")