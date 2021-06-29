import json


def read_tok_file(toks_fp):
    with open(toks_fp, encoding="utf-8") as tok_file:
        for line in tok_file.readlines():
            curr = json.loads(line.strip())
            yield curr


if __name__ == "__main__":
    with open("MAPS/tfes_post_map.json") as f:
        post_map = json.load(f)

    with open("tfes_posts_anon.json", "w", encoding="utf-8") as f:
        for post in read_tok_file("tfes_posts.json"):
            curr_entry = "{}\n".format(json.dumps([post_map[post[0]], post[1]]))
            f.write(curr_entry)

    print("Finished")