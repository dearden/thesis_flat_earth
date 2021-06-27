import json
import os
import re

if __name__ == "__main__":
    chunk_dir = input("Enter directory path with original chunks in:\n")
    tok_dir = input("Enter directory with tokenised files in:\n")
    lookup_table = input("Enter filepath of lookup table:\n")

    with open(lookup_table, "r", encoding="utf-8") as lookup_file:
        lookup_dic = json.load(lookup_file)

    for chunk_num, lookup_entry in lookup_dic.items():
        with open(os.path.join(chunk_dir, f"{chunk_num}.json"), encoding="utf-8") as curr_chunk_file:
            curr_chunk = json.load(curr_chunk_file)
            curr_chunk_length = len(curr_chunk)

        with open(os.path.join(tok_dir, f"{chunk_num}.json"), "r", encoding="utf-8") as tok_chunk_file:
            curr_tok_chunk_idx = [l[0] for l in tok_chunk_file]
            curr_tok_chunk_length = len(curr_tok_chunk_idx)

        print(f"{chunk_num:10} {curr_chunk_length:10} {curr_tok_chunk_length:10}")
