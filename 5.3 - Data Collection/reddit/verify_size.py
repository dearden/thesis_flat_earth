import json
import os
import re

if __name__ == "__main__":
    chunk_dir = input("Enter directory path with original chunks in:\n")
    tok_dir = input("Enter directory with tokenised files in:\n")
    lookup_table = input("Enter filepath of lookup table:\n")

    file_reg = r'(\d+).json'

    with open(lookup_table, "r", encoding="utf-8") as lookup_file:
        lookup_dic = json.load(lookup_file)

    sub_dic = dict()
    tok_chunk_dic = dict()

    for chunk_num, lookup_entry in lookup_dic.items():
        with open(os.path.join(chunk_dir, f"{chunk_num}.json"), encoding="utf-8") as curr_chunk_file:
            curr_chunk = json.load(curr_chunk_file)
            curr_chunk_length = len(curr_chunk)
            curr_name = f"{lookup_entry[0]}_{lookup_entry[1]}.json"
            if curr_name in sub_dic:
                sub_dic[curr_name] += curr_chunk_length
            else:
                sub_dic[curr_name] = curr_chunk_length

        with open(os.path.join(tok_dir, f"{chunk_num}.json"), "r", encoding="utf-8") as tok_chunk_file:
            curr_tok_chunk_idx = [l[0] for l in tok_chunk_file]
            curr_tok_chunk_length = len(curr_tok_chunk_idx)

            if curr_name in tok_chunk_dic:
                tok_chunk_dic[curr_name] += curr_tok_chunk_length
            else:
                tok_chunk_dic[curr_name] = curr_tok_chunk_length

    for sub, count in sub_dic.items():

        with open(os.path.join(tok_dir, sub), "r", encoding="utf-8") as curr_tok_file:
            curr_tok_idx = [l[0] for l in curr_tok_file]
            curr_tok_length = len(curr_tok_idx)

        print(f"{sub:30} {count:10} {tok_chunk_dic[sub]:10} {curr_tok_length:10}")
