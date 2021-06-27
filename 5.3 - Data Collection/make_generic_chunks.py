import os
import json
import re


if __name__ == '__main__':
    directory = input("Enter directory path with chunks in:\n")

    file_reg = r'(\w+)\-(\w+)\-chunk\-(\d+)\.json'

    lookup_dic = dict()

    i = 1
    for filename in os.listdir(directory):
        m = re.fullmatch(file_reg, filename)
        if m:
            sub = m.group(1)
            contrib_type = m.group(2)
            chunk_num = m.group(3)

            lookup_dic[i] = (sub, contrib_type, chunk_num)

            os.rename(os.path.join(directory, filename), os.path.join(directory, f"{i}.json"))
            i += 1

    with open(os.path.join(directory, "LOOKUP.json"), "w", encoding="utf-8") as out_file:
        json.dump(lookup_dic, out_file)