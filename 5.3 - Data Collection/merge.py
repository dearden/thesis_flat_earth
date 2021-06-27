import json
import os
import re

if __name__ == "__main__":
    directory = input("Enter directory path with chunks in:\n")
    lookup_table = input("Enter filepath of lookup table:\n")

    file_reg = r'(\d+).json'

    with open(lookup_table, "r", encoding="utf-8") as lookup_file:
        lookup_dic = json.load(lookup_file)

    i = 1
    for filename in os.listdir(directory):
        m = re.fullmatch(file_reg, filename)
        if m:
            lookup_entry = lookup_dic[m.group(1)]
            with open(os.path.join(directory, filename), encoding="utf-8") as curr_chunk:
                for line in curr_chunk:
                    with open(os.path.join(directory, f"{lookup_entry[0]}_{lookup_entry[1]}.json"), "a", encoding="utf-8") as curr_out:
                        curr_out.write(line)
