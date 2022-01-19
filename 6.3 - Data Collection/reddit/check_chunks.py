import json
import os


sub_name = input("ENTER SUB NAME:\n")

sample_fp = input("ENTER FP OF SAMPLE:\n")
chunk_fp = input("ENTER FP OF CHUNKS:\n")


with open(sample_fp, encoding="utf-8") as in_file:
    idxs = json.load(in_file)


num_chunks = int(input("ENTER NUMBER OF CHUNKS:\n"))

com_or_sub = input("comments/submissions?:\n")

running_total = 0

unique_indices = list()

for i in range(1, num_chunks+1):
    curr_fp = os.path.join(chunk_fp, f"{sub_name}-{com_or_sub}-chunk-{i}.json")

    print(curr_fp)

    with open(curr_fp, encoding="utf-8") as in_file:
        curr = json.load(in_file)
        curr_idxs = list(curr.keys())

        print(f"Length of chunk {i}: {len(curr_idxs)}")
        # for idx in curr_idxs:
        #     assert idx in idxs
            
        unique_indices += curr_idxs

        # print("All indexes in sample")

        running_total += len(curr_idxs)

        print("------------------------------------------------------------")

print(f"Total num posts: {running_total}")

print(f"Total num unique posts: {len(set(unique_indices))}")