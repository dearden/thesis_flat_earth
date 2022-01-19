from psaw import PushshiftAPI
import json
import sys
import os
from datetime import datetime

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(1, dir_path)
from scrape_subreddit import scrape_comments


def check_dir(dir_name):
    """
    Checks if a directory exists. Makes it if it doesn't.
    """
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


if __name__ == "__main__":
    if len(sys.argv) > 3:
        subreddit = sys.argv[1]
        out_dir = sys.argv[2]
        after = sys.argv[3]
    else:
        subreddit = input("Enter subreddit to scrape:\n")
        out_dir = input("Enter output directory:\n")
        after = input("Enter Epoch to search after:\n")

    check_dir(out_dir)
    # Get the current time
    startTime = datetime.now()
    # Scrape the comments
    scrape_comments(subreddit, out_dir, after=after)
    # Record how long it took
    print(f"Time taken to get comments: {datetime.now() - startTime}")