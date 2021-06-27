from psaw import PushshiftAPI
import json
import sys
import os
from datetime import datetime
from tenacity import retry


def check_dir(dir_name):
    """
    Checks if a directory exists. Makes it if it doesn't.
    """
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def get_fields(fields, dic):
    out_dic = dict()
    for field in fields:
        out_dic[field] = dic[field] if field in dic else None
    return out_dic


def last_utc(fp):
    pass


def scrape_comments_with_retry(subreddit, out_dir, after=None):
    pass    


def scrape_comments(subreddit, out_dir, after=None):
    # Set the API
    api = PushshiftAPI()

    # Get a generator which will give us each submission in turn
    comments = api.search_comments(subreddit=subreddit, sort="asc", after=after)

    # Write each submission to the file one at a time.
    comment_fields = ["id", "created_utc", "body", "link_id", "parent_id", "author", "author_fullname", "author_flair_text", "score", "permalink"]
    for curr_comment in comments:
        try:
            with open(os.path.join(out_dir, f"{subreddit}-comments.json"), "a", encoding="utf-8") as comFile:
                curr = get_fields(comment_fields, curr_comment._asdict())
                comFile.write(json.dumps(curr) + "\n")
                print(f"Processed {curr['id']}")
        except Exception as e:
            print(repr(e))
            # with open(os.path.join(out_dir, "com_errors.json"), "a", encoding="utf-8") as errFile:
            #     errFile.write(json.dumps([repr(e), curr_comment._asdict()]) + "\n")
            pass
        

def scrape_submissions(subreddit, out_dir, after=None):
    # Set the API
    api = PushshiftAPI()

    # Get a generator which will give us each submission in turn
    submissions = api.search_submissions(subreddit=subreddit, sort="asc", after=after)

    # Write each submission to the file one at a time.
    submission_fields = ["id", "title", "selftext", "created", "author", "author_fullname", "author_flair_text", "upvote_ratio", "score", "url", "permalink"]
    for curr_submission in submissions:
        try:
            with open(os.path.join(out_dir, f"{subreddit}-submissions.json"), "a", encoding="utf-8") as subFile:
                curr = get_fields(submission_fields, curr_submission._asdict())
                subFile.write(json.dumps(curr) + "\n")
                print(f"Processed {curr['id']}")
        except Exception as e:
            print(repr(e))
            # with open(os.path.join(out_dir, "sub_errors.json"), "a", encoding="utf-8") as errFile:
            #     errFile.write(json.dumps([repr(e), curr_submission._asdict()]) + "\n")
            pass



def scrape_subreddit(subreddit, out_dir, after=None):
    check_dir(out_dir)

    # Get the current time
    startTime = datetime.now()

    scrape_submissions(subreddit, out_dir, after=after)

    subTime = datetime.now() - startTime
    startTime = datetime.now()

    scrape_comments(subreddit, out_dir, after=after)

    comTime = datetime.now() - startTime
    print("\n-------------------------------------------")
    print(f"Time taken to get submissions: {subTime}")
    print(f"Time taken to get comments: {comTime}")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        subreddit = sys.argv[1]
        out_dir = sys.argv[2]
    else:
        subreddit = input("Enter subreddit to scrape:\n")
        out_dir = input("Enter output directory:\n")

    scrape_subreddit(subreddit, out_dir)