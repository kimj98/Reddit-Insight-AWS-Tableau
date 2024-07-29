import asyncio
from extract import *
from load import *
from io import StringIO
import sys
import os

# Add the parent directory of 'src' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))




if __name__=="__main__":
    posts, comments = asyncio.run(fetch_past_day())
    load_to_s3(posts, "posts")
    load_to_s3(comments, "comments")