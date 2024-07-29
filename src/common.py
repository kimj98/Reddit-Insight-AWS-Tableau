
import praw
import pandas as pd
from datetime import datetime, timedelta
from datetime import date
import time

def init_subreddit(subreddit):
    reddit = praw.Reddit(
    client_id='nCsnyO_1J7VWc3wmiGvbbg',
    client_secret='_PDHYQvWXiK08YnTOTGiy4iiUeBIXg',
    user_agent='sentimentanalysis by /u/Professional_Ball_58'
    )
    
    subreddit = reddit.subreddit(subreddit)
    return subreddit

def fetch_comments(post):
    comments = []
    post.comments.replace_more(limit=0)
    for comment in post.comments.list():
        comments.append({
            "id": comment.id,
            "post_id": post.id,
            "author": comment.author.name if comment.author else 'N/A',
            "created_utc": datetime.fromtimestamp(comment.created_utc),
            "body": comment.body,
            "score": comment.score,
            "parent_id": comment.parent_id,
            "is_submitter": comment.is_submitter,
            "distinguished": comment.distinguished,
            "gilded": comment.gilded,
        })
    return comments

def fetch_post_info(post):
    post_info = {
        "id": post.id,
        "title": post.title,
        "author": post.author.name if post.author else 'N/A',
        "created_utc": datetime.fromtimestamp(post.created_utc),
        "selftext": post.selftext,
        "url": post.url,
        "upvote_ratio": post.upvote_ratio,
        "ups": post.ups,
        "downs": post.downs,
        "score": post.score,
        "num_comments": post.num_comments,
        "permalink": post.permalink,
        "subreddit": post.subreddit.display_name,
        "subreddit_id": post.subreddit_id,
        "link_flair_text": post.link_flair_text,
        "stickied": post.stickied,
        "over_18": post.over_18,
        "spoiler": post.spoiler,
        "locked": post.locked,
        "edited": post.edited,
        "is_original_content": post.is_original_content,
        "is_self": post.is_self,
        "is_video": post.is_video,
        "distinguished": post.distinguished,
        "gilded": post.gilded
    }
    comments = fetch_comments(post)
    return post_info, comments

def fetch_posts_and_comments(subreddit, date):
    subreddit = init_subreddit(subreddit)

    selected_date = datetime.strptime(date, "%Y-%m-%d")
    start_datetime = datetime.combine(selected_date, datetime.min.time())
    posts = []
    comments = []

    for post in subreddit.new(limit=None):  
        post_created = datetime.fromtimestamp(post.created_utc)
        if post_created < start_datetime:
            break
        post_info, post_comments = fetch_post_info(post)
        posts.append(post_info)
        comments.extend(post_comments)

    return pd.DataFrame(posts), pd.DataFrame(comments)

if __name__=="__main__":
    start_time = time.time()
    selected_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    posts, comments = fetch_posts_and_comments("wallstreetbets", selected_date)
    print(posts)
    print(comments)     
    end_time = time.time()
    print(end_time-start_time)