import asyncio
import asyncpraw
import pandas as pd
from datetime import datetime, timedelta, date
import aiohttp


async def init_reddit(client_session):
    reddit = asyncpraw.Reddit(
        client_id='nCsnyO_1J7VWc3wmiGvbbg',
        client_secret='_PDHYQvWXiK08YnTOTGiy4iiUeBIXg',
        user_agent='sentimentanalysis by /u/Professional_Ball_58',
        requestor_kwargs={'session': client_session}
    )
    return reddit

async def fetch_comments(reddit, post_id):
    comments = []
    try:
        submission = await reddit.submission(id=post_id)
        await submission.comments.replace_more(limit=0)
        comment_list = submission.comments.list()
        if isinstance(comment_list, list):
            for comment in comment_list:
                comments.append({
                    "id": comment.id,
                    "post_id": post_id,
                    "author": comment.author.name if comment.author else 'N/A',
                    "created_utc": datetime.fromtimestamp(comment.created_utc),
                    "body": comment.body,
                    "score": comment.score,
                })
        else:
            print(f"Warning: comment_list is not iterable for post {post_id}")
    except Exception as e:
        print(f"Error fetching comments for post {post_id}: {e}")
    return comments

async def fetch_post_info(post):
    post_info = {
        "id": post.id,
        "title": post.title,
        "author": post.author.name if post.author else 'N/A',
        "created_utc": datetime.fromtimestamp(post.created_utc),
        "selftext": post.selftext,
        "upvote_ratio": post.upvote_ratio,
        "ups": post.ups,
        "downs": post.downs,
        "score": post.score,
        "num_comments": post.num_comments
    }
    return post_info

async def fetch_post_with_comments(reddit, post):
    post_info = await fetch_post_info(post)
    post_comments = await fetch_comments(reddit, post.id)
    return post_info, post_comments

async def fetch_posts_and_comments(subreddit_name, date):
    async with aiohttp.ClientSession() as client_session:
        reddit = await init_reddit(client_session)
        subreddit = await reddit.subreddit(subreddit_name)

        selected_date = datetime.strptime(date, "%Y-%m-%d")
        start_datetime = datetime.combine(selected_date, datetime.min.time())
        posts = []
        comments = []

        fetch_tasks = []
        async for post in subreddit.new(limit=None):
            post_created = datetime.fromtimestamp(post.created_utc)
            if post_created < start_datetime:
                break
            fetch_tasks.append(fetch_post_with_comments(reddit, post))

        results = await asyncio.gather(*fetch_tasks)
        for post_info, post_comments in results:
            posts.append(post_info)
            comments.extend(post_comments)

    return pd.DataFrame(posts), pd.DataFrame(comments)

async def fetch_past_day():
    selected_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    posts, comments = await fetch_posts_and_comments("wallstreetbets", selected_date)
    return posts, comments

