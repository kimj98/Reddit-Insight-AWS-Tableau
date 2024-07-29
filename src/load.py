import boto3
from datetime import date, timedelta
from io import StringIO

def load_to_s3(df,title):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    date_today = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    file_name = f"{title}_{date_today}.csv"
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket="kimj98bucket", Key=file_name, Body=csv_buffer.getvalue())