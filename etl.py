import requests
import json
import pandas as pd

def getSubmissions(limit):
    url = 'https://api.pushshift.io/reddit/search/submission/?subreddit=borrow&size=' + str(limit)
    r = requests.get(url)
    data = json.loads(r.text)
    return pd.json_normalize(data['data'])