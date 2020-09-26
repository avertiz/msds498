import requests
import json

def getSubmissions(limit):
    url = 'https://api.pushshift.io/reddit/search/submission/?subreddit=borrow&size=' + str(limit)
    r = requests.get(url)
    data = json.loads(r.text)
    return data['data']