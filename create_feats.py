import RDS_config
import psycopg2
import re
import pandas as pd

connection = psycopg2.connect(host = RDS_config.host,
                                  port = RDS_config.port,
                                  user = RDS_config.user,
                                  password = RDS_config.password,
                                  dbname= RDS_config.dbname)

df = pd.read_sql("""  SELECT DISTINCT author 
                      FROM borrow_submissions
                      WHERE 1 = 1
                      AND LEFT(title, 5) = '[REQ]'
                      AND author != '[deleted]'
                        """, connection)

bot_mentions = pd.read_sql("""SELECT regexp_matches(body, '\/u\/(.*?)\s') as author
                              FROM loans_bot
                           """, connection)
bot_mentions['author'] = bot_mentions['author'].str[0]
bot_mentions['author'] = bot_mentions['author'].str.replace('\:','')
bot_mentions['author'] = bot_mentions['author'].str.replace('\-','')
bot_mentions = bot_mentions.groupby("author")['author'].count().reset_index(name="bot_mentions")

df = pd.merge(df, bot_mentions, on='author', how='left')
df['bot_mentions'].fillna(0, inplace=True)

queries = [
""" SELECT author, count(*) as num_posts_borrow
    FROM author_submissions
    WHERE 1 = 1
    AND subreddit = 'borrow'
    AND selftext != '[removed]'
    GROUP BY author
""",
""" SELECT author, AVG(regexp_matches) as average_loan
    FROM  (SELECT author, (regexp_matches(LEFT(title, 13), '[0-9]+\.?[0-9]*'))[1]::numeric
            FROM author_submissions
            WHERE 1 = 1
            AND subreddit = 'borrow'
            AND selftext != '[removed]') as loans
    WHERE 1 = 1
    GROUP BY author
""",
""" SELECT author, count(*) as num_posts
    FROM author_submissions
    WHERE 1 = 1
    AND subreddit != 'borrow'
    AND selftext != '[removed]'
    GROUP BY author
""",
""" SELECT author, count(*) as num_subreddits
    FROM 
    (SELECT DISTINCT author, subreddit
     FROM author_submissions) as subreddits
    WHERE 1 = 1
    GROUP BY author
""",
""" SELECT author, CAST(MAX(created_utc) as INTEGER) - CAST(MIN(created_utc) as INTEGER) as author_age 
    FROM author_submissions
    WHERE 1 = 1
    GROUP BY author
""",
""" SELECT author, AVG(CAST(score as INTEGER)) as avg_post_score 
    FROM author_submissions
    WHERE 1 = 1
    GROUP BY author
""",
""" SELECT author, SUM(CASE
                            WHEN is_self = 'True' THEN 1
                            WHEN is_self = 'False' THEN 0
                       END) / CAST(COUNT(*) as DECIMAL) as self_post_ratio
    FROM author_submissions
    WHERE 1 = 1
    GROUP BY author
""",
""" SELECT distinct replace(substring(title from '\/(.*?)\)'), 'u/', '') as author, 1 as label
    FROM borrow_submissions
    WHERE 1 = 1
    AND LEFT(title, 8) = '[UNPAID]'
"""
]
    
for query in queries:
    results = pd.read_sql(query, connection)
    df = pd.merge(df, results, on='author', how='left')

df.update(df[['bot_mentions', 'num_posts', 'num_subreddits', 'label']].fillna(0))
    
######## DROP observations with no posts in borrow ########

series = pd.notnull(df["num_posts_borrow"])

df = df[series]

print(df.head(20))

########### This didnt work ###########
# # Labels

# loans_bot = pd.read_sql("""
#                         SELECT *
#                         FROM (SELECT SUBSTRING(body, 30, POSITION(':' IN body) - 30) as author, 
#                                      body, 
#                                      created_utc,
#                                      ROW_NUMBER () OVER (PARTITION BY SUBSTRING(body, 30, POSITION(':' IN body) - 30)
#                                                          ORDER BY created_utc desc)
#                               FROM loans_bot
#                               WHERE 1 = 1                              
#                               AND body like 'Here is my information on%') as body_text                              
#                         WHERE 1 = 1
#                         AND ROW_NUMBER = 1
#                         """, connection)

# unpaid = pd.DataFrame(columns = ['author', 'unpaid', 'label'])

# for index, row in loans_bot.iterrows():
#     s = row['body']
#     borrower = row['author']
#     pattern = "\|" + borrower + "\|(.*?)\["
#     unpaid_count = re.findall(pattern, s)
#     count = 0
#     if len(unpaid_count) >= 1:
#         for result in unpaid_count:
#             if "UNPAID" in result:
#                 count += 1
#     if count >= 1:
#         unpaid = unpaid.append({'author': borrower, 'unpaid': count, 'label': 1}, ignore_index=True)
#     else:
#         unpaid = unpaid.append({'author': borrower, 'unpaid': count, 'label': 0}, ignore_index=True)
        
# df = pd.merge(df, unpaid, on='author', how='left')