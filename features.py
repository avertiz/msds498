import RDS_config
import psycopg2
import re
import time
import etl
import pandas as pd

connection = psycopg2.connect(host = RDS_config.host,
                                  port = RDS_config.port,
                                  user = RDS_config.user,
                                  password = RDS_config.password,
                                  dbname= RDS_config.dbname)

def get_feats(connection, df):
    df = df['author']
    bot_mentions = pd.read_sql("""SELECT regexp_matches(body, '\/u\/(.*?)\s') as author
                                  FROM loans_bot
                               """, connection)
    bot_mentions['author'] = bot_mentions['author'].str[0]
    bot_mentions['author'] = bot_mentions['author'].str.replace('\:','')
    bot_mentions['author'] = bot_mentions['author'].str.replace('\-','')
    bot_mentions = bot_mentions.groupby("author")['author'].count().reset_index(name="bot_mentions")
    df = pd.merge(df, bot_mentions, on='author', how='left')
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
    
    df.update(df[['bot_mentions', 
                  'num_posts_borrow', 
                  'average_loan',
                  'num_posts',
                  'num_subreddits',
                  'author_age',
                  'avg_post_score',
                  'self_post_ratio']].fillna(0))

    return(df)

# df = etl.getPushShiftData(subreddit = 'borrow', size = '200')
# df['created_utc'] = df.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x['created_utc'])), axis=1)
# table = df.loc[(df['title'].str.contains("[REQ]")) & (df['selftext'] != '[removed]'), ['author', 'title', 'full_link', 'created_utc']]
# table = table.head(15)
# table['Default Likelihood'] = None

# print(get_feats(connection = connection, df = table))