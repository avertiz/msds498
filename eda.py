import RDS_config
import psycopg2
import pandas as pd

connection = psycopg2.connect(host = RDS_config.host,
                                  port = RDS_config.port,
                                  user = RDS_config.user,
                                  password = RDS_config.password,
                                  dbname= RDS_config.dbname)
                                  
# Number of posts
num_posts = pd.read_sql(""" SELECT author,
                        FROM author_submissions, count(*) as num_posts, 
                        WHERE 1 = 1
                        AND subreddit != 'borrow'
                        GROUP BY author_submissions
                        """, connection)
                        
# Number of posts in borrow subreddit
num_posts_borrow = pd.read_sql(""" SELECT author,
                        FROM author_submissions, count(*) as num_posts_borrow, 
                        WHERE 1 = 1
                        AND subreddit = 'borrow'
                        AND removed_by_category 
                        GROUP BY author_submissions
                        """, connection)
