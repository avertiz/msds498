import requests
import json
import time
import pandas as pd

def getPushShiftData(**kwargs):
    r = requests.get('https://api.pushshift.io/reddit/search/submission/', params = kwargs)
    data = json.loads(r.text)
    return pd.json_normalize(data['data'])

def updateBorrowSubmissionsTable(connection, new_or_old = 'new', request_times = 5):
    
    old_row_count = pd.read_sql(""" SELECT count(*) FROM borrow_submissions """, connection)
    
    for i in range(request_times):
    
        if new_or_old == 'new':
            last_post = pd.read_sql(""" SELECT max(created_utc) FROM borrow_submissions""", connection)
            last_post = last_post['max'].iloc[0]
            submissions = getPushShiftData(subreddit = 'borrow', size = '500', after = last_post)
        elif new_or_old == 'old':
            last_post = pd.read_sql(""" SELECT min(created_utc) FROM borrow_submissions""", connection)
            last_post = last_post['min'].iloc[0]
            submissions = getPushShiftData(subreddit = 'borrow', size = '500', before = last_post)
        
        submissions = submissions.applymap(str)
        
        for row in range(len(submissions)):
            try:
                query = """ INSERT into borrow_submissions (
                                                all_awardings,
                                                allow_live_comments,
                                                author,
                                                author_flair_css_class,
                                                author_flair_richtext,
                                                author_flair_text,
                                                author_flair_type,
                                                author_fullname,
                                                author_patreon_flair,
                                                author_premium,
                                                awarders,
                                                can_mod_post,
                                                contest_mode,
                                                created_utc,
                                                domain,
                                                full_link,
                                                id,
                                                is_crosspostable,
                                                is_meta,
                                                is_original_content,
                                                is_reddit_media_domain,
                                                is_robot_indexable,
                                                is_self,
                                                is_video,
                                                link_flair_background_color,
                                                link_flair_richtext,
                                                link_flair_text_color,
                                                link_flair_type,
                                                locked,
                                                media_only,
                                                no_follow,
                                                num_comments,
                                                num_crossposts,
                                                over_18,
                                                parent_whitelist_status,
                                                permalink,
                                                pinned,
                                                pwls,
                                                retrieved_on,
                                                score,
                                                selftext,
                                                send_replies,
                                                spoiler,
                                                stickied,
                                                subreddit,
                                                subreddit_id,
                                                subreddit_subscribers,
                                                subreddit_type,
                                                thumbnail,
                                                title,
                                                total_awards_received,
                                                treatment_tags,
                                                upvote_ratio,
                                                url,
                                                whitelist_status,
                                                wls)
                            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            WHERE NOT EXISTS (SELECT id 
                                              FROM borrow_submissions 
                                              WHERE id = %s)
                            """
                list_ = [submissions['all_awardings'].iloc[row],
                        submissions['allow_live_comments'].iloc[row],
                        submissions['author'].iloc[row],
                        submissions['author_flair_css_class'].iloc[row],
                        submissions['author_flair_richtext'].iloc[row],
                        submissions['author_flair_text'].iloc[row],
                        submissions['author_flair_type'].iloc[row],
                        submissions['author_fullname'].iloc[row],
                        submissions['author_patreon_flair'].iloc[row],
                        submissions['author_premium'].iloc[row],
                        submissions['awarders'].iloc[row],
                        submissions['can_mod_post'].iloc[row],
                        submissions['contest_mode'].iloc[row],
                        submissions['created_utc'].iloc[row],
                        submissions['domain'].iloc[row],
                        submissions['full_link'].iloc[row],
                        submissions['id'].iloc[row],
                        submissions['is_crosspostable'].iloc[row],
                        submissions['is_meta'].iloc[row],
                        submissions['is_original_content'].iloc[row],
                        submissions['is_reddit_media_domain'].iloc[row],
                        submissions['is_robot_indexable'].iloc[row],
                        submissions['is_self'].iloc[row],
                        submissions['is_video'].iloc[row],
                        submissions['link_flair_background_color'].iloc[row],
                        submissions['link_flair_richtext'].iloc[row],
                        submissions['link_flair_text_color'].iloc[row],
                        submissions['link_flair_type'].iloc[row],
                        submissions['locked'].iloc[row],
                        submissions['media_only'].iloc[row],
                        submissions['no_follow'].iloc[row],
                        submissions['num_comments'].iloc[row],
                        submissions['num_crossposts'].iloc[row],
                        submissions['over_18'].iloc[row],
                        submissions['parent_whitelist_status'].iloc[row],
                        submissions['permalink'].iloc[row],
                        submissions['pinned'].iloc[row],
                        submissions['pwls'].iloc[row],
                        submissions['retrieved_on'].iloc[row],
                        submissions['score'].iloc[row],
                        submissions['selftext'].iloc[row],
                        submissions['send_replies'].iloc[row],
                        submissions['spoiler'].iloc[row],
                        submissions['stickied'].iloc[row],
                        submissions['subreddit'].iloc[row],
                        submissions['subreddit_id'].iloc[row],
                        submissions['subreddit_subscribers'].iloc[row],
                        submissions['subreddit_type'].iloc[row],
                        submissions['thumbnail'].iloc[row],
                        submissions['title'].iloc[row],
                        submissions['total_awards_received'].iloc[row],
                        None, # submissions['treatment_tags'].iloc[row],
                        None, # submissions['upvote_ratio'].iloc[row],
                        submissions['url'].iloc[row],
                        submissions['whitelist_status'].iloc[row],
                        submissions['wls'].iloc[row],                               
                        submissions['id'].iloc[row]]
                cursor = connection.cursor()
                cursor.execute(query, list_)
                connection.commit()            
            except Exception as e:
                connection.rollback()
                print('Could not insert', submissions['url'].iloc[row], e)
        
        time.sleep(2.1)
            
    new_row_count = pd.read_sql("""SELECT count(*) FROM borrow_submissions """, connection)

    return(new_row_count['count'].iloc[0] - old_row_count['count'].iloc[0])

def getNewAuthorSubmissions(author):
    submissions = getPushShiftData(author = author, size = '500')
    print(author, len(submissions))
    early_post = submissions['created_utc'].min()
    while early_post:
        more_submissions = getPushShiftData(author = author, size = '500', before = early_post)
        time.sleep(2.2)
        if len(more_submissions) != 0:
            early_post = more_submissions['created_utc'].min()
            submissions = submissions.append(more_submissions, ignore_index = True)
            print(author, len(more_submissions), len(submissions))
        else:
            early_post = 0
    return(submissions)
    
def insertAuthorSubmissions(submissions, connection):
    for row in range(len(submissions)):
        try:
            query = """ INSERT into author_submissions(
                                            all_awardings,
                                            allow_live_comments,
                                            author,
                                            author_flair_css_class,
                                            author_flair_richtext,
                                            author_flair_text,
                                            author_flair_type,
                                            author_fullname,
                                            author_patreon_flair,
                                            author_premium,
                                            awarders,
                                            can_mod_post,
                                            contest_mode,
                                            created_utc,
                                            domain,
                                            full_link,
                                            id,
                                            is_crosspostable,
                                            is_meta,
                                            is_original_content,
                                            is_reddit_media_domain,
                                            is_robot_indexable,
                                            is_self,
                                            is_video,
                                            link_flair_background_color,
                                            link_flair_richtext,
                                            link_flair_text_color,
                                            link_flair_type,
                                            locked,
                                            media_only,
                                            no_follow,
                                            num_comments,
                                            num_crossposts,
                                            over_18,
                                            parent_whitelist_status,
                                            permalink,
                                            pinned,
                                            pwls,
                                            retrieved_on,
                                            score,
                                            selftext,
                                            send_replies,
                                            spoiler,
                                            stickied,
                                            subreddit,
                                            subreddit_id,
                                            subreddit_subscribers,
                                            subreddit_type,
                                            thumbnail,
                                            title,
                                            total_awards_received,
                                            treatment_tags,
                                            upvote_ratio,
                                            url,
                                            whitelist_status,
                                            wls)
                        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        WHERE NOT EXISTS (SELECT id 
                                          FROM author_submissions 
                                          WHERE id = %s)
                        """
            list_ = [submissions['all_awardings'].iloc[row],
                    submissions['allow_live_comments'].iloc[row],
                    submissions['author'].iloc[row],
                    submissions['author_flair_css_class'].iloc[row],
                    submissions['author_flair_richtext'].iloc[row],
                    submissions['author_flair_text'].iloc[row],
                    submissions['author_flair_type'].iloc[row],
                    submissions['author_fullname'].iloc[row],
                    submissions['author_patreon_flair'].iloc[row],
                    submissions['author_premium'].iloc[row],
                    submissions['awarders'].iloc[row],
                    submissions['can_mod_post'].iloc[row],
                    submissions['contest_mode'].iloc[row],
                    submissions['created_utc'].iloc[row],
                    submissions['domain'].iloc[row],
                    submissions['full_link'].iloc[row],
                    submissions['id'].iloc[row],
                    submissions['is_crosspostable'].iloc[row],
                    submissions['is_meta'].iloc[row],
                    submissions['is_original_content'].iloc[row],
                    submissions['is_reddit_media_domain'].iloc[row],
                    submissions['is_robot_indexable'].iloc[row],
                    submissions['is_self'].iloc[row],
                    submissions['is_video'].iloc[row],
                    submissions['link_flair_background_color'].iloc[row],
                    submissions['link_flair_richtext'].iloc[row],
                    submissions['link_flair_text_color'].iloc[row],
                    submissions['link_flair_type'].iloc[row],
                    submissions['locked'].iloc[row],
                    submissions['media_only'].iloc[row],
                    submissions['no_follow'].iloc[row],
                    submissions['num_comments'].iloc[row],
                    submissions['num_crossposts'].iloc[row],
                    submissions['over_18'].iloc[row],
                    submissions['parent_whitelist_status'].iloc[row],
                    submissions['permalink'].iloc[row],
                    submissions['pinned'].iloc[row],
                    submissions['pwls'].iloc[row],
                    submissions['retrieved_on'].iloc[row],
                    submissions['score'].iloc[row],
                    submissions['selftext'].iloc[row],
                    submissions['send_replies'].iloc[row],
                    submissions['spoiler'].iloc[row],
                    submissions['stickied'].iloc[row],
                    submissions['subreddit'].iloc[row],
                    submissions['subreddit_id'].iloc[row],
                    submissions['subreddit_subscribers'].iloc[row],
                    submissions['subreddit_type'].iloc[row],
                    submissions['thumbnail'].iloc[row],
                    submissions['title'].iloc[row],
                    submissions['total_awards_received'].iloc[row],
                    submissions['treatment_tags'].iloc[row],
                    None, # submissions['upvote_ratio'].iloc[row],
                    submissions['url'].iloc[row],
                    submissions['whitelist_status'].iloc[row],
                    submissions['wls'].iloc[row],                               
                    submissions['id'].iloc[row]]
            cursor = connection.cursor()
            cursor.execute(query, list_)
            connection.commit()            
        except Exception as e:
            connection.rollback()
            print('Could not insert', submissions['url'].iloc[row], e)
    
def updateAuthorSubmissionsTable(connection, refresh_or_add):
    
    old_row_count = pd.read_sql(""" SELECT count(*) FROM author_submissions """, connection)
    
    if refresh_or_add == 'add':
        new_authors = pd.read_sql(""" SELECT DISTINCT author 
                                      FROM borrow_submissions
                                      WHERE 1 = 1
                                      AND LEFT(title, 5) = '[REQ]'
                                      AND author != '[deleted]'
                                      AND author NOT IN (SELECT DISTINCT author from author_submissions)
                                  """, connection)
        
        for auth in new_authors['author']:
            submissions = getNewAuthorSubmissions(author = auth)
            submissions = submissions.applymap(str)
            insertAuthorSubmissions(submissions = submissions, connection = connection)
            
    # elif refresh_or_add == 'refresh':
    
    new_row_count = pd.read_sql("""SELECT count(*) FROM author_submissions """, connection)

    return(new_row_count['count'].iloc[0] - old_row_count['count'].iloc[0])
    
def getPushShiftCommentData(**kwargs):
    r = requests.get('https://api.pushshift.io/reddit/search/comment/', params = kwargs)
    data = json.loads(r.text)
    return pd.json_normalize(data['data'])

def updateLoansBotTable(new_or_old, request_times, connection):

    old_row_count = pd.read_sql(""" SELECT count(*) FROM loans_bot """, connection)
        
    for i in range(request_times):
    
        if new_or_old == 'new':
            last_post = pd.read_sql(""" SELECT max(created_utc) FROM loans_bot""", connection)
            last_post = last_post['max'].iloc[0]
            comments = getPushShiftCommentData(author = 'LoansBot', size = '500', after = last_post)
            time.sleep(2.1)
        elif new_or_old == 'old':
            last_post = pd.read_sql(""" SELECT min(created_utc) FROM loans_bot""", connection)
            last_post = last_post['min'].iloc[0]
            comments = getPushShiftCommentData(author = 'LoansBot', size = '500', before = last_post)
            time.sleep(2.1)
        
        comments = comments.applymap(str)
        
        for row in range(len(comments)):
            try:
                query = """ INSERT into loans_bot(
                                all_awardings,
                                associated_award,
                                author,
                                author_flair_background_color,
                                author_flair_css_class,
                                author_flair_richtext,
                                author_flair_template_id,
                                author_flair_text,
                                author_flair_text_color,
                                author_flair_type,
                                author_fullname,
                                author_patreon_flair,
                                author_premium,
                                awarders,
                                body,
                                collapsed_because_crowd_control,
                                comment_type,
                                created_utc,
                                id,
                                is_submitter,
                                link_id,
                                locked,
                                no_follow,
                                parent_id,
                                permalink,
                                retrieved_on,
                                score,
                                send_replies,
                                stickied,
                                subreddit,
                                subreddit_id,
                                top_awarded_type,
                                total_awards_received,
                                treatment_tags)
                        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                        WHERE NOT EXISTS (SELECT id 
                                          FROM loans_bot 
                                          WHERE id = %s)
                        """
                list_ = [   comments['all_awardings'].iloc[row],
                            comments['associated_award'].iloc[row],
                            comments['author'].iloc[row],
                            comments['author_flair_background_color'].iloc[row],
                            comments['author_flair_css_class'].iloc[row],
                            comments['author_flair_richtext'].iloc[row],
                            comments['author_flair_template_id'].iloc[row],
                            comments['author_flair_text'].iloc[row],
                            comments['author_flair_text_color'].iloc[row],
                            comments['author_flair_type'].iloc[row],
                            comments['author_fullname'].iloc[row],
                            comments['author_patreon_flair'].iloc[row],
                            None, # comments['author_premium'].iloc[row],
                            comments['awarders'].iloc[row],
                            comments['body'].iloc[row],
                            comments['collapsed_because_crowd_control'].iloc[row],
                            None, # comments['comment_type'].iloc[row],
                            comments['created_utc'].iloc[row],
                            comments['id'].iloc[row],
                            comments['is_submitter'].iloc[row],
                            comments['link_id'].iloc[row],
                            comments['locked'].iloc[row],
                            comments['no_follow'].iloc[row],
                            comments['parent_id'].iloc[row],
                            comments['permalink'].iloc[row],
                            comments['retrieved_on'].iloc[row],
                            comments['score'].iloc[row],
                            comments['send_replies'].iloc[row],
                            comments['stickied'].iloc[row],
                            comments['subreddit'].iloc[row],
                            comments['subreddit_id'].iloc[row],
                            None, # comments['top_awarded_type'].iloc[row],
                            comments['total_awards_received'].iloc[row],
                            None, # comments['treatment_tags'].iloc[row],
                            comments['id'].iloc[row]]
                cursor = connection.cursor()
                cursor.execute(query, list_)
                connection.commit()            
            except Exception as e:
                connection.rollback()
                print('Could not insert', comments['permalink'].iloc[row], e)
               
    new_row_count = pd.read_sql("""SELECT count(*) FROM loans_bot """, connection)

    return(new_row_count['count'].iloc[0] - old_row_count['count'].iloc[0])