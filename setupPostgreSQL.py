import psycopg2
import pandas as pd

def createBorrowSubmissionTable(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(
            """CREATE TABLE borrow_submissions(
            all_awardings                           text,
            allow_live_comments                     text,
            author                                  text,
            author_flair_css_class                  text,
            author_flair_richtext                   text,
            author_flair_text                       text,
            author_flair_type                       text,
            author_fullname                         text,
            author_patreon_flair                    text,
            author_premium                          text,
            awarders                                text,
            can_mod_post                            text,
            contest_mode                            text,
            created_utc                             text,
            domain                                  text,
            full_link                               text,
            id                                      text,
            is_crosspostable                        text,
            is_meta                                 text,
            is_original_content                     text,
            is_reddit_media_domain                  text,
            is_robot_indexable                      text,
            is_self                                 text,
            is_video                                text,
            link_flair_background_color             text,
            link_flair_richtext                     text,
            link_flair_text_color                   text,
            link_flair_type                         text,
            locked                                  text,
            media_only                              text,
            no_follow                               text,   
            num_comments                            text,
            num_crossposts                          text,
            over_18                                 text,
            parent_whitelist_status                 text,
            permalink                               text,
            pinned                                  text,
            pwls                                    text,
            retrieved_on                            text,
            score                                   text,
            selftext                                text,
            send_replies                            text,
            spoiler                                 text,
            stickied                                text,
            subreddit                               text,
            subreddit_id                            text,
            subreddit_subscribers                   text,
            subreddit_type                          text,
            thumbnail                               text,
            title                                   text,
            total_awards_received                   text,
            treatment_tags                          text,
            upvote_ratio                            text,
            url                                     text,
            whitelist_status                        text,
            wls                                     text)"""
            )
        connection.commit()
        columns = pd.read_sql("""SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'borrow_submissions'""",
                          connection)
        print('borrow_submissions columns:', columns)
    except:
        print('Falied to create borrow_submissions table')
        connection.rollback()
        connection.close()
        
def createLoansBotTable(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(
            """CREATE TABLE loans_bot(
                                all_awardings	                text,
                                associated_award	            text,
                                author	                        text,
                                author_flair_background_color	text,
                                author_flair_css_class	        text,
                                author_flair_richtext	        text,
                                author_flair_template_id	    text,
                                author_flair_text	            text,
                                author_flair_text_color         text,
                                author_flair_type	            text,
                                author_fullname	                text,
                                author_patreon_flair	        text,
                                author_premium	                text,
                                awarders	                    text,
                                body	                        text,
                                collapsed_because_crowd_control	text,
                                comment_type	                text,
                                created_utc	                    text,
                                id	                            text,
                                is_submitter	                text,
                                link_id	                        text,
                                locked	                        text,
                                no_follow	                    text,
                                parent_id	                    text,
                                permalink	                    text,
                                retrieved_on	                text,
                                score	                        text,
                                send_replies	                text,
                                stickied	                    text,
                                subreddit                   	text,
                                subreddit_id	                text,
                                top_awarded_type	            text,
                                total_awards_received	        text,
                                treatment_tags	                text
                                )"""
            )
        connection.commit()
        columns = pd.read_sql("""SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'loans_bot'""",
                          connection)
        print('loans_bot columns:', columns)
    except:
        print('Falied to create loans_bot table')
        connection.rollback()
        connection.close()