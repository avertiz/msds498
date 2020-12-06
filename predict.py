from google.cloud import automl_v1beta1 as automl
import pandas as pd

def get_predictions(client, features_df):
    predictions = []
    for index, row in features_df.iterrows():
        inputs = {'author_age': row['author_age'],
                'avg_post_score': row['avg_post_score'],
                'bot_mentions': row['bot_mentions'],
                'num_posts': row['num_posts'],
                'num_posts_borrow': row['num_posts_borrow'],
                'num_subreddits': row['num_subreddits'],
                'self_post_ratio': row['self_post_ratio'],
                'average_loan': row['average_loan']}
        response = client.predict(model_display_name= 'borrowers_20201108060610', inputs=inputs)
        for result in response.payload:
            if result.tables.value == '1.0':
                prediction = [row['author'], result.tables.score]
                predictions.append(prediction)
    predictions = pd.DataFrame(predictions, columns =['author', 'Default Likelihood'])
    return(predictions)