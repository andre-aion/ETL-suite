import nltk
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import praw
import plotly.graph_objs as go
from multiprocessing.pool import ThreadPool
from pdb import set_trace

reddit = praw.Reddit(client_id='IDdPUTJQ2Y9XdA', \
                         client_secret='X-Eds7kMJe0nDKcR6MRwjt9LzGw', \
                         user_agent='amdatt_scraper', \
                         username='qrctrini', \
                         password='C@ppadre5')

companies = ["Aion", "Bitcoin", "Cardano", "Litecoin"]
subreddit_list = ['BlockChain', 'CryptoCurrency', 'CryptoCurrencies',
                  'altcoin', 'icocrypto', 'Crypto_Currency_News']
sia = SIA()

def sentiment_analysis(s):
    pol_score = sia.polarity_scores(s)
    # print(pol_score)
    return pol_score['compound'], \
           pol_score['neg'], \
           pol_score['neu'], \
           pol_score['pos']


def search_subreddit(sub, topic, topics_dict):
    topics_dict = {"subreddit": [],
                   "title": [],
                   "score": [],
                   "id": [],
                   "comms_num": [],
                   "created": [],
                   "body": []}
    #for sub in subreddit_list:
    for submission in reddit.subreddit(sub).search(topic.lower(), limit=1000):
        topics_dict["subreddit"].append(sub)
        topics_dict["title"].append(submission.title)
        topics_dict["score"].append(submission.score)
        topics_dict["id"].append(submission.id)
        topics_dict["comms_num"].append(submission.num_comments)
        topics_dict["created"].append(submission.created)
        topics_dict["body"].append(submission.selftext)

    return topics_dict


def get_sentiment(topic):
    sorting_method = 'comments'

    # define blockchain reddits as a set so we don't get duplicates
    topics_dict = {"subreddit": [],
                   "title": [],
                   "score": [],
                   "id": [],
                   "comms_num": [],
                   "created": [],
                   "body": []}

    pool0 = ThreadPool(len(subreddit_list))
    threads0 = []
    '''
    for sub in subreddit_list:
        for submission in reddit.subreddit(sub).search(topic.lower(), limit=1000):
            topics_dict["subreddit"].append(sub)
            topics_dict["title"].append(submission.title)
            topics_dict["score"].append(submission.score)
            topics_dict["id"].append(submission.id)
            topics_dict["comms_num"].append(submission.num_comments)
            topics_dict["created"].append(submission.created)
            topics_dict["body"].append(submission.selftext)
    '''
    for sub in subreddit_list:
        threads0.append(pool0.apply_async(search_subreddit, (sub,topic,topics_dict)))

    for thread in threads0:
        topics_dict.update(thread.get())


    df = pd.DataFrame(topics_dict)
    df['timestamp'] = pd.to_datetime(df['created'], unit='s')

    df = df.sort_values(by='timestamp').reset_index()

    df['compound'], df['negative'], df['neutral'], df['positive'] = zip(*df['title'].apply(sentiment_analysis))

    df['date'] = df['timestamp'].dt.date
    df.index = df['timestamp']
    df_summary = pd.DataFrame()
    period = 'M'
    df_summary['compound'] = df['compound'].resample(period).mean()
    df_summary['negative'] = df['negative'].resample(period).mean()
    df_summary['neutral'] = df['neutral'].resample(period).mean()
    df_summary['positive'] = df['positive'].resample(period).mean()

    return df_summary

def all_sentiments(topic):
    df = get_sentiment(topic.lower())
    traces = []
    col_list = ['compound','negative','neutral', 'positive']
    for col in col_list:
        traces.append(go.Scatter(
            x=df.index,
            y=df[col],
            mode='lines',
            name=col
        )
        )
    return traces

def get_company_sentiment(company,sentiment):
    df={}
    df[company]=get_sentiment(company)
    return go.Scatter(
                x=df[company].index,
                y=df[company][sentiment],
                mode='lines',
                name=company
            )


def compare_sentiment(sentiment):
    traces = []
    threads = []
    pool = ThreadPool(len(companies))
    for ii in range(len(companies)):
        threads.append(pool.apply_async(get_company_sentiment, (companies[ii],sentiment)))

    #check thread return value
    for thread in threads:
        traces.append(thread.get())
    pool.close()
    return traces


'''    
def compare_sentiment(sentiment):
    companies = ["Aion","Bitcoin","Cardano","Litecoin"]
    df = {}
    traces = []
    for company in companies:
        df[company] = get_sentiment(company)
        traces.append(go.Scatter(
            x=df[company].index,
            y=df[company][sentiment],
            mode='lines',
            name=company
        )
        )
    return traces
'''