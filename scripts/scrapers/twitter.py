import asyncio
import os
import sys
import csv
import datetime
from datetime import datetime,date, timedelta
from statistics import mean

import tweepy
from dateutil.relativedelta import relativedelta

from scripts.scraper_interface import Scraper
from scripts.utils.scraper_utils import get_proxies, get_random_scraper_data
from scripts.utils.mylogger import mylogger
from scripts.ETL.checkpoint import Checkpoint
from config.checkpoint import checkpoint_dict
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd

analyser = SentimentIntensityAnalyzer()

logger = mylogger(__file__)

class TwitterLoader(Scraper):

    tweets_dict = {
        'timestamp':[],
        'twitter_mentions':[],
        'twitter_positive': [],
        'twitter_compound':[],
        'twitter_neutral':[],
        'twitter_negative':[],
        'twitter_emojis_positive': [],
        'twitter_emojis_compound': [],
        'twitter_emojis_neutral': [],
        'twitter_emojis_negative': [],
        'twitter_emojis_count':[],
        'twitter_reply_hashtags':[],
        'twitter_replies_from_followers': [],
        'twitter_replies_from_following': [],
    }

    tweets_dict_groupby = {
        'twitter_mentions': 'sum',
        'twitter_positive': 'mean',
        'twitter_compound': 'mean',
        'twitter_neutral': 'mean',
        'twitter_negative': 'mean',
        'twitter_emojis_positive': 'mean',
        'twitter_emojis_compound': 'mean',
        'twitter_emojis_neutral': 'mean',
        'twitter_emojis_negative': 'mean',
        'twitter_emojis_count': 'sum',
        'twitter_replies_from_followers':'sum',
        'twitter_replies_from_following':'sum',
        'twitter_reply_hashtags':'sum'
    }

    def __init__(self, items_dct):
        Scraper.__init__(self,collection='github')
        self.items = sorted(list(items_dct.keys()))
        self.item_name = self.items[0]
        self.url = 'https://twitter.com/search?q={}&src=typd'

        # checkpointing
        self.scraper_name = 'twitterscraper'
        self.checkpoint_key =  self.scraper_name
        self.key_params = 'checkpoint:' + self.checkpoint_key
        self.checkpoint_column = 'compound'
        self.dct = checkpoint_dict[self.checkpoint_key]
        self.offset = self.initial_date
        self.update_period = 'hourly'
        self.scrape_period = 'history'

    def item_is_up_to_date(self, item_name, timestamp):
        try:
            # nested item name for mongo (if necessary)
            offset = self.get_item_offset(item_name)
            if offset >= timestamp:
                logger.warning('%s up to timestamp offset:yesterday=%s:%s',item_name,offset,timestamp)
                return True
            return False
        except Exception:
            logger.error("item is_up_to_date", exc_info=True)

    def am_i_up_to_date(self, offset_update=None):
        try:

            today = datetime.combine(datetime.today().date(), datetime.min.time())
            if self.update_period == 'monthly':
                today = datetime(today.year, today.month+1, 1, 0, 0, 0)  # get first day of month
                self.reference_date = today - relativedelta(months=1)
            elif self.update_period == 'daily':
                self.reference_date = today - timedelta(days=1)
            elif self.update_period == 'hourly':
                now = datetime.now()
                now = datetime(now.year, now.month, now.day, now.hour, 0, 0)
                now = today
                self.reference_date = now - timedelta(hours=24)


            self.offset_update = offset_update
            if offset_update is not None:
                self.offset, self.offset_update = self.reset_offset(offset_update)
                # first check if the ETL table is up to timestamp
                logger.warning('my max date:%s', self.offset)
                offset = self.offset
                if isinstance(self.offset, date):
                    offset = datetime.combine(self.offset, datetime.min.time())

                if offset < self.reference_date:
                    return False
                else:
                    return True
            else:
                counter = 0
                for item in self.items:
                    item_name = item +'.'+self.checkpoint_column
                    if self.item_is_up_to_date(item_name,self.reference_date):
                        counter += 1
                if counter >= len(self.items):
                    return True
                else:
                    return False

        except Exception:
            logger.error('am i up to timestamp', exc_info=True)

    def process_item(self,items_to_save, item_name):
        try:
            for idx, item in enumerate(items_to_save['timestamp']):
                if isinstance(item,str):
                    item = datetime.strptime(item, self.DATEFORMAT)
                #logger.warning('item before save:%s',item)
                for col in list(items_to_save.keys()):
                    #logger.warning('col:%s', col)
                    if col != 'timestamp':
                        if col in ['month','day','year','hour']:
                            nested_search = col
                        else:
                            nested_search = item_name+'.'+col
                        #logger.warning('timestamp:%s',item)
                        #logger.warning('%s:%s:', nested_search,items_to_save[col][idx])

                        self.pym.db[self.collection].update_one(
                        {'timestamp': items_to_save['timestamp'][idx]},
                            {'$set':
                                 {
                                   nested_search:items_to_save[col][idx]
                                 }
                            },
                            upsert=True)


            #logger.warning("%s item added to MongoDB database!",format(self.item_name))
        except Exception:
            logger.error('process item', exc_info=True)

    def int_to_datetime(self, timestamp):
        try:
            if isinstance(timestamp,str):
                timestamp = int(timestamp)
            tmp = datetime.fromtimestamp(timestamp)
            return datetime(tmp.year,tmp.month,tmp.day,tmp.hour,0,0)
        except Exception:
            logger.error('int to datetime', exc_info=True)
        
    def rename_items(self,items_dct):
        try:
            rename = {
                'aion': 'aion_network'
            }
            items_dict = items_dct.copy()
            for key, value in rename.items():
                if key in items_dict.keys():
                    items_dict[value] = items_dict.pop(key)
                    if items_dict[value] == key:
                        items_dict[value] = value
            return items_dct
        except Exception:
            logger.error('rename items',exc_info=True)

    def sentiment_analyzer_scores(self,sentence,tweets_dict):
        try:
            #logger.warning('text being analyzed:%s', sentence)
            score = analyser.polarity_scores(sentence)
        except Exception:
            score = {
                'pos':0,
                'neg':0,
                'new':0,
                'compound':0
            }
            logger.warning('sentiment analyzer failed')

        return score

    def extract_data_from_tweet(self,li,tweets_dict):
        try:
            # get data mentions
            div = li.find('div',attrs={'class':'tweet'})
            #logger.warning('div:%s',div)
            try:
                tmp = div['data-mentions'].split(' ')
                tmp_len = len(tmp)
            except Exception:
                tmp_len = 0
                logger.warning('data mentions does not exist for this tweet')
            tweets_dict['twitter_mentions'].append(tmp_len)

            # text
            p = li.find('p',attrs={'class':'js-tweet-text'})
            #logger.warning('tweet text:%s', p.text)
            
            score = self.sentiment_analyzer_scores(p.text,tweets_dict)
            tweets_dict['twitter_positive'].append(score['pos'])
            tweets_dict['twitter_negative'].append(score['neg'])
            tweets_dict['twitter_neutral'].append(score['neu'])
            tweets_dict['twitter_compound'].append(score['compound'])

            # count hastags
            try:
                a_vec = p.find('a',attrs={'class':'twitter-hashtag'})
                if a_vec is not None:
                    tweets_dict['twitter_reply_hashtags'].append(len(a_vec))
                else:
                    tweets_dict['twitter_reply_hashtags'].append(0)
            except Exception:
                tweets_dict['twitter_reply_hashtags'].append(0)
                logger.warning('reply hastags failed')


            # replies from followers, followed
            tmp_div = li.find('div',attrs={'class','js-stream-tweet'})
            if tmp_div['data-you-follow'] == 'false':
                tweets_dict['twitter_replies_from_following'].append(0)
            else:
                tweets_dict['twitter_replies_from_following'].append(1)

            if tmp_div['data-follows-you'] == 'false':
                tweets_dict['twitter_replies_from_followers'].append(0)
            else:
                tweets_dict['twitter_replies_from_followers'].append(1)

            # emojis
            tweets_dict = self.emoji_summary(p, tweets_dict)

            return tweets_dict

        except Exception:
            logger.error('sentiment analyzer', exc_info=True)

    def emoji_summary(self, p, tweets_dict):
        try:
            temp_dict = {
                'twitter_emojis_positive': [],
                'twitter_emojis_neutral': [],
                'twitter_emojis_negative': [],
                'twitter_emojis_compound': [],
                'twitter_emojis_count':[]
            }
            emojis = p.find_all('img', attrs={'class': 'Emoji'})
            logger.warning('emojis:%s',emojis)
            if len(emojis) > 0:
                for emoji in emojis:
                    logger.warning('LINE 235, emoji:%s',emoji['title'])
                    score = self.sentiment_analyzer_scores(emoji['title'],temp_dict)
                    temp_dict['twitter_emojis_positive'].append(score['pos'])
                    temp_dict['twitter_emojis_negative'].append(score['neg'])
                    temp_dict['twitter_emojis_neutral'].append(score['neu'])
                    temp_dict['twitter_emojis_compound'].append(score['compound'])

                for key in temp_dict.keys():
                    if key != 'twitter_emojis_count':
                        tweets_dict[key].append(round(mean(temp_dict[key]),3))
                    else:
                        tweets_dict[key].append(len(emojis))
                logger.warning('emoji temp dict:%s', temp_dict)
            else:
                logger.warning('Else: NO EMOJIS')
                for key in temp_dict.keys():
                    tweets_dict[key].append(0)
            '''
            except Exception:
                logger.warning('Exception: NO EMOJIS')
                for key in temp_dict.keys():
                    tweets_dict[key].append(0)
            '''

            return tweets_dict

        except Exception:
            logger.error('sentiment analyzer', exc_info=True)

    def general_stats(self, soup,tweets_dict):
        try:
            ul_stats = soup.find('ul', attrs={'class': 'ProfileCardStats-statList'})
            tags = ul_stats.find_all('a')
            # data you block
            for tag in tags:
                # get label
                label = tag.find('span', attrs={'class':'ProfileCardStats-statLabel'}).get_text()
                label = self.rename_dict[label]
                # get values
                span = tag.find('span',attrs={'class':'ProfileCardStats-statValue'})
                value = int(span['data-count'])

                # get rid of , if necesary
                tweets_dict[label].append(value)
            return tweets_dict
        except Exception:
            logger.error('general stats', exc_info=True)

    async def update(self):
        try:
            for self.item_name in self.items:
                item_name = self.item_name + '.' + self.checkpoint_column
                if self.item_is_up_to_date(item_name,self.reference_date):
                    logger.warning('%s financial index is up to date', self.item_name)
                else:
                    # CHECK EACH ITEM
                    offset = self.get_item_offset("FOR TWEETS PER HOUR")

                    url = self.url.format('aion_network')
                    # launch url
                    self.driver.implicitly_wait(30)
                    logger.warning('url loaded:%s', url)
                    self.driver.get(url)
                    await asyncio.sleep(10)

                    soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    #logger.warning('soup:%s', soup)


                    # ----------- COIN STATS get follow, following, tweets
                    tweets_dict = self.tweets_dict.copy()
                    tweets_container = soup.find('ol', attrs={'class': 'stream-items'})
                    for li in tweets_container.find_all('li',attrs={'class':'stream-item'}): # loop through tweets
                        # get datetime
                        div = li.find('div',attrs={'class':'stream-item-header'})
                        span = div.find('span',attrs={'class':'_timestamp'})
                        tweet_timestamp = self.int_to_datetime(span["data-time"])
                        logger.warning('tweet timestamp:%s',tweet_timestamp)
                        if tweet_timestamp <= offset and self.scrape_period != 'history':
                            break
                        # stop when offset/checkpoint is encountered
                        tweets_dict['timestamp'].append(tweet_timestamp)
                        tweets_dict = self.extract_data_from_tweet(li,tweets_dict)

                    for item in tweets_dict.keys():
                        logger.warning('key:length=%s:%s', item, len(tweets_dict[item]))

                    # summarize by the hour
                    df = pd.DataFrame.from_dict(tweets_dict)
                    df = df.groupby(['timestamp']).agg(self.tweets_dict_groupby)
                    df = df.reset_index()
                    item_to_save = df.to_dict('list')
                    #logger.warning('item to save:%s',item_to_save)
                    # CHECKPOINT AND SAVE
                    self.process_item(item_to_save,self.item_name)

                    # PAUSE THE LOADER, SWITCH THE USER AGENT, SWITCH THE IP ADDRESS
                    self.driver.close()  # close currently open browsers
                    self.update_proxy()

        except Exception:
            logger.error('update', exc_info=True)

    async def run(self,offset_update):
        #self.initialize_table()
        """
        --offset up_date takes the form
        offset_update = {
            'start': datetime.strptime('2018-06-20 00:00:00,self.DATEFORMAT'),
            'end': datetime.strptime('2018-08-12 00:00:00,self.DATEFORMAT')
        }
        """
        while True:
            if self.am_i_up_to_date(offset_update):
                logger.warning("%s UP TO DATE- WENT TO SLEEP FOR %s HOURS",self.table,self.window)
                await asyncio.sleep(self.window*60*60)
            else:
                await  asyncio.sleep(1)
            await self.update()
