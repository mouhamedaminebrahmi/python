from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from time import sleep
import json
import datetime
import sys
import os
import math
import glob
import csv
import zipfile
import zlib
from tweepy import TweepError
import twitter as t
import time
import csv
from tweepy.auth import OAuthHandler
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

import tweepy
import json
import math
import glob
import csv
import zipfile
import zlib
from tweepy import TweepError
from time import sleep
from pprint import pprint



def saisir_date():
	print ("Date (JJ/MM/AAAA) :")
	sdate = input()
	while(len(sdate) !=10):
		print( "Veuillez respecter les 8 caracteres demandes")
		print ("Date (JJ/MM/AAAA) :" )
		sdate = input()
	while(sdate[2] != '/' or sdate[5] != '/'):
		#controle des nombres jours et mois
		print ("Veuillez respecter le format date JJ/MM/AAAA requis")
		print ("Date (JJ/MM/AAAA) : " )
		sdate = input()
	
# verifier la saisie de chiffres
	[sjour,smois,sannee] = sdate.split("/")
	try :
		Deb = datetime.datetime(int(sannee), int(smois), int(sjour))  
		return Deb
	except ValueError as ExceptionDate : 
		print (" ----:) Erreur de saisie : Corrigez SVP " , ExceptionDate )
		return -1
#print(end)

start=-1
while(start==-1):
	start=saisir_date()
print("Date Debut Scrap :",start)
print(" - - - - Date Fin SCRAP -------- ")
end=-1
while(end==-1):
	end=saisir_date()
print ("Date Fin Scrap :",end)


keywords =input( 'Fournissez votre keywords : ')
print("Keywords ", keywords)

delay = 1  # time to wait on each page load before reading the page
driver = webdriver.Firefox()  # options are Chrome() Firefox() Safari()

twitter_ids_filename = 'all_ids.json'
days = (end - start).days + 1
id_selector = '.time a.tweet-timestamp'
tweet_selector = 'li.js-stream-item'
keywords = keywords.lower()
ids = []



def format_day(date):
    day = '0' + str(date.day) if len(str(date.day)) == 1 else str(date.day)
    month = '0' + str(date.month) if len(str(date.month)) == 1 else str(date.month)
    year = str(date.year)
    return '-'.join([year, month, day])

def form_url(since, until):
    p1 = 'https://twitter.com/search?f=tweets&vertical=default&q=%3A'
    p2 =  keywords + '%20since%3A' + since + '%20until%3A' + until + 'include%3Aretweets&src=typd'
    return p1 + p2

def increment_day(date, i):
    return date + datetime.timedelta(days=i)

	
def  brahmi_fork(start, brahmi_fork_day,w):
    print('Start')
    driver = webdriver.Firefox()	
    for day in range(brahmi_fork_day):
        d1 = format_day(increment_day(start, 0))
        d2 = format_day(increment_day(start, 1))
        url = form_url(d1, d2)
        print(url)
        print(d1)
        driver.get(url)
        sleep(delay)

        try:
            found_tweets = driver.find_elements_by_css_selector(tweet_selector)
            increment = 10

            while len(found_tweets) >= increment:
                print('scrolling down to load more tweets')
                driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                sleep(delay)
                found_tweets = driver.find_elements_by_css_selector(tweet_selector)
                increment += 10

            print('{} tweets found, {} total'.format(len(found_tweets), len(ids)))

            w = os.fdopen(w, 'w')

            for tweet in found_tweets:
                try:
                    id = tweet.find_element_by_css_selector(id_selector).get_attribute('href').split('/')[-1]
                    w.write(id+' ')
                    ids.append(id)
                except StaleElementReferenceException as e:
                    print('lost element reference', tweet)

        except NoSuchElementException:
            print('no tweets on this day')

        start = increment_day(start, 1)


    try:
        with open(twitter_ids_filename) as f:
            all_ids = ids + json.load(f)
            data_to_write = list(set(all_ids))
            print('tweets found on this scrape: ', len(ids))
            print('total tweet count: ', len(data_to_write))
    except FileNotFoundError:
        with open(twitter_ids_filename, 'w') as f:
            all_ids = ids
            data_to_write = list(set(all_ids))
            print('tweets found on this scrape: ', len(ids))
            print('total tweet count: ', len(data_to_write))

    with open(twitter_ids_filename, 'w') as outfile:
        json.dump(data_to_write, outfile)




print('all done here')
driver.close()



r, w = os.pipe()

brahmi_fork_day=round(days/4)
for fk in range (1,4):
    pid = os.fork()
    start=increment_day(start,brahmi_fork_day)    
    if pid == 0:
        brahmi_fork(start,brahmi_fork_day,w)
        exit()
    else:
        print('Père')
        	
for i in range(fk):
    finished = os.waitpid(0, 0)
    print(finished)



os.close(w)
r = os.fdopen(r)
print ("Parent reading")
str = r.read()
print ("text =", str)


# CHANGE THIS TO THE USER YOU WANT
user = 'tweet'
consumer_key = "qbarY1OeOBcsTudKdiqh1KxO7"
consumer_secret = "1q9GwYAjHQMDfrH6i2Tapy70ykzFSwS2UwQhgN6iyJhP6nB0tw"
access_token = "2345698221-2uEqzSxaoEa4cfDRL7puYdWlZTPIvSj64yTcxUz"
access_token_secret = "FI0PlGGnMZnpz4OmDbQW4f44PCsOXcp2f0o5IvYC1LHyF"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

user = user.lower()
output_file = '{}.json'.format(user)
output_file_short = '{}_short.json'.format(user)
compression = zipfile.ZIP_DEFLATED

with open('all_ids.json') as f:
    ids = json.load(f)

print('total ids: {}'.format(len(ids)))

all_data = []
start = 0
end = 100
limit = len(ids)
i = math.ceil(limit / 100)

for go in range(i):
    print('currently getting {} - {}'.format(start, end))
    sleep(6)  # needed to prevent hitting API rate limit
    id_batch = ids[start:end]
    start += 100
    end += 100
    tweets = api.statuses_lookup(id_batch)
    for tweet in tweets:
        all_data.append(dict(tweet._json))

print('metadata collection complete')
print('creating master json file')
with open(output_file, 'w') as outfile:
    json.dump(all_data, outfile)

print('creating ziped master json file')
zf = zipfile.ZipFile('{}.zip'.format(user), mode='w')
zf.write(output_file, compress_type=compression)
zf.close()

results = []

def is_retweet(entry):
    return 'retweeted_status' in entry.keys()

def get_source(entry):
    if '<' in entry["source"]:
        return entry["source"].split('>')[1].split('<')[0]
    else:
        return entry["source"]

with open(output_file) as json_data:
    data = json.load(json_data)
    for entry in data:
        t = {
            "created_at": entry["created_at"],
            "text": entry["text"],
            "in_reply_to_screen_name": entry["in_reply_to_screen_name"],
            "retweet_count": entry["retweet_count"],
            "favorite_count": entry["favorite_count"],
            "source": get_source(entry),
            "id_str": entry["id_str"],
            "is_retweet": is_retweet(entry)
        }
        results.append(t)

print('creating minimized json master file')
with open(output_file_short, 'w') as outfile:
    json.dump(results, outfile)

data = json.load(open('tweet.json'))
data1 = json.load(open('tweet_short.json'))
pprint(data)
pprint(data1)

mydb = MySQLdb.connect(host='localhost',
    user='brahmi',
    passwd='root',
    db='tweet')
cursor = mydb.cursor()
traffic = json.load(open('tweet_short.json'))

query = "insert into medicoes values (?,?,?,?,?,?,?)"
columns = ['list_text', 'list_is_retweet', 'list_retweet_count', 'list_in_reply_to_screen_name', 'list_id_str', 'list_source','list_created_at','list_favorite_count']
for timestamp, data in traffic.iteritems():
    keys = (timestamp,) + tuple(data[c] for c in columns)
    c = db.cursor()
    c.execute(query, keys)
    c.close()
