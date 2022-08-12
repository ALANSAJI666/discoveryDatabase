import openpyxl
import schedule
from schedule import every, repeat, run_pending
# from API_backend import search_keyword
import schedule
import csv
import logging
from functools import wraps
from flask import Flask,request,jsonify
from flask_restful import Api, Resource
import json
import threading
import functools
import multiprocessing
import time
import threading
from threading import Timer
import uuid
import datetime
from datetime import date
from datetime import timedelta
import pandas as pd
# import mysqldb
import mysql.connector
import os
from statistics import median, mean
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from bs4 import BeautifulSoup
from scrapper import YTstats
# API_KEY_list = ['AIzaSyCji9HQ0Q__eJkV9zqq2rop-vGZBiYQUpo','AIzaSyBJhzfLbEHhs-aOcVsB32PL-IvsR3r5yFI']
# API_KEY = 'AIzaSyBhTFcHvpvSch0j-k37QzZIx6ontTHsfBg'
# API_KEY = 'AIzaSyB6Q5PKF3sc0BqjbIju8UPN0xzYBXDr2IE'
# API_KEY = 'AIzaSyDzZMVp203AxREJyObVQRHcxuwKmLA-8OE'
# API_KEY = 'AIzaSyC7aNzK4YmgKozr3DqUTAXv_abWm4JgC_0'
# API_KEY = 'AIzaSyDfc-BRNTWiF314m3ejTXyGv8tn-14iqp8'
# API_KEY= 'AIzaSyDfc-BRNTWiF314m3ejTXyGv8tn-14iqp8'
# API_KEY = "AIzaSyBuaXkTas_ivhGRkXC1L2zF-93OdSyQKBM"
# 'AIzaSyDWdIKzYBoW6PvY2pt2S10vP5ZKOm-ANnc'
# 'AIzaSyBH2QrXX1kRObjo5IN2ay13I-y-to0nru8'
# API_KEY = 'AIzaSyC7aNzK4YmgKozr3DqUTAXv_abWm4JgC_0'
# API_KEY_list= ['AIzaSyDWdIKzYBoW6PvY2pt2S10vP5ZKOm-ANnc', 'AIzaSyBH2QrXX1kRObjo5IN2ay13I-y-to0nru8', 'AIzaSyC7aNzK4YmgKozr3DqUTAXv_abWm4JgC_0' ,'AIzaSyDfc-BRNTWiF314m3ejTXyGv8tn-14iqp8', 'AIzaSyDfc-BRNTWiF314m3ejTXyGv8tn-14iqp8' ]
# API_KEY = API_KEY_list[0]

app = Flask(__name__)

@app.route('/')
def index():
    return 'Hello!'
#
# api_list2 = []
# with open('api_key_list - Sheet1.csv', 'r') as api_list_csv:
#     csv_reader = csv.reader(api_list_csv)
#     print(csv_reader)
#     for line in csv_reader:
#         api_list2.append(line[1])
#         print(line[1])
#     api_list2 = api_list2[1:]
#
#     api_k =  api_list2[0]
# api_list_csv.close()
#
#
# with open('api_key_list - Sheet1.csv', 'r') as api_list_csv:
#     csv_writer = csv.writer(api_list_csv)
#     # csv_writer.writefield()


logging.basicConfig(filename = 'exc_logger.log', level= logging.INFO,
                    format =  '%(asctime)s:%(levelname)s:%(message)s')
i = 0




i=0
run = True







def create_logger():
    # create a logger object

    logger = logging.getLogger(f'exc_logger-{date.today()}')
    logger.setLevel(logging.INFO)

    # c reate a file to store all the
    # logged exceptions
    logfile = logging.FileHandler(f'exc_logger-{date.today()}.log')

    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    # logfile.setFormatter(formatter)
    # logger.addHandle r(logfile)

    return logger

logger = create_logger()


# you will find a log file
# created in a given path
# print(logger)



# def daily_exc_logger():
#     global logger
#     logger = create_logger()
#     return logger
#
def mysqlConnector():
    try:
        db = mysql.connector.connect(
            host = 'ls-2d68fc7fe891f66db76054864a0b99dbae55e9b5.cxktmwagp6v8.us-east-1.rds.amazonaws.com',
            user = "dbmasteruser",
            passwd = "baron321E!",
            database = "flutchdatabase"

        # flutchDatabase

        )
        mycursor = db.cursor()
    except Exception as err:
            print("No internet connection.")
            # logger.exception(err)

try:
    db = mysql.connector.connect(
        host='ls-2d68fc7fe891f66db76054864a0b99dbae55e9b5.cxktmwagp6v8.us-east-1.rds.amazonaws.com',
        user="dbmasteruser",
        passwd="baron321E!",
        database="flutchdatabase"

        # flutchDatabase

    )
    mycursor = db.cursor()
except Exception as err:
    print("No internet connection.")
    # logger.exception(err)

mycursor.execute('SELECT api_key FROM api_key_table ')
result = mycursor.fetchall()
api_key_list = [i[0] for i in result]
api_key_list_og = api_key_list
last_key = api_key_list[-1]

mycursor.execute('SELECT count FROM api_key_table ')
temp = mycursor.fetchall()
count_list = [i[0] for i in temp]

for i,count_initial in enumerate(count_list):
    if count_initial < 50:
        # API_KEY = api_key_list[i]
        api_list_temp = api_key_list[:i]
        print(api_list_temp)
        api_list_temp.reverse()
        print(api_list_temp)
        api_key_list = api_key_list[i:]
        print(api_key_list)
        api_key_list.extend(api_list_temp)
        print(api_key_list)
        API_KEY = api_key_list[0]
        break










# @repeat(every(1).second)
@repeat(every(40).seconds)
def parallel_runner():
    # API_KEY = api_key_list_og[0]
    # mycursor.execute("update api_key_table set count = 0 where id = 1")
    # api_key_list = api_key_list_og
    # logger = create_logger()
    # dumpAvgViews()
    t1 = threading.Thread(target=dumpAvgViews)
    # t2 = threading.Thread(target=flutch_table_input_receiver)

    # starting thread 1
    t1.start()
    # starting thread 2
    # t2.start()

    # wait until thread 1 is completely executed
    # t1.join()
    # wait until thread 2 is completely executed
    # t2.join()



def exception(logger):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                issue = "exception in "+func.__name__+"\n"
                issue = issue+"=============\n"
                logger.exception(issue)
                # raise
        return wrapper
    return decorator


@exception(logger)
def dumpAvgViews():
    start = time.perf_counter()
    print("Reading Input File...")
    global api_key_list
    global API_KEY
    global last_key
    # 'AIzaSyDQy4inucHSk_D3rUFAYxMwUqCRxbs3RyY'
    # 'AIzaSyC7Z2fEgCTjTRA6s6y73R9uP3b7RKk-7MM'
    # API_KEY_list = ['AIzaSyC7aNzK4YmgKozr3DqUTAXv_abWm4JgC_0','AIzaSyBH2QrXX1kRObjo5IN2ay13I-y-to0nru8','AIzaSyCqGfhwGOva6MYmAu3TopmyV03tkvvb6J8']
    # API_KEY_list= ['AIzaSyClI0cOhDQE2f2BdE9WlMhFfQGVGBgB_UQ','AIzaSyA-YFh-OGkmzmF0FUzeltW8hK5_xJm30Fs', 'AIzaSyC4gTEWj-pwFi96L-KDkDLe_eDynM2Y9Vk']
    # n = 58 * (len(API_KEY_list))


    duration =7

    # time_delt1 = timedelta(days=7)
    # if date < datetime.datetime(2022,6,5):
    #     mycursor.execute('SELECT Link FROM flutch_table ')
    #     result = mycursor.fetchall()
    #     final_result = [i[0] for i in result]
    #     url_list= final_result[:(len(final_result)//7)+1]
    #     final_result= final_result[(len(final_result)//7)+1:]
    # elif date == datetime.datetime(2022,6,5):
    #     mycursor.execute('SELECT Link FROM flutch_table ')
    #     result = mycursor.fetchall()
    #     final_result = [i[0] for i in result]
    #
    # else:
    #     date1 = date.today() -time_delt1
    #     mycursor.execute(f'SELECT Link FROM flutch_table where date="{date1}" ')
    #     result = mycursor.fetchall()
    #     final_result = [i[0] for i in result]
    try:
        mycursor.execute('SELECT Link FROM flutch_table where scraped = 0 LIMIT 10')
    except Exception as err:
        time.sleep(20)
        mysqlConnector()
        mycursor.execute('SELECT Link FROM flutch_table where scraped = 0 LIMIT 10')

    temp_result = mycursor.fetchall()
    final_result = [i[0] for i in temp_result]
    # for result in final_result:
    #     mycursor.execute(f' UPDATE flutch_table SET scraped = 1 where Link = "{result}" ')
    mycursor.execute(f'SELECT user_id FROM flutch_yt_historic_data ; ')
    result_user_id = mycursor.fetchall()
    user_id_list = [k[0] for k in result_user_id]
    if len(final_result)< 10:

        print("yes")
        l = len(final_result)
        r = 10 - l
        print(r)
        final_result1 =[]
        time_delt1 = timedelta(days=7)
        date1 = date.today() - time_delt1
        mycursor.execute(f'SELECT user_id FROM flutch_yt_historic_data where date <="{date1}" limit {r}; ')
        result_id = mycursor.fetchall()
        final_result_id = [k[0] for k in result_id]

        for id in final_result_id:
            mycursor.execute(f'SELECT Link FROM flutch_table where id_bin = "{id}" ')
            result2 = mycursor.fetchall()
            result3 = [k[0] for k in result2]
            final_result1.extend(result3)



        # result1 = mycursor.fetchall()
        # final_result1 = [i[0] for i in result1]

        final_result.extend(final_result1)








    # mycursor.execute('SELECT Link FROM flutch_table ')
    # result = mycursor.fetchall()
    # final_result = [i[0] for i in result]





#
#     # print(final_result)
    channel_ids = getChannelIDs(final_result)
    channel_data_list = []

    i = 0
    t = 0
    x = 0
    invalid_url=[]
    # API_KEY = API_KEY_list[0]
    # print(API_KEY)
    # mycursor.execute('SELECT id_bin FROM flutch_table LIMIT 2,3 ')
    # result_id = mycursor.fetchall()
    # final_result_id = [i[0] for i in result_id]



    print("Getting Channel Statistics...")
    url_dict = {final_result[i]: channel_ids[i] for i in range(len(final_result))}
    for url, channel_id in url_dict.items():
        # t += 1
        # x += 1
        # print(x)
        #
        # if t == 58:
        #     t = 0
        #     # i += 1
        #     temp = API_KEY_list.pop(0)
        #     API_KEY_list= API_KEY_list.append(temp)
        #     API_KEY = API_KEY_list[0]
        #     print(API_KEY)
        mycursor.execute(f'SELECT count FROM api_key_table where api_key = "{API_KEY}"')
        temp = mycursor.fetchone()
        print(temp)
        x = temp[0]
        x = x + 1
        # if API_KEY == last_key and x ==51:
        #     break
        print(x)
        if x == 51:
            print(api_key_list)
            temp = api_key_list.pop(0)
            api_key_list.append(temp)
            print(api_key_list)
            API_KEY = api_key_list[0]
            print(API_KEY)
            x = 1

        mycursor.execute(f'UPDATE api_key_table SET count ="{x}"   WHERE api_key ="{API_KEY}" ')
        yt = YTstats(API_KEY, channel_id)
        if channel_id != '0':

            yt.get_channel_statistics()
            video_data = yt.get_videos_data()
#             # print(video_data)


            views, likes, comments = [], [], []
            for video_id in video_data:
                views.append(int(video_data[video_id]['views']))

                likes.append(int(video_data[video_id]['likes']))
                comments.append(int(video_data[video_id]['comments']))
            # view_history = {channel_id: views}
            # mycursor.execute('SELECT id_bin FROM flutch_table  ')
            # result_id = mycursor.fetchall()
            # final_result_id = [i[0] for i in result_id]
#             # yt_view_value = [final_result_id[ind], str(date.today()),view_history[channel_id] ]


            # avgViews = findAvg(views)
            # print(avgViews)
            yt.channel_statistics['avgViews'] = findAvg(views)
            yt.channel_statistics['avgLikes'] = findAvg(likes)
            yt.channel_statistics['avgComments'] = findAvg(comments)
            # print(yt.channel_statistics['subscriberCount'])
            if 'subscriberCount' in yt.channel_statistics and yt.channel_statistics['subscriberCount'] != '0':
                yt.channel_statistics['engagementRate'] = (int(yt.channel_statistics['avgLikes']) + int(yt.channel_statistics['avgComments']))/int(yt.channel_statistics['subscriberCount'])
            else:
                yt.channel_statistics['engagementRate'] = 0
                yt.channel_statistics['subscriberCount'] = 0
            if 'viewCount' not in yt.channel_statistics:
                yt.channel_statistics['viewCount'] = 0


            # query1= 'INSERT INTO flutch_table (Avg Views) VALUES (%s)'
            # mycursor.executemany(query1, avgViews)
        else:
            yt.channel_statistics = {'avgViews': 0,'avgLikes':0,'avgComments':0,'engagementRate':0, 'subscriberCount':0, 'viewCount': 0}
        mycursor.execute(f'SELECT id_bin FROM flutch_table WHERE Link = "{url}";')
        result_id = mycursor.fetchall()
        final_result_id = [k[0] for k in result_id]
        #         # print(row)
        id = final_result_id[0]
        mycursor.execute(f'SELECT scraped FROM flutch_table WHERE id_bin = "{id}";')
        temp_scraped = mycursor.fetchall()
        temp2_scraped = [k[0] for k in temp_scraped]
        #         # print(row)
        scraped_value = temp2_scraped[0]
        if scraped_value == 0:
            mycursor.execute(f'UPDATE flutch_table SET scraped = 1 WHERE id_bin = "{id}";')
        print(yt.channel_statistics)
        print(int(yt.channel_statistics['subscriberCount']))
        view_value = [ str(date.today()), int(yt.channel_statistics['subscriberCount'] or 0 ), float(yt.channel_statistics['avgViews']),
                      float(yt.channel_statistics['avgLikes']), float(yt.channel_statistics['avgComments']), float(yt.channel_statistics['engagementRate']),
                      int(yt.channel_statistics['viewCount'] ),id]
        #         avg_view_values.append(int(i_df['viewCount'][ind]))
        print(view_value)
        if id not in user_id_list:
            query3 = "INSERT INTO flutch_yt_historic_data ( date, Subscribers, Avg_Views,Avg_Likes,Avg_Comments,Engagement_rate, Channel_Views, user_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ;"
        else:
            query3 = "UPDATE flutch_yt_historic_data SET date = %s,Subscribers= %s,Avg_Views = %s,Avg_Likes = %s,Avg_Comments = %s, Engagement_rate = %s, Channel_Views = %s where user_id = %s  "
        mycursor.execute(query3, view_value)
        db.commit()
        if float(yt.channel_statistics['avgViews']) == 0.0:
            # invalid_url.append(id)
            mycursor.execute(f'SELECT id_bin FROM flutch_table WHERE Link = "{url}";')
            temp_id = mycursor.fetchall()
            invalid_url_id_list = [k[0] for k in temp_id]
            #         # print(row)
            invalid_url_id = invalid_url_id_list[0]
            print(invalid_url_id_list[0])
            mycursor.execute(f'INSERT INTO invalid_urls_table (invalid_url) VALUES ("{invalid_url_id}") ')
            db.commit()
        finish = time.perf_counter()

        # print(yt.channel_statistics)
        # channel_data_list.append(yt.channel_statistics)

#     i_df = pd.DataFrame(channel_data_list, index=channel_ids)
#     i_df.index.name = 'Channel ID'
#     i_df['Channel URL'] = final_result
#     cols = ["Channel URL", "subscriberCount",  "Category", 'Language',"avgViews","avgLikes", "avgComments", "engagementRate","viewCount",
#             "publishedAt", "videoCount"]
#     i_df = i_df.reindex(columns=cols)
#     i_df = i_df.where(pd.notnull(i_df), None)
#     # print(i_df)
#     avg_view_values = []
#     for i,ind in enumerate(i_df.index):
#         mycursor.execute(f'SELECT id_bin FROM flutch_table WHERE Link = "{i_df["Channel URL"][ind]}";')
#         result_id = mycursor.fetchall()
#         final_result_id = [k[0] for k in result_id]
# #         # print(row)
#         id = final_result_id[0]
#         mycursor.execute(f'SELECT scraped FROM flutch_table WHERE id_bin = "{id}";')
#         temp_scraped = mycursor.fetchall()
#         temp2_scraped = [k[0] for k in temp_scraped]
#         #         # print(row)
#         scraped_value = temp2_scraped[0]
#         if scraped_value == 0:
#             mycursor.execute(f'UPDATE flutch_table SET scraped = 1 WHERE id_bin = "{id}";')
#
#         view_value = [id, str(date.today()),int(i_df['subscriberCount'][ind] or 0),float(i_df['avgViews'][ind]),float(i_df['avgLikes'][ind]),float(i_df['avgComments'][ind]),float(i_df['engagementRate'][ind]),int(i_df['viewCount'][ind] or 0)]
# #         avg_view_values.append(int(i_df['viewCount'][ind]))
#         print(view_value)
#         query3 = "INSERT INTO flutch_yt_historic_data (user_id, date, Subscribers, Avg_Views,Avg_Likes,Avg_Comments,Engagement_rate, Channel_Views) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ;"
#         mycursor.execute(query3,view_value)
#         db.commit()
#         if float(i_df['avgViews'][ind]) == 0.0:
#             invalid_url.append(id)
#         finish = time.perf_counter()
#     print(f'Finished in {round(finish - start,2)}seconds')





@exception(logger)
def findAvg(views):

    df2 = pd.DataFrame(views)
    if len(views)!=0:
        med = median(views)
    Q1 = df2.quantile(0.25)
    Q3 = df2.quantile(0.75)
    IQR = Q3 - Q1
    idx_list = df2.index[((df2 < (Q1 - 1.5 * IQR)) | (df2 > (Q3 + 1.5 * IQR))).any(axis=1)].tolist()

    for idx in idx_list:
        views[idx] = med
    avg =0
    if len(views)!=0:
        avg = mean(views)
    return avg

@exception(logger)
def getChannelIDs(channel_urls):
    channel_ids = []
    for url in channel_urls:
        if '/user' in url:
            url= url.replace('/user', '')


        if '/videos' in url:
            url = url[:-7]
        if '/about' in url:
            url = url[:-6]



        if '/c/' in url:
            # resp = requests.get(url)
            # session = requests.Session()
            # retry = Retry(connect=3, backoff_factor=0.5)
            # adapter = HTTPAdapter(max_retries=retry)
            # session.mount('http://', adapter)
            # session.mount('https://', adapter)
            #
            # resp = session.get(url)
            resp = requests.get(url)
            # resp.end_headers()
            soup = BeautifulSoup(resp.text, 'html.parser')
            try:
                channel_id = soup.select_one('meta[property="og:url"]')['content'].strip('/').split('/')[-1]
            except:
                print(url)
                channel_id = '0'
        else:
            channel_id = url.split('/').pop()
        channel_ids.append(channel_id)
        print(channel_id)

    return channel_ids
# #


def dumpYtHistoricData():
    query3 = "INSERT INTO flutch_yt_historic_data (user_id, date, Subscribers, Avg_Views,Avg_Likes,Avg_Comments,Engagement_rate, Channel_Views) SELECT user_id, date, Subscribers, Avg_Views,Avg_Likes,Avg_Comments,Engagement_rate, Channel_Views FROM flutch_yt_statistics ;"
    mycursor.execute(query3)
    mycursor.execute("DELETE FROM flutch_yt_statistics")
    dumpAvgViews()


@exception(logger)
def flutch_table_dump():

    print("Reading Input File...")
    df = pd.read_excel("flutch_main sheet.xlsx", engine='openpyxl', sheet_name=0)
    invalid_urls = []
    # print(df)
    # df['UID'] =
    # df.fillna(0)
    # 'AIzaSyCji9HQ0Q__eJkV9zqq2rop-vGZBiYQUpo', 'AIzaSyBJhzfLbEHhs-aOcVsB32PL-IvsR3r5yFI','AIzaSyDQy4inucHSk_D3rUFAYxMwUqCRxbs3RyY'
    # 'AIzaSyBIWtD3zxsN8lRdEa2zErwSRWL5cFzwCE8','AIzaSyCWGN3tL2PtGIyQf9MjZg3KyZrb-MEeIkM'
    # API_KEY_list = ['AIzaSyDqsVs69MGVk8fRhT-R69yoFovxZHzlTl0','AIzaSyBH2QrXX1kRObjo5IN2ay13I-y-to0nru8','AIzaSyCYh4H-8lvVw4eYwpRvFS1sgZnIxFodEJE','AIzaSyCqGfhwGOva6MYmAu3TopmyV03tkvvb6J8']
    # API_KEY_list=   ['AIzaSyDs1E9b0whSOk4Qoy1pK0QYnq64caLEz00','AIzaSyCji9HQ0Q__eJkV9zqq2rop-vGZBiYQUpo', 'AIzaSyC7Z2fEgCTjTRA6s6y73R9uP3b7RKk-7MM','AIzaSyC4gTEWj-pwFi96L-KDkDLe_eDynM2Y9Vk','AIzaSyDzZMVp203AxREJyObVQRHcxuwKmLA-8OE','AIzaSyBJhzfLbEHhs-aOcVsB32PL-IvsR3r5yFI','AIzaSyBIWtD3zxsN8lRdEa2zErwSRWL5cFzwCE8', 'AIzaSyCWGN3tL2PtGIyQf9MjZg3KyZrb-MEeIkM','AIzaSyDqsVs69MGVk8fRhT-R69yoFovxZHzlTl0','AIzaSyClI0cOhDQE2f2BdE9WlMhFfQGVGBgB_UQ']
    i=0
    t=0
    x= 0
    # API_KEY = API_KEY_list[i]
    global API_KEY
    global api_key_list
    global last_key
    print(API_KEY)
    df = df.where(pd.notnull(df), None)
    all_values = []
    # 30: 50
    # 17
    # UC - WX23zRiuLfHdVpTAwEcaw
    mycursor.execute('SELECT Link FROM flutch_table ')
    result = mycursor.fetchall()
    final_result = [i[0] for i in result]
    for ind in df.index[3408: 3605]:

        url = [df['CHANNEL URL/videos'][ind]]




        #
        # global i
        # global t
        # t+=1
        # x+=1
        # print(x)
        #
        # if t==65:
        #     t= 0
        #     temp = API_KEY_list.pop(0)
        #     API_KEY_list = API_KEY_list.append(temp)
        #     API_KEY = API_KEY_list[0]
        #     print(API_KEY)
        mycursor.execute(f'SELECT count FROM api_key_table where api_key = "{API_KEY}"')
        temp = mycursor.fetchone()
        print(temp)
        x = temp[0]
        x = x + 1
        # if API_KEY == last_key and x ==51:
        #     return
        print(x)
        if x == 51:
            print(api_key_list)
            temp = api_key_list.pop(0)
            api_key_list.append(temp)
            print(api_key_list)
            API_KEY = api_key_list[0]
            print(API_KEY)
            x = 1

        mycursor.execute(f'UPDATE api_key_table SET count ="{x}"   WHERE api_key ="{API_KEY}" ')
        channel_id = getChannelIDs(url)
        print( channel_id[0])
        yt = YTstats(API_KEY, channel_id[0])

        if channel_id != '0' and url not in final_result:

            yt.get_channel_statistics()
            video_data = yt.get_videos_data()
            # print(yt.channel_statistics)
        # channel_data_list.append(yt.channel_statistics)
        # , yt.channel_statistics['Channel title'],
        # print(row)
        # df['Contact'][ind] = df['Contact'][ind] or 0
        id = str(uuid.uuid1())
        value = [id, str(date.today()) ,yt.channel_statistics['Channel title'],df['CHANNEL URL/videos'][ind],yt.channel_statistics['Language'],yt.channel_statistics['Category'],df['Email'][ind],str(df['Contact'][ind]), str('Excel sheet'), 0]
        print(value)
        query = "INSERT INTO flutch_table (id_bin, date, Channel_name, Link, Language, Category, Email,phone_no ,Source,scraped) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        if yt.channel_statistics['Channel title'] ==None:
            invalid_urls.append(id)


        mycursor.execute(query,value)
        db.commit()
        final_result.append(df['CHANNEL URL/videos'][ind])
        # except KeyError:
        #     invalid_urls.append(df['CHANNEL URL/videos'][ind])
    text = '\n'.join(listitem[0] for listitem in invalid_urls)


        # all_values.append(value)

    # print(all_values)

    # print(df.index)
    # query = "INSERT INTO flutch_table (id_bin, date, Channel_name, Link, Language, Category, Email,phone_no ,Source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    # mycursor.executemany(query, all_values)
    # db.commit()
@exception(logger)
def dump_costs():
    print("Reading Input File...")
    df = pd.read_excel("flutch_daily_database.xlsx", engine='openpyxl', sheet_name=0)
    # print(df)
    # df['UID'] =
    # df.fillna(0)
    df = df.where(pd.notnull(df), None)
    all_price_values = []
    mycursor.execute('SELECT id_bin FROM flutch_table  ')
    result_id = mycursor.fetchall()
    final_result_id = [i[0] for i in result_id]
    id1_list = []
    for ind in df.index:
        url = df['Link'][ind]
        mycursor.execute(f'SELECT id_bin FROM flutch_table WHERE Link = "{url}"')
        result_id = mycursor.fetchall()
        final_result_id = [i[0] for i in result_id]
        if final_result_id != [] :
            id1 =final_result_id[0]
            if id1 not in id1_list:
                id1_list.append(id1)
                price_value = [id1 ,str(date.today()),df['Cost per Integration'][ind],df['Cost per Dedicated'][ind]]
                print(price_value)
                all_price_values.append(price_value)
    price_query = "INSERT INTO flutch_pricings(user_id,date,Cost_per_integration, Cost_per_dedicated)  VALUES (%s,%s,%s,%s);"
    mycursor.executemany(price_query, all_price_values)
    db.commit()

@exception(logger)
def flutch_table_updater(keyword):
    categories = ['', 'Film & Animation', 'Autos & Vehicles', '', '', '', '', '', '', '', 'Music', '', '', '', '',
                  'Pets & Animals', '', 'Sports', 'Short Movies', 'Travel & Events', 'Gaming', 'Videoblogging',
                  'People & Blogs', 'Comedy', 'Entertainment', 'News & Politics', 'How to Beauty & Style', 'Education',
                  'Science & Technology', 'Nonprofits & Activism', 'Movies', 'Anime/Animation', 'Action/Adventure',
                  'Classics', 'Comedy', 'Documentary', 'Drama', 'Family', 'Foreign', 'Horror', 'Sci-Fi/Fantasy', 'Thriller',
                  'Shorts', 'Shows', 'Trailers']

    lang_dict = {'as': 'Assamese', 'bn': 'Bengali', 'bh': 'Bihari', 'en': 'English', 'gu': 'Gujarati', 'hi': 'Hindi',
                 'ks': 'Kashmiri', 'ml': 'Malayalam', 'mr': 'Marathi', 'pa': 'Punjabi', 'ta': 'Tamil', 'te': 'Telugu'}
    global API_KEY
    global api_key_list
    global last_key
    try:
        mycursor.execute('SELECT keyword FROM search_keywords')
    except Exception as err:
        time.sleep(20)
        mysqlConnector()
        mycursor.execute('SELECT keyword FROM search_keywords')
    temp_keyword = mycursor.fetchall()
    keyword_table = [i[0] for i in temp_keyword]
    if keyword in keyword_table:
        return
    keyword_value = [str(uuid.uuid1()), str(date.today()),keyword]
    keyword_query = "INSERT INTO search_keywords (id, date, keyword) VALUES (%s,%s,%s) ;"
    mycursor.execute(keyword_query,keyword_value)
    db.commit()
    invalid_urls= []



    yt = YTstats(API_KEY, 'channel_id')
    channels = yt.getChannels1(keyword)
    print(channels)

    mycursor.execute('SELECT Link FROM flutch_table')
    result = mycursor.fetchall()
    final_result = [i[0] for i in result]
    for channelId in channels.keys():
        print(channels[channelId]['url'])
        print(channelId)
        if channels[channelId]['url'] not in final_result:
            print('yes')
            mycursor.execute(f'SELECT count FROM api_key_table where api_key = "{API_KEY}"')
            temp = mycursor.fetchone()
            print(temp)
            x = temp[0]
            x = x + 1
            # if API_KEY == last_key and x == 51:
            #     return
            print(x)
            if x == 51:
                print(api_key_list)
                temp = api_key_list.pop(0)
                api_key_list.append(temp)
                print(api_key_list)
                API_KEY = api_key_list[0]
                print(API_KEY)
                x = 1

            mycursor.execute(f'UPDATE api_key_table SET count ="{x}"   WHERE api_key ="{API_KEY}" ')
            yt = YTstats(API_KEY, channelId)
            yt.get_channel_statistics()
            video_data = yt.get_videos_data()
            id = str(uuid.uuid1())
            value = [id, str(date.today()),channels[channelId]['title'], channels[channelId]['url'],
                     yt.channel_statistics['Language'],yt.channel_statistics['Category'], str('Scraper'),0]
            print(value)
            query = "INSERT INTO flutch_table (id_bin, date, Channel_name, Link, Language, Category,Source, scraped) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"
            if channels[channelId]['title'] == None:
                invalid_urls.append(id)

            mycursor.execute(query, value)
            db.commit()
            final_result.append(channels[channelId]['url'])
    # text = '\n'.join(listitem[0] for listitem in invalid_urls)
    # logger.write(text)
    # logger.close()


# mycursor.execute("DELETE FROM flutch_yt_statistics")
# db.commit()
# dumpAvgViews()
# flutch_table_dump()

def scheduler1():

    global i

    # dumpAvgViews()

    # i += 10

    while run:
        # schedule.run_pending()

        # schedule.every(300).seconds.do(functools.partial(dumpAvgViews()))
        dumpAvgViews()
        time.sleep(300)

def flutch_table_input_receiver():

    while 1:
        s_keyword = input('Enter search keyword:')
        flutch_table_updater(s_keyword)

@app.route('/users', methods=["GET", "POST"])
def users():
    print("users endpoint reached...")
    if request.method == "GET":
        with open("users.json", "r") as f:
            data = json.load(f)
            data.append({
                "username": "user4",
                "pets": ["hamster"]
            })

            return Flask.jsonify(data)
    if request.method == "POST":
        received_data = request.get_json()
        print(f"received data: {received_data}")
        message = str(received_data['data'])
        flutch_table_updater(message)
        return_data = {
            "status": "success",
            "message": f"received: {message}"
        }
        return Flask.Response(response=json.dumps(return_data), status=201)
def api_backend_runner():
    app.run(debug=True)



# if __name__ == '__main__':
    # app.run(debug=True)

#     # creating thread
#     t1 = threading.Thread(target=dumpAvgViews)
#     # t2 = threading.Thread(target=flutch_table_updater,args=("tech",))
#     t2 = threading.Thread(target=api_backend_runner)
#     # # starting thread 1
#     t1.start()
#     # # starting thread 2
#     t2.start()
#     # logging.info ('Invalid_url_ids')
#     #
#     # # wait until thread 1 is completely executed
#     t1.join()
#     # # wait until thread 2 is completely executed
#     # t2.join()
#
#
#     # flutch_table_dump()
#     # dumpAvgViews()
#     # scheduler1()
#     # flutch_table_updater("tech")
# # schedule.every(300).seconds.do(printr())
# dumpAvgViews()
# while run:
#     run_pending()
#     time.sleep(1)
# print('Y')
# # #
#         # request.get(url)

#
#         # schedule.every(300).seconds.do(functools.partial(dumpAvgViews()))
#         # dumpAvgViews()


# flutch_table_updater('lifestyle tamil')
# requests.exceptions.RequestExcept
# flutch_table_dump()
# dump_costs()
