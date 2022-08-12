import json
import requests
from datetime import datetime, date
import pandas as pd
from bs4 import BeautifulSoup
import os.path
import time
from datetime import timedelta

categories = ['', 'Film & Animation', 'Autos & Vehicles', '', '', '', '', '', '', '', 'Music', '', '', '', '',
              'Pets & Animals', '', 'Sports', 'Short Movies', 'Travel & Events', 'Gaming', 'Videoblogging',
              'People & Blogs', 'Comedy', 'Entertainment', 'News & Politics', 'How to Beauty & Style', 'Education',
              'Science & Technology', 'Nonprofits & Activism', 'Movies', 'Anime/Animation', 'Action/Adventure',
              'Classics', 'Comedy', 'Documentary', 'Drama', 'Family', 'Foreign', 'Horror', 'Sci-Fi/Fantasy', 'Thriller',
              'Shorts', 'Shows', 'Trailers']

lang_dict = {'as': 'Assamese', 'bn': 'Bengali', 'bh': 'Bihari', 'en': 'English', 'gu': 'Gujarati', 'hi': 'Hindi',
             'ks': 'Kashmiri', 'ml': 'Malayalam', 'mr': 'Marathi', 'pa': 'Punjabi', 'ta': 'Tamil', 'te': 'Telugu'}


class YTstats:

    def __init__(self, api_key, channel_id):
        self.api_key = api_key
        self.channel_id = channel_id
        self.channel_statistics = None
        self.videos_statistics = None

    def get_channel_statistics(self,):
        """Extract the channel statistics"""
        url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics&id={self.channel_id}&key={self.api_key}'

        json_url = requests.get(url)
        # print(1)


        data = json.loads(json_url.text)
        try:
            data = data['items'][0]['statistics']
        except KeyError:
            print('Could not get channel statistics')
            data = {}

        data['publishedAt'], data['Channel title'] = self.get_channel_snippet()
        if "hiddenSubscriberCount" in data:
            del data["hiddenSubscriberCount"]

        self.channel_statistics = data

        return data



    def get_channel_snippet(self):
        """Extract the channel statistics"""
        url = f'https://www.googleapis.com/youtube/v3/channels?part=snippet&id={self.channel_id}&key={self.api_key}'


        json_url = requests.get(url)
        # print(1)
        # x += 1
        data = json.loads(json_url.text)
        try:
            data = data['items'][0]['snippet']
            title = data['title']
            publish_time = data['publishedAt'].split('T')
            publish_time[1] = publish_time[1][:-1]
            dec_idx = publish_time[1].find('.')
            if dec_idx != -1:
                publish_time[1] = publish_time[1][:dec_idx]

            publish_time = datetime.strftime(datetime.strptime(' '.join(publish_time), '%Y-%m-%d %H:%M:%S'),
                                             '%Y-%m-%d %H:%M:%S')
        except KeyError:
            print('Could not get channel snippet')
            data = {}
            title = None
            publish_time = None

        return publish_time, title

    def _get_video_ids(self, limit=None):
        """
        Extract all videos ids and return it
        """
        url = f"https://www.googleapis.com/youtube/v3/search?key={self.api_key}&channelId={self.channel_id}&part=snippet,id&type=video&order=date"
        video_ids = {}
        if limit is not None and isinstance(limit, int):
            url += "&maxResults=" + str(limit)

        json_url = requests.get(url)
        # print(100)
        data = json.loads(json_url.text)
        if 'items' not in data:
            print('Error! Could not get correct channel data!\n', data)
        else:
            item_data = data['items']
            for item in item_data:
                try:
                    kind = item['id']['kind']
                    published_at = item['snippet']['publishedAt']
                    title = item['snippet']['title']
                    if kind == 'youtube#video':
                        video_id = item['id']['videoId']
                        video_ids[video_id] = {'publishedAt': published_at, 'title': title}
                except KeyError as e:
                    print('Error! Could not extract data from item:\n', item)

        return video_ids

    def _get_single_video_data(self, video_id, part):
        """
        Extract further information for a single video
        parts can be: 'snippet', 'statistics', 'contentDetails', 'topicDetails'
        """

        url = f"https://www.googleapis.com/youtube/v3/videos?part={part}&id={video_id}&key={self.api_key}"

        json_url = requests.get(url)
        # print(1)
        data = json.loads(json_url.text)
        try:
            # print(data)
            # if len(data['items'])>0:

            # if part in data['items'][0]:
            data = data['items'][0][part]
            # print(data)
        except KeyError as e:
            print(f'Error! Could not get {part} part of data: \n{data}')
            time.sleep(60)

            data = {}
            json_url = requests.get(url)
            # print(1)
            data = json.loads(json_url.text)
            data = data['items'][0][part]
        return data

    def get_videos_data(self):
        latest_videos = {}
        language = ''
        video_ids = self._get_video_ids(limit=50)
        for video_id in video_ids:
            if len(latest_videos) >= 5:
                break

            snippet = self._get_single_video_data(video_id, 'snippet')
            statistics = self._get_single_video_data(video_id, 'statistics')
            contentDetails = self._get_single_video_data(video_id, 'contentDetails')

            if 'publishedAt' in snippet:
                publish_time = snippet['publishedAt'].split('T')
                publish_time[1] = publish_time[1][:-1]

                dec_idx = publish_time[1].find('.')
                if dec_idx != -1:
                    publish_time[1] = publish_time[1][:dec_idx]

                publish_time = datetime.strptime(' '.join(publish_time), '%Y-%m-%d %H:%M:%S')

            else:
                publish_time = date.today() - timedelta(days=2)

            notShorts = (contentDetails['duration'].find('M') != -1 or contentDetails['duration'].find(
                'H') != -1) and contentDetails['duration'] != 'PT1M' and contentDetails['duration'] != 'PT1M1S'

            if (datetime.now() - publish_time).days > 1 and notShorts:
                latest_videos[video_id] = {}
                latest_videos[video_id]['views'] = statistics['viewCount']
                latest_videos[video_id]['published_at'] = datetime.strftime(publish_time, '%Y-%m-%d %H:%M:%S')

                # if 'likeCount' in statistics:
                #         latest_videos[video_id]['likes'] = statistics['likeCount']
                # else:
                #     latest_videos[video_id]['likes'] = 0
                #
                # if 'commentCount' in statistics:
                #     latest_videos[video_id]['comments'] = statistics['commentCount']
                # else:
                #     latest_videos[video_id]['comments'] = 0
                # latest_videos[video_id]['duration'] = contentDetails['duration']
                # latest_videos[video_id]['duration'] = str(latest_videos[video_id]['duration'][2:])
                # latest_videos[video_id]['duration'] = latest_videos[video_id]['duration'].replace('M',':' ).replace('S','').replace('H',':')

                if 'categoryId' in snippet:
                    latest_videos[video_id]['category_id'] = snippet['categoryId']

                if 'defaultAudioLanguage' in snippet:
                    latest_videos[video_id]['language'] = snippet['defaultAudioLanguage']
                    language = snippet['defaultAudioLanguage']
                    if language in lang_dict:
                        self.channel_statistics['Language'] = lang_dict[language]
                    else:
                        self.channel_statistics['Language'] = ' '
                else:
                    self.channel_statistics['Language'] = ' '
                latest_videos[video_id]['published_at'] = datetime.strftime(publish_time, '%Y-%m-%d %H:%M:%S')

                if 'likeCount' in statistics:
                    latest_videos[video_id]['likes'] = statistics['likeCount']
                else:
                    latest_videos[video_id]['likes'] = 0

                if 'commentCount' in statistics:
                    latest_videos[video_id]['comments'] = statistics['commentCount']
                else:
                    latest_videos[video_id]['comments'] = 0
                latest_videos[video_id]['duration'] = contentDetails['duration']

        cat_freq = {}
        for video_id in latest_videos:
            if latest_videos[video_id]['category_id'] not in cat_freq:
                cat_freq[latest_videos[video_id]['category_id']] = 1
            else:
                cat_freq[latest_videos[video_id]['category_id']] += 1

        max_freq = 0
        category = None
        for cat_id in cat_freq:
            if cat_freq[cat_id] > max_freq:
                max_freq = cat_freq[cat_id]
                category = categories[int(cat_id)]
        # if 'Category' in self.channel_statistics:
        if category != None:
            self.channel_statistics['Category'] = category
        else:
            self.channel_statistics['Category']= None
        if language in lang_dict:
            self.channel_statistics['Language'] = lang_dict[language]
        else:
            self.channel_statistics['Language'] = 'English'

        self.videos_statistics = latest_videos
        return latest_videos

    '''-----GETTING CHANNEL DATABASE-----'''

    def getChannels(self, category, language, region_code):
        lang_code= list(lang_dict.keys())[list(lang_dict.values()).index(language)]
        url = f'https://www.googleapis.com/youtube/v3/search?key={self.api_key}&part=snippet&type=channel&q={category}&relevanceLanguage={lang_code}&regionCode={region_code}&maxResults=50'


        channels, nxtToken = self.get_channels_per_page(url, category, lang_code)
        pages = 0

        while nxtToken is not None and pages < 1:
            next_url = url + "&pageToken=" + nxtToken
            next_page_channels, npt = self.get_channels_per_page(next_url, category, lang_code)
            channels.update(next_page_channels)
            pages += 1

        # with open('db.json', 'w') as outfile:
        #     json.dump(channels, outfile, indent=4)

        # self.write_json(channels, 'db.json')
        return channels

    def getChannels1(self, keyword):
        # lang_code = list(lang_dict.keys())[list(lang_dict.values()).index(language)]
        region_code = 'IN'
        url = f'https://www.googleapis.com/youtube/v3/search?key={self.api_key}&part=snippet&type=channel&q={keyword}&regionCode={region_code}&maxResults=50'

        channels, nxtToken = self.get_channels_per_page(url)
        pages = 0

        while nxtToken is not None and pages < 1:
            next_url = url + "&pageToken=" + nxtToken
            next_page_channels, npt = self.get_channels_per_page(next_url)
            channels.update(next_page_channels)
            pages += 1

        # with open('db.json', 'w') as outfile:
        #     json.dump(channels, outfile, indent=4)

        # self.write_json(channels, 'db.json')
        return channels

    def write_json(self, new_data, filename):
        with open(filename, 'r+') as file:
            # First we load existing data into a dict.
            data_json = json.load(file)
            print('data_json len', len(data_json))
            # data_dict = dict(json.load(file))
            for channel_id in new_data:
                if channel_id not in data_json:
                    data_json[channel_id] = new_data[channel_id]
            # Sets file's current position at offset.
            file.seek(0)
            # convert back to json.
            json.dump(data_json, file, indent=4)

    def get_channels_per_page(self, url ):
        # url = f'https://www.googleapis.com/youtube/v3/search?key={self.api_key}&part=snippet&type=channel&q={keyword}&regionCode={region_code}&maxResults=50'

        json_url = requests.get(url)
        data = json.loads(json_url.text)

        channels_per_page = {}
        if 'items' not in data:
            print('Error! Could not get correct channels data!\n', data)
            return channels_per_page, None

        nextPageToken = data.get("nextPageToken", None)
        item_data = data['items']
        print(item_data)

        for item in item_data:
            try:
                title = item['snippet']['title']
                # category = item['snippet']['title']
                channel_id = item['id']['channelId']
                channel_url = 'https://www.youtube.com/channel/' + channel_id + '/videos'
                channels_per_page[channel_id] = {'title': title, 'url': channel_url, 'category': 'c',
                                                 'language': 'l'}
            except KeyError as e:
                print('Error! Could not extract data from item:\n', item)

        return channels_per_page, nextPageToken

    '''-----LIVE VIDEO ANALYTICS-----'''

    def live_stats(self):

        live_df = pd.read_excel("temp2.xlsx", engine='openpyxl', sheet_name=0)

        live_video_urls = list(live_df['Video Live link'])
        video_dates = list(live_df['Video Live date'])
        avgViews = list(live_df['avgViews'])

        month_dict = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                      'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

        # for i in range(len(video_dates)):
        #     v_date = datetime.strftime(video_dates[i], "%d%m%y")
        #     v_date = datetime.strptime(v_date, "%d%m%y")
        #     video_dates[i] = v_date

        channel_ids, live_video_ids = self.getChannelIDs([], live_video_urls)

        live_view_count, video_title = [], []
        for live_id in live_video_ids:
            statistics = self._get_single_video_data(live_id, 'statistics')
            snippet = self._get_single_video_data(live_id, 'snippet')

            live_view_count.append(int(statistics['viewCount']))
            video_title.append(snippet['title'])

        n = len(avgViews)
        if os.path.isfile("live_video_data.xlsx"):
            temp_df = pd.read_excel("live_video_data.xlsx", engine='openpyxl', sheet_name=0)
            target_days = list(temp_df['targetDays'])
        else:
            target_days = [0] * n
        for i in range(n):
            if avgViews[i] <= live_view_count[i]:
                target_days[i] = (datetime.now() - video_dates[i]).days
        return live_view_count, target_days

    def get_avg_views(self, live_id):

        video_ids = self._get_video_ids(limit=50)

        latest_videos, views = [], []
        for video_id in video_ids:
            if len(latest_videos) >= 5:
                break

            snippet = self._get_single_video_data(video_id, 'snippet')
            statistics = self._get_single_video_data(video_id, 'statistics')
            contentDetails = self._get_single_video_data(video_id, 'contentDetails')

            if 'publishedAt' in snippet:
                publish_time = snippet['publishedAt'].split('T')
                publish_time[1] = publish_time[1][:-1]

                dec_idx = publish_time[1].find('.')
                if dec_idx != -1:
                    publish_time[1] = publish_time[1][:dec_idx]

                publish_time = datetime.strptime(' '.join(publish_time), '%Y-%m-%d %H:%M:%S')

            else:
                publish_time = date.today() - datetime.timedelta(days=2)

            notShorts = (contentDetails['duration'].find('M') != -1 or contentDetails['duration'].find(
                'H') != -1) and contentDetails['duration'] != 'PT1M' and contentDetails['duration'] != 'PT1M1S'

            if (datetime.now() - publish_time).days > 1 and notShorts and video_id != live_id:
                latest_videos.append(video_id)
                views.append(int(statistics['viewCount']))

        snippet = self._get_single_video_data(live_id, 'snippet')

        return latest_videos, views, snippet['title']

    def getChannelIDs(self, channel_urls, video_urls):
        channel_ids, video_ids = [], []
        for url in channel_urls:
            url = url[:-7]
            if '/c/' in url:
                resp = requests.get(url)
                soup = BeautifulSoup(resp.text, 'html.parser')
                channel_id = soup.select_one('meta[property="og:url"]')['content'].strip('/').split('/')[-1]
            else:
                channel_id = url.split('/').pop()
            channel_ids.append(channel_id)

        for url in video_urls:
            video_id = url.split('/')[-1]
            video_ids.append(video_id)

        return channel_ids, video_ids

    '''------CHANNEL ANALYTICS------'''
