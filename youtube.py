#!/usr/bin/env python3
"""
    Provides YouTube APIv3 Functionality via Python. 
    Returns a DataFrame when searching YouTube. 
"""
import datetime, random
import pandas as pd
import googleapiclient.discovery
import googleapiclient.errors

class YouTubeAPI:
    def __init__(self,developerKey):
        self.api_service_name = "youtube"
        self.api_version = "v3"
        self.developerKey = developerKey
        self.youtube = googleapiclient.discovery.build(\
                       self.api_service_name, self.api_version,developerKey=developerKey)

    def search(self, query, published_after=None, max_count=5):
        '''
            Send a query to youtube and get a response.
            Parameters:
                published_after: a datetime object
                max_count: max number of records to be returned in query. 
            Returns:
                A DataFrame of videos with meta data.

        '''
        if published_after:
            published_after = datetime.datetime.strftime(published_after, "%Y-%m-%dT%H:%M:%SZ")
        request = self.youtube.search().list(
            part="snippet",
            maxResults=max_count,
            order=random.choice(["date","rating","relevance","title","viewCount"]),
            publishedAfter=published_after,
            q=query,
            regionCode="US",
            relevanceLanguage="en",
            eventType='completed',
            safeSearch="moderate",
            type="video",
            videoCaption="any", 
            videoDuration=random.choice(['short','medium','long','any']),
            videoDefinition=random.choice(["high","any"]),
            videoEmbeddable="true",
            videoLicense=random.choice(["any","creativeCommon","youtube"]),
            videoSyndicated="true"
        )
        return self.parse_search(request.execute(),query)

    def get_metadata(self, videoID):
        '''
            Sends a request to get meta data of a YouTube video.
            Parameters:
                videoID: a string of  YouTube Video ID.
            Returns:
                Raw dictionary of YouTube's reponse. 

        '''
        request = self.youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videoID
        )
        return request.execute()

    def parse_search(self,response,query):
        '''
            Organize raw data from reponse to a Dataframe.
            Parameters:
                response: a raw dictionary response from YouTube.
                query: a string of query sent to YouTube.
            Returns:
                Pandas Dataframe of Video info and meta data. 
        '''
        if response !=[]:
            columns = ['videoID','title','description','channelTitle','channelID','thumbnail',\
                'publishTime', 'categoryID','viewCount','likeCount', 'dislikeCount','commentCount',\
                'query']
            items = []
            for item in response['items']:
                video_data = self.parse_video(item)
                assert video_data != []
                meta = self.get_metadata(video_data[0])
                assert video_data[0] == meta['items'][0]['id'] #meta belongs to right video
                meta_data = self.parse_meta(meta)
                record = video_data + meta_data + [query]
                items.append(record)
            return pd.DataFrame(items[1:], columns=columns)
        else:
            print("NO RESULTS FOUND!")

    def parse_video(self, item):
        '''
            Parse item info.
            Parameters:
                item: a single video dictionary from reponse['items']
            Returns:
                A list containing video info. 

        '''
        videoID = item['id']['videoId']
        title = item['snippet']['title']
        desc = item['snippet']['description']
        channelTitle = item['snippet']['channelTitle']
        channelID = item['snippet']['channelId']
        thumbnail = item['snippet']['thumbnails']['high']['url']
        publishTime = item['snippet']['publishTime']
        return [videoID, title, desc, channelTitle, channelID, thumbnail, publishTime]

    def parse_meta(self, meta):
        '''
            Parse meta info.
            Parameters:
                meta: a meta response dictionary from YouTube.
            Returns:
                A list containing meta info.
        '''
        cat_dct = {1:"Film-n-Animation", 2:"Automotive", 10:"Music",\
               15:"Pets-n-Animals", 17:"Sports",18:"Short-Movies",\
               19:"Travel-n-Events", 20:"Gaming", 21:"Videoblogging",\
               22:"People-n-Blogs", 23:"Comedy", 24:"Entertainment",\
               25:"News-n-Politics",26:"Howto-n-Style",27:"Education",\
               28:"Science-n-Technology", 29:"Nonprofits-n-Activism",\
               30:"Movies", 31:"Anime-Animation",32:"Action-Adventure",\
               33:"Classics",34:"Comedy",35:"Documentary",36:"Drama",\
               37:"Family",38:"Foreign",39:"Horror",40:"Sci-Fi-n-Fantasy",\
               41:"Thriller",42:"Shorts",43:"Shows",44:"Trailers"}
        stats = {'categoryID':'-404','viewCount':'-404', 'likeCount':'-404','dislikeCount':'-404',\
                 'commentCount':'-404'}
        try:
            stats['categoryID'] = cat_dct[int(meta['items'][0]['snippet']['categoryId'])]

        except:
            print('SYSTEM WARNING!: Category not found in dictionary, using categoryID (' +\
                  meta['items'][0]['snippet']['categoryId'] + ').')
            stats['categoryID'] = meta['items'][0]['snippet']['categoryId']

        for stat in meta['items'][0]['statistics'].keys():
            if stat in stats.keys():
                stats[stat] = meta['items'][0]['statistics'][stat]

        return [stats[data] for data in stats.keys()]
