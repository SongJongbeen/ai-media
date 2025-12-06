from internet_killer.newsGetter import newsGetter
from internet_killer.prompt import prompt
import random

class promptGenerator:
    def __init__(self):
        self.news_getter = newsGetter()
        self.prompt = prompt

    def generate_prompt(self):
        news_headlines = self.news_getter.get_headlines()
        subreddit = ['4chan', 'facepalm', 'Drama', 'WallStreetBets', 'unpopularopinion', 'NoStupidQuestions', 'thatHappened', 
        'FragileWhiteRedditor', 'LateStageCapitalism', 'Antiwork', 'AmITheAsshole']
        self.prompt = self.prompt.format(news_headlines=news_headlines, subreddit=random.choice(subreddit))
        return self.prompt

# 나중에 뉴스 선택 따로, 이후 커뮤니티 따로
