import os
from newsapi import NewsApiClient
from dotenv import load_dotenv

class newsGetter:
    def __init__(self):
        load_dotenv()
        self.news_api_key = os.getenv("NEWS_API_KEY")
        self.news_api = NewsApiClient(api_key=self.news_api_key)
        self.news = []

    def get_headlines(self, language="en", country="us"):
        headlines = self.news_api.get_top_headlines(language=language, country=country)
        articles = headlines["articles"]

        for article in articles:
            news_item = {
                "title": article["title"],
                "description": article["description"],
                "date": article["publishedAt"]
            }
            self.news.append(news_item)
        return self.news
