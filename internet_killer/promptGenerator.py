from internet_killer.newsGetter import newsGetter
from internet_killer.prompt import subreddit_guide, strategy_guide
import random
import re

class promptGenerator:
    def __init__(self):
        self.news_getter = newsGetter()

    def generate_news_prompt(self, news):
        news_sys_prompt = "You are an AI agent specialized in selecting the most suitable news headline for generating a highly viral and controversial Reddit post. It has to be (1) trending, (2) controversial, and (3) stimulating headline."
        news_user_prompt = f"Here are the news headlines:\n{news}\n\nPlease return the selected headline."
        return news_sys_prompt, news_user_prompt

    def generate_subreddit_prompt(self, headline):
        subreddit_sys_prompt = "You are an AI agent specialized in selecting the most suitable subreddit for the selected headline. It has to be relevant to the headline and also consider the subreddit's environment, post style, and typical sentiment bias."
        subreddit_user_prompt = f"Here is the selected headline:\n{headline}\n\nPlease return the selected subreddit. Refer to the subreddit_guide for more information."
        subreddit_user_prompt += f"\n\n{subreddit_guide}"
        return subreddit_sys_prompt, subreddit_user_prompt

    def extract_subreddit_style(self, subreddit):
        pattern = rf'\n{subreddit}:\n(.*?)(?=\n\nr/|\n---|\n##|\Z)'
        match = re.search(pattern, subreddit_guide, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    def generate_post_prompt(self, headline, subreddit, subreddit_style):
        post_sys_prompt = "You are an AI agent specialized in generating a highly viral and controversial Reddit post. It has to be relevant to the headline, written in the style of the subreddit, and follow the subreddit's typical sentiment bias. Do refer to the subreddit\'s style. Do refer to the strategy guid I give you. The goal is to make it as controversial and viral as possible."
        post_user_prompt = f"Here is the selected headline:\n{headline}\n\nHere is the selected subreddit:\n{subreddit}\n\nHere is the subreddit's style:\n{subreddit_style}\n\nHere is the strategy guide:\n{strategy_guide}\n\nPlease return the generated post."
        return post_sys_prompt, post_user_prompt
