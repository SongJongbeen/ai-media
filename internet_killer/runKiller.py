from internet_killer.newsGetter import newsGetter
from internet_killer.promptGenerator import promptGenerator
from internet_killer.runGrok import runGrok
import datetime

class runKiller:
    def __init__(self, artifact=None):
        self.news_getter = newsGetter()
        self.promptGenerator = promptGenerator()
        self.runGrok = runGrok()

    def run(self):
        # # getting News headlines
        # news = self.news_getter.get_headlines()

        # # Selecting the headline
        # news_sys_prompt, news_user_prompt = self.promptGenerator.generate_news_prompt(news)
        # selected_headline = self.runGrok.run(news_sys_prompt, news_user_prompt)

        selected_headline = "{'title': 'December 19, 2025 — Jeffrey Epstein files released - CNN', 'description': 'The Justice Department has released files tied to convicted sex offender Jeffrey Epstein.', 'date': '2025-12-20T06:15:00Z'}"

        # # Selecting the subreddit
        # subreddit_sys_prompt, subreddit_user_prompt = self.promptGenerator.generate_subreddit_prompt(selected_headline)
        # selected_subreddit = self.runGrok.run(subreddit_sys_prompt, subreddit_user_prompt)

        selected_subreddit = "r/conspiracy"

        # Extracting the subreddit style
        subreddit_style = self.promptGenerator.extract_subreddit_style(selected_subreddit)

        # Generating the post
        post_sys_prompt, post_user_prompt = self.promptGenerator.generate_post_prompt(selected_headline, selected_subreddit, subreddit_style)
        generated_post = self.runGrok.run(post_sys_prompt, post_user_prompt)

        with open('results/' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '.txt', 'w', encoding='utf-8') as file:
            file.write(generated_post)

        print(generated_post)


rk = runKiller()
rk.run()
