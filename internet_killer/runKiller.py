from internet_killer.newsGetter import newsGetter
from internet_killer.promptGenerator import promptGenerator
from internet_killer.runGrok import runGrok
import datetime
import random

class runKiller:
    def __init__(self):
        self.news_getter = newsGetter()
        self.promptGenerator = promptGenerator()
        self.runGrok = runGrok()

    def run(self, artifact=False):
        # getting News headlines
        news = self.news_getter.get_headlines()

        # Selecting the headline
        news_sys_prompt, news_user_prompt = self.promptGenerator.generate_news_prompt(news)
        selected_headline = self.runGrok.run(news_sys_prompt, news_user_prompt)

        # Selecting the subreddit
        subreddit_sys_prompt, subreddit_user_prompt = self.promptGenerator.generate_subreddit_prompt(selected_headline)
        selected_subreddit = self.runGrok.run(subreddit_sys_prompt, subreddit_user_prompt)

        # Extracting the subreddit style
        subreddit_style = self.promptGenerator.extract_subreddit_style(selected_subreddit)

        # Generating the post
        post_sys_prompt, post_user_prompt = self.promptGenerator.generate_post_prompt(selected_headline, selected_subreddit, subreddit_style)
        generated_post = self.runGrok.run(post_sys_prompt, post_user_prompt)

        with open('results/' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '.txt', 'w', encoding='utf-8') as file:
            file.write(generated_post)

        print(generated_post)

        if artifact:
            generated_post = self.add_artifact(generated_post)
            with open('results/' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '_artifact.txt', 'w', encoding='utf-8') as file:
                file.write(generated_post)

            print(generated_post)

    def add_artifact(self, generated_post):
        homoglyphs = {
            'a': 'а',  # U+0061 → U+0430
            'e': 'е',  # U+0065 → U+0435
            'o': 'о',
            'p': 'р',
            'c': 'с',
            'y': 'у',
            'x': 'х',
        }

        ZWSP = '\u200B'
        ZWNJ = '\u200C'

        spaces = [
            ' ',      # U+0020 Normal Space
            '\u00A0', # U+00A0 No-Break Space
            '\u2009', # U+2009 Thin Space
        ]

        result = []
        for i, char in enumerate(generated_post):
            # 1. Homoglyph 치환 (10% 확률)
            if char.lower() in homoglyphs and random.random() < 0.1:
                # 대소문자 유지
                if char.isupper():
                    result.append(homoglyphs[char.lower()].upper())
                else:
                    result.append(homoglyphs[char.lower()])

            # 2. 일반 공백을 다른 공백으로 치환 (30% 확률)
            elif char == ' ' and random.random() < 0.3:
                result.append(random.choice(spaces))

            # 3. 원본 문자 유지
            else:
                result.append(char)

            # 4. Zero-width 문자 랜덤 삽입 (5% 확률로 단어 사이에)
            if char == ' ' and random.random() < 0.05:
                result.append(random.choice([ZWSP, ZWNJ]))

        generated_post = ''.join(result)
        return generated_post


rk = runKiller()
rk.run(artifact=True)
