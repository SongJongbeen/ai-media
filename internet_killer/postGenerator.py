import os
import httpx
from openai import OpenAI
from dotenv import load_dotenv
from internet_killer.promptGenerator import promptGenerator

class postGenerator:
    def __init__(self):
        load_dotenv()
        self.client = OpenAI(api_key=os.getenv("XAI_API_KEY"), base_url="https://api.x.ai/v1")
        self.promptGenerator = promptGenerator()
        self.prompt = self.promptGenerator.generate_prompt()

    def generate_post(self):
        response = self.client.chat.completions.create(
            model="grok-4", # gpt-5.1
            messages=[
                {"role": "system", "content": "You are an AI agent specialized in generating highly viral and controversial Reddit posts."},
                {"role": "user", "content": self.prompt},
            ]
        )

        output = response.choices[0].message.content

        return output

pg = postGenerator()
print('making prompt...')
prompt = pg.prompt
print('making post...')
post = pg.generate_post()
print('saving...')

with open("prompt.txt", "w", encoding="utf-8") as f:
    f.write(prompt)
with open("post.txt", "w", encoding="utf-8") as f:
    f.write(post)
