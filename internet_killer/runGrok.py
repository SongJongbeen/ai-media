import os
import httpx
from openai import OpenAI
from dotenv import load_dotenv

class runGrok:
    def __init__(self):
        load_dotenv()
        self.client = OpenAI(api_key=os.getenv("XAI_API_KEY"), base_url="https://api.x.ai/v1")

    def run(self, system_prompt, user_prompt):
        response = self.client.chat.completions.create(
            model="grok-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )

        output = response.choices[0].message.content
        return output
