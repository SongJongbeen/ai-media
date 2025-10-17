import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # News API 설정
    NEWS_API_KEY = os.getenv("NEWS_API_KEY")
        
    # 뉴스 설정
    MAX_NEWS_COUNT = 100
    
settings = Settings()
