import requests
import json
import os
import sys
from dotenv import load_dotenv
from datetime import datetime, date
from typing import List, Dict, Optional
import pandas as pd
import feedparser

try:
    from config.settings import settings
except ImportError:
    load_dotenv()
    class DefaultSettings:
        NEWS_API_KEY = os.getenv("NEWS_API_KEY")
        MAX_NEWS_COUNT = 100
    settings = DefaultSettings()

class GlobalNewsFetcher:
    """NewsAPI를 통해 전세계 뉴스를 수집하고 저장"""
    
    def __init__(self):
        self.api_key = settings.NEWS_API_KEY
        self.base_url = "https://newsapi.org/v2"
        self.data_dir = "internet_killer/data"
        self.today_str = date.today().strftime("%Y%m%d")
        self.csv_file = f"{self.data_dir}/news_data_{self.today_str}.csv"
        self.json_file = f"{self.data_dir}/news_data_{self.today_str}.json"
        
        self.rss_sources = [
            {"url": "https://feeds.yna.co.kr/news", "name": "연합뉴스", "country": "kr"},
            {"url": "https://www.mk.co.kr/rss/30000001/", "name": "매일경제", "country": "kr"},
            {"url": "http://news.chosun.com/site/data/rss/rss.xml", "name": "조선일보", "country": "kr"},
            {"url": "http://feeds.bbci.co.uk/news/rss.xml", "name": "BBC", "country": "gb"},
            {"url": "https://www.theguardian.com/world/rss", "name": "Guardian", "country": "gb"},
            {"url": "http://rss.cnn.com/rss/edition.rss", "name": "CNN", "country": "us"},
        ]

        # 데이터 폴더 생성
        os.makedirs(self.data_dir, exist_ok=True)
        
    def check_today_data_exists(self) -> bool:
        """오늘 날짜의 데이터가 이미 있는지 확인"""
        return os.path.exists(self.csv_file) and os.path.exists(self.json_file)
    
    def collect_all_news(self, force_update: bool = False) -> Dict:
        """전 세계 뉴스 수집 및 저장"""
        
        if self.check_today_data_exists() and not force_update:
            print(f"✅ 오늘 날짜({self.today_str})의 뉴스 데이터가 이미 수집되어 있습니다!")
            print(f"📁 파일 위치: {self.csv_file}, {self.json_file}")
            return {"status": "already_exists", "date": self.today_str}
            
        if not self.api_key:
            print("❌ NEWS_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
            return {"status": "error", "message": "API key missing"}
        
        print(f"🌍 전세계 뉴스 수집 시작... (날짜: {self.today_str})")
        all_articles = []
        
        # 다양한 국가와 카테고리에서 뉴스 수집
        countries_and_categories = [
            # 한국 뉴스
            {"country": "kr", "category": None, "name": "한국 전체"},
            {"country": "kr", "category": "technology", "name": "한국 기술"},
            {"country": "kr", "category": "business", "name": "한국 경제"},
            {"country": "kr", "category": "entertainment", "name": "한국 연예"},
            
            # 미국 뉴스  
            {"country": "us", "category": None, "name": "미국 전체"},
            {"country": "us", "category": "technology", "name": "미국 기술"},
            {"country": "us", "category": "business", "name": "미국 경제"},
            {"country": "us", "category": "politics", "name": "미국 정치"},
            
            # 영국 뉴스
            {"country": "gb", "category": None, "name": "영국 전체"},
            {"country": "gb", "category": "business", "name": "영국 경제"},
            {"country": "gb", "category": "politics", "name": "영국 정치"},
            
            # 글로벌 주요 소스들
            {"sources": "bbc-news,cnn,reuters,associated-press,techcrunch", "name": "글로벌 주요 매체"}
        ]
        
        for config in countries_and_categories:
            print(f"📰 수집 중: {config['name']}")
            articles = self._fetch_news_by_config(config)
            all_articles.extend(articles)
            
        print("📡 RSS 피드에서 추가 뉴스 수집...")
        for rss_config in self.rss_sources:
            print(f"📰 RSS 수집 중: {rss_config['name']}")
            rss_articles = self._fetch_rss_news(rss_config)
            all_articles.extend(rss_articles)

        # 중복 제거 (URL 기준)
        unique_articles = self._remove_duplicates(all_articles)
        print(f"✨ 총 {len(unique_articles)}개의 고유한 뉴스를 수집했습니다!")
        
        # 저장
        self._save_to_files(unique_articles)
        
        return {
            "status": "success", 
            "date": self.today_str,
            "total_articles": len(unique_articles),
            "files": [self.csv_file, self.json_file]
        }
    
    def _fetch_news_by_config(self, config: Dict) -> List[Dict]:
        """설정에 따라 뉴스 가져오기"""
        try:
            url = f"{self.base_url}/top-headlines"
            params = {
                "apiKey": self.api_key,
                "pageSize": 20,  # 각 카테고리당 20개씩
            }

            if config.get("country") == "kr":
                params["country"] = "kr"
            elif config.get("country") == "gb":
                params["country"] = "gb"
                params["language"] = "en"
            elif "sources" in config:
                params["sources"] = config["sources"]
                params["language"] = "en"
            else:
                params["country"] = config["country"]
                params["language"] = "en"

            if config.get("category"):
                params["category"] = config["category"]

            print(f"🔍 API 요청: {params}")

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            print(f"📊 응답: {data.get('totalResults', 0)}개 결과")

            articles = []
            for article in data.get("articles", []):
                if article["title"] and article["description"]:  # 제목과 내용이 있는 것만
                    processed_article = {
                        "id": f"{config['name']}_{len(articles)}",
                        "title": article["title"],
                        "description": article["description"],
                        "content": article.get("content", "")[:1000] + "..." if article.get("content") else "",
                        "url": article["url"],
                        "source_name": article["source"]["name"],
                        "category": config["name"],
                        "country": config.get("country", "global"),
                        "published_at": article["publishedAt"],
                        "collected_at": datetime.now().isoformat(),
                        "author": article.get("author", "Unknown")
                    }
                    articles.append(processed_article)
                    
            return articles
            
        except Exception as e:
            print(f"❌ {config['name']} 수집 실패: {str(e)}")
            return []
    
    def _fetch_rss_news(self, rss_config: Dict) -> List[Dict]:
        """RSS 피드에서 뉴스 가져오기"""
        try:
            print(f"🔍 RSS 요청: {rss_config['url']}")
        
            feed = feedparser.parse(rss_config['url'])
            articles = []
        
            for entry in feed.entries[:10]:  # RSS에서는 10개만
                processed_article = {
                    "id": f"{rss_config['name']}_{len(articles)}",
                    "title": entry.title,
                    "description": getattr(entry, 'summary', entry.title)[:200] + "...",
                    "content": getattr(entry, 'content', [{}])[0].get('value', '')[:1000] + "..." if hasattr(entry, 'content') else "",
                    "url": entry.link,
                    "source_name": rss_config['name'],
                    "category": f"{rss_config['name']} RSS",
                    "country": rss_config['country'],
                    "published_at": getattr(entry, 'published', datetime.now().isoformat()),
                    "collected_at": datetime.now().isoformat(),
                    "author": getattr(entry, 'author', "Unknown")
                }
                articles.append(processed_article)
            
            print(f"📊 RSS 응답: {len(articles)}개 결과")
            return articles
        
        except Exception as e:
            print(f"❌ RSS {rss_config['name']} 수집 실패: {str(e)}")
            return []
    
    def _remove_duplicates(self, articles: List[Dict]) -> List[Dict]:
        """URL 기준으로 중복 제거"""
        seen_urls = set()
        unique_articles = []
        
        for article in articles:
            if article["url"] not in seen_urls:
                seen_urls.add(article["url"])
                unique_articles.append(article)
                
        return unique_articles
    
    def _save_to_files(self, articles: List[Dict]):
        """CSV와 JSON 파일로 저장"""
        try:
            # DataFrame 생성
            df = pd.DataFrame(articles)
            
            # CSV 저장
            df.to_csv(self.csv_file, index=False, encoding='utf-8-sig')
            print(f"✅ CSV 저장 완료: {self.csv_file}")
            
            # JSON 저장 (메타데이터 포함)
            json_data = {
                "collection_date": self.today_str,
                "collection_timestamp": datetime.now().isoformat(),
                "total_articles": len(articles),
                "articles": articles
            }
            
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            print(f"✅ JSON 저장 완료: {self.json_file}")
            
        except Exception as e:
            print(f"❌ 파일 저장 실패: {str(e)}")
    
    def load_today_data(self) -> Optional[pd.DataFrame]:
        """오늘 저장된 데이터를 불러오기"""
        try:
            if os.path.exists(self.csv_file):
                df = pd.read_csv(self.csv_file)
                print(f"📊 불러온 데이터: {len(df)}개 뉴스")
                return df
            else:
                print("❌ 오늘 날짜의 데이터가 없습니다.")
                return None
        except Exception as e:
            print(f"❌ 데이터 로드 실패: {str(e)}")
            return None

# 실행부
if __name__ == "__main__":
    fetcher = GlobalNewsFetcher()
    
    print("🚀 글로벌 뉴스 수집 시스템")
    print("=" * 50)
    
    # 뉴스 수집
    result = fetcher.collect_all_news()
    
    if result["status"] == "success":
        print(f"\n🎉 수집 완료! 총 {result['total_articles']}개의 뉴스를 저장했습니다.")
        
        # 저장된 데이터 미리보기
        df = fetcher.load_today_data()
        if df is not None:
            print("\n📋 수집된 뉴스 카테고리별 통계:")
            category_stats = df['category'].value_counts()
            for category, count in category_stats.items():
                print(f"  • {category}: {count}개")

