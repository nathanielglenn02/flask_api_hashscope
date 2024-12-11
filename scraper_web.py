import pandas as pd
from pygooglenews import GoogleNews
from datetime import datetime, timedelta

def scrape_google_news(category, max_results=100, output_path="technology_news_fixed.csv"):
    """
    Scrape Google News for a specific category and save to CSV.
    
    Args:
        category (str): Category to search for.
        max_results (int): Maximum number of articles to fetch.
        output_path (str): Path to save the CSV file.
    Returns:
        str: Path to the saved CSV file.
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        gn = GoogleNews()
        search_query = f"{category} after:{start_date_str} before:{end_date_str}"
        data = []

        while len(data) < max_results:
            results = gn.search(search_query, when='7d')
            articles = results['entries']

            if not articles:
                break

            for article in articles:
                title = article.get('title', 'No title')
                source = article.get('source', {}).get('title', 'Unknown source')
                published = article.get('published', 'No date')
                link = article.get('link', 'No link')

                data.append({
                    'full_text': title,
                    'source': source,
                    'created_at': published,
                    'url': link
                })

                if len(data) >= max_results:
                    break

        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)
        print(f"Data successfully scraped and saved to {output_path}")
        return output_path
    except Exception as e:
        print(f"Error during scraping: {e}")
        raise RuntimeError(f"Error during scraping: {e}")
