import requests
from bs4 import BeautifulSoup
import time
import datetime
import csv
import time as sleeptime
import json


TRUSTPILOT_BASE_URL = "https://de.trustpilot.com/review/www.mifcom.de"
TRUSTEDSHOPS_API_URL = "https://profile-pages-reviews-api.trustedshops.com/chl-fd22f4a5-9c97-42db-8644-c703fc56d3b7"
EKOMI_BASE_URL = "https://www.ekomi.de/zertifikat.php?id=mifcomcomputer&js=true&sort=1&s={page}&filter=alle_certificate"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0"
}
MAX_PAGES = 10

def extract_trustpilot_review_data(article):
    review = {}
    review_content = article.find(attrs={"data-review-content": True})
    title_elem = review_content.find(attrs={"data-service-review-title-typography": True}) if review_content else None
    review["TITLE"] = title_elem.text.strip() if title_elem else ""
    text_elem = review_content.find(attrs={"data-service-review-text-typography": True}) if review_content else None
    review["TEXT"] = text_elem.text.strip() if text_elem else ""
    rating_elem = article.find(attrs={"data-service-review-rating": True})
    review["RATING"] = rating_elem.get("data-service-review-rating") if rating_elem else ""
    time_elem = article.find("time")
    review["DATE"] = time_elem.get("datetime") if time_elem else ""
    name_elem = article.find(attrs={"data-consumer-name-typography": True})
    review["NAME"] = name_elem.text.strip() if name_elem else "unknown"
    review["SOURCE"] = "Trustpilot"
    return review

def crawl_trustpilot_reviews():
    page = 1
    all_reviews = []
    while page <= MAX_PAGES:
        print(f"[Trustpilot] Crawling page {page}")
        url = f"{TRUSTPILOT_BASE_URL}?page={page}"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"[Trustpilot] Failed to retrieve page {page}")
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        overview_section = soup.find(attrs={"data-reviews-overview-section": True})
        if not overview_section:
            print(f"[Trustpilot] No review overview section found on page {page}, stopping.")
            break
        articles = overview_section.find_all("article")
        if not articles:
            print(f"[Trustpilot] No more articles found on page {page}, stopping.")
            break
        for article in articles:
            review_data = extract_trustpilot_review_data(article)
            all_reviews.append(review_data)
        page += 1
        sleeptime.sleep(2)
    return all_reviews

def parse_ts_date(ts):
    try:
        dt = datetime.datetime.fromtimestamp(ts / 1000, datetime.UTC)
        return dt.isoformat()
    except Exception:
        return ""

def crawl_trustedshops_reviews():
    all_reviews = []
    page_index = 0
    page_size = 100
    while page_index < MAX_PAGES:
        print(f"[TrustedShops] Crawling pageIndex {page_index}")
        url = f"{TRUSTEDSHOPS_API_URL}?pageSize={page_size}&pageIndex={page_index}&order=DESC&sortBy=date"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"[TrustedShops] Failed to retrieve pageIndex {page_index}")
            break
        data = resp.json()
        reviews = data.get("reviews", [])
        if not reviews:
            print(f"[TrustedShops] No more reviews found at pageIndex {page_index}, stopping.")
            break
        for r in reviews:
            review = {
                "TITLE": r.get("title") if r.get("title") else "unknown",
                "TEXT": r.get("comment") if r.get("comment") else "unknown",
                "RATING": r.get("rating"),
                "DATE": parse_ts_date(r["createdAt"]) if r.get("createdAt") else "",
                "NAME": "unknown",
                "SOURCE": "TrustedShops"
            }
            all_reviews.append(review)
        if len(reviews) < page_size:
            print(f"[TrustedShops] Fewer than {page_size} reviews found at pageIndex {page_index}, stopping.")
            break
        page_index += 1
        sleeptime.sleep(1)
    return all_reviews

def parse_ekomi_timestamp(ts):
    try:
        dt = datetime.datetime.fromtimestamp(int(ts), datetime.UTC)
        return dt.isoformat()
    except Exception:
        return ""

def crawl_ekomi_reviews():
    all_reviews = []
    page = 1
    while page <= MAX_PAGES:
        print(f"[eKomi] Crawling page {page}")
        url = EKOMI_BASE_URL.format(page=page)
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"[eKomi] Failed to retrieve page {page}")
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        review_tds = soup.find_all("td", class_="review-data")
        if not review_tds:
            print(f"[eKomi] No more review-data found on page {page}, stopping.")
            break
        for td in review_tds:
            review = {
                "TITLE": "",
                "NAME": "",
                "SOURCE": "eKomi"
            }
            # DATE
            time_elem = td.find("time")
            if time_elem:
                span_dt = time_elem.find("span", class_="datetime")
                if span_dt and span_dt.has_attr("timestamp"):
                    review["DATE"] = parse_ekomi_timestamp(span_dt["timestamp"])
                else:
                    review["DATE"] = ""
            else:
                review["DATE"] = ""
            # TEXT
            comment_div = td.find("div", class_="review-item-body review-coments")
            if comment_div:
                inner_div = comment_div.find("div")
                if inner_div:
                    review["TEXT"] = inner_div.get_text(strip=True)
                else:
                    review["TEXT"] = ""
            else:
                review["TEXT"] = ""
            # RATING
            user_value_div = td.find("div", class_="user-value")
            if user_value_div:
                span_current = user_value_div.find("span", class_="current")
                if span_current:
                    review["RATING"] = span_current.text.strip()
                else:
                    review["RATING"] = ""
            else:
                review["RATING"] = ""
            all_reviews.append(review)
        page += 1
        sleeptime.sleep(1)
    return all_reviews

if __name__ == "__main__":
    print("Starting crawl for Trustpilot...")
    tp_reviews = [] #crawl_trustpilot_reviews()
    print(f"Trustpilot crawl complete. {len(tp_reviews)} reviews found.")

    print("Starting crawl for TrustedShops...")
    ts_reviews = [] #crawl_trustedshops_reviews()
    print(f"TrustedShops crawl complete. {len(ts_reviews)} reviews found.")

    print("Starting crawl for eKomi...")
    ekomi_reviews = crawl_ekomi_reviews()
    print(f"eKomi crawl complete. {len(ekomi_reviews)} reviews found.")

    all_reviews = tp_reviews + ts_reviews + ekomi_reviews

    # Save reviews as JSON
    with open("all_reviews.json", "w", encoding="utf-8") as f:
        json.dump(all_reviews, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(all_reviews)} reviews to all_reviews.json")
