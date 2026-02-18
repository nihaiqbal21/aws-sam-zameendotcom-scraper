import os
import boto3
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["BUCKET_NAME"]  # read bucket name from environment

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

FIELDS = [
    "url",
    "title",
    "price",
    "location",
    "beds",
    "baths",
    "area_sqft",
    "created_at",
    "updated_at",
    "agent_image",
]

def safe_text(el):
    return el.get_text(strip=True) if el else None

def parse_listing(li):
    data = {field: None for field in FIELDS}

    link = li.find("a", class_="d870ae17")
    data["url"] = "https://www.zameen.com" + link["href"] if link else None

    data["title"] = safe_text(li.find("h2", class_="_36dfb99f"))
    data["price"] = safe_text(li.find("span", class_="dc381b54"))
    data["location"] = safe_text(li.find("div", class_="db1aca2f"))

    data["beds"] = safe_text(li.find("span", attrs={"aria-label": "Beds"}))
    data["baths"] = safe_text(li.find("span", attrs={"aria-label": "Baths"}))
    data["area_sqft"] = safe_text(li.find("span", attrs={"aria-label": "Area"}))

    data["created_at"] = safe_text(
        li.find("span", attrs={"aria-label": "Listing creation date"})
    )
    data["updated_at"] = safe_text(
        li.find("span", attrs={"aria-label": "Listing updated date"})
    )

    agent_img = li.find("img", attrs={"aria-label": "Agency photo"})
    data["agent_image"] = (
        agent_img.get("src")
        or agent_img.get("data-src")
        or agent_img.get("data-srcset")
        if agent_img else None
    )

    return data

def upload_json_to_s3(data):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"zameen/json/listings_{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False),
        ContentType="application/json"
    )

    return key

def lambda_handler(event, context):
    results = []

    page_limit = 10

    # API Gateway override
    if event.get("queryStringParameters"):
        page_limit = int(
            event["queryStringParameters"].get("pages", page_limit)
        )

    for page in range(1, page_limit + 1):
        url = f"https://www.zameen.com/Homes/Lahore-1-{page}.html"

        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        listings = soup.find_all("li", role="article")

        for li in listings:
            results.append(parse_listing(li))

    if not results:
        return {
            "statusCode": 204,
            "body": json.dumps({"message": "No listings found"}),
            "headers": {"Content-Type": "application/json"}
        }

    s3_key = upload_json_to_s3(results)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total_listings": len(results),
            "s3_bucket": BUCKET_NAME,
            "s3_key": s3_key,
            "sample": results[:5]
        }),
        "headers": {"Content-Type": "application/json"}
    }
