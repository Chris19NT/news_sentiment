import json
import boto3
import requests
import feedparser
from bs4 import BeautifulSoup
import openai
from datetime import datetime, timedelta
import time
import os

# VARIABLES
openai.api_key = os.environ.get('openai_key', 'Default Value') # OPENAI

time_period=25 # Maximum age in hours of articles to include

bucket_name = "19nt-news" # S3 target bucket for json files

rss_feed_urls = [
    {"source": "TechCrunch", "url": "https://techcrunch.com/feed/"},
    {"source": "CNBC", "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html"},
    {"source": "Wired", "url": "https://www.wired.com/feed/rss"},
    {"source": "Forbes", "url": "https://www.forbes.com/innovation/feed2"},
    {"source": "VentureBeat", "url": "http://feeds.feedburner.com/venturebeat/SZYF"},
    {"source": "CNET", "url": "https://www.cnet.com/rss/news/"},
    {"source": "ZDNet", "url": "https://www.zdnet.com/news/rss.xml"},
    {"source": "InfoWorld", "url": "https://www.infoworld.com/uk/index.rss"},
    {"source": "Mashable", "url": "https://mashable.com/feeds/rss/all"},
    {"source": "BBC Business", "url": "http://feeds.bbci.co.uk/news/business/rss.xml"},
    {"source": "BBC World", "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"source": "Yahoo", "url": "https://finance.yahoo.com/rss/topstories"},
    {"source": "CNN US", "url": "http://rss.cnn.com/rss/cnn_us.rss"},
    {"source": "CNN World", "url": "http://rss.cnn.com/rss/cnn_world.rss"},
    {"source": "Dow Jones", "url": "https://feeds.a.dj.com/rss/RSSWSJD.xml"},
    {"source": "CoinDesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
    {"source": "CoinTelegraph", "url": "https://cointelegraph.com/rss"},
    {"source": "Daily AI", "url": "https://dailyai.com/feed/"},
    {"source": "FT", "url": "https://www.ft.com/?format=rss"}
]

search_keywords = [
    'Interest Rate',
    'Microsoft',
    'Nvidia',
    'Bitcoin',
    'CBDC',
    'Stablecoin',
    'Openai',
    'Anthropic',
    'Cohere'
]

# FUNCTIONS

print('Loading function')

def send_email(subject, body, sender, recipients):
    ses_client = boto3.client('ses', region_name='us-east-1')  # Replace with your desired AWS region

    try:
        response = ses_client.send_email(
            Source=sender,
            Destination={'ToAddresses': recipients},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        print("Email sent successfully! Message ID: " + 
response['MessageId'])
    except Exception as e:
        print("Error sending email: " + str(e))

def write_json_to_s3(bucket_name, file_name, data):
    import boto3
    import json
    from datetime import datetime

    # Initialize a session using Amazon S3
    s3 = boto3.client('s3')
    
    # Serialize the JSON data
    json_data = json.dumps(data)
    
    # Write the JSON data to S3
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data, ContentType='application/json')



def parse_date(published_date_str):
    # Manual conversion of some known timezones to their UTC offsets.
    timezone_mappings = {
        'EDT': '-0400',
        'EST': '-0500',
        'CST': '-0600',
        'PST': '-0800'
        # Add more mappings as needed
    }
    
    for tz, offset in timezone_mappings.items():
        published_date_str = published_date_str.replace(tz, offset)

    formats = ["%a, %d %b %Y %H:%M:%S %z","%a, %d %b %Y %H:%M:%S %Z"]
    
    for fmt in formats:
        try:
            return datetime.strptime(published_date_str, fmt)
        except ValueError:
            continue

    return None

def is_old(published_date_str):
    published_date = parse_date(published_date_str)
                    
    if published_date is not None:
        # Get the current time and date
        current_date = datetime.now(published_date.tzinfo)
        # Calculate the time difference
        time_difference = current_date - published_date
        # Check if it's more than 24 hours old
        if time_difference < timedelta(hours=time_period):
            return False
        else:
            return True

def ai_sentiment(title_in):
    response = openai.ChatCompletion.create(
#        model="gpt-3.5-turbo",
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You will be provided with a news headline, and your task is to classify its sentiment as positive, neutral, or negative."},
            {"role": "user", "content": title_in}
        ],
        temperature=0,
        max_tokens=64,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )
    return response.choices[0].message['content'].strip()

def scrape_article_text(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    article_body = soup.find("article")
    if article_body is None:
        return None
    article_text = article_body.get_text(separator="\n")
    return article_text.strip()

def ai_summarize(article_in):
    scraped_text = scrape_article_text(article_in)
    if not scraped_text:
        return ""
    response = openai.ChatCompletion.create(
#        model="gpt-3.5-turbo",
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You will be provided with a news article, and your task is to summarize it."},
            {"role": "user", "content": scraped_text}
        ],
        temperature=0,
        max_tokens=250,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )
    return response.choices[0].message['content'].strip()

# main function to process stories (entries) in each RSS feed (feed_url) looking for keywords
def process_feeds(feed_urls, keywords=None):
    header = "Last 24hrs news sentiment:\n\n"
    body = ""
    counter = 0

    for keyword in keywords: # we are grouping results by topic (keyword) so this is the main for loop
        print("\n\nSearch term:", "\033[1m", keyword, "\033[0m")
        body += "-----------------\n"
        body += keyword + ": \n"
        matches = ""
        sntmt_pos = 0
        sntmt_neg = 0
        sntmt_neu = 0
        for feed_url in feed_urls: # check each entry title (ie news headline) in each feed_url (ie RSS feed) for the keywords (ie relevant topics)
            feed = feedparser.parse(feed_url['url'])
            print("Processing feed:", feed_url['source'])
                    
            if feed.status == 200:
                for entry in feed.entries:
                    if hasattr(entry, 'title') and hasattr(entry,'link') and hasattr(entry,'published'):
                        if not is_old(entry.published):
                            if keyword.lower() in entry.title.lower(): # if true, we have a match
                                counter += 1
                                print(entry.link)
                                matches += entry.title + " ("
                                sntmt = ai_sentiment(entry.title)
                                matches += sntmt + ")\n"
                                matches += entry.link + "\n"
                                if "positive" in sntmt.lower():
                                    sntmt_pos = sntmt_pos + 1
                                elif "negative" in sntmt.lower():
                                    sntmt_neg = sntmt_neg + 1
                                elif "neutral" in sntmt.lower():
                                    sntmt_neu = sntmt_neu + 1
            else:
                print("Error fetching feed")
        sent_count = sntmt_pos + sntmt_neg + sntmt_neu
        
        header += keyword + ": "
        
        if sent_count > 0:
            score = round((sntmt_pos - sntmt_neg) / sent_count,1)
            if score > 0.33:
                sntmt = "positive"
            elif score < -0.33: 
                sntmt = "negative"
            else:
                sntmt = "neutral"
            header += sntmt + " " + str(score) + " (" + str(sntmt_pos) + "," + str(sntmt_neu) + "," + str(sntmt_neg) + ")\n"
        else:
            score = 0
            sntmt = "na"
            header += sntmt
            matches += "No news\n"
            
        body += matches 

        #Prepare JSON data
        data = {"datestamp": datetime.now().isoformat(),"subject": keyword,"sentiment": sntmt,"score": score,"positive": sntmt_pos,"neutral": sntmt_neu, "negative":sntmt_neg}
        file_name = "sentiment/" + datetime.now().isoformat() + "-technews.json" # Keep this code here as there will be multiple writes in the for loop
        print(data)
        print(file_name + "\n")
        write_json_to_s3(bucket_name, file_name, data)
        print(score)
    if counter == 0:
        results += "No matching searches found today"
        
    body += "\nEnd of message\n"
    results = header + "\n" + body
    print("\n\n")
    print(results)
    return results



def lambda_handler(event, context):
#    body = test_openai_installation()
    body = process_feeds(rss_feed_urls, search_keywords)
    subject = 'Allison: Finance News Sentiment'
    sender = "chris@19nt.com"
    recipients = [
        "chrismicallison@gmail.com"
        ]
    send_email(subject, body, sender, recipients)
    return body	

