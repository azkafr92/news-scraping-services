# news-scraping-services

run scrapyrt on web_crawler folder (port 5000)  ->  <code>scrapyrt -p 5000</code>.
url format : http://localhost:(port)/crawl.json?spider_name=(spider_name)&url=(url)

also, on crawler_dashboard folder, run  ->  <code>python getDataset.py</code>
this is the API to get dataset from database

then, run the app on the same folder -> <code>python app.py</code>
