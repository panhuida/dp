import requests

# 替换为你自己的 Bearer Token
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAFl42gEAAAAAOTZI7bcwvQl%2BuNDCOoP3V7Qx4oY%3DFkQJ3U6pbDWVpOASeVGG0ASibqtLwIoolsDCWcQSmjzQMgS34m"

# 设置请求头
headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}"
}

# 查询参数
params = {
    "query": "Data Engineering",  # 搜索关键词
    "max_results": 10,            # 免费套餐最大限制
    "tweet.fields": "created_at,author_id,text,lang"
}

# 请求 URL
url = "https://api.twitter.com/2/tweets/search/recent"

# 发起请求
response = requests.get(url, headers=headers, params=params)
response.raise_for_status()
tweets = response.json()

# 打印推文内容
for tweet in tweets.get("data", []):
    print(f"[{tweet['created_at']}] @{tweet['author_id']}: {tweet['text']}\n")
