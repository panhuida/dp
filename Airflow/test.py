import requests

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAFl42gEAAAAAOTZI7bcwvQl%2BuNDCOoP3V7Qx4oY%3DFkQJ3U6pbDWVpOASeVGG0ASibqtLwIoolsDCWcQSmjzQMgS34m"
USERNAME = "zainalipengjian"  # 不带 @，例如 "elonmusk"

url = f"https://api.twitter.com/2/users/by/username/{USERNAME}"
headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

response = requests.get(url, headers=headers)
data = response.json()

print(data)