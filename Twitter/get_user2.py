import requests
import json
import time
from typing import List, Dict, Optional

class TwitterFollowingFetcher:
    def __init__(self, bearer_token: str = None, api_key: str = None, api_secret: str = None, 
                 access_token: str = None, access_token_secret: str = None):
        """
        初始化 Twitter API 客户端
        
        Args:
            bearer_token: Twitter API Bearer Token（推荐）
            api_key: API Key（备选方案）
            api_secret: API Secret Key（备选方案）
            access_token: Access Token（备选方案）
            access_token_secret: Access Token Secret（备选方案）
        """
        self.base_url = "https://api.twitter.com/2"
        
        if bearer_token:
            self.headers = {
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json"
            }
            self.auth_type = "bearer"
        elif all([api_key, api_secret, access_token, access_token_secret]):
            # 使用 OAuth 1.0a 认证（需要额外的库）
            try:
                from requests_oauthlib import OAuth1
                self.auth = OAuth1(api_key, api_secret, access_token, access_token_secret)
                self.headers = {"Content-Type": "application/json"}
                self.auth_type = "oauth1"
            except ImportError:
                raise ImportError("使用 OAuth 1.0a 需要安装: pip install requests-oauthlib")
        else:
            raise ValueError("必须提供 bearer_token 或完整的 OAuth 1.0a 凭据")
    
    def get_user_id(self, username: str) -> Optional[str]:
        """
        根据用户名获取用户 ID
        
        Args:
            username: Twitter 用户名（不含@符号）
            
        Returns:
            用户 ID 或 None
        """
        url = f"{self.base_url}/users/by/username/{username}"
        
        if self.auth_type == "oauth1":
            response = requests.get(url, headers=self.headers, auth=self.auth)
        else:
            response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            data = response.json()
            return data['data']['id']
        elif response.status_code == 403:
            print("❌ 认证失败，请检查以下事项：")
            print("1. 确保你的 Twitter 开发者账号已激活")
            print("2. 确保已创建 Project 并将 App 附加到 Project")
            print("3. 确保 App 有读取权限")
            print("4. 确保使用的是正确的 Bearer Token")
            print(f"错误详情: {response.text}")
            return None
        else:
            print(f"获取用户 ID 失败: {response.status_code} - {response.text}")
            return None
    
    def get_following_users(self, user_id: str, max_results: int = 1000) -> List[Dict]:
        """
        获取指定用户关注的所有用户
        
        Args:
            user_id: 用户 ID
            max_results: 每次请求的最大结果数（1-1000）
            
        Returns:
            关注的用户列表
        """
        all_following = []
        pagination_token = None
        
        # 设置用户字段，获取更详细的信息
        user_fields = [
            "id", "name", "username", "description", "public_metrics",
            "verified", "created_at", "profile_image_url", "location"
        ]
        
        while True:
            url = f"{self.base_url}/users/{user_id}/following"
            
            params = {
                "max_results": min(max_results, 1000),  # API 限制最大 1000
                "user.fields": ",".join(user_fields)
            }
            
            if pagination_token:
                params["pagination_token"] = pagination_token
            
            try:
                if self.auth_type == "oauth1":
                    response = requests.get(url, headers=self.headers, params=params, auth=self.auth)
                else:
                    response = requests.get(url, headers=self.headers, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'data' in data:
                        all_following.extend(data['data'])
                        print(f"已获取 {len(data['data'])} 个用户，总计 {len(all_following)} 个")
                    
                    # 检查是否有下一页
                    if 'meta' in data and 'next_token' in data['meta']:
                        pagination_token = data['meta']['next_token']
                        # 添加延迟以避免速率限制
                        time.sleep(1)
                    else:
                        break
                        
                elif response.status_code == 429:
                    # 处理速率限制
                    print("达到速率限制，等待 15 分钟...")
                    time.sleep(15 * 60)
                    continue
                    
                else:
                    print(f"请求失败: {response.status_code} - {response.text}")
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"请求异常: {e}")
                break
        
        return all_following
    
    def get_my_following(self, username: str) -> List[Dict]:
        """
        获取自己关注的所有用户
        
        Args:
            username: 自己的 Twitter 用户名
            
        Returns:
            关注的用户列表
        """
        # 首先获取自己的用户 ID
        user_id = self.get_user_id(username)
        if not user_id:
            return []
        
        print(f"获取到用户 ID: {user_id}")
        
        # 获取关注的用户列表
        following_users = self.get_following_users(user_id)
        
        return following_users
    
    def save_to_file(self, following_users: List[Dict], filename: str = "twitter_following.json"):
        """
        将关注用户列表保存到文件
        
        Args:
            following_users: 关注的用户列表
            filename: 保存的文件名
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(following_users, f, ensure_ascii=False, indent=2)
            print(f"数据已保存到 {filename}")
        except Exception as e:
            print(f"保存文件失败: {e}")
    
    def export_to_csv(self, following_users: List[Dict], filename: str = "twitter_following.csv"):
        """
        将关注用户列表导出为 CSV 文件
        
        Args:
            following_users: 关注的用户列表
            filename: CSV 文件名
        """
        try:
            import pandas as pd
            
            # 提取主要字段
            simplified_data = []
            for user in following_users:
                simplified_data.append({
                    'id': user.get('id'),
                    'name': user.get('name'),
                    'username': user.get('username'),
                    'description': user.get('description', ''),
                    'followers_count': user.get('public_metrics', {}).get('followers_count', 0),
                    'following_count': user.get('public_metrics', {}).get('following_count', 0),
                    'tweet_count': user.get('public_metrics', {}).get('tweet_count', 0),
                    'verified': user.get('verified', False),
                    'created_at': user.get('created_at'),
                    'location': user.get('location', ''),
                    'profile_image_url': user.get('profile_image_url', '')
                })
            
            df = pd.DataFrame(simplified_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"CSV 数据已保存到 {filename}")
            
        except ImportError:
            print("需要安装 pandas: pip install pandas")
        except Exception as e:
            print(f"导出 CSV 失败: {e}")
    
    def print_summary(self, following_users: List[Dict]):
        """
        打印关注用户的摘要信息
        
        Args:
            following_users: 关注的用户列表
        """
        if not following_users:
            print("没有获取到关注的用户")
            return
        
        print(f"\n=== 关注用户摘要 ===")
        print(f"总关注用户数: {len(following_users)}")
        
        # 统计认证用户数量
        verified_count = sum(1 for user in following_users if user.get('verified', False))
        print(f"认证用户数: {verified_count}")
        
        # 显示前 10 个用户
        print(f"\n前 10 个关注的用户:")
        for i, user in enumerate(following_users[:10], 1):
            verified_mark = "✓" if user.get('verified', False) else ""
            followers = user.get('public_metrics', {}).get('followers_count', 0)
            print(f"{i:2d}. @{user.get('username')} {verified_mark}")
            print(f"     {user.get('name')} | 粉丝: {followers:,}")


def main():
    """
    主函数 - 使用示例
    """
    # 方法1: 使用 Bearer Token（推荐）
    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAFl42gEAAAAAOTZI7bcwvQl%2BuNDCOoP3V7Qx4oY%3DFkQJ3U6pbDWVpOASeVGG0ASibqtLwIoolsDCWcQSmjzQMgS34m"
    
    # 方法2: 使用 OAuth 1.0a（备选方案）
    API_KEY = "mltQhOMqAtE6PGj2JiuyMbBVN"
    API_SECRET = "2x6WZRsc1ndJoLmMSvV2iUbC8MsXNIYgbioWdNkwLiml0nACp7"
    ACCESS_TOKEN = "256530038-aJ9THr9YXo4dxXsV6AmaiROF47GOTYui0Xm2Bohb"
    ACCESS_TOKEN_SECRET = "ra5pfnO1bbJpOmPRPoYJwF87xNiNEOKRuFeMw58b43hWd"
    
    # 配置你的 Twitter 用户名（不含@符号）
    MY_USERNAME = "zainalipengjian"
    
    try:
        # 创建客户端（方法1）
        # fetcher = TwitterFollowingFetcher(bearer_token=BEARER_TOKEN)
        
        # 创建客户端（方法2 - 如果方法1失败）
        fetcher = TwitterFollowingFetcher(
            api_key=API_KEY,
            api_secret=API_SECRET,
            access_token=ACCESS_TOKEN,
            access_token_secret=ACCESS_TOKEN_SECRET
        )
        
        # 获取关注的用户列表
        print("开始获取关注用户列表...")
        following_users = fetcher.get_my_following(MY_USERNAME)
        
        if following_users:
            # 打印摘要
            fetcher.print_summary(following_users)
            
            # 保存为 JSON 文件
            fetcher.save_to_file(following_users, "my_twitter_following.json")
            
            # 导出为 CSV 文件（需要安装 pandas）
            fetcher.export_to_csv(following_users, "my_twitter_following.csv")
        
        else:
            print("获取关注用户列表失败")
            
    except Exception as e:
        print(f"执行失败: {e}")
        print("\n🔧 故障排除建议:")
        print("1. 检查 Twitter Developer Portal 设置")
        print("2. 确保 App 附加到 Project")
        print("3. 验证 API 密钥和令牌")
        print("4. 确认账号有适当的 API 访问级别")


if __name__ == "__main__":
    main()