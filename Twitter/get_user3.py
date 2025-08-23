import requests
import json
import time
from typing import List, Dict, Optional

class TwitterFollowingFetcher:
    def __init__(self, bearer_token: str):
        """
        初始化 Twitter API 客户端
        
        Args:
            bearer_token: Twitter API v2 的 Bearer Token
        """
        self.bearer_token = bearer_token
        self.base_url = "https://api.twitter.com/2"
        self.headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json"
        }
    
    def get_user_id(self, username: str) -> Optional[str]:
        """
        根据用户名获取用户ID
        
        Args:
            username: Twitter 用户名（不包含@符号）
            
        Returns:
            用户ID字符串，如果未找到则返回None
        """
        url = f"{self.base_url}/users/by/username/{username}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data:
                return data['data']['id']
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"获取用户ID时出错: {e}")
            return None
    
    def get_following_users(self, user_id: str, max_results: int = 1000) -> List[Dict]:
        """
        获取指定用户关注的所有用户
        
        Args:
            user_id: 用户ID
            max_results: 每次请求的最大结果数（1-1000）
            
        Returns:
            包含所有关注用户信息的列表
        """
        url = f"{self.base_url}/users/{user_id}/following"
        
        # 设置请求参数
        params = {
            "max_results": min(max_results, 1000),  # API限制最大1000
            "user.fields": "id,name,username,description,public_metrics,verified,created_at,profile_image_url"
        }
        
        all_following = []
        pagination_token = None
        
        while True:
            # 如果有分页令牌，添加到参数中
            if pagination_token:
                params["pagination_token"] = pagination_token
            
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                # 检查是否有数据
                if 'data' in data:
                    all_following.extend(data['data'])
                    print(f"已获取 {len(data['data'])} 个用户，总计: {len(all_following)}")
                else:
                    print("没有找到关注的用户")
                    break
                
                # 检查是否有下一页
                if 'meta' in data and 'next_token' in data['meta']:
                    pagination_token = data['meta']['next_token']
                    # 添加延迟以避免达到速率限制
                    time.sleep(1)
                else:
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"请求出错: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"响应内容: {e.response.text}")
                break
        
        return all_following
    
    def save_to_file(self, following_list: List[Dict], filename: str = "following_users.json"):
        """
        将关注用户列表保存到文件
        
        Args:
            following_list: 关注用户列表
            filename: 保存的文件名
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(following_list, f, ensure_ascii=False, indent=2)
            print(f"关注用户列表已保存到 {filename}")
        except Exception as e:
            print(f"保存文件时出错: {e}")
    
    def print_summary(self, following_list: List[Dict]):
        """
        打印关注用户的摘要信息
        
        Args:
            following_list: 关注用户列表
        """
        if not following_list:
            print("没有关注的用户")
            return
        
        print(f"\n=== 关注用户摘要 ===")
        print(f"总关注用户数: {len(following_list)}")
        print(f"\n前10个用户:")
        
        for i, user in enumerate(following_list[:10], 1):
            name = user.get('name', 'N/A')
            username = user.get('username', 'N/A')
            followers = user.get('public_metrics', {}).get('followers_count', 0)
            verified = "✓" if user.get('verified', False) else ""
            
            print(f"{i:2d}. {name} (@{username}) {verified}")
            print(f"    粉丝数: {followers:,}")
            if user.get('description'):
                desc = user['description'][:50] + "..." if len(user['description']) > 50 else user['description']
                print(f"    简介: {desc}")
            print()


def main():
    # 配置你的 Twitter API v2 Bearer Token
    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAFl42gEAAAAAOTZI7bcwvQl%2BuNDCOoP3V7Qx4oY%3DFkQJ3U6pbDWVpOASeVGG0ASibqtLwIoolsDCWcQSmjzQMgS34m"
    
    # 你的 Twitter 用户名（不包含@符号）
    USERNAME = "zainalipengjian"
    
    # 创建客户端实例
    client = TwitterFollowingFetcher(BEARER_TOKEN)
    
    print("开始获取关注用户列表...")
    
    # 1. 获取用户ID
    print(f"正在获取用户 @{USERNAME} 的ID...")
    user_id = client.get_user_id(USERNAME)
    
    if not user_id:
        print(f"无法找到用户 @{USERNAME}")
        return
    
    print(f"用户ID: {user_id}")
    
    # 2. 获取关注的用户列表
    print("正在获取关注用户列表...")
    following_users = client.get_following_users(user_id)
    
    if not following_users:
        print("没有获取到关注用户")
        return
    
    # 3. 显示摘要
    client.print_summary(following_users)
    
    # 4. 保存到文件
    client.save_to_file(following_users)
    
    print(f"\n完成！共获取 {len(following_users)} 个关注用户的信息")


# 使用示例
if __name__ == "__main__":
    main()
