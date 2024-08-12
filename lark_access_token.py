import requests
import json
from requests.exceptions import RequestException
import logging

#读取当前refresh_token和user_token
def read_refresh_token(file_path):
    """Reads the first line (refresh token) and the remaining lines from the file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            first_line = file.readline().strip()
            remaining_lines = file.readlines()
            return first_line, remaining_lines
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
#写入新的refresh_token和user_token
def write_refresh_token(file_path, new_token, remaining_lines):
    """Writes the new refresh token and remaining lines back to the file."""
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(new_token + '\n')
            file.writelines(remaining_lines)
    except Exception as e:
        raise Exception(f"An error occurred while writing to the file: {e}")
#获取app_token
def get_app_token():
    """Requests the app token from the API."""
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    payload = json.dumps({
        "app_id": "cli_a6218b07f5ba900e",
        "app_secret": "7iTGJiQe6q0BCRnxRpsfvbFBlHgDUbQ3"
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=payload)
    response.raise_for_status()
    return response.json()['tenant_access_token']
#获取新的refresh_token和user_token
def get_new_tokens(old_refresh_token, app_token):
    """Requests new refresh and user access tokens from the API."""
    url = "https://open.feishu.cn/open-apis/authen/v1/oidc/refresh_access_token"
    payload = json.dumps({
        "grant_type": "refresh_token",
        "refresh_token": old_refresh_token
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {app_token}'
    }


    for attempt in range(1,  3):
        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()
            if 'data' in  response.json():
                data = response.json()['data']
                return data['refresh_token'], data['access_token']
            else:
                print(f'refresh token attempt {attempt} failed: {response.code} - {response.msg}')
        except RequestException as e:
            print(f'Attempt {attempt} raised exception: {e}')
            
#一个刷新token的完整调用函数
def renew_token(file_path):
    """Renews the token and writes the new refresh token to the file."""
    old_refresh_token, remaining_lines = read_refresh_token(file_path)
    app_token = get_app_token()
    new_refresh_token, user_access_token = get_new_tokens(old_refresh_token, app_token)
    write_refresh_token(file_path, new_refresh_token, user_access_token)
    return user_access_token
