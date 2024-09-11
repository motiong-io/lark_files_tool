import requests
import lark_oapi as lark
from lark_oapi.api.drive.v1 import *
import json
import time
import os
from requests.exceptions import RequestException
import lark_access_token as key_generate
from minio import Minio
from minio.error import S3Error
import config
import pandas as pd


# 开发平台获取的用户token：https://open.feishu.cn/api-explorer/cli_a6218b07f5ba900e
global user_token
# 文件下载路径
file_path = 'temp'
download_path=os.path.join(os.getcwd(),file_path)
#更新用户token--只读取
def get_user_token1():
    refresh_token,user_token= key_generate.read_refresh_token("refresh_token.txt")
    return user_token[0]
#更新用户token--刷新并读取
def get_user_token2():
    user_token = key_generate.renew_token("refresh_token.txt")
    return user_token
#通用api请求格式，自动重试
def request_with_retry(url, method='GET', headers=None, data=None, params=None, retries=10, delay=2):
    authorization_flag=0
    if headers is None:
        user_token = get_user_token1()
        headers = {'Authorization': 'Bearer ' + user_token,    }
    for attempt in range(1, retries + 3):
        try:
            response = requests.request(method, url, headers=headers, data=data, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                print(f'Attempt {attempt} failed: {response.status_code} - {response.text}')
                if (response.json()['code'] == 99991668 or response.json()['code'] == 99991677) and authorization_flag==0:
                    user_token= get_user_token1()
                    authorization_flag=1
                    headers = {'Authorization': 'Bearer ' + user_token,    }
                elif (response.json()['code'] == 99991668 or response.json()['code'] == 99991677) and authorization_flag==1:
                    user_token= get_user_token2()
                    authorization_flag=2
                    headers = {'Authorization': 'Bearer ' + user_token,    }
                else:
                    authorization_flag=0
                    return response.json()
        except RequestException as e:
            print(f'Attempt {attempt} raised exception: {e}')
        
        time.sleep(delay)
    return None
#所有文件的通用下载接口
def lark_cloud_downloader(item_data,version):
    print(f"Processing item: {item_data['name']} ({item_data['type']}), token: {item_data['token']}")
    if item_data['type'] == 'file':
        return file_downloader(file_token=item_data['token'], version= version, is_pdf=True)
    else:
        temp1 = output_mission(item_data['token'], item_data['type'])
        if temp1 and 'data' in temp1:
            item_data['ticket'] = temp1['data'].get('ticket')
            time.sleep(0.8)
            item_data['file_token'] = get_file_token_with_retry(item_data['ticket'], item_data['token'])
            if item_data['file_token']:
                return file_downloader(file_token=item_data['file_token'],initial_token=item_data['token'],version=version, is_pdf=False)
            else:
                return 2,"Failed to get file token"  # 表示出错
        else:
            return 2, f'Fetch cloud doc ticket failed: {temp1}'  # 表示出错
# 文件下载函数
def file_downloader(file_token, initial_token=None, version=1, is_pdf=True, ):
    # 创建 client
    client = lark.Client.builder() \
        .enable_set_token(True) \
        .log_level(lark.LogLevel.INFO) \
        .build()

    # 构造请求对象
    if is_pdf:
        request = DownloadFileRequest.builder() \
            .file_token(file_token) \
            .build()
    else:
        request = DownloadExportTaskRequest.builder() \
            .file_token(file_token) \
            .build()

    # 发起请求
    user_token = get_user_token1()
    option = lark.RequestOption.builder().user_access_token(user_token).build()
    authorization_flag = 0

    # 自动重试try to download the file
    for attempt in range(1, 10):
        try:
            if is_pdf:
                response = client.drive.v1.file.download(request, option)
            else:
                response = client.drive.v1.export_task.download(request, option)

            if not response.success():
                print(f'Download Attempt {attempt} failed: {response.code} - {response.msg}')
                if (response.code == 99991668 or response.code == 99991677) and authorization_flag == 0:
                    user_token = get_user_token1()
                    authorization_flag = 1
                    option = lark.RequestOption.builder().user_access_token(user_token).build()
                    time.sleep(5)
                elif (response.code == 99991668 or response.code == 99991677) and authorization_flag == 1:
                    user_token = get_user_token2()
                    authorization_flag = 2
                    option = lark.RequestOption.builder().user_access_token(user_token).build()
                    time.sleep(5)
                elif response.code == 99991400:
                    time.sleep(10)
                else:
                    authorization_flag = 0
                    return 2, response.msg
        except RequestException as e:
            error_msg=f'Attempt {attempt} raised exception: {e}.client.drive.v1.file.download failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}'
            print(error_msg)
            lark.logger.error(error_msg)
            return 2, error_msg

    # if download successful, 处理业务结果
    try:
        os.makedirs(download_path, exist_ok=True)
        if is_pdf:
            token=file_token
        else:
            token=initial_token
        if version == 1:
            file_name = f"{token}_{response.file_name}"
        else:
            if is_pdf:
                file_name, file_extension = os.path.splitext(response.file_name)
                file_name = f"{token}_{file_name}_{version}{file_extension}"
            else:
                file_name = f"{token}_{response.file_name}_{version}"

        sanitized_file_name = sanitize_filename(file_name)
        with open(f"{download_path}/{sanitized_file_name}", "wb") as f:
            f.write(response.file.read())
    except FileNotFoundError as e:
        return 2, e
    except Exception as e:
        return 2, e

    return 0, sanitized_file_name


#下载前，文件名非法字符替换
def sanitize_filename(filename):
    # 定义要替换的字符
    invalid_chars = '<>:"/\\\\|?*'
    # 替换无效字符为下划线
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename
#云文档类文件下载任务临时ticket获取
def output_mission(token='', file_type='', file_extension_type=''):
    url = "https://open.feishu.cn/open-apis/drive/v1/export_tasks"
    if file_type == 'sheet' or file_type == 'bitable':
        file_extension_type = 'xlsx'
    elif file_type == 'docx' or file_type == 'doc':
        file_extension_type = 'docx'

    payload = json.dumps({
        "file_extension": file_extension_type,
        "token": token,
        "type": file_type
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + user_token
    }
    response_json = request_with_retry(url, method="POST", headers=headers, data=payload)
    return response_json
#云文档类文件临时token获取
def get_file_token_with_retry(ticket, token, retries=10, delay=2, backoff_factor=2):
    delay_factor = delay
    for attempt in range(retries):
        temp2 = mission_status(ticket, token)
        if temp2:
            file_token = temp2.get('data', {}).get('result', {}).get('file_token')
            if file_token:
                return file_token
            else:
                message=temp2.get('data', {}).get('result', {}).get('msg')
                print(f"Error occurred: {message}")
                return None
        print(f"Retry attempt {attempt + 1} failed, retrying in {delay_factor} seconds...")
        time.sleep(delay_factor)
        delay_factor += backoff_factor
    return None
#云文档类文件下载任务状态获取
def mission_status(task_ticket='', task_token=''):
    url = f"https://open.feishu.cn/open-apis/drive/v1/export_tasks/{task_ticket}?token={task_token}"
    headers = {
        'Authorization': 'Bearer ' + user_token
    }

    response_json = request_with_retry(url, headers=headers)
    return response_json
#上传文件到minio
def upload_to_minio(bucket_name, object_name, file_path):
    minio_client = get_minio_client()
    stats=minio_stats(bucket_name, object_name)
    if stats==1:
        print(f"File {object_name} already exists in bucket {bucket_name}.")
        return True,0
    else:
        try:
            minio_client.fput_object(
                bucket_name,
                object_name,
                file_path
            )
            print(f"File {object_name} is successfully uploaded.")
            return True,1  # 上传成功
        except S3Error as err:
            print(f"Error occurred: {err}")
            return False,err  # 上传失败            
#读取minio配置
def get_minio_client():
    minio_config = config.minio_config
    return Minio(
        minio_config['endpoint'],
        access_key=minio_config['access_key'],
        secret_key=minio_config['secret_key'],
        secure=minio_config['secure']
    )
#检查minio中是否存在当前文件
def minio_stats(bucket_name, object_name):
    minio_client = get_minio_client()
    try:
        stat = minio_client.stat_object(bucket_name, object_name)
        return 1
    except S3Error as e:
        if e.code == 'NoSuchKey':
            return 0
        else:
            print(f"Error occurred while checking object: {e}")
            return 0    
#将JSON数据统一字段并追加写入CSV文件
def json_to_append_csv(json_data, file_path):
    """
    将JSON数据追加写入CSV文件，并处理字段不一致的情况。
    
    参数：
    json_data (str): 包含JSON数据的字符串。
    file_path (str): 要追加保存的CSV文件的路径。
    """
    try:
        # 将JSON数据转换为DataFrame
        df = pd.json_normalize(json_data)
        
        # 检查文件是否存在
        if not os.path.isfile(file_path):
            # 如果文件不存在，写入表头并新建文件
            df.to_csv(file_path, index=True, mode='w')
        else:
            # 如果文件存在，不写入表头，追加数据
            df.to_csv(file_path, index=False, mode='a', header=False)
        
        print(f"CSV文件已成功追加保存到 {file_path}")
    except json.JSONDecodeError as e:
        print(f"JSON解析错误：{e}")
    except Exception as e:
        print(f"发生错误：{e}")

