from lark_oapi.api.drive.v1 import *
import csv
import lark_cloud_document as lark_file
import pandas as pd
import os


folder_list_path='extracted_folders.csv'
files_path='cloud_drive_files.csv'
visited_folders_path='visited_folders.csv'
global visited_folders
#主函数，循环扫描文件夹列表
def Scan_folders(folders):
    for folder in folders:
        folder_token = folder['Token']
        if folder_token in visited_folders:
            print(f"Folder {folder_token} already visited, skipping to avoid loop.")
        else:
            print(f"Scanning folder: {folder['Name']}，{folder_token}")
            all_files = get_all_files(folder=folder_token)
            print(f" {len(all_files)} files found in foler {folder['Name']}")
#给定一个文件夹token，递归获取所有文件的metadata，并保存到csv文件中
def get_all_files(size=50, token='', folder=''):
    files=[]
    url = "https://open.feishu.cn/open-apis/drive/v1/files?direction=DESC&order_by=EditedTime"
    payload = {'page_size': size, 'page_token': token, 'folder_token': folder}
    response_json = lark_file.request_with_retry(url, params=payload)
    if not response_json:
        return [], None  # 返回空列表和 None 表示分页结束
    if 'files' in response_json['data']:
        files+=response_json['data']['files']
        while response_json['data']['has_more'] is True:
            payload['page_token'] = response_json['data']['next_page_token']
            response_json = lark_file.request_with_retry(url, params=payload)
            files += response_json['data']['files']
        for file in files:
            print(file['type'],':',file['name'])
        lark_file.json_to_append_csv(files, files_path)
        for item in response_json['data']['files']:
            if item['type'] == 'folder':
                print(f"Scanning folder: {item['name']}，{item['token']}")
                files+=get_all_files(folder=item['token'])
        visited_folders.add(folder)
        save_set_to_csv(visited_folders, visited_folders_path)
    return files
#读取已扫描的文件夹列表csv文件到集合
def read_csv_to_set(file_path):
    # 检查文件是否存在
    if os.path.exists(file_path):
        # 从CSV文件读取数据到一个集合
        df = pd.read_csv(file_path)
        return set(df['Values'])
    else:
        # 如果文件不存在，返回一个空集合
        return set()
#保存已扫描的文件夹列表集合到csv文件
def save_set_to_csv(my_set, file_path):
    
    # 将集合转换为DataFrame
    df = pd.DataFrame(list(my_set), columns=['Values'])
    
    # 将DataFrame增量更新到CSV文件（覆盖写入）
    df.to_csv(file_path, index=False)
if __name__ == '__main__':
    # 读取已访问的文件夹列表
    visited_folders = read_csv_to_set(visited_folders_path)

    # 从 extracted_folders.csv 中读取所有 folder_token
    with open(folder_list_path, 'r', encoding='utf-8') as folder_list:
        folders = list(csv.DictReader(folder_list))
    Scan_folders(folders)
