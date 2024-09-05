from builtins import print

from lark_oapi.api.drive.v1 import *
import csv

from sqlalchemy import text, create_engine

import lark_cloud_document as lark_file
import pandas as pd
import os
import logging
import json
import schedule

cloud_drive_files_path='cloud_drive_files.csv'
bucket_name = 'raw-knowledge'
minio_path = 'MotionG/CloudDriveFiles'
download_path=os.path.join(os.getcwd(), 'temp')

minio_client = lark_file.get_minio_client()
folder_list_path='extracted_folders.csv'
files_path='cloud_drive_files.csv'
visited_folders_path='visited_folders.csv'

lark_file.user_token = lark_file.get_user_token1()
pathlib = ['MotionG/SpacesFiles/', 'MotionG/CloudDriveFiles/']
global visited_folders
#主函数，循环扫描文件夹列表
def isNull(bucket_objects):
    try:
        next(bucket_objects)
        return False
    except StopIteration:
        return True
def isInDB(fileName):
    a = False
    for path in pathlib:
        path = path + fileName
        bucket_objects = minio_client.list_objects('raw-knowledge', prefix=path)
        if isNull(bucket_objects):
            print(path+'不在数据库中')
            continue
        else:
            print(path+'在数据库中')
            a = True
            return a
    return a
def upload(file):
    # 配置日志记录，仅记录 ERROR 级别的日志
    logging.basicConfig(filename='error1.log', level=logging.ERROR,
                        format='%(asctime)s %(levelname)s %(message)s')

    # 确保其他模块的日志记录级别设置为 WARNING 或更高
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    fail_list = []
    valid_types = {'doc', 'sheet', 'bitable', 'docx', 'file'}
    if file['type'] == 'shortcut':
        obj = {
                "name": file['name'],
                "token": file['shortcut_info']['target_token'],
                "type": file['shortcut_info']['target_type'],
            }
    else:
        obj = {
                "name": file['name'],
                "token": file['token'],
                "type": file['type']
            }
    if obj['type'] in valid_types:
        result,comment = lark_file.lark_cloud_downloader(obj)
        if result == 0:
            print(f"Downloaded item: {obj['name']} ({obj['type']}), token: {obj['token']}")
            object_path=f"{minio_path}/{comment}"
            downloaded_file_path = os.path.join(download_path, comment)
            upload_result,upload_comment = lark_file.upload_to_minio(bucket_name,object_path,downloaded_file_path)   #上传到minio
            if upload_result == True:
                os.remove(downloaded_file_path)   #删除临时文件
                #shutil.rmtree(download_path)   #清空临时文件夹
        elif result == 2:
            # 将 item_data 写入 error.log 并指定编码为 UTF-8
            with open('error.log', 'a', encoding='utf-8') as f:
                f.write(json.dumps(obj, ensure_ascii=False) + '\n')
            logging.error(
                f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}")
            fail_list.append(obj)
        else:
            logging.error(
                f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}")
            fail_list.append(obj)
    else:
        print(f"Skipping row: {file['name']}. Invalid type:{file['type']}")

    if fail_list:
        print('Failed list:')
        print(json.dumps(fail_list, indent=4, ensure_ascii=False))
    print('Download mission completed.')
def Scan_folders(folders):
    for folder in folders:
        folder_token = folder['Token']
        # if folder_token in visited_folders:
        #     print(f"Folder {folder_token} already visited, skipping to avoid loop.")
        # else:
        print(f"Scanning folder: {folder['Name']}，{folder_token}")
        all_files = get_all_files(folder=folder_token)
        print(f" {len(all_files)} files found in foler {folder['Name']}")
#给定一个文件夹token，递归获取所有文件的metadata，并保存到csv文件中
def get_all_files(size=50, token='', folder=''):
    engine = create_engine('postgresql://intern:123456@postgresql.middleware.dev.motiong.net:5432/lark_files_backup')
    connection = engine.connect()
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
            '''
            To do
            判断在不在数据库；
                （在）     在判断时间 是否更新；
                    更新了则重新上传并且数据库记录
                （不在）    上传到数据库并记录
            '''
            file['version'] = 0
            file['versioncount'] = 0
            file['filepath'] = f"https://minio.middleware.dev.motiong.net/browser/raw-knowledge/MotionG/CloudDriveFiles/{file['token']}_{file['name']}"
            query = text("SELECT * FROM cloud_drive_files WHERE token = :token")
            result = connection.execute(query, {'token': file['token']})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            if not df.empty:
                # 数据库查到这个token
                fileName = f"{file['token']}_{file['name']}"
                if not isInDB(fileName):
                    # 如果在意外条件下，文件记录出现在数据库但不在minio里，则上传到桶里
                    upload(file)
                max_version_row = df.loc[df['version'].idxmax()]
                if int(file['modified_time']) > int(max_version_row['modified_time']):
                    # 查到token并且有更新（本次更新时间>已有的更新记录时间）
                    file['version'] = int(max_version_row['version']) + 1
                    file['versioncount'] = int(max_version_row['versioncount']) + 1
                    file['filepath'] = f"https://minio.middleware.dev.motiong.net/browser/raw-knowledge/MotionG/SpacesFiles/{file['token']}_{file['name']}_{file['version']}"
                    file['name'] = f"{file['name']}_{file['version']}"
                    update_query = text("""
                                        UPDATE cloud_drive_files
                                        SET versioncount = :new_versioncount
                                        WHERE token = :token AND version = :version
                                        """)
                    for index, row in df.iterrows():
                        connection.execute(update_query, {
                            'new_versioncount': int(row['versioncount']),
                            'token': row['token'],
                            'version': int(row['version'])
                        })
                    connection.commit()
                    newdf = pd.json_normalize(file)
                    newdf.to_sql('cloud_drive_files', con=engine, index=False, if_exists='append')
                else:
                    # 查到token并且如果没更新就不动
                    pass
            else:
                print(file['filepath'] + '上传Minio')
                fileName = f"{file['token']}_{file['name']}"
                if not isInDB(fileName):  # 不在minio 传到minio桶
                    upload(file)
                # 把文件的元数据插入数据库
                df = pd.json_normalize(file)
                df.rename(columns={'shortcut_info.target_token': 'shortcut_info_target_token'}, inplace=True)
                df.rename(columns={'shortcut_info.target_type': 'shortcut_info_target_type'}, inplace=True)
                df.to_sql('cloud_drive_files', con=engine, index=False, if_exists='append')
        lark_file.json_to_append_csv(files, files_path)
        for item in response_json['data']['files']:
            if item['type'] == 'folder':
                print(f"Scanning folder: {item['name']}，{item['token']}")
                files+=get_all_files(folder=item['token'])
        # print(file)
        visited_folders.add(folder)
        save_set_to_csv(visited_folders, visited_folders_path)
    connection.close()
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

def job():
    visited_folders = read_csv_to_set(visited_folders_path)
    with open(folder_list_path, 'r', encoding='utf-8') as folder_list:
        folders = list(csv.DictReader(folder_list))
    Scan_folders(folders)


schedule.every().day.at('19:00').do(job)


if __name__ == '__main__':
    while (True):
        schedule.run_pending()
        time.sleep(60)
