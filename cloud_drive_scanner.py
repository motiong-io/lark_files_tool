from builtins import print

from lark_oapi.api.drive.v1 import *
import csv

from sqlalchemy import text, create_engine
from sqlalchemy.pool import QueuePool


import lark_cloud_document as lark_file
import pandas as pd
import os
import logging
import json
import schedule
import time
import config
cloud_drive_files_path='cloud_drive_files.csv'
bucket_name = 'raw-knowledge'
minio_path = 'MotionG/CloudDriveFiles'
download_path=os.path.join(os.getcwd(), 'temp')

minio_client = lark_file.get_minio_client()
folder_list_path='extracted_folders.csv'
files_path='cloud_drive_files.csv'
visited_folders_path='visited_folders.csv'
updated_files_path='updated_files.csv'

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
def isInMinIO(fileName):
    global source_category
    a = False
    match source_category:
        case 'CloudDrive':
            path = pathlib[1]
        case 'SpacesFiles':
            path = pathlib[0]
        case _:
            return False
    path = path + fileName
    bucket_objects = minio_client.list_objects('raw-knowledge', prefix=path)
    if isNull(bucket_objects):
        print(path+'  ::不在:: 文件服务器中')
    else:
        print(path+'  ::在::   文件服务器中')
        a = True
    return a
#上传所有未上传的文件
def upload_new_files():
    print("-----------------------------------------------\n\nnow uploading updated files\n\n-----------------------------------------------")
    query = text("SELECT * FROM cloud_drive_files WHERE is_uploaded = '0'")
    connection = engine.connect()
    result = connection.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    connection.close()
    file_dict = df.to_dict(orient='records')

    # 配置日志记录，仅记录 ERROR 级别的日志
    logging.basicConfig(filename='lark_download_error.log', level=logging.ERROR,
                        format='%(asctime)s %(levelname)s %(message)s')

    # 确保其他模块的日志记录级别设置为 WARNING 或更高
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    fail_list = []
    remaining_files = file_dict.copy()

    for attempt in range(3):
        if not remaining_files:
            break
        current_files = remaining_files.copy()
        remaining_files = []
        for file in current_files:
            file, fail_list = upload(file, fail_list)
            if file['is_uploaded'] == '0' or (file['is_uploaded'] == '2' and 'Unsupported type' not in file['error_msg']):
                remaining_files.append(file)
            update_uploadstatus_db(file)
        

    if fail_list:
        print('Failed list:')
        print(json.dumps(fail_list, indent=4, ensure_ascii=False))
    print('Upload mission completed.')
def upload(file,fail_list):
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
                file['is_uploaded'] = '1'
                
        elif result == 2:
            # 将 item_data 写入 error.log 并指定编码为 UTF-8
            with open('error.log', 'a', encoding='utf-8') as f:
                f.write(json.dumps(obj, ensure_ascii=False) + '\n')
            logging.error(
                f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}")
            fail_list.append(obj)
            file['is_uploaded'] = '2'
            file['error_msg'] = comment
        else:
            logging.error(
                f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}")
            fail_list.append(obj)
            file['is_uploaded'] = '2'
            file['error_msg'] = comment
    else:
        msg=f"Unsupported type:{file['type']}"
        print(f"Skipping row: {file['name']}.{msg}")
        file['is_uploaded'] = '2'
        file['error_msg'] = msg
    return file,fail_list
def update_uploadstatus_db(file):
    global engine
    connection = engine.connect()
    update_query = text("""
                    UPDATE cloud_drive_files
                    SET is_uploaded = :new_is_uploaded
                        error_msg = :new_error_msg
                    WHERE token = :token 
                    """)

    try:
        connection.execute(update_query, {
                    'new_is_uploaded': file['is_uploaded'],
                    'new_error_msg': file['error_msg']
                })
        connection.commit()
        print("update db success")
    except Exception as e:
        print(f"Error occurred while updating data in cloud_drive_files table: {e}")
        with open('db_error_log.txt', 'a') as log_file:
            log_file.write(f"Error occurred while :::updating entry AFTER UPLOAD::: in cloud_drive_files table: {e}\n The data is: {file['type']},{file['name']}, {file['token']}")
        connection.rollback()
        print("update db failed")
    connection.close()
#遍历扫描所有文件夹
def Scan_folders(folders):
    global source_category
    source_category = 'CloudDrive'
    for folder in folders:
        folder_token = folder['Token']
        if folder_token in visited_folders:
            print(f"Folder {folder_token} already visited, skipping to avoid loop.")
        else:
            print(f"Scanning folder: {folder['Name']},{folder_token}")
            #all_files = get_all_files(folder=folder_token)
            error,updated_files_count,new_files_count=get_updated_files(folder=folder_token)

            if error==0:
                print(f"All files checked successfully in folder {folder['Name']}, {updated_files_count} files updated, {new_files_count} new files found")
            elif error>0:
                print(f"Some files failed to check in folder {folder['Name']}, {error} files failed, {updated_files_count} files updated, {new_files_count} new files found")
            else:
                continue
#扫描一个文件夹下所有子项，并对比入库
def get_updated_files(size=50, token='', folder=''):
    global visited_folders

    files=[]
    url = "https://open.feishu.cn/open-apis/drive/v1/files?direction=DESC&order_by=EditedTime"
    payload = {'page_size': size, 'page_token': token, 'folder_token': folder}
    
    try:
        response_json = lark_file.request_with_retry(url, params=payload)
        if not response_json:
            return 0,0,0  # 返回空列表和 None 表示分页结束
        
        if 'files' in response_json['data']:
            new_files=response_json['data']['files']
            files+=new_files
            for a_file in new_files:
                print("found file:",a_file['type'],":",a_file['name'],"_",a_file['token'])
            #分页遍历一个文件夹的所有子文件
            while response_json['data']['has_more'] is True:
                payload['page_token'] = response_json['data']['next_page_token']
                response_json = lark_file.request_with_retry(url, params=payload)
                a_file=response_json['data']['files']
                files += a_file
                print("found file:",a_file['type'],":",a_file['name'],"_",a_file['token'])
            #如果子文件类型是文件夹，递归遍历   
            for item in response_json['data']['files']:
                if item['type'] == 'folder':
                    print(f"Scanning folder: {item['name']},{item['token']}")
                    files+=get_updated_files(folder=item['token'])
            #遍历所有文件，检查是否在数据库,并进行对应规则更新
            error=0
            updated_files_count=0
            new_files_count=0
            for file in files:
                retries = 3
                for attempt in range(retries):
                    error,flag=checkDB(file)
                    if error==0:
                        if flag==1:
                            updated_files_count+=1
                        elif flag==2:
                            new_files_count+=1
                        break
                    else:
                        print(f"Checking with DB attempt {attempt + 1} failed")
                        if attempt == retries - 1:
                            error += 1
        visited_folders.add(folder)
        save_set_to_csv(visited_folders, visited_folders_path)
        return error,updated_files_count,new_files_count
    except Exception as e:
        print(f"Error occurred while scanning folder: {e}")
        return -1,0,0
    
        # print(file)

#检查文件是否在数据库，如果不在则插入，如果在则比对更新时间
def checkDB(file):
    file = dict(file)  # 把 file转换为字典格式
    print("checking file:", file['type'], ":", file['name'], "_", file['token'])
    file['version']=0
    file['versioncount']=0


    global engine
    connection = engine.connect()
    query = text("SELECT * FROM cloud_drive_files WHERE token = :token")
    result = connection.execute(query, {'token': file['token']})
    rows = result.fetchall()
    columns = result.keys()
    if len(columns) != len(rows[0]):
        print("Error: Number of columns does not match number of parameters fetched from the database.")
    df = pd.DataFrame(rows, columns=columns)
    df_dict = df.to_dict(orient='records')[0] if not df.empty else {}
    error = 0
    flag = 0
    is_updated = False

    if not df.empty:
        print("This is an old file, checking Minio...")
        fileName = f"{file['token']}_{file['name']}"
        if not isInMinIO(fileName):
            match int(df_dict['is_uploaded']):
                case 0:
                    print(f"File not uploaded, code: 0")
                case 2:
                    print(f"File met error when uploaded, code: 2")
                case _:
                    print(f"File metadate is found in db, but file is not found in MinIO, will try again... ")
                    file['is_uploaded'] = '0'
                    file['filepath']=[]
                    is_updated = True


        if int(file['modified_time']) > int(df_dict['modified_time']):
            print("This file has been updated, updating db...")
            old_version_row = df_dict.copy()
            old_version_row['token'] = f"{old_version_row['token']}_{old_version_row['version']}"
            old_version_row['versioncount'] = int(old_version_row['versioncount']) + 1
            old_version_df = pd.DataFrame([old_version_row])
            try:
                old_version_df.to_sql('cloud_drive_files', con=engine, index=False, if_exists='append')
            except Exception as e:
                print(f"Error occurred while inserting old version data into cloud_drive_files table: {e}")
                with open('db_error_log.txt', 'a') as log_file:
                    log_file.write(f"Error occurred while :::inserting old version entry::: into cloud_drive_files table: {e}\n The data is: {old_version_row}")
                connection.rollback()
                error = 1

            file['version'] = int(df_dict['version']) + 1
            file['versioncount'] = int(df_dict['versioncount']) + 1
            file['filepath'] = ''
            #file['filepath'] = f"https://minio.middleware.dev.motiong.net/browser/raw-knowledge/MotionG/CloudDriveFiles/{file['token']}_{file['name']}_{file['version']}"
            file['is_uploaded'] = '0'
            is_updated = True

        if is_updated:
            update_query = text("""
                UPDATE cloud_drive_files
                SET version = :new_version,
                    versioncount = :new_versioncount,
                    filepath = :new_filepath,
                    modified_time = :new_modified_time,
                    is_uploaded = :new_is_uploaded
                WHERE token = :token 
            """)

            try:
                connection.execute(update_query, {
                    'new_versioncount': int(file['versioncount']),
                    'token': file['token'],
                    'new_version': int(file['version']),
                    'new_filepath': file['filepath'],
                    'new_modified_time': file['modified_time'],
                    'new_is_uploaded': file['is_uploaded']
                })
                connection.commit()
                print("update db success")
                flag = 1
            except Exception as e:
                print(f"Error occurred while updating data in cloud_drive_files table: {e}")
                with open('db_error_log.txt', 'a') as log_file:
                    log_file.write(f"Error occurred while :::updating entry::: in cloud_drive_files table: {e}\n The data is: {file}")
                connection.rollback()
                print("update db failed")
                error = 1
        else:
            print("There has no update for this file, skipped.")
    else:
        print("This is a new file, adding to db...")
        file['filepath'] = f"https://minio.middleware.dev.motiong.net/browser/raw-knowledge/MotionG/CloudDriveFiles/{file['token']}_{file['name']}"
        file['is_uploaded'] = '0'
        df = pd.json_normalize(file)
        df.rename(columns={'shortcut_info.target_token': 'shortcut_info_target_token'}, inplace=True)
        df.rename(columns={'shortcut_info.target_type': 'shortcut_info_target_type'}, inplace=True)
        try:
            df.to_sql('cloud_drive_files', con=engine, index=False, if_exists='append')
            print("insert db success")
            flag = 2
        except Exception as e:
            print(f"Error occurred while inserting data into cloud_drive_files table: {e}")
            with open('db_error_log.txt', 'a') as log_file:
                log_file.write(f"Error occurred while :::inserting new entry::: into cloud_drive_files table: {e}\n The data is: {file}")
            connection.rollback()
            print("insert db failed")
            error = 1

    connection.close()
    return error, flag
#读取已扫描的文件夹列表csv文件到集合
def read_csv_to_set(file_path):
    # 检查文件是否存在
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        return set(df['FolderToken'])
    else:
        # 如果文件不存在，返回一个空集合
        return set()
#保存已扫描的文件夹列表集合到csv文件
def save_set_to_csv(my_set, file_path):
    
    # 将集合转换为DataFrame
    df = pd.DataFrame(list(my_set), columns=['Values'])
    
    # 将DataFrame增量更新到CSV文件（覆盖写入）
    df.to_csv(file_path, index=False)
#初始化已访问文件夹列表
def init_visited_folders():
    if os.path.exists(visited_folders_path):
        os.remove(visited_folders_path)
    with open(visited_folders_path, 'w', encoding='utf-8') as f:
        f.write('FolderToken\n')
    visited_folders = set()
    return visited_folders
#初始化数据库
def init_db():
    DB_CONNECTION_STRING = config.DB_CONNECTION_STRING#config是自己写的文件，配置信息；调用数据库
    engine = create_engine(DB_CONNECTION_STRING,
                           pool_size=10,         # 增加连接池大小
                           max_overflow=20,      # 增加最大溢出连接数
                           pool_timeout=30,      # 设置连接超时时间
                           pool_recycle=1800,    # 设置连接回收时间，防止连接被数据库服务器关闭
                           poolclass=QueuePool)  # 使用QueuePool连接池

    return engine
#从数据库中读取所有文件夹列表
def load_folders_from_db(engine):
    query = text("SELECT * FROM extracted_folders")
    connection = engine.connect()
    result = connection.execute(query)
    connection.close()
    folders = []
    for row in result.fetchall():
        folder = {
            "No": row[0],
            "Name": row[1],
            "Token": row[2]
        }
        folders.append(folder)
    return folders
#处理已访问文件夹和剩余文件夹列表
def scan_process_folders(folders):
    print("-----------------------------------------------\n\nnow scanning folders\n\n-----------------------------------------------")
    global visited_folders
    visited_folders = read_csv_to_set(visited_folders_path)
    remaining_folders = [folder for folder in folders if folder['Token'] not in visited_folders]
    while remaining_folders:
        remaining_folders_copy = remaining_folders.copy()
        try:
            Scan_folders(remaining_folders)
            remaining_folders = [folder for folder in folders if folder['Token'] not in visited_folders]
            if remaining_folders==remaining_folders_copy:
                count+=1
                if count>3:
                    break
            else:
                count=0
        except Exception as e:
            print(f"Error occurred: {e}")
#按流程执行所有主要任务
def job():
    global visited_folders  # 声明全局变量
    visited_folders = init_visited_folders()
    global engine
    engine = init_db()
    #todo: scan folders list & update db

    #test
    # TheFolder={'Name':'test','Token':'fldcngkQUmusWCT4itm4I95EKPd'}
    # Scan_folders([TheFolder])

    folders=load_folders_from_db(engine)
    scan_process_folders(folders)
    upload_new_files()
#设置定时任务
schedule.every().day.at('19:00').do(job)
if __name__ == '__main__':
    #test
    job()
    while (True):
        schedule.run_pending()
        time.sleep(60)
