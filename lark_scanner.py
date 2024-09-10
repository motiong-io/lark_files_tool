from lark_oapi.api.drive.v1 import *
import csv
from sqlalchemy import text, create_engine
from sqlalchemy.pool import QueuePool
import lark_cloud_document as lark_file
import pandas as pd
import os
import sys
import fcntl
import logging
import json
import schedule
import time
import datetime
import config

minio_client = lark_file.get_minio_client()
bucket_name = 'raw-knowledge'
download_path = os.path.join(os.getcwd(), 'temp')

global visited_folders, visited_spaces
visited_folders_path = 'visited_folders.csv'
visited_spaces_path = 'visited_spaces.csv'

lark_file.user_token = lark_file.get_user_token1()

#设置stdin为非阻塞模式
fd=sys.stdin.fileno()
fl=fcntl.fcntl(fd,fcntl.F_GETFL)
fcntl.fcntl(fd,fcntl.F_SETFL,fl|os.O_NONBLOCK)

# 自定义CSV格式化器
class CSVFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()

    def format(self, record):
        record.message = record.getMessage()
        return f'{record.asctime},{record.name},{record.levelname},{record.message},{record.error_category}'

# 自定义CSV文件处理器
class CSVFileHandler(logging.FileHandler):
    def __init__(self,):
        self.log_filename = self.create_initial_log_file()
        super().__init__(self.log_filename, mode='a', encoding='utf-8', delay=False)
        self.writer = csv.writer(self.stream)
        self.writer.writerow(['Timestamp', 'Logger Name', 'Level', 'Message', 'Error Category'])
    def create_initial_log_file(self):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"log_{timestamp}.csv"

    def emit(self, record):
        try:
            if not hasattr(record, 'asctime'):
                record.asctime = self.formatter.formatTime(record, self.formatter.datefmt)
            msg = self.format(record)
            self.writer.writerow(msg.split(','))
            self.flush()
        except Exception:
            self.handleError(record)
    def rotate_log_file(self):
        self.stream.close()
        base, ext = os.path.splitext(self.log_filename)
        suffix = 1
        new_log_file = f"{base}_{suffix}{ext}"
        while os.path.exists(new_log_file):
            suffix += 1
            new_log_file = f"{base}_{suffix}{ext}"
        self.log_filename = new_log_file
        self.baseFilename = self.log_filename
        self.stream = self._open()
        self.writer = csv.writer(self.stream)
        self.writer.writerow(['Timestamp', 'Logger Name', 'Level', 'Message', 'Error Category'])


def init_logger():
    global logger, csv_handler
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)

    # 配置CSV文件处理器
    csv_handler = CSVFileHandler()
    csv_formatter = CSVFormatter()
    csv_handler.setFormatter(csv_formatter)
    logger.addHandler(csv_handler)

    # 配置终端输出处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

# 自定义日志记录函数，添加错误分类
def log_with_category(logger, level, message, category):
    extra = {'error_category': category}
    if level == 'debug':
        logger.debug(message, extra=extra)
    elif level == 'info':
        logger.info(message, extra=extra)
    elif level == 'warning':
        logger.warning(message, extra=extra)
    elif level == 'error':
        logger.error(message, extra=extra)
    elif level == 'critical':
        logger.critical(message, extra=extra)

def check_log_file_size():
    log_file = csv_handler.log_filename
    if os.path.exists(log_file):
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if len(lines) > 990000:
                csv_handler.rotate_log_file()
      

# 主函数，循环扫描文件夹列表
def isNull(bucket_objects):
    try:
        next(bucket_objects)
        return False
    except StopIteration:
        return True

def isInMinIO(fileName, cat_flag):
    syntax = lark_syntax(cat_flag)
    path = syntax['path'] + fileName
    try:
        stats = minio_client.stat_object(bucket_name, path)
        log_with_category(logger, 'info', path + '  ::在::   文件服务器中', 'MinIO')
        return True
    except Exception as e:
        log_with_category(logger, 'info', path + '  ::不在:: 文件服务器中', 'MinIO')
        return False

# 上传所有未上传的文件
def upload_new_files(cat_flag):
    try:
        check_log_file_size()
    except Exception as e:
        log_with_category(logger, 'error', f"Error checking log file size: {e}", 'lark_scanner')
        return
    table_name = lark_syntax(cat_flag)['category']
    log_with_category(logger, 'info', f"-----------------------------------------------\n\nnow uploading updated {table_name}\n\n-----------------------------------------------", '')
    query = text(f"SELECT * FROM {table_name} WHERE is_uploaded = '0'")
    try:
        result = execute_query_with_retry(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        log_with_category(logger, 'error', f"Error executing query: {e}", 'postgres')
        return
    file_dict = df.to_dict(orient='records')

    fail_list = []
    remaining_files = file_dict.copy()

    for attempt in range(3):
        if not remaining_files:
            break
        current_files = remaining_files.copy()
        remaining_files = []
        for file in current_files:
            file, fail_list = upload(file, fail_list, cat_flag)
            if file['is_uploaded'] == '0' or (file['is_uploaded'] == '2' and 'Unsupported type' not in file['error_msg']):
                remaining_files.append(file)
            update_uploadstatus_db(file, cat_flag)

    if fail_list:
        log_with_category(logger,'info',f'Upload mission completed with {len(fail_list)} files failed','')
        log_with_category(logger, 'error', 'Failed list:', 'MinIO')
        log_with_category(logger, 'error', json.dumps(fail_list, indent=4, ensure_ascii=False), 'MinIO')
    log_with_category(logger, 'info', 'Upload mission completed without any failed.', '')

def upload(file, fail_list, cat_flag):
    syntax = lark_syntax(cat_flag)
    name, token, type, minio_path = syntax['name'], syntax['token'], syntax['type'], syntax['path']
    version = file['version']
    file['filepath'] = ''
    file['error_msg'] = ''
    valid_types = {'doc', 'sheet', 'bitable', 'docx', 'file', 'slides'}
    if file[type] == 'shortcut':
        obj = {
            "name": file[name],
            "token": file['shortcut_info_target_token'],
            "type": file['shortcut_info_target_type'],
        }
    else:
        obj = {
            "name": file[name],
            "token": file[token],
            "type": file[type]
        }
    if obj['type'] in valid_types:
        result, comment = lark_file.lark_cloud_downloader(obj, version)
        if result == 0:
            log_with_category(logger, 'info', f"Downloaded item: {obj['name']} ({obj['type']}), token: {obj['token']}", 'upload')
            object_path = minio_path + comment
            downloaded_file_path = os.path.join(download_path, comment)
            upload_result, upload_comment = lark_file.upload_to_minio(bucket_name, object_path, downloaded_file_path)  # 上传到minio
            if upload_result:
                os.remove(downloaded_file_path)  # 删除临时文件
                log_with_category(logger, 'info', f"Successfully uploaded item: {obj['name']} ({obj['type']}), token: {obj['token']}", 'upload')
                file['is_uploaded'] = '1'
                file['filepath'] = f"https://storage.minio.middleware.dev.motiong.net/raw-knowledge/{object_path}"

            else:
                log_with_category(logger, 'error', f"File is downloaded, but failed to upload to MinIO: {obj['name']} ({obj['type']}), token: {obj['token']}。:::REASON:{upload_comment}.\nDetails: {json.dumps(obj, ensure_ascii=False)}", 'lark_downloader')
                fail_list.append(obj)
                file['is_uploaded'] = '2'
                file['error_msg'] = upload_comment
        elif result == 2:
            log_with_category(logger, 'error', f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}。:::REASON:{comment}.\nDetails: {json.dumps(obj, ensure_ascii=False)}", 'lark_downloader')
            fail_list.append(obj)
            file['is_uploaded'] = '2'
            file['error_msg'] = comment
        else:
            log_with_category(logger, 'error', f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}。:::REASON:{comment}.\nDetails: {json.dumps(obj, ensure_ascii=False)}", 'lark_downloader')
            fail_list.append(obj)
            file['is_uploaded'] = '2'
            file['error_msg'] = comment
    else:
        msg = f"Unsupported type:{file[type]}"
        log_with_category(logger, 'info', f"Skipping row: {file[name]}.:::REASON:{msg}", 'lark_downloader')
        file['is_uploaded'] = '2'
        file['error_msg'] = msg
    return file, fail_list

def lark_syntax(cat_flag):
    return {
        'name': 'name' if cat_flag == 1 else 'title',
        'token': 'token' if cat_flag == 1 else 'obj_token',
        'type': 'type' if cat_flag == 1 else 'obj_type',
        'modified_time': 'modified_time' if cat_flag == 1 else 'obj_edit_time',
        'category':'cloud_drive_files' if cat_flag == 1 else 'space_nodes',
        'path': 'MotionG/CloudDriveFiles/' if cat_flag == 1 else 'MotionG/SpacesFiles/',
        'data_name': 'files' if cat_flag == 1 else 'items',
        'folder_token': 'Token' if cat_flag == 1 else 'space_id',
        'list_table':'extracted_folders' if cat_flag == 1 else 'space_list',
        'folder_node':'folder' if cat_flag == 1 else 'node'
    }
def process_file_name(file,syntax):
    name,token,type = syntax['name'],syntax['token'],syntax['type']
    if file['version'] == 1:
        version=''
    else:
        version = f"_{file['version']}"

    if file[type] == 'bitable' or file[type] == 'sheet':
        fileName = f"{file[token]}_{file[name]}{version}.xlsx"
    elif file[type] == 'slides':
        fileName = f"{file[token]}_{file[name]}{version}.pptx"
    elif file[type] == 'file':
        filename,extension = os.path.splitext(file[name])
        fileName = f"{file[token]}_{filename}{version}{extension}"
    else:
        fileName = f"{file[token]}_{file[name]}{version}.{file[type]}"
    return fileName

def update_uploadstatus_db(file, cat_flag):
    syntax = lark_syntax(cat_flag)
    type,name,table_name,token = syntax['type'],syntax['name'],syntax['category'],syntax['token']
    global engine
    update_query = text(f"""
                    UPDATE {table_name}
                    SET is_uploaded = :new_is_uploaded,
                        error_msg = :new_error_msg,
                        filepath = :new_filepath
                    WHERE {token} = :token 
                    """)

    try:
        execute_query_with_retry(update_query, {
            'new_is_uploaded': file['is_uploaded'],
            'new_error_msg': file['error_msg'],
            'new_filepath': file['filepath'],
            'token': file[token]
        })
        log_with_category(logger, 'info', "update db success", 'postgres')
    except Exception as e:
        log_with_category(logger, 'error', f"Error occurred while :::updating entry AFTER UPLOAD::: in {table_name} table: {e}\n The data is: {file[type]},{file[name]}, {file[token]}", 'postgres')

# 遍历扫描所有文件夹
def Scan_folders(folders, cat_flag):
    for folder in folders:
        try:
            check_log_file_size()
        except Exception as e:
            log_with_category(logger, 'error', f"Error checking log file size: {e}", 'lark_scanner')
            return
        folder_token_name = lark_syntax(cat_flag)['folder_token']
        folder_token = folder[folder_token_name]
        if folder_token in visited_folders:
            log_with_category(logger, 'info', f"Folder {folder_token} already visited, skipping to avoid loop.", '')
        else:
            log_with_category(logger, 'info', f"Scanning folder: {folder['Name']},{folder_token}", '')
            try:    
                files = get_all_files(folder=folder_token, cat_flag=cat_flag)
                error, updated_files_count, new_files_count = scan_process_updated_files(files,cat_flag)
            except Exception as e:
                log_with_category(logger, 'error', f"Error occurred while scanning folder in folders: {e}", 'lark_scanner')
        
            if cat_flag==1:
                visited_folders.add(folder['token'])
                save_set_to_csv(visited_folders, visited_folders_path)
            else:
                visited_spaces.add(folder['space_id'])
                save_set_to_csv(visited_spaces, visited_spaces_path)  


            if error == 0:
                log_with_category(logger, 'info', f"All files checked successfully in folder {folder['Name']}, {updated_files_count} files updated, {new_files_count} new files found", '')
            elif error > 0:
                log_with_category(logger, 'warning', f"Some files failed to check in folder {folder['Name']}, {error} files failed, {updated_files_count} files updated, {new_files_count} new files found", 'postgres')
            else:
                continue

# 扫描一个文件夹下所有子项，并对比入库
def get_all_files(size=50, token='', folder='', parent_node_token='', cat_flag=1):
    '''size: 每次请求的文件数
    token: 分页token
    folder: 文件夹或space token
    cat_flag: 分类标志，1为云文档，0为知识库'''
    global visited_folders
    files = []
    syntax = lark_syntax(cat_flag)
    url = "https://open.feishu.cn/open-apis/drive/v1/files?direction=DESC&order_by=EditedTime" if cat_flag == 1 else f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{folder}/nodes"
    payload = {'page_size': size, 'page_token': token, 'folder_token': folder} if cat_flag == 1 else {'page_size': size, 'page_token': token, 'parent_node_token': parent_node_token}
    name,type,file_token, data_name , folder_node = syntax['name'],syntax['type'],syntax['token'],syntax['data_name'],syntax['folder_node']
    try:
        response_json = lark_file.request_with_retry(url, params=payload)
        if not response_json:
            return 0, 0, 0  # 返回空列表和 None 表示分页结束

        if data_name in response_json['data']:
            new_files = response_json['data'][data_name]
            files += new_files
            for a_file in new_files:
                log_with_category(logger, 'info', f"found file: {a_file[type]} : {a_file[name]} _ {a_file[file_token]}", '')
            # 分页遍历一个文件夹的所有子文件
            while response_json['data']['has_more'] is True:
                payload['page_token'] = response_json['data']['next_page_token']
                response_json = lark_file.request_with_retry(url, params=payload)
                a_file = response_json['data'][data_name]
                files += a_file
                log_with_category(logger, 'info', f"found file: {a_file[type]} : {a_file[name]} _ {a_file[file_token]}", '')
            # 如果子文件类型是文件夹，递归遍历
            for item in response_json['data'][data_name]:
                if item[type] == 'folder' or item.get('has_child', False) is True:
                    log_with_category(logger, 'info', f"Scanning {folder_node}: {item[name]},{item[file_token]}", '')
                    if cat_flag==1 and item[type] == 'folder':
                        files += get_all_files(folder=item[file_token], cat_flag=cat_flag)
                    elif cat_flag==0 and item['has_child'] is True:
                        files += get_all_files(folder=folder, parent_node_token=item['node_token'], cat_flag=cat_flag)
    except Exception as e:
        log_with_category(logger, 'error', f"Error occurred while scanning one folder: {e}", 'lark_scanner')

    return files

def scan_process_updated_files(files,cat_flag):

    # 遍历所有文件，检查是否在数据库,并进行对应规则更新
    error = 0
    updated_files_count = 0
    new_files_count = 0
    
    # 确保 files 是一个可迭代对象
    if not isinstance(files, (list, tuple)):
        log_with_category(logger, 'error', f"Expected files to be a list or tuple, but got {type(files)}", 'lark_scanner')
        return -1, 0, 0
    try:
        for file in files:
            retries = 3
            for attempt in range(retries):
                try:
                    error, flag = checkDB(file, cat_flag)
                    if not isinstance((error, flag), tuple):
                        raise TypeError(f"Expected checkDB to return a tuple, but got {type((error, flag))}")
                except TypeError as e:
                    log_with_category(logger, 'error', f"TypeError occurred in checkDB: {e}", 'lark_scanner')
                    error = 1
                    flag = 0
                if error == 0:
                    if flag == 1:
                        updated_files_count += 1
                    elif flag == 2:
                        new_files_count += 1
                    break
                else:
                    log_with_category(logger, 'warning', f"Checking with DB attempt {attempt + 1} failed", 'postgres')
                    if attempt == retries - 1:
                        error += 1
    except TypeError as e:
        log_with_category(logger, 'error', f"TypeError occurred while iterating files: {e}", 'lark_scanner')
        return -1, 0, 0


    return error, updated_files_count, new_files_count


# 检查文件是否在数据库，如果不在则插入，如果在则比对更新时间
def checkDB(file, cat_flag):
    syntax=lark_syntax(cat_flag)
    name,token,type,modified_time,table_name = syntax['name'],syntax['token'],syntax['type'],syntax['modified_time'],syntax['category']
    file = dict(file)  # 把 file转换为字典格式
    log_with_category(logger, 'info', f"checking file: {file[type]} : {file[name]} _ {file[token]}", '')
    file['version'] = 1
    file['versioncount'] = 1

    query = text(f"SELECT * FROM {table_name} WHERE {token} = :token")
    try:
        result = execute_query_with_retry(query, {'token': file[token]})
        rows = result.fetchall()
        columns = result.keys()
    except Exception as e:
        log_with_category(logger, 'error', f"Error executing query: {e}", 'postgres')
        return 1, 0  # 返回错误码和标志

    if not rows:
        df_dict = {}
    else:
        try:
            columns = list(columns)
            if not isinstance(columns, (list, tuple)) or not isinstance(rows, (list, tuple)):
                log_with_category(logger, 'error', "Error: columns or rows is not iterable", 'fetching_postgres')
                return 1, 0  # 返回错误码和标志
            if len(columns) != len(rows[0]):
                log_with_category(logger, 'error', "Error: Number of columns does not match number of parameters fetched from the database.", 'fetching_postgres')
                return 1, 0  # 返回错误码和标志
        except IndexError as e:
            log_with_category(logger, 'error', f"Error accessing rows: {e}", 'fetching_postgres')
            return 1, 0  # 返回错误码和标志

    df = pd.DataFrame(rows, columns=columns)
    df_dict = df.to_dict(orient='records')[0] if not df.empty else {}
    error = 0
    flag = 0
    is_updated = False

    if not df.empty:
        log_with_category(logger, 'info', "This is an old file, checking Minio...", '')
        fileName = process_file_name(file,syntax)

        
        match int(df_dict['is_uploaded']):
            case 0:
                log_with_category(logger, 'info', f"File not uploaded, code: 0", '')
            case 1:
                if not isInMinIO(fileName,cat_flag):
                    log_with_category(logger, 'warning', f"File metadata is found in db,showing it was uploaded before, but file is not found in MinIO, will try again... ", 'MinIO')
                    file['is_uploaded'] = '0'
                    file['filepath'] = ''
                    is_updated = True                
            case 2:
                log_with_category(logger, 'info', f"File met error when uploaded, code: 2", '')
            case _:
                log_with_category(logger, 'ERROR', f"File not uploaded, code: {file['is_uploaded']}", '')

        if int(file[modified_time]) > int(df_dict[modified_time]):
            log_with_category(logger, 'info', "This file has been updated, updating db...", '')

            old_version_row = df_dict.copy()
            old_version_row[token] = f"{old_version_row[token]}_{old_version_row['version']}"
            old_version_row['versioncount'] = int(old_version_row['versioncount']) + 1
            old_version_df = pd.DataFrame([old_version_row])
            try:
                to_sql_with_retry(old_version_df,table_name,False,'append')
            except Exception as e:
                log_with_category(logger, 'error', f"Error occurred while :::inserting old version entry::: into {table_name} table: {e}\n The data is: {old_version_row}", 'postgres')
                error = 1

            file['version'] = int(df_dict['version']) + 1
            file['versioncount'] = int(df_dict['versioncount']) + 1
            file['filepath'] = ''
            file['is_uploaded'] = '0'
            is_updated = True

        if is_updated:
            update_query = text(f"""
                UPDATE {table_name}
                SET version = :new_version,
                    versioncount = :new_versioncount,
                    filepath = :new_filepath,
                    {modified_time} = :new_modified_time,
                    is_uploaded = :new_is_uploaded
                WHERE {token} = :token 
            """)
            try:
                execute_query_with_retry(update_query, {
                    'new_versioncount': int(file['versioncount']),
                    'token': file[token],
                    'new_version': int(file['version']),
                    'new_filepath': file['filepath'],
                    'new_modified_time': file[modified_time],
                    'new_is_uploaded': file['is_uploaded']
                })
                log_with_category(logger, 'info', "update db success", '')
                flag = 1
            except Exception as e:
                log_with_category(logger, 'error', f"Error occurred while :::updating entry::: in {table_name} table: {e}\n The data is: {file}", 'postgres')
                log_with_category(logger, 'info', "update db failed", '')
                error = 1
        else:
            log_with_category(logger, 'info', "There has no update for this file, skipped.", '')
    else:
        log_with_category(logger, 'info', "This is a new file, adding to db...", '')
        file['filepath'] = ''
        file['is_uploaded'] = '0'
        df = pd.json_normalize(file)
        if cat_flag==1:
            df.rename(columns={'shortcut_info.target_token': 'shortcut_info_target_token'}, inplace=True)
            df.rename(columns={'shortcut_info.target_type': 'shortcut_info_target_type'}, inplace=True)
        try:
            to_sql_with_retry(df,table_name,False,'append')
            log_with_category(logger, 'info', "insert db success", '')
            flag = 2
        except Exception as e:
            log_with_category(logger, 'error', f"Error occurred while :::inserting new entry::: into cloud_drive_files table: {e}\n The data is: {file}", 'postgres')
            error = 1
    return error, flag

# 读取已扫描的文件夹列表csv文件到集合
def read_csv_to_set(file_path):
        # 检查文件是否存在
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        if file_path == visited_folders_path:
            return set(df['FolderToken'])
        else:
            return set(df['SpaceID'])
    else:
        # 如果文件不存在，返回一个空集合
        return set()

# 保存已扫描的文件夹列表集合到csv文件
def save_set_to_csv(my_set, file_path):
    # 将集合转换为DataFrame
    df = pd.DataFrame(list(my_set), columns=['Values'])
    # 将DataFrame增量更新到CSV文件（覆盖写入）
    df.to_csv(file_path, index=False)

# 初始化已访问文件夹列表
def init_visited_folders():
    if os.path.exists(visited_folders_path):
        os.remove(visited_folders_path)
    with open(visited_folders_path, 'w', encoding='utf-8') as f:
        f.write('FolderToken\n')
    visited_folders = set()
    if os.path.exists(visited_spaces_path):
        os.remove(visited_spaces_path)
    with open(visited_spaces_path, 'w', encoding='utf-8') as f:
        f.write('SpaceID\n')
    visited_spaces = set()
    return visited_folders,visited_spaces

# 初始化数据库
def init_db():
    DB_CONNECTION_STRING = config.DB_CONNECTION_STRING
    engine = create_engine(DB_CONNECTION_STRING,
                           pool_size=20,         # 增加连接池大小
                           max_overflow=40,      # 增加最大溢出连接数
                           pool_timeout=30,      # 设置连接超时时间
                           pool_recycle=1800,    # 设置连接回收时间，防止连接被数据库服务器关闭
                           poolclass=QueuePool)  # 使用QueuePool连接池

    return engine
# 执行查询并处理连接池已满的情况
def execute_query_with_retry(query, params=None):
    global engine
    connection = None
    try:
        connection = engine.connect()
        result = connection.execute(query, params)
        return result
    except Exception as e:
        if "QueuePool limit of size" in str(e):
            log_with_category(logger, 'error', f"Met error: {e}, Connection pool is full, disposing all connections", 'postgres')
            engine.dispose()  # 清空连接池
            engine = init_db()  # 重新初始化数据库连接
            connection = engine.connect()
            result = connection.execute(query, params)
            return result
        else:
            raise
    finally:
        if connection:
            connection.close()

# 执行to_sql并处理连接池已满的情况
def to_sql_with_retry(df, table_name, index, if_exists):
    global engine
    connection = None
    try:
        connection = engine.connect()
        df.to_sql(table_name, engine, index=index, if_exists=if_exists)
    except Exception as e:
        if "QueuePool limit of size" in str(e):
            log_with_category(logger, 'error', f"Met error: {e}, Connection pool is full, disposing all connections", 'postgres')
            engine.dispose()  # 清空连接池
            engine = init_db()  # 重新初始化数据库连接
            df.to_sql(table_name, con=engine, index=index, if_exists=if_exists)
    finally:
        if connection:
            connection.close()
# 从数据库中读取所有文件夹列表
def load_folders_from_db(cat_flag):
    list_table = lark_syntax(cat_flag)['list_table']
    global engine
    query = text(f"SELECT * FROM {list_table}")
    try:
        result = execute_query_with_retry(query)
        folders = [row._asdict() for row in result.fetchall()]
    except Exception as e:
        log_with_category(logger, 'error', f"Error occurred while loading folders from db: {e}", 'postgres')
        return []
    return folders

# 处理已访问文件夹和剩余文件夹列表
def scan_process_folders(folders):
    log_with_category(logger, 'info', "-----------------------------------------------\n\nnow scanning folders\n\n-----------------------------------------------", '')
    global visited_folders
    visited_folders = read_csv_to_set(visited_folders_path)
    remaining_folders = [folder for folder in folders if folder['Token'] not in visited_folders]
    while remaining_folders:
        remaining_folders_copy = remaining_folders.copy()
        try:
            Scan_folders(remaining_folders,1)
            remaining_folders = [folder for folder in folders if folder['Token'] not in visited_folders]
            if remaining_folders==remaining_folders_copy:
                count+=1
                if count>3:
                    break
            else:
                count=0
        except Exception as e:
            log_with_category(logger, 'error', f"Error occurred: {e}", 'lark_scanner')

# 处理已访问空间和剩余空间列表
def scan_process_spaces(spaces):
    log_with_category(logger, 'info', "-----------------------------------------------\n\nnow scanning Spaces\n\n-----------------------------------------------", '')
    global visited_spaces
    visited_spaces = read_csv_to_set(visited_spaces_path)
    remaining_spaces = [space for space in spaces if space['space_id'] not in visited_spaces]
    while remaining_spaces:
        remaining_spaces_copy = remaining_spaces.copy()
        try:
            Scan_folders(remaining_spaces,0)
            remaining_spaces = [space for space in spaces if space['space_id'] not in visited_spaces]
            if remaining_spaces==remaining_spaces_copy:
                count+=1
                if count>3:
                    break
            else:
                count=0
        except Exception as e:
            log_with_category(logger, 'error', f"Error occurred: {e}", 'lark_space_scanner')

# 按流程执行所有主要任务
def job():
    '''initiallizaiton'''
    global visited_folders, visited_spaces  # 声明全局变量
    visited_folders,visited_spaces = init_visited_folders()
    global engine
    engine = init_db()
    init_logger()
    
    '''scan folders and spaces list'''
    #todo: scan folders list & update db
    #todo: scan spaces list & update db  
    '''scan,update and upload spaces files'''
    spaces=load_folders_from_db(0)
    scan_process_spaces(spaces)    
    upload_new_files(0)

    '''scan,update and upload cloudy drive files'''
    folders=load_folders_from_db(1)
    scan_process_folders(folders)    
    upload_new_files(1)

# 设置定时任务
schedule.every(12).hours.do(job)
if __name__ == '__main__':

    job()
    while (True):
        schedule.run_pending()
        time.sleep(60)
