import logging
import os
import time

import schedule
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

import lark_cloud_document as lark_file
import csv
import json

space_list_path = 'space_list.csv'
nodes_path = 'nodes.csv'
minio_path = 'MotionG/SpacesFiles'
file_path = 'temp'
bucket_name = 'raw-knowledge'
download_path = os.path.join(os.getcwd(), file_path)
minio_client = lark_file.get_minio_client()
pathlib = ['MotionG/SpacesFiles/', 'MotionG/CloudDriveFiles/']
lark_file.user_token = lark_file.get_user_token1()


# 获取账号可见范围内所有空间的列表，并存入csv文件，可一周或一月执行一次
def space_list():
    url = "https://open.feishu.cn/open-apis/wiki/v2/spaces?lang=en&page_size=50"
    payload = {'page_size': 50, 'page_token': ''}
    response_s = lark_file.request_with_retry(url, params=payload)
    spaces = response_s['data']['items']
    while response_s['data']['has_more'] is True:
        payload['page_token'] = response_s['data']['page_token']
        response_s = lark_file.request_with_retry(url, params=payload)
        spaces += response_s['data']['items']
    lark_file.json_to_append_csv(spaces, space_list_path)
    return spaces


# 根据space_id获取该空间下的所有节点的详细信息，并存入csv文件
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
            print(path + '不在数据库中')
            continue
        else:
            print(path + '在数据库中')
            a = True
            return a
    return a


def upload(file):
    # 配置日志记录，仅记录 ERROR 级别的日志
    logging.basicConfig(filename='error.log', level=logging.ERROR,
                        format='%(asctime)s %(levelname)s %(message)s')

    # 确保其他模块的日志记录级别设置为 WARNING 或更高
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    fail_list = []
    valid_types = {'doc', 'sheet', 'bitable', 'docx', 'file'}
    obj = {
        "name": file['title'],
        "token": file['obj_token'],
        "type": file['obj_type']
    }
    if obj['type'] in valid_types:
        result, comment = lark_file.lark_cloud_downloader(obj)
        if result == 0:
            print(f"Downloaded item: {obj['name']} ({obj['type']}), token: {obj['token']}")
            object_path = f"{minio_path}/{comment}"
            downloaded_file_path = os.path.join(download_path, comment)
            upload_result = False
            if not isInDB(comment):
                upload_result, upload_comment = lark_file.upload_to_minio(bucket_name, object_path,
                                                                          downloaded_file_path)  # 上传到minio
            if upload_result:
                os.remove(downloaded_file_path)  # 删除临时文件
                # shutil.rmtree(download_path)   #清空临时文件夹
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
        print(f"Skipping row: {file['title']}. Invalid type:{file['obj_type']}")
    if fail_list:
        print('Failed list:')
        print(json.dumps(fail_list, indent=4, ensure_ascii=False))
    print('Download mission completed.')


def update(file, curVersion):
    # 配置日志记录，仅记录 ERROR 级别的日志
    curVersion += 1
    logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
    # 确保其他模块的日志记录级别设置为 WARNING 或更高
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    fail_list = []
    valid_types = {'doc', 'sheet', 'bitable', 'docx', 'file'}
    obj = {
        "name": file['title'],
        "token": file['obj_token'],
        "type": file['obj_type']
    }
    if obj['type'] in valid_types:
        result, comment = lark_file.lark_cloud_downloader(obj)
        if result == 0:
            print(f"Downloaded item: {obj['name']} ({obj['type']}), token: {obj['token']}")
            object_path = f"{minio_path}/{comment}_{curVersion}"
            '''
                上面一行在目标路径结尾加上_版本号，其他没动
            '''
            downloaded_file_path = os.path.join(download_path, comment)
            upload_result = False
            if not isInDB(comment):
                upload_result, upload_comment = lark_file.upload_to_minio(bucket_name, object_path,
                                                                          downloaded_file_path)  # 上传到minio
            if upload_result:
                os.remove(downloaded_file_path)  # 删除临时文件
                # shutil.rmtree(download_path)   #清空临时文件夹
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
        print(f"Skipping row: {file['title']}. Invalid type:{file['obj_type']}")
    if fail_list:
        print('Failed list:')
        print(json.dumps(fail_list, indent=4, ensure_ascii=False))
    print('Download mission completed.')


def space_node_obj(space_id, parent_node_token=''):
    engine = create_engine(
        'postgresql://intern:123456@postgresql.middleware.dev.motiong.net:5432/lark_files_backup')
    # 创建连接
    files = []
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
    payload = {'page_size': 50, 'page_token': '', 'parent_node_token': parent_node_token}
    response = lark_file.request_with_retry(url, params=payload)
    print(response)
    if 'items' in response['data']:
        files += response['data']['items']
        while response['data']['has_more'] is True:
            payload['page_token'] = response['data']['page_token']
            response = lark_file.request_with_retry(url, params=payload)
            files += response['data']['items']
        for file in files:
            file['version'] = 0
            file['versioncount'] = 0
            file[
                'filepath'] = f"https://minio.middleware.dev.motiong.net/browser/raw-knowledge/MotionG/SpacesFiles/{file['obj_token']}_{file['title']}"
            '''
            检查检查编辑日期判断是否有更新，如果更新了则上传minio并在数据库更新编辑日期和版本号;
            没有更新直接不管
            '''
            connection = engine.connect()
            query = text("SELECT * FROM nodes WHERE obj_token = :obj_token")
            result = connection.execute(query, {'obj_token': file['obj_token']})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            if not df.empty:
                # 根据索引提取最大 version 的行
                fileName = f"{file['obj_token']}_{file['title']}"
                if not isInDB(fileName):
                    # 如果在意外条件下，文件记录出现在数据库但不在minio里，则上传到桶里
                    upload(file)
                max_version_row = df.loc[df['version'].idxmax()]
                if int(file['obj_edit_time']) > int(max_version_row['obj_edit_time']):  # 本次文件的数据更新时间, 数据库中记录的更新时间
                    # 更新minio, 更新数据库中所有版本总数
                    update(file, int(max_version_row['version']))
                    file['version'] = int(max_version_row['version']) + 1
                    file['versioncount'] = int(max_version_row['versioncount']) + 1
                    file[
                        'filepath'] = f"https://minio.middleware.dev.motiong.net/browser/raw-knowledge/MotionG/SpacesFiles/{file['obj_token']}_{file['title']}_{file['version']}"
                    file['title'] = f"{file['title']}_{file['version']}"
                    # 更新数据库中所有条目的版本总数
                    update_query = text("""
                            UPDATE nodes
                            SET versioncount = :new_versioncount
                            WHERE obj_token = :obj_token AND version = :version
                            """)
                    for index, row in df.iterrows():
                        connection.execute(update_query, {
                            'new_versioncount': int(row['versioncount']),
                            'obj_token': row['obj_token'],
                            'version': int(row['version'])
                        })
                    connection.commit()
                    newdf = pd.json_normalize(file)
                    newdf.to_sql('nodes', con=engine, index=False, if_exists='append')
                    #  新增一条新的记录
                else:
                    pass
            else:
                print(file['filepath'] + '上传Minio')
                fileName = f"{file['obj_token']}_{file['title']}"
                if not isInDB(fileName):  # 不在minio 传到minio桶
                    upload(file)
                # 把文件的元数据插入数据库
                df = pd.json_normalize(file)
                df.to_sql('nodes', con=engine, index=False, if_exists='append')
                # 直接加入数据库
                pass
            print(file['title'])
            connection.close()
        lark_file.json_to_append_csv(files, nodes_path)
        num = 0
        for item in response['data']['items']:
            if item['has_child'] is True:
                num += 1
                files += space_node_obj(space_id, item['node_token'])
    return files


# 主函数，判断该处理哪些空间列表，并调用函数进行处理
def space_node_obj_summary():
    print("Begin to check space_id...")
    # spaces_list = space_list()                # 获取所有空间的列表

    # 从 nodes list 中读取所有已提取的 space_id
    extracted_spaces = set()
    with open(nodes_path, 'r', encoding='utf-8') as nodes_file:
        nodes_list = list(csv.DictReader(nodes_file))
        for node in nodes_list:
            extracted_spaces.add(node['space_id'])

    # 从 space_list.csv 中读取所有 space_id
    with open(space_list_path, 'r', encoding='utf-8') as space_list_file:
        spaces_list = list(csv.DictReader(space_list_file))
    print("Begin to summary all objs...")
    for space in spaces_list:  # 遍历所有space_id
        space_id = space['space_id']
        if space_id not in extracted_spaces:  # 如果该 space_id 未被提取过，则进行提取，否则跳过
            space_node_obj('7312380977362255874')
            # space_node_obj(space_id)
    print("All spaces have been extracted.")


schedule.every().day.at('19:00').do(space_node_obj_summary)

if __name__ == '__main__':
    while (True):
        schedule.run_pending()
        time.sleep(60)