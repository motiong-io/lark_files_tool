import os.path
import time

import pandas as pd

from lark_cloud_document import get_minio_client, minio_stats
import lark_cloud_document as lark_file
from sqlalchemy import create_engine, text
import DBcfg as config

def isNull(bucket_objects):
    try:
        next(bucket_objects)
        return False
    except StopIteration:
        return True

import psycopg2


def upload():
    # Database connection parameters
    conn_str = "dbname=lark_files_backup user=intern password=123456 host=postgresql.middleware.dev.motiong.net port=5432"

    try:
        # Establish connection
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                # 删除表（如果存在）
                cur.execute("DROP TABLE IF EXISTS cloud_drive_files")
                conn.commit()
                print("Table 'cloud_drive_files' has been successfully deleted if it existed.")

                # 创建表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS cloud_drive_files (
                    created_time BIGINT,
                    modified_time BIGINT,
                    name VARCHAR(255),
                    owner_id VARCHAR(255),
                    parent_token VARCHAR(255),
                    token VARCHAR(255),
                    type VARCHAR(50),
                    url TEXT,
                    shortcut_info_target_token VARCHAR(255),
                    shortcut_info_target_type VARCHAR(50),
                    version INT,
                    versioncount INT,
                    filepath VARCHAR(1024)
                );
                """)
                conn.commit()
                print("Table 'nodes' has been successfully created.")

                # 使用 COPY 命令插入数据
                # 将 DataFrame 保存为临时 CSV 文件，并指定 UTF-8 编码
                temp_csv = 'temp_data.csv'
                df = pd.read_csv('cloud_test_file.csv')
                df.to_csv(temp_csv, index=False, header=False, encoding='utf-8')
                with open(temp_csv, encoding='utf-8') as f:
                    cur.copy_expert("COPY cloud_drive_files FROM stdin WITH (FORMAT CSV)", f)
                conn.commit()
                print("Data has been successfully inserted into 'cloud_drive_files' table.")

                # 删除临时文件
                os.remove(temp_csv)

    except Exception as e:
        print(f"An error occurred: {e}")
def test():
    # minio_client = get_minio_client()
    # 上传 upload_result,upload_comment = lark_file.upload_to_minio(bucket_name='raw-knowledge', object_name=f'MotionG/test_1.csv', file_path=f'D:/test.csv')
    # 覆盖 minio_client.fput_object(bucket_name='raw-knowledge', object_name=f'MotionG/test.csv', file_path=f'D:/test.csv')
    # bucket_objects = minio_client.list_objects('raw-knowledge', prefix='MotionG/test')
    '''
    MotionG/CloudDriveFiles/A0V7bkUdroSB1ExKxTdc8NkgnAh_demo1.vi
    '''
    engine = create_engine(
        'postgresql://intern:123456@postgresql.middleware.dev.motiong.net:5432/lark_files_backup')
    # 创建连接
    connection = engine.connect()

    # create_table_sql = text("""
    #         CREATE TABLE IF NOT EXISTS extracted_folders (
    #             No INT PRIMARY KEY,
    #             Name VARCHAR(255),
    #             Token VARCHAR(255)
    #         );
    #         """)
    # connection.execute(create_table_sql)
    # print("Table 'extracted_folders' has been successfully created or already exists.")

    # 读取 CSV 文件
    # df = pd.read_csv('extracted_folders.csv')
    #
    # # 将数据写入数据库表
    # df.to_sql('extracted_folders', con=engine, index=False, if_exists='append')
    # print("Data has been successfully inserted into 'extracted_folders' table.")
    # 读取 CSV 文件

    # drop_table_sql = text("DROP TABLE IF EXISTS company_info")
    # connection.execute(drop_table_sql)
    # print("Table 'company_info' has been successfully deleted.")
    # query = text("SELECT * FROM nodes")
    # result = connection.execute(query)
    # # 将结果转换为 Pandas DataFrame
    # df = pd.DataFrame(result.fetchall(), columns=result.keys())
    # print("Data from nodes:")
    # print(df)
    # update_query = text("""
    #     UPDATE nodes
    #     SET versioncount = :new_versioncount
    #     WHERE obj_token = :obj_token AND version = :version
    #     """)
    # df['versioncount']+=1
    # for index, row in df.iterrows():
    #     connection.execute(update_query, {
    #         'new_versioncount': int(row['versioncount']),
    #         'obj_token': 'PIWUdV0cooU7kTxoOT8cxxdin0Z',
    #         'version': int(row['version'])
    #     })
    # connection.commit()
    query = text("SELECT * FROM cloud_drive_files")
    result = connection.execute(query)
    # 将结果转换为 Pandas DataFrame
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    print("Data from nodes:")
    print(df)
    # print(df['version'],df['versioncount'])
    # df['versioncount']+=1
    # print(df['version'])
    # update_query = text("""
    # UPDATE nodes
    # SET versioncount = :new_versioncount
    # WHERE obj_token = :obj_token AND version = :version
    # """)
    # for index, row in df.iterrows():
    #     # 执行更新编辑日期与版本号的sql
    #     connection.execute(update_query, {
    #         'new_versioncount': int(row['versioncount']),
    #         'obj_token': 'PIWUdV0cooU7kTxoOT8cxxdin0Z',
    #         'version': int(row['version'])
    #     })
    # query = text("SELECT * FROM nodes where obj_token=:obj_token")
    # result = connection.execute(query, {'obj_token': 'PIWUdV0cooU7kTxoOT8cxxdin0Z'})
    # # 将结果转换为 Pandas DataFrame
    # df = pd.DataFrame(result.fetchall(), columns=result.keys())
    # print(df['version'],df['versioncount'])
    connection.close()
# upload()
import schedule
def job():
    print('work')
schedule.every().day.at('11:44').do(job)

while(True):
    schedule.run_pending()
    time.sleep(1)


'''
{'code': 0, 'data': {'has_more': False, 'items': [{'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': True, 'node_create_time': '1716370663', 'node_token': 'AYnmwB4J6ikDpTksrppcK59lnGz', 'node_type': 'origin', 'obj_create_time': '1716370663', 'obj_edit_time': '1716371457', 'obj_token': 'RuNcdNu1ZoudoAxn8FQcv6bln3d', 'obj_type': 'docx', 'origin_node_token': 'AYnmwB4J6ikDpTksrppcK59lnGz', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': '', 'space_id': '7312380977362255874', 'title': 'Domain Knowledge + Workflow'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': True, 'node_create_time': '1702546369', 'node_token': 'RRHVw6pH3ituKfkT1XTcqPdSnEf', 'node_type': 'origin', 'obj_create_time': '1702546369', 'obj_edit_time': '1716371414', 'obj_token': 'GnR7dDQhyosm12xqEEgcytmanvc', 'obj_type': 'docx', 'origin_node_token': 'RRHVw6pH3ituKfkT1XTcqPdSnEf', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': '', 'space_id': '7312380977362255874', 'title': '知识寻源+获取'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': True, 'node_create_time': '1706077925', 'node_token': 'Ezn8w7wZ2idu0YkErNCcX6I3ntf', 'node_type': 'origin', 'obj_create_time': '1706077925', 'obj_edit_time': '1706077933', 'obj_token': 'RlASdBHBhoEca5xGHz5cDjQqntC', 'obj_type': 'docx', 'origin_node_token': 'Ezn8w7wZ2idu0YkErNCcX6I3ntf', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': '', 'space_id': '7312380977362255874', 'title': '临时、过期'}], 'page_token': ''}, 'msg': 'success'}
Domain Knowledge + Workflow
知识寻源+获取
临时、过期
CSV文件已成功追加保存到 nodes.csv
{'code': 0, 'data': {'has_more': False, 'items': [{'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': True, 'node_create_time': '1716370724', 'node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'node_type': 'origin', 'obj_create_time': '1716370724', 'obj_edit_time': '1716370729', 'obj_token': 'BeMIdUN7joY5Pjxa3DvckTXlnhe', 'obj_type': 'docx', 'origin_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'AYnmwB4J6ikDpTksrppcK59lnGz', 'space_id': '7312380977362255874', 'title': '会议记录'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': True, 'node_create_time': '1716371772', 'node_token': 'BWaqwyJi0ibliokzEXJcA23Yn9e', 'node_type': 'origin', 'obj_create_time': '1716371772', 'obj_edit_time': '1719974433', 'obj_token': 'HssCd2SQmoit5HxNj0bcfokUn2H', 'obj_type': 'docx', 'origin_node_token': 'BWaqwyJi0ibliokzEXJcA23Yn9e', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'AYnmwB4J6ikDpTksrppcK59lnGz', 'space_id': '7312380977362255874', 'title': 'MVP1.0开发'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': True, 'node_create_time': '1720428298', 'node_token': 'SskmwKathieOH1kj8rrcbtPGnre', 'node_type': 'origin', 'obj_create_time': '1720428298', 'obj_edit_time': '1720592213', 'obj_token': 'ALNndXR8BombhAxC3BIc47e0nBe', 'obj_type': 'docx', 'origin_node_token': 'SskmwKathieOH1kj8rrcbtPGnre', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'AYnmwB4J6ikDpTksrppcK59lnGz', 'space_id': '7312380977362255874', 'title': '知识工厂任务规划'}, {'creator': 'ou_a783aaf74a1dc17054c773f639788ab8', 'has_child': True, 'node_create_time': '1723169251', 'node_token': 'ARhSwI1iSivam5kHAu3cehuhnOd', 'node_type': 'origin', 'obj_create_time': '1723169251', 'obj_edit_time': '1724634969', 'obj_token': 'Bu62d9qAcoXYoaxp8Uuc7SBrnWh', 'obj_type': 'docx', 'origin_node_token': 'ARhSwI1iSivam5kHAu3cehuhnOd', 'origin_space_id': '7312380977362255874', 'owner': 'ou_a783aaf74a1dc17054c773f639788ab8', 'parent_node_token': 'AYnmwB4J6ikDpTksrppcK59lnGz', 'space_id': '7312380977362255874', 'title': 'Architecture'}], 'page_token': ''}, 'msg': 'success'}
会议记录
MVP1.0开发
知识工厂任务规划
Architecture
CSV文件已成功追加保存到 nodes.csv
{'code': 0, 'data': {'has_more': False, 'items': 
[{'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 
'has_child': False, 'node_create_time': '1724659704', 
'node_token': 'CLMWwQ7CXi4LVJkXndicw8knnHd', 'node_type': 'origin', 
'obj_create_time': '1724659704', 'obj_edit_time': '1724663498', 
'obj_token': 'GFQAdZcwdoTZ8hxnIvHcMPTXnee', 'obj_type': 'docx', 
'origin_node_token': 'CLMWwQ7CXi4LVJkXndicw8knnHd', 'origin_space_id': '7312380977362255874', 
'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 
'space_id': '7312380977362255874', 'title': '240826 update'}, 
{'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': False, 'node_create_time': '1723772882', 
'node_token': 'XmeIwKGHUifIQjkS7zUcge8Snkb', 'node_type': 'origin', 'obj_create_time': '1723772882', 
'obj_edit_time': '1723772907', 'obj_token': 'Rt3FdN90sogd15x4hGcc3LsUnXL', 'obj_type': 'docx', 
'origin_node_token': 'XmeIwKGHUifIQjkS7zUcge8Snkb', 'origin_space_id': '7312380977362255874', 
'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 
'space_id': '7312380977362255874', 'title': '240815 知识工作汇报'}, 
{'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': False, 'node_create_time': '1723448220', 
'node_token': 'JRtNw0jFGirBogkBUyscuiLDnp7', 'node_type': 'origin', 'obj_create_time': '1723448220', 
'obj_edit_time': '1723772785', 'obj_token': 'Gb9TdpjZzoBjY7xrqbZcf4tgnzc', 'obj_type': 'docx', 
'origin_node_token': 'JRtNw0jFGirBogkBUyscuiLDnp7', 'origin_space_id': '7312380977362255874', 
'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 
'space_id': '7312380977362255874', 'title': 'Current Work Summary 240813'}, 
{'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': False, 'node_create_time': '1721374074', 'node_token': 'U1QTwa0rQig6Xck64qTcC9D1nff', 'node_type': 'origin', 'obj_create_time': '1721374074', 'obj_edit_time': '1721379182', 'obj_token': 'Va1ddvNANokon0xQHHRcw12MnVd', 'obj_type': 'docx', 'origin_node_token': 'U1QTwa0rQig6Xck64qTcC9D1nff', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'space_id': '7312380977362255874', 'title': 'DC场景与方案第一轮收集结果review 2024年7月19日'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': True, 'node_create_time': '1719909171', 'node_token': 'SWZuwjHoSiJZXXkSPkAcV8ePnzf', 'node_type': 'origin', 'obj_create_time': '1719909171', 'obj_edit_time': '1724037967', 'obj_token': 'UJBldb4oqolKinx8o0KcEREknyb', 'obj_type': 'docx', 'origin_node_token': 'SWZuwjHoSiJZXXkSPkAcV8ePnzf', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'space_id': '7312380977362255874', 'title': 'FY 2024 Q2 Review - Knowledge Factory'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': False, 'node_create_time': '1716370804', 'node_token': 'EOEzweS7aiLfYbkNMOyc7iqIntf', 'node_type': 'origin', 'obj_create_time': '1716370804', 'obj_edit_time': '1716433977', 'obj_token': 'PIWUdV0cooU7kTxoOT8cxxdin0Z', 'obj_type': 'docx', 'origin_node_token': 'EOEzweS7aiLfYbkNMOyc7iqIntf', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'space_id': '7312380977362255874', 'title': '05-22 信息同步 重新规划'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': False, 'node_create_time': '1716349951', 'node_token': 'TizBwxIUei8oO0kz48acd2brnHb', 'node_type': 'origin', 'obj_create_time': '1716349951', 'obj_edit_time': '1716370790', 'obj_token': 'OKZndDGw7oWYmVxWadOc7aFWnDg', 'obj_type': 'docx', 'origin_node_token': 'TizBwxIUei8oO0kz48acd2brnHb', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'space_id': '7312380977362255874', 'title': '05-22知识中心MVP功能细节'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': False, 'node_create_time': '1715908460', 'node_token': 'NBJvweJ3MiQZzMkLiDtcKh9unRd', 'node_type': 'origin', 'obj_create_time': '1715908460', 'obj_edit_time': '1716370784', 'obj_token': 'II8JdhYvCom7dQxMcKdcMIF1n3g', 'obj_type': 'docx', 'origin_node_token': 'NBJvweJ3MiQZzMkLiDtcKh9unRd', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'space_id': '7312380977362255874', 'title': '05-21知识中心MVP需求'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': False, 'node_create_time': '1715845206', 'node_token': 'J26wwlLP1iRF45klIffcljmjnhb', 'node_type': 'origin', 'obj_create_time': '1715845206', 'obj_edit_time': '1716370770', 'obj_token': 'U6etdvehUo7QVUx8XSnc7Xafn9b', 'obj_type': 'docx', 'origin_node_token': 'J26wwlLP1iRF45klIffcljmjnhb', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'space_id': '7312380977362255874', 'title': '05-16知识建设和管理方案'}, {'creator': 'ou_c6c7700b20f7c84a7505420680f4f094', 'has_child': False, 'node_create_time': '1720513562', 'node_token': 'YijwwVN2uinuBCkwNxDcvHU1n9c', 'node_type': 'origin', 'obj_create_time': '1720513562', 'obj_edit_time': '1720513763', 'obj_token': 'Hywid7Sgiox1yAxGmBnc8LxLnrc', 'obj_type': 'docx', 'origin_node_token': 'YijwwVN2uinuBCkwNxDcvHU1n9c', 'origin_space_id': '7312380977362255874', 'owner': 'ou_c6c7700b20f7c84a7505420680f4f094', 'parent_node_token': 'XZ78wuy6Ei8MmSkOrF0cvkXvnLf', 'space_id': '7312380977362255874', 'title': '龙哥协助清单'}
], 'page_token': ''}, 'msg': 'success'}

240826 update
240815 知识工作汇报
Current Work Summary 240813
DC场景与方案第一轮收集结果review 2024年7月19日
FY 2024 Q2 Review - Knowledge Factory
05-22 信息同步 重新规划
05-22知识中心MVP功能细节
05-21知识中心MVP需求
05-16知识建设和管理方案
龙哥协助清单
CSV文件已成功追加保存到 nodes.csv

'''
    # # 获取桶中的数据信息,不查子文件夹中的数据
    # bucket_objects = minio_client.list_objects(barrel)
    # for bucket_object in bucket_objects:
    #     print(bucket_object.object_name)
    # stats=minio_stats(bucket_name, object_name)














    begindef checkDB(file):
    print("checking file:",file['type'],":",file['name'],"_",file['token'])
    global engine
    connection = engine.connect()
    query = text("SELECT * FROM cloud_drive_files WHERE token = :token")
    result = connection.execute(query, {'token': file['token']})
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df_dict = df.to_dict(orient='records')
    error=0
    flag=0
    is_updated=False
    if not df.empty:
        print("This is an old file, checking Minio...")
        # 数据库查到这个token
        fileName = f"{file['token']}_{file['name']}"
        if not isInMinIO(fileName):
            # 如果在意外条件下，文件记录出现在数据库但不在minio里，则上传到桶里
            file['is_uploaded'] = '0'
            is_updated=True

        #max_version_row = df.loc[df['version'].idxmax()]
        if int(file['modified_time']) > int(df_dict['modified_time']):
            # 查到token并且有更新（本次更新时间>已有的更新记录时间）
            print("This file has been updated, updating db...")
            #创建新的row，记录上一版本
            old_version_row = df_dict.copy()
            old_version_row['token'] = f"{old_version_row['token']}_{old_version_row['version']}"
            old_version_row['versioncount'] = int(old_version_row['versioncount']) + 1
            # 将old_version_row写入postgres
            old_version_df = pd.DataFrame([old_version_row])
            try:
                old_version_df.to_sql('cloud_drive_files', con=engine, index=False, if_exists='append')
            except Exception as e:
                print(f"Error occurred while inserting old version data into cloud_drive_files table: {e}")
                with open('db_error_log.txt', 'a') as log_file:
                    log_file.write(f"Error occurred while :::inserting old version entry::: into cloud_drive_files table: {e}\n The data is: {old_version_row}")
                connection.rollback()
                error=1
            #在已有表上更新当前版本
            file['version'] = int(df_dict['version']) + 1
            file['versioncount'] = int(df_dict['versioncount']) + 1
            file['filepath'] = f"https://minio.middleware.dev.motiong.net/browser/raw-knowledge/MotionG/SpacesFiles/{file['token']}_{file['name']}_{file['version']}"
            file['is_uploaded'] = '0'
            is_updated=True

        else:
            # 查到token并且如果没更新就不动
            pass
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
                flag=1
            except Exception as e:
                print(f"Error occurred while updating data in cloud_drive_files table: {e}")
                with open('db_error_log.txt', 'a') as log_file:
                    log_file.write(f"Error occurred while :::updating entry::: in cloud_drive_files table: {e}\n The data is: {file}")
                connection.rollback()
                print("update db failed")
                error=1
    else:
        print("This is a new file, adding to db...")
        file['version'] = 0
        file['versioncount'] = 0
        file['filepath'] = f"https://minio.middleware.dev.motiong.net/browser/raw-knowledge/MotionG/CloudDriveFiles/{file['token']}_{file['name']}"
        file['is_uploaded'] = '0'
        df = pd.json_normalize(file)
        df.rename(columns={'shortcut_info.target_token': 'shortcut_info_target_token'}, inplace=True)
        df.rename(columns={'shortcut_info.target_type': 'shortcut_info_target_type'}, inplace=True)
        try:
            df.to_sql('cloud_drive_files', con=engine, index=False, if_exists='append')
            print("insert db success")
            flag=2
        except Exception as e:
            print(f"Error occurred while inserting data into cloud_drive_files table: {e}")
            with open('db_error_log.txt', 'a') as log_file:
                log_file.write(f"Error occurred while :::inserting new entry::: into cloud_drive_files table: {e}\n The data is: {file}")
            connection.rollback()
            print("insert db failed")
            error=1
    connection.close()
    return error,flag
