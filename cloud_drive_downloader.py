from lark_oapi.api.drive.v1 import *
import json
import os
import csv
import logging
import lark_cloud_document as lark_file
import pandas as pd

# 文件下载路径
cloud_drive_files_path='cloud_drive_files.csv'
bucket_name = 'raw-knowledge'
minio_path = 'MotionG/CloudDriveFiles'
file_path = 'temp'
download_path=os.path.join(os.getcwd(),file_path)

#主函数，循环下载文件、上传minio、更新csv记录
def mission_operator():
    # 配置日志记录，仅记录 ERROR 级别的日志
    logging.basicConfig(filename='error1.log', level=logging.ERROR,
                        format='%(asctime)s %(levelname)s %(message)s')

    # 确保其他模块的日志记录级别设置为 WARNING 或更高
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    fail_list = []
    valid_types = {'doc', 'sheet', 'bitable', 'docx', 'file'}
    # 读取 nodes.csv 文件
    df=pd.read_csv(cloud_drive_files_path, encoding='utf-8')
    # 遍历每一行检查目标字段
    for index, row in df.iterrows():
        if row['is_downloaded'] !=1 and row['is_downloaded'] !=2:
            if row['type'] == 'shortcut':
                obj = {
                        "name": row['name'],
                        "token": row['shortcut_info.target_token'],
                        "type": row['shortcut_info.target_type']
                    }
            else:
                obj = {
                        "name": row['name'],
                        "token": row['token'],
                        "type": row['type']
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
                        df.at[index,'is_downloaded']=1
                elif result == 2:
                    # 将 item_data 写入 error.log 并指定编码为 UTF-8
                    with open('error.log', 'a', encoding='utf-8') as f:
                        f.write(json.dumps(obj, ensure_ascii=False) + '\n')
                    logging.error(
                        f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}")
                    fail_list.append(obj)
                    df.at[index,'is_downloaded']=2
                else:
                    logging.error(
                        f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}")
                    fail_list.append(obj)
            else:
                print(f"Skipping row: {row['name']}. Invalid type:{row['type']}")
                df.at[index,'is_downloaded']=2
            #只更新有变动的记录
            with open(cloud_drive_files_path, 'r', encoding='utf-8') as f:
                reader = list(csv.reader(f))
            updated_row=df.iloc[index].tolist()
            reader[index+1]=updated_row
            with open(cloud_drive_files_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(reader)

    if fail_list:
        print('Failed list:')
        print(json.dumps(fail_list, indent=4, ensure_ascii=False))
    print('Download mission completed.')

if __name__ == '__main__':
    lark_file.user_token=lark_file.get_user_token1()
    mission_operator()
