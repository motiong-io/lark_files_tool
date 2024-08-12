from lark_oapi.api.drive.v1 import *
import json
import os
import csv
import logging
import lark_cloud_document as lark_file
import pandas as pd

# 文件下载路径

nodes_path='nodes.csv'
temp_csv_path='temp.csv'
bucket_name = 'raw-knowledge'
minio_path = 'MotionG/SpacesFiles'
file_path = 'temp'
download_path=os.path.join(os.getcwd(),file_path)

#lark_file.get_user_token2()
lark_file.user_token=lark_file.get_user_token1()



obj = {
        "name":'参考资料：MotorHost上位机功能清单',
        "token":'shtcncYm1vBr3FZ9dLZtSKyiozh',
        "type": 'sheet'
    }


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

else:
    logging.error(
        f"Failed to download item: {obj['name']} ({obj['type']}), token: {obj['token']}")

print('done')

