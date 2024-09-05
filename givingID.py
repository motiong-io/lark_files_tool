# from BtoC import generate_folder_name, check_and_upload, upload_CSV_before_review, get_minio_client
import DBcfg as config
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text


# 主逻辑
def main(file_path):
    minio_client = get_minio_client()
    minio_info = config.minio_config
    bucket_name = minio_info['bucket']
    upload_path = minio_info['path_after_review']

    files_data = get_data_from_minio(minio_client, bucket_name, file_path)
    print(files_data)

    folder_name = generate_folder_name()  # 生成文件夹名称
    DB_CONNECTION_STRING = config.DB_CONNECTION_STRING  # config是自己写的文件，配置信息；调用数据库
    engine = create_engine(DB_CONNECTION_STRING)

    for file_name, data in files_data:  # 针对拿到的csv，对每一个文件进行处理
        processed_data = update_pub_id_and_uid(file_name, data, engine)
        upload_processed_files(minio_client, bucket_name, upload_path, folder_name, file_name, processed_data)

def accessDB():
    DB_CONNECTION_STRING = config.DB_CONNECTION_STRING_read_write  # config是自己写的文件，配置信息；调用数据库
    engine = create_engine(DB_CONNECTION_STRING)
    processed_data = update_pub_id_and_uid('file_name', 'data', engine)
# 从MinIO读取CSV文件
def get_data_from_minio(minio_client, bucket_name, file_path):
    """
    从指定MinIO bucket的特定路径读取所有CSV文件。

    :param bucket_name: MinIO中的存储桶名称
    :param prefix: 文件所在的特定路径或前缍（可选）
    :return: 包含文件名和Pandas DataFrame的元组列表
    """
    data = []
    # 仅从指定前缀读取对象
    objects = minio_client.list_objects(bucket_name, prefix=file_path, recursive=True)
    for obj in objects:
        # 处理异常情况，如获取对象时发生错误
        response = minio_client.get_object(bucket_name, obj.object_name)
        csv_data = pd.read_csv(response)
        data.append((obj.object_name, csv_data))
    return data


# 上传数据回MinIO
def upload_processed_files(minio_client, bucket_name, upload_path, folder_name, file_name, data_frame):
    csv_buffer = data_frame.to_csv(index=False)
    object_name = f'{upload_path}/{folder_name}/{file_name}'
    try:
        minio_client.put_object(bucket_name, object_name, csv_buffer.encode('utf-8'), len(csv_buffer.encode('utf-8')))
        print(f"File {file_name} is successfully uploaded to path {object_name}.")
    except Exception as e:
        print(f"Failed to upload {object_name}. Error: {e}")


# 定义十六进制函数，用于后续十六进制运算
def hex_increment(hex_string):
    return hex(int(hex_string, 16) + 1)[2:].upper().zfill(len(hex_string))


def update_pub_id_and_uid(file_name, df, engine):
    uid_increment = 0  # 初始化一个增量变量，每次循环后递增，会一起赋给最终uid，确保它的唯一性
    category = file_name.split('/')[-1].split('_tab.csv')[0]  # 例如从路径中获取 'linear_motion_modules'，获取唯一表名。把搜索到的东西看作数组，唯一表示
    table_name = file_name.split('/')[-1].replace('.csv', '')  # 提取文件名前缀，作为表名
    with engine.connect() as connection:  # 链接数据库
        for i in range(len(df)):
            # 每次大循环前，更新最新的 UID 基数
            query = text("SELECT COALESCE(MAX(uid), 0) FROM latest_id")
            max_uid_result = connection.execute(query)  # 调用现有数据库中的最大uid
            base_uid = max_uid_result.fetchone()[0]

            for index, row in df.iterrows():
                if pd.isna(row['pub_id']):
                    brand = row['brand']

                    # 查询 Category ID Index 使用 tablename
                    query = text(
                        "SELECT index FROM category_id_index WHERE category = :category")  # SQL语句。：category表示变量，对应定义中的category
                    category_result = connection.execute(query, {'category': category}).fetchone()  # execute执行
                    if category_result is None:
                        print(f"No category index found for category: {category}")
                        continue
                    category_index = category_result[0]

                    # 查询 Brand ID Index 使用品牌
                    query = text("SELECT index FROM brand_id_index WHERE brand = :brand")
                    brand_result = connection.execute(query, {'brand': brand}).fetchone()
                    if brand_result is None:
                        print(f"No brand index found for brand: {brand}")
                        continue
                    brand_index = brand_result[0]

                    # 构造新的 pub_id (简化版，可能需具体调整)
                    new_pub_id_prefix = f"1{category_index}{brand_index}"
                    query = text(
                        "SELECT latest_id, uid FROM latest_id WHERE latest_id LIKE :prefix || '%' ORDER BY latest_id DESC LIMIT 1")
                    result = connection.execute(query, {'prefix': new_pub_id_prefix})
                    result_row = result.fetchone()

                    if result_row:
                        # 现有的 latest_id 存在，递增 latest_id
                        latest_id = result_row[0]
                        latest_id_num_part = latest_id[len(new_pub_id_prefix):]
                        latest_id = f"{new_pub_id_prefix}{hex_increment(latest_id_num_part)}"
                        current_uid = result_row[1]
                    else:
                        latest_id = new_pub_id_prefix + '0001'  # 如果没找到，创建一个新的
                        current_uid = base_uid + uid_increment  # 更新 current_uid，确保唯一性
                        uid_increment += 1  # 每次创建新 pub_id 时递增

                    # 更新 latest_id 表
                    update_query = text("""
                    INSERT INTO latest_id (latest_id, uid)
                    VALUES (:latest_id, :uid) 
                    ON CONFLICT (uid) DO UPDATE SET 
                    latest_id = EXCLUDED.latest_id
                    """)  # insert into插入，ON CONFLICT主键，主键是数据库的重要信息，主键的信息唯一且不能重复，主键值冲突则更新
                    connection.execute(update_query, {'latest_id': latest_id, 'uid': current_uid})  # 在链接的基础上执行语句

                    # 更新 DataFrame 中的 pub_id
                    df.at[index, 'pub_id'] = latest_id

                # 更新 pub_uid
                if pd.isna(row['pub_uid']):
                    query_text = "SELECT MAX(pub_uid) FROM {}".format(table_name)
                    query = text(query_text)
                    max_pub_uid = connection.execute(query).fetchone()[0]
                    new_pub_uid = max_pub_uid + 1 if max_pub_uid else 1
                    df.at[index, 'pub_uid'] = new_pub_uid

    return df


if __name__ == "__main__":
    file_path = 'Components_to_be_reviewed/ababababababababa'
    main(file_path)
