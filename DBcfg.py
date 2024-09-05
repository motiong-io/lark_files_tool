# MinIO connection details
minio_config ={
    'endpoint': 'storage.minio.middleware.dev.motiong.net',
    'access_key': '25Q8eCqlcu0ey7be7QOx',
    'secret_key': 'EUKDWIghj2NWIwMcPj2mX4BtFmEkieVWicaVxtqe',
    'secure': False,  # Change to True if you're using HTTPS
    'bucket': 'raw-knowledge',
    'path_before_review':'components_to_be_reviewed',
    'path_after_review':'components_to_be_uploaded'
}


# postgres数据库配置信息

DB_CONNECTION_STRING_read_only = 'postgresql://read_only:7Wre5WCvl75a@postgresql.middleware.dev.motiong.net:5432/root_components_library'
DB_CONNECTION_STRING_read_write = 'postgresql://motiong:zK6ksBKKUn9E3rJbKN53@postgresql.middleware.dev.motiong.net:5432/root_components_library'
#ve_design_plan_db