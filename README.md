1. 目前完成以下功能：
1.1. 从postgres读取 空间列表（space_list）和云盘文件夹列表（extracted_folders）
1.2. 遍历扫描所有空间和云盘文件夹，获取所有文件的metadata，与postgres对应库进行比对
1.2.1. 如果文件不存在，则上传到minio，并追加metadata到postgres
1.2.2. 如果文件已存在，但有新版本，则更新postgres，并上传新版本到minio
1.3. 增加日志记录功能，包括info、warning和error，并且error根据执行函数标记了类型，便于排查问题
1.4. 以上功能每12小时定时执行一次

2. 相关数据库和服务器：
2.1 文件服务器路径如下：
* 云盘文档：minio.middleware.dev.motiong.net/raw-knowledge/MotionG/CloudDriveFiles
* 知识库文档：minio.middleware.dev.motiong.net/raw-knowledge/MotionG/SpacesFiles
* 销售会议记录：minio.middleware.dev.motiong.net/raw-knowledge/MotionG/Sales Meetings （注：当前为手动完成妙记 视频和文本的备份）
2.2 数据库：
* 云盘文件元数据：postgresql.middleware.dev.motiong.net/lark_files_backup     cloud_drive_files
* 知识库文件元数据：postgresql.middleware.dev.motiong.net/lark_files_backup   space_nodes
* 云盘文件夹列表：postgresql.middleware.dev.motiong.net/lark_files_backup     extracted_folders
* 知识库列表：postgresql.middleware.dev.motiong.net/lark_files_backup         space_list

3. 各脚本功能如下：
* config.py                            ：minio和postgres配置
* lark_access_token.py                 ：管理授权token的刷新
* lark_cloud_document.py               ：函数库，一些通用函数在这里
* refresh_token.txt                    ：保存最新token的文件
* lark_scanner.py                      ：主脚本，负责调用其他脚本，并定期执行

4. 使用方法：
直接运行lark_scanner.py，运行后保持在后台，即可自动定期执行

5. Todo:
5.1. 写一个脚本，处理code2的情况
5.2. 更新Postgres中所有filepath：加storage，去掉browser