1. 目前已完成第一轮的metadata获取，存放在各csv文件中,介绍如下：
* cloud_drive_files.csv                ：云盘所有文件列表
* extracted_folders.csv                ：云盘用户文件夹列表
* nodes.csv                            ：知识库所有文件列表
* space_list.csv                       ：知识库列表
* visited_folders.csv                  ：云盘已扫描过的文件夹token列表，是云盘扫描脚本的一个中间文件
2. 目前在进行第一轮的文件下载和上传minio，速度较慢
3. 各脚本功能如下：
* cloud_drive_downloader.py            ：根据云盘文件列表下载并上传文件
* cloud_drive_metadata_scanner.py      ：扫描所有云盘，将所有文件的metadata存入csv
* wiki_downloader.py                   ：根据知识库文件列表下载并上传文件
* wiki_metadate_scanner.py             ：扫描所有知识库，将所有文件的metadata存入csv
* config.py                            ：minio配置
* lark_access_token.py                 ：管理授权token的刷新
* lark_cloud_document.py               ：函数库，一些通用函数在这里
* refresh_token.txt                    ：保存最新token的文件
4. 使用方法：
  以下4个脚本为可执行脚本，直接运行即可，无需参数
* cloud_drive_downloader.py            
* cloud_drive_metadata_scanner.py      
* wiki_downloader.py                  
* wiki_metadate_scanner.py             
