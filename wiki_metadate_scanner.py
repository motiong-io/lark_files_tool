import lark_cloud_document as lark_file
import csv

space_list_path='space_list.csv'
nodes_path='nodes.csv'
#获取账号可见范围内所有空间的列表，并存入csv文件，可一周或一月执行一次
def space_list():                       
    url = "https://open.feishu.cn/open-apis/wiki/v2/spaces?lang=en&page_size=50"
    payload = {'page_size': 50, 'page_token': ''}
    response_s = lark_file.request_with_retry(url, params=payload)
    spaces= response_s['data']['items']
    while response_s['data']['has_more'] is True:
        payload['page_token'] = response_s['data']['page_token']
        response_s = lark_file.request_with_retry(url,  params=payload)
        spaces += response_s['data']['items']
    lark_file.json_to_append_csv(spaces, space_list_path)
    return spaces
#根据space_id获取该空间下的所有节点的详细信息，并存入csv文件
def space_node_obj(space_id, parent_node_token=''):         
    files=[]
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
    payload = {'page_size': 50, 'page_token': '', 'parent_node_token':parent_node_token}
    response= lark_file.request_with_retry(url,  params=payload)
    if 'items' in response['data']:
        files+=response['data']['items']
        while response['data']['has_more'] is True:
            payload['page_token'] = response['data']['page_token']
            response = lark_file.request_with_retry(url, params=payload)
            files += response['data']['items']
        for file in files:
            print(file['title'])
        lark_file.json_to_append_csv(files, nodes_path)
        for item in response['data']['items']:
            if item['has_child'] is True:
                files+=space_node_obj(space_id, item['node_token'])
    return files
#主函数，判断该处理哪些空间列表，并调用函数进行处理
def space_node_obj_summary():                   
    print("Begin to check space_id...")
    # spaces_list = space_list()                # 获取所有空间的列表


    #从 nodes list 中读取所有已提取的 space_id
    extracted_spaces=set()
    with open(nodes_path, 'r', encoding='utf-8') as nodes_file:
        nodes_list = list(csv.DictReader(nodes_file))
        for node in nodes_list:
            extracted_spaces.add(node['space_id'])

    # 从 space_list.csv 中读取所有 space_id
    with open(space_list_path, 'r', encoding='utf-8') as space_list_file:
        spaces_list = list(csv.DictReader(space_list_file))
    print("Begin to summary all objs...")
    for space in spaces_list:                   #遍历所有space_id
        space_id = space['space_id']            
        if space_id not in extracted_spaces:    #如果该 space_id 未被提取过，则进行提取，否则跳过
            space_node_obj(space_id)
    print("All spaces have been extracted.")

if __name__ == '__main__': 
    space_node_obj_summary()

