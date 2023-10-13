import pandas as pd
import numpy as np
import os
import re
import glob
import uuid
import time
import ast

'''
    处理CSV文件，形成用于导入NEO4J的数据格式以及字段要求。
    get_node():获取BODY以及INSTANCE节点CSV文件
    get_body2instance()：获取BODY与INSTANCE关系CSV文件
    get_instance_relation()：获取INSTANCE关系CSV文件
    get_body_relation():获取BODY关系CSV文件
'''
class Processor():
    def __init__(self,args) -> None:
        self.args = args
        self.node_path = os.path.join(args["DATA_PATH"],args["siteID"],'nodes')
        self.relation_path = os.path.join(args["DATA_PATH"],args["siteID"],'relations')
        self.data_dir = os.path.join(args["NEO4J_DATA_PATH"],args["siteID"])
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
      
    def write_by_label(self,node_df):
        grouped = node_df.groupby('label')
        for label, group_df in grouped:
            for l in label.split('@'):
                if re.match('\\d+',l):
                    label = label.replace(l,'_'+l)
            output_filename = os.path.join(self.data_dir,label+'.csv')
            group_df.to_csv(output_filename, index=False,encoding='utf-8')
    
    def write_by_type(self,node_df):
        grouped = node_df.groupby('type')
        for label, group_df in grouped:
            for l in label.split('@'):
                if re.match('\\d+',l):
                    label = label.replace(l,'_'+l)
            output_filename = os.path.join(self.data_dir,label+'.csv')
            group_df.to_csv(output_filename, index=False,encoding='utf-8')
    
   

    def get_node(self):
        #遍历node文件夹的所有CSV并合并成一个DataFrame
        node_df = get_dataframe(self.node_path)
        #获取NEO4J导入字段
        id = node_df['id'].map(lambda x : x.replace('@','/'))
        node_name = node_df['nodeName']
        label = node_df['attribute']
        lastSiteNode = node_df['lastSiteNodeId'].map(lambda x : x.replace('@','/') if pd.notnull(x) else x)
        bodySiteNodeId = node_df['bodySiteNodeId'].map(lambda x : x.replace('@','/') if pd.notnull(x) else x)                                 
        sn_type = node_df['snType']
        #生成新的CSV文件
        arrays = np.array([id,node_name,label,sn_type,
                            lastSiteNode,bodySiteNodeId]).T
        node_df = pd.DataFrame(arrays,columns=['id','node_name','label','sn_type',
                                'lastSiteNode','bodySiteNodeId'])
        self.write_by_label(node_df)

    def get_instance_relation(self):
        if(not os.path.exists(self.relation_path + '//Instance.csv')):return
        instance_relation_df = pd.read_csv(self.relation_path + '//Instance.csv')
        #获取NEO4J导入字段
        start_id = instance_relation_df['from']
        pid = instance_relation_df['to']
        type = instance_relation_df['type']
       
        arrays = np.array([start_id,pid,type]).T
        instance_relation_df = pd.DataFrame(arrays, columns=['startId','pid','type'])
        #ID处理成结点后24位ID
        instance_relation_df['startId'] = instance_relation_df['startId'].map(lambda x: x.replace('@','/') if pd.notnull(x) else x)
        instance_relation_df['pid'] = instance_relation_df['pid'].map(lambda x: x.replace('@','/') if pd.notnull(x) else x)

        instance_relation_df.to_csv(
            os.path.join(self.data_dir, 'instance_relation.csv'),
            index=False,
            encoding='utf-8',
        )

#查看属于哪种关系
def find_end(row):
    if row['relationType'] == '11':
        if row['assStaticSNList'] is not None and pd.notnull(row['assStaticSNList']):
            sn_list = ast.literal_eval(row['assStaticSNList'])
            sn_list.append(row['assSimpleSN'])
            return sn_list
        else: return []
    # if row['relationType'] == '11' :
    #     sn_list = ast.literal_eval(row['assStaticSNList']) if pd.notnull(row['assStaticSNList']) else []
    #     return sn_list
    if row['relationType'] == '00' or row['relationType'] == '10':
        return [row['assSimpleSN']] 

def generate_id():
    unique_uuid = uuid.uuid4() 
    # 获取当前时间戳 
    timestamp = int(time.time()) 
    # 将UUID的前8位与时间戳拼接，并使用替换掉其中的短横线和冒号 
    unique_id = str(unique_uuid)[:8] + str(timestamp) 
    unique_id = unique_id.replace("-", "").replace(":", "")
    # 如果不足24位，使用0填充 
    if len(unique_id) < 24: unique_id += "0" * (24 - len(unique_id)) 
    return unique_id[:24] # 截取前24位

def get_dataframe(path):
    folder_path = path+'/*.csv'  
    file_paths = glob.glob(folder_path)
    dataframes = []  # 存储所有CSV文件的数据框

    for file_path in file_paths:
        df = pd.read_csv(file_path)
        dataframes.append(df)
    
    return pd.concat(dataframes, ignore_index=True)
