import pandas as pd
import numpy as np
import os
import re
import glob
import uuid
import time

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

    #获得本体与实例间关系csv
    def get_body2instance(self,df):
        df = df[df['bodySiteNodeId'].notna()]
        #头节点ID
        start_id =  df['id'].map(lambda x : x.split('@')[1])
        #尾节点ID
        end_id = df['bodySiteNodeId'].map(lambda x : x.split('@')[1])
        #生成新的CSV文件
        arrays = np.array([start_id,end_id]).T
        df = pd.DataFrame(arrays,columns=['startId','endId'])
        df.to_csv(
                        os.path.join(self.data_dir,'relation_b2i.csv'),
                        index=False,
                        encoding='utf-8',
                        )   

    def get_node(self):
        #遍历node文件夹的所有CSV并合并成一个DataFrame
        node_df = get_dataframe(self.node_path)
        self.get_body2instance(node_df)
        #获取NEO4J导入字段
        id = node_df['id'].map(lambda x : x.split('@')[1])
        node_name = node_df['nodeName']
        label = node_df['attribute']
        type = node_df['type']
        lastSiteNode = node_df['lastSiteNodeId']
        labelCollections = node_df['labelCollections']
        virtualTreeList = node_df['virtualTreeList']
        structureList = node_df['structureList']
        sn_type = node_df['snType']
        #生成新的CSV文件
        arrays = np.array([id,node_name,label,sn_type,
                          type,lastSiteNode,labelCollections,virtualTreeList,
                          structureList]).T
        node_df = pd.DataFrame(arrays,columns=['id','node_name','label','sn_type',
                                'type','last_site_nodeId','label_collections','virtualTreeList',
                                'structureList'])
        self.write_by_label(node_df)

    def get_instance_relation(self):
        instance_relation_df = pd.read_csv(self.relation_path + '//Instance.csv')
        #获取NEO4J导入字段
        start_id = instance_relation_df['siteNodeId']
        pid = instance_relation_df['pid']
        virtualTreeList = instance_relation_df['virtualTreeList']
        bodyRelationId = instance_relation_df['bodyRelationId']
        #根据timestamp生成INSTANCE的ID
        relationId = instance_relation_df.apply(lambda row:
                                generate_id()
                                if pd.notnull(row['siteNodeId']) and pd.notnull(row['pid'])
                                else np.nan,axis=1
                                )
        #生成新的CSV文件
        arrays = np.array([start_id,pid,relationId,
                           virtualTreeList,bodyRelationId
                           ]).T
        instance_relation_df = pd.DataFrame(arrays, columns=['startId','pid','relationId',
                                                             'virtualTreeList','bodyRelationId'
                                                         ])
        #ID处理成结点后24位ID
        instance_relation_df['startId'] = instance_relation_df['startId'].map(lambda x: x.split('@')[1] if pd.notnull(x) else x)
        instance_relation_df['pid'] = instance_relation_df['pid'].map(lambda x: x.split('@')[1] if pd.notnull(x) else x)

        instance_relation_df.to_csv(
            os.path.join(self.data_dir, 'instance_relation.csv'),
            index=False,
            encoding='utf-8',
        )

    
    def get_body_relation(self):
        body_relation_df = pd.read_csv(self.relation_path+'//Body.csv')

        #获取NEO4J导入字段
        body_relation_df['relationType'] = body_relation_df['relationType'].astype(str).replace({'0': '00'})
        relation_type = body_relation_df['relationType']
        #add_attribute()增加属性，返回增加属性后DataFrame
        body_relation_df = add_attribute(body_relation_df)
        start_id = body_relation_df['siteNodeId']
        end_id = body_relation_df['assSimpleSN']
        ID = body_relation_df['id']
        relationName = body_relation_df['name']
        structureList = body_relation_df['structureList']
        #关系信息{'treeId':treeId,'treeName':treeName}
        relationInfo = body_relation_df.apply(lambda row: {'treeId':row['treeId'],'treeName':row['name']}, axis=1)
        NNRelationList = body_relation_df['NNRelationList']
        SSRelationList = body_relation_df['SSRelationList']
        SNRelationList = body_relation_df['SNRelationList']
        SNSRelationList = body_relation_df['SNSRelationList']

        #生成新的CSV文件
        arrays = np.array([start_id,relation_type,end_id,relationInfo,ID,
                           relationName,structureList,
                           NNRelationList,SSRelationList,SNRelationList,SNSRelationList
                          ]).T
        body_relation_df = pd.DataFrame(arrays,columns=['startId','relationType','endId','relationInfo','ID',
                                        'relationName','structureList',
                                        'NNRelationList','SSRelationList','SNRelationList','SNSRelationList'
                                        ])
        body_relation_df['startId'] = body_relation_df['startId'].map(lambda x : x.split('@')[1] if pd.notnull(x) else x)
        body_relation_df['endId'] = body_relation_df['endId'].map(lambda x : x.split('@')[1] if pd.notnull(x) else x)
        body_relation_df.to_csv(
                        os.path.join(self.data_dir,'body_relation.csv'),
                        index=False,
                        encoding='utf-8',
                        )   

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



def add_attribute(data):
    '''
    在CSV中新增四列，且一行中(即一个本体关系)至多有一个属性不为空，这四个属性只会出现一种
        NNRelationList:如果是 nn普通->普通 即relationtype 为 00 对应字段为siteNodeId-> asssimpleSN(id) 
        SSRelationList:如果是 ss静态->静态 即 relationtype 为 11 对应字段为siteNodeId -> assStaticSNRList(ids)
        SNRelationList:如果是 sn静态->普通 即 relationtype 为 10对应字段为siteNodeId -> asssimplesN(id) 
        SNSRelationList:如果是 sns静态->普通 即 relationtype 为 10对应字段为 siteNodeld ->assSimplesN(id)并目steNodeId ->assstaticSNRList(ids)
    '''
    data['NNRelationList'] = data.apply(lambda row:
                                {'siteNodeId':row['siteNodeId'],'assSimpleSN':row['assSimpleSN']}
                                if row['relationType'] == '00' 
                                else np.nan,axis=1
                                )

    data['SSRelationList'] = data.apply(lambda row:
                                {'siteNodeId':row['siteNodeId'],'assStaticSNRList':row['assStaticSNRList']}
                                if row['relationType'] == '11' 
                                else np.nan,axis=1
                                )

    data['SNRelationList'] = data.apply(lambda row:
                                {'siteNodeId':row['siteNodeId'],'assSimpleSN':row['assSimpleSN']}
                                if row['relationType'] == '10' 
                                else np.nan,axis=1
                                )

    data['SNSRelationList'] = data.apply(lambda row:
                                {'siteNodeId':row['siteNodeId'],'assSimpleSN':row['assSimpleSN']}
                                if row['relationType'] == '10' and  pd.notnull(row['assStaticSNRList'])
                                else np.nan,axis=1 
                                ) 
    return data
