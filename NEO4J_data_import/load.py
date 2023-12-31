from py2neo import Graph,Node,Relationship
import pandas as pd 
import os
import shutil
import logging

#自定义logger对象，用于记录load的操作日志 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('load.log', encoding='utf-8')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',datefmt="%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

'''
    自定义Result类，用于记录load的操作结果
    包括节点导入个数和关系导入个数以及相应的导入信息，默认都为空
'''
class Result():
    def __init__(self) -> None:
        self.node_num = 0
        self.relation_num = 0
        self.node_info = []
        self.relation_info = []
    
    # 结果合并返回一个新对象
    def merge(self,other):
        new = Result()
        new.node_num = self.node_num + other.node_num
        new.relation_num = self.relation_num + other.relation_num
        new.node_info = self.node_info + other.node_info
        new.relation_info = self.relation_info + other.relation_info
        return new
    
class Loader():
    def __init__(self,args) -> None:
        self.graph = Graph("http://localhost:7474/", auth=("neo4j", "lml2000326"),name = "neo4j")
        self.graph.delete_all()
        self.args = args
        self.result = Result()
        self.import_dir = os.path.join(args["NEO4J_PATH"],'import\\'+args["siteID"])
        if not os.path.exists(self.import_dir):
            os.makedirs(self.import_dir)
        self.__move__()
    
    #将本项目下的导入数据移动并覆盖至NEO4J目录下的import文件夹中
    def __move__(self):
        data_dir = os.path.join(self.args["NEO4J_DATA_PATH"],self.args["siteID"])
        print(data_dir)
        assert os.path.exists(data_dir),'源文件不存在'
        for file_name in os.listdir(data_dir):
            file = os.path.join(data_dir,file_name)
            shutil.copy2(file, self.import_dir)
    
    def load_node(self) -> Result:    
        #导入节点
        logger.info('******导入程序已启动*******')
        #导入BODY的cypher语句
        body_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{file_name}" AS line\n'''.format(file_name=self.args["siteID"]+'\\BODY.csv') +\
                      '''MERGE (n:{label} {{nodeId: COALESCE(line.id, -1),nodeName:line.node_name,
                                      snType:line.sn_type,type:COALESCE(line.type,'null') ,
                                      virtualTreeList:COALESCE(line.virtualTreeList,'null'),
                                      structureList:COALESCE(line.structureList,'null'),
                                      lastSiteNodeId:COALESCE(line.lastSiteNodeId,'null'),
                                      labelCollections:COALESCE(line.labelCollections,'null')
                                    }})\n'''.format(label='body') +\
                       '''return n'''
        #运行cypher,body_res记录返回结点n信息
        body_res = self.graph.run(body_cypher).data()
        
        #导入BODY的cypher语句
        instance_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{file_name}" AS line\n'''.format(file_name=self.args["siteID"]+'\\INSTANCE.csv') +\
                          '''MERGE (n:{label} {{nodeId: COALESCE(line.id, -1),nodeName:line.node_name,
                                      snType:line.sn_type,type:COALESCE(line.type,'null'),
                                      virtualTreeList:COALESCE(line.virtualTreeList,'null'),
                                      structureList:COALESCE(line.structureList,'null'),
                                      lastSiteNodeId:COALESCE(line.lastSiteNodeId,'null'),
                                      labelCollections:COALESCE(line.labelCollections,'null')
                                    }})\n'''.format(label='instance') +\
                          '''return n'''
        #运行cypher,instance_res记录返回结点n信息                  
        instance_res = self.graph.run(instance_cypher).data()
        logger.info('导入节点成功')
        logger.debug(f'导入本体节点:{len(body_res)}')
        logger.debug(f'导入实体节点:{len(instance_res)}')
        #记录导入节点信息
        self.result.node_num += len(body_res) + len(instance_res)
        self.result.node_info.append(f'导入本体节点:{len(body_res)}')
        self.result.node_info.append(f'导入实体节点:{len(instance_res)}')
        #导入BODY与INSTANCE关系的cypher语句
        relation2_filename = os.path.join(self.args["siteID"]+'/relation_b2i.csv')
        relation2_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=relation2_filename) +\
            '''MATCH (from{nodeId:line.startId}),(to{nodeId:line.endId})\n''' +\
            '''MERGE (from)-[r:{relation}]-> (to)\n'''.format(relation="is_instance") +\
            '''RETURN r'''
        #运行cypher,b2i_res记录返回关系r信息
        b2i_res = self.graph.run(relation2_cypher).data()
        logger.info('导入实体与实例关系成功')
        logger.debug(f'导入实体-实例关系:{len(b2i_res)}')
        #记录导入关系信息
        self.result.relation_num += len(b2i_res)
        self.result.relation_info.append(f'导入实体-实例关系:{len(b2i_res)}')
        return self.result
        

    def load_relation(self) -> Result:
        body_relation_filename = os.path.join(self.args["siteID"]+'/body_relation.csv')
        #导入BODY关系的cypher语句
        body_relation_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=body_relation_filename) +\
            '''MATCH (from{nodeId:line.startId}),(to{nodeId:line.endId})\n''' +\
            '''MERGE (from)-[r:{relation}{{relationType:line.relationType,
                                           ID:line.ID,
                                           relationName:line.relationName,
                                           StructureList:COALESCE(line.structureList,'null'),
                                           relationInfo:line.relationInfo
                                         }}]-> (to)\n'''.format(relation="belong_to") +\
            '''SET r.NNRelationList = \n''' +\
            '''CASE WHEN line.NNRelationList IS NOT NULL THEN line.NNRelationList ELSE NULL END\n''' +\
            '''SET r.SSRelationList = \n''' +\
            '''CASE WHEN line.SSRelationList IS NOT NULL THEN line.SSRelationList ELSE NULL END\n''' +\
            '''SET r.SNRelationList = \n''' +\
            '''CASE WHEN line.SNRelationList IS NOT NULL THEN line.SNRelationList ELSE NULL END\n''' +\
            '''SET r.SNSRelationList = \n''' +\
            '''CASE WHEN line.SNSRelationList IS NOT NULL THEN line.SNSRelationList ELSE NULL END\n''' +\
            '''RETURN r'''
        #运行cypher,body_relation_res记录返回关系r信息
        body_relation_res = self.graph.run(body_relation_cypher).data()
        logger.info('导入实体关系成功')
        logger.debug(f'导入实体关系:{len(body_relation_res)}')
        #记录导入关系信息
        self.result.relation_num += len(body_relation_res)
        self.result.relation_info.append(f'导入实体关系:{len(body_relation_res)}')

        instance_relation_filename = os.path.join(self.args["siteID"]+'/instance_relation.csv')
        #导入INSTANCE关系的cypher语句
        instance_relation_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=instance_relation_filename) +\
            '''MATCH (from{nodeId:line.startId}),(to{nodeId:line.pid})\n''' +\
            '''MERGE (from)-[r:{relation}{{relationId:line.relationId}}]-> (to)\n'''.format(relation="belong_to") +\
            '''RETURN r'''
        #运行cypher,instance_relation_res记录返回关系r信息
        instance_relation_res = self.graph.run(instance_relation_cypher).data()
        logger.info('导入实例关系成功')
        logger.debug(f'导入实例关系:{len(instance_relation_res)}')
        #记录导入关系信息
        self.result.relation_num += len(instance_relation_res)
        self.result.relation_info.append(f'导入实例关系:{len(instance_relation_res)}')
        return self.result
       




