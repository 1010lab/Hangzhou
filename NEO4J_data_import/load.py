from py2neo import Graph,Node,Relationship
import pandas as pd 
import os
import shutil
import logging
from tree import create_relation

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
    
    
    def to_string(self):
        # res = {'node_num':self.node_num,'relation_num':self.relation_num,
        #     'node_info':self.node_info,'relation_info':self.relation_info}
        return self.__dict__
    
class Loader():
    def __init__(self,args) -> None:
        self.graph = Graph("http://localhost:7474/", auth=("neo4j", "123"))
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
        assert os.path.exists(data_dir),'源文件不存在'
        for file_name in os.listdir(data_dir):
            file = os.path.join(data_dir,file_name)
            shutil.copy2(file, self.import_dir)
    
    def load_node(self) -> Result:
        self.__move__()    
        #导入节点
        logger.info('******导入程序已启动*******')
        #导入BODY的cypher语句
        body_cypher = '''USING PERIODIC COMMIT 500\n'''+\
                      '''LOAD CSV WITH HEADERS FROM "file:///{file_name}" AS line\n'''.format(file_name=self.args.siteID+'\\BODY.csv') +\
                      '''MERGE (n:{label} {{
                            nodeId:line.id,nodeName:line.node_name,
                            snType:line.sn_type,type:COALESCE(line.type,[]) ,
                            virtualTreeList:split(line.virtualTreeList, ','),
                            structureList:COALESCE(line.structureList,[]),
                            lastSiteNode:split(line.lastSiteNode, ','),
                            labelColList:split(line.labelColList,',')
                        }})\n'''.format(label='body') +\
                       '''return n'''
        #运行cypher,body_res记录返回结点n信息
        body_res = self.graph.run(body_cypher).data()
        
        #导入BODY的cypher语句
        instance_cypher = '''USING PERIODIC COMMIT 500\n'''+\
                          '''LOAD CSV WITH HEADERS FROM "file:///{file_name}" AS line\n'''.format(file_name=self.args["siteID"]+'\\INSTANCE.csv') +\
                          '''MERGE (n:{label} {{
                                nodeId: COALESCE(line.id, -1),nodeName:line.node_name,
                                snType:line.sn_type,type:COALESCE(line.type,'null'),
                                virtualTreeList:COALESCE(line.virtualTreeList,'null'),
                                structureList:COALESCE(line.structureList,'null'),
                                lastSiteNodeId:COALESCE(line.lastSiteNodeId,'null'),
                                labelColList:split(line.labelColList,',')
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
        relation2_cypher = '''USING PERIODIC COMMIT 500\n'''+\
            '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=relation2_filename) +\
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
        body_relation_cypher = '''USING PERIODIC COMMIT 500\n'''+\
            '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=body_relation_filename) +\
            '''MATCH (from{nodeId:line.startId}),(to{nodeId:line.endId})\n''' +\
            '''MERGE (from)-[r:{relation}{{
                relationType:line.relationType,
                relationId:line.relationId,
                relationName:line.relationName,
                StructureList:COALESCE(line.structureList,'null'),
                treeId:split(line.treeId, ','),
                treeName:COALESCE(line.treeName,'null')
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
        instance_relation_cypher = '''USING PERIODIC COMMIT 500\n'''+\
            '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=instance_relation_filename) +\
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
    
    def tree_relation(self,tree,type):
        rel_num = 0
        treeId = tree.treeId
        tree_node = tree.create_node(self.graph)
        if(type == 'virtualTree'):
        #查找到某虚拟树的根节点
            cypher = f'''
                        MATCH (n:body)
                        WHERE  NOT (n)-[:belong_to]->() AND "{treeId}" IN n.virtualTreeList
                        RETURN n'''
            root_node = self.graph.run(cypher).data()
            rel_num += len(root_node)
            for body_node in root_node:
                body_node = body_node['n']
                create_relation(body_node,'is_root',tree_node,self.graph)
            logger.info('导入虚拟树节点成功')
            self.result.node_num += rel_num
            self.result.node_info.append(f'导入虚拟树节点:{rel_num}')
            logger.debug(f'导入虚拟树关系:{rel_num}')
            self.result.relation_num += rel_num
            self.result.relation_info.append(f'导入虚拟树关系:{rel_num}')   

        #建立标签与节点间的关系
        if(type == 'labelCollection'):
            for id in tree.nodeIdLists:
                cypher = f'''MATCH (n) WHERE n.nodeId = "{id}" RETURN n'''
                node = self.graph.run(cypher).data()[0]['n']
                create_relation(tree_node,'is_label',node,self.graph)
        return self.result




