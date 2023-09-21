from py2neo import Graph
import os
import shutil
import logging
import time
from neo4j import GraphDatabase
from tree import create_tree_node,create_label_col_node

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
        return self.__dict__
    
class Loader():
    def __init__(self,args) -> None:
        self.graph = Graph("http://localhost:7474/", auth=("neo4j", "123"))
        URI = "neo4j://localhost"
        AUTH = ("neo4j", "123")
        with GraphDatabase.driver(URI, auth=AUTH) as self.driver:
            self.driver.verify_connectivity()
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
        logger.info('******导入站点:'+self.args.siteID+'*******')
        #导入BODY的cypher语句
        body_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{file_name}" AS line\n'''.format(file_name=self.args.siteID+'\\BODY.csv') +\
                      '''MERGE (n:{label} {{
                            nodeId:line.id,nodeName:line.node_name,
                            snType:line.sn_type,type:COALESCE(line.type,[]) ,
                            virtualTreeList:split(line.virtualTreeList, ','),
                            structureList:split(line.structureList,','),
                            lastSiteNode:COALESCE(line.lastSiteNode, 'null'),
                            labelColList:split(line.labelColList,','),
                            remark:"备注信息",
                            fileType:COALESCE(line.fileType,'null'),
                            siteID:"{siteID}"
                        }})\n'''.format(label='body',siteID = self.args["siteID"]) +\
                       '''return n'''
        #运行cypher,body_res记录返回结点n信息
        records,summary,keys = self.driver.execute_query(
                body_cypher,
                database_="neo4j",
            )               
        body_res = len(records)
        logger.debug(f'导入本体节点:{body_res},导入时间:{summary.result_available_after}ms')
        #导入BODY的cypher语句
        instance_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{file_name}" AS line\n'''.format(file_name=self.args["siteID"]+'\\INSTANCE.csv') +\
                          '''MERGE (n:{label} {{
                                nodeId: COALESCE(line.id, -1),nodeName:line.node_name,
                                snType:line.sn_type,type:COALESCE(line.type,'null'),
                                virtualTreeList:COALESCE(line.virtualTreeList,'null'),
                                structureList:COALESCE(line.structureList,'null'),
                                lastSiteNodeId:COALESCE(line.lastSiteNodeId,'null'),
                                labelColList:split(line.labelColList,','),
                                remark:"备注信息",
                                fileType:COALESCE(line.fileType,'null'),
                                siteID:"{siteID}"
                            }})\n'''.format(label='instance',siteID = self.args["siteID"]) +\
                          '''return n'''
        #运行cypher,instance_res记录返回结点n信息
        records,summary,keys = self.driver.execute_query(
                instance_cypher,
                database_="neo4j",
            )                    
        instance_res = len(records)
        logger.debug(f'导入实体节点:{instance_res},导入时间:{summary.result_available_after}ms')
        logger.info('导入节点成功')
        #记录导入节点信息
        self.result.node_num += body_res + instance_res
        self.result.node_info.append(f'导入本体节点:{body_res}')
        self.result.node_info.append(f'导入实体节点:{instance_res}')
        #导入BODY与INSTANCE关系的cypher语句
        relation2_filename = os.path.join(self.args["siteID"]+'/relation_b2i.csv')
        relation2_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=relation2_filename) +\
            '''MATCH (from{nodeId:line.startId}),(to{nodeId:line.endId})\n''' +\
            '''MERGE (from)-[r:{relation}]-> (to)\n'''.format(relation="is_instance",siteID = self.args["siteID"]) +\
            '''SET r.siteID = "{siteID}"\n'''.format(siteID = self.args["siteID"])+\
            '''RETURN r'''
        #运行cypher,b2i_res记录返回关系r信息
        records,summary,keys = self.driver.execute_query(
                relation2_cypher,
                database_="neo4j",
            )     
        b2i_res = len(records)
        logger.info('导入实体与实例关系成功')
        logger.debug(f'导入实体-实例关系:{b2i_res},导入时间:{summary.result_available_after}ms')
        #记录导入关系信息
        self.result.relation_num += b2i_res
        self.result.relation_info.append(f'导入实体-实例关系:{b2i_res}')
        return self.result
        
    def load_relation(self) -> Result:
        body_relation_filename = os.path.join(self.args["siteID"]+'/body_relation.csv')
        #导入BODY关系的cypher语句
        if(os.path.exists(self.import_dir+'/body_relation.csv')):
            body_relation_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=body_relation_filename) +\
                '''MATCH (from{nodeId:line.startId}),(to{nodeId:line.endId})\n''' +\
                '''MERGE (from)-[r:{relation}{{
                    relationType:line.relationType,
                    relationId:line.relationId,
                    relationName:line.relationName,
                    labelList:COALESCE(line.labelList,'null'),
                    structureList:split(line.structureList,','),
                    treeId:split(line.treeId, ','),
                    treeName:COALESCE(line.treeName,'null')
                }}]-> (to)\n'''.format(relation="belong_to",siteID = self.args["siteID"]) +\
                '''SET r.siteID = "{siteID}"\n'''.format(siteID = self.args["siteID"])+\
                '''RETURN r'''
            #运行cypher,body_relation_res记录返回关系r信息
            records,summary,keys = self.driver.execute_query(
                    body_relation_cypher,
                    database_="neo4j",
                )
            body_relation_res = len(records)
            logger.info('导入实体关系成功')
            logger.debug(f'导入实体关系:{body_relation_res},导入时间:{summary.result_available_after}ms')
            #记录导入关系信息
            self.result.relation_num += body_relation_res
            self.result.relation_info.append(f'导入实体关系:{body_relation_res}')

        if(os.path.exists(self.import_dir+'/instance_relation.csv')):
            instance_relation_filename = os.path.join(self.args["siteID"]+'/instance_relation.csv')
            #导入INSTANCE关系的cypher语句
            instance_relation_cypher = '''LOAD CSV WITH HEADERS FROM "file:///{relation_file}" AS line\n'''.format(relation_file=instance_relation_filename) +\
                '''MATCH (from{nodeId:line.startId}),(to{nodeId:line.pid})\n''' +\
                '''MERGE (from)-[r:{relation}{{
                    relationId:line.relationId,
                    bodyRelationId:COALESCE(line.bodyRelationId,'null'),
                    groupId:COALESCE(line.groupId,'null')
                    }}]-> (to)\n'''.format(relation="belong_to") +\
                '''SET r.siteID = "{siteID}"\n'''.format(siteID = self.args["siteID"]) +'''RETURN r'''
            #运行cypher,instance_relation_res记录返回关系r信息
            records,summary,keys = self.driver.execute_query(
                    instance_relation_cypher,
                    database_="neo4j",
                )
            instance_relation_res = len(records)
            logger.info('导入实例关系成功')
            logger.debug(f'导入实例关系:{instance_relation_res},导入时间:{summary.result_available_after}ms')
            #记录导入关系信息
            self.result.relation_num += instance_relation_res
            self.result.relation_info.append(f'导入实例关系:{instance_relation_res}')
        #根据bodyrelationId来找出实例关系间的relationType
        rel_type_cypher = '''MATCH (:instance)-[r1]->(:instance)
                        WITH r1,r1.bodyRelationId AS id
                        MATCH (:body)-[r2]-(:body)
                        WhERE r2.relationId = id
                        SET r1.relationType = r2.relationType
                        RETURN r1'''
        self.driver.execute_query(rel_type_cypher,database_="neo4j")
        return self.result
    
    def set_siteId(self):
        cypher1 = f'''MATCH ()-[r]->()
                    WHERE NOT EXISTS(r.siteID)  
                    SET r.siteID = "{self.args["siteID"]}"'''
        self.graph.run(cypher1)
        cypher2 = f'''MATCH (n)
                    WHERE NOT EXISTS(n.siteID) AND EXISTS(n.nodeName) 
                    SET n.siteID = "{self.args["siteID"]}"'''
        self.graph.run(cypher2)

    def get_tree(self,virtualTreeObject,nodeId,func_name): 
        tree_list=[]
        tree_id_list = []
        for row,id in zip(virtualTreeObject,nodeId):
            for item in row:
                #保证创建的虚拟树是唯一的，但其中的值不能更新
                #创建Node类，不同于Node结点
                childrens = item.get('children') or None
                if  item['id'] not in tree_id_list:
                    if func_name == 'create_tree':
                        tree_node,treeId = create_tree_node(item)
                    else:
                        tree_node,treeId = create_label_col_node(item)
                    #创建Node结点
                    #创建标签并创建标签与标签组的关系
                    node = tree_node.create_node(self.graph)
                    #创建树与根结点，或创建标签组并创建标签
                    self.tree_relation(tree_node,node) if func_name == 'create_tree' else self.label_relation(tree_node,node,id,childrens)
                    tree_list.append(node)
                    tree_id_list.append(treeId)
                #若存在此树，则添加节点id和节点name
                else:
                    #用于指向需要添加的TreeNode
                    index = tree_id_list.index(item['id'])
                    node = tree_list[index]
                    if func_name == 'create_label_col': self.label_relation(tree_node,node,id,childrens)
        logger.info('导入虚拟树节点成功:'+time.ctime()) if func_name == 'create_tree' else logger.info('导入标签组节点成功:'+time.ctime())

    def tree_relation(self,tree_node,node):
        treeId = tree_node.treeId
        #查找到某虚拟树的根节点
        cypher = f'''MATCH (n:body)
            WHERE "{treeId}" IN n.virtualTreeList
            OPTIONAL MATCH (n)-[r]->(m)
            WHERE "{treeId}" IN m.virtualTreeList AND "{treeId}" IN r.treeId
            WITH n, r, m
            WHERE n IS NOT NULL AND r IS NULL AND m IS NULL
            RETURN n'''
        root_node = self.graph.run(cypher).data()
        for body_node in root_node:
            body_node = body_node['n']
            tree_node.create_relation(body_node,'is_root',node,self.graph)

    def label_relation(self,tree_node,node,id,childrens):
        #建立标签与节点间的关系
            #查找与对应标准结点的关系
        cypher = f'''MATCH (n) WHERE n.nodeId = "{id}" RETURN n'''
        basic_node = self.graph.run(cypher).data()[0]['n']
        label_list = [{"key":children.get("name"),"value":children.get("value")} for children in childrens]
        tree_node.create_relation(node,'is_label',basic_node,label_list,self.graph)




