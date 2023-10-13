import os
import shutil
import logging
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv


#自定义logger对象，用于记录load的操作日志 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('load-another.log', encoding='utf-8')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',datefmt="%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

#加载环境变量中的neo4j账户信息
path = os.path.join(os.getcwd(),'.env')
load_dotenv(path)
NEO4J_USER_EX = os.environ.get("NEO4J_USER_EX")
NEO4J_PASSWORD_EX = os.environ.get("NEO4J_PASSWORD_EX")

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
        URI = "bolt://10.215.28.242:7688"
        
        AUTH = (NEO4J_USER_EX, NEO4J_PASSWORD_EX)
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
            shutil.copy(file, self.import_dir)
    
    def load_node(self) -> Result:
        self.__move__()    
        #导入节点
        logger.info('******导入资料包及关系程序已启动*******')
        logger.info('******导入图数据库: '+self.URI+'******')
        logger.info('******导入站点:'+self.args.siteID+'*******')
        #导入BODY的cypher语句
        file_name=self.args.siteID+'//BODY.csv'
        ins_cypher = f'''CALL apoc.periodic.iterate('
                CALL apoc.load.csv("file:///{file_name}" ,{{nullValues:["na"]
                        }})
                YIELD map AS line
                RETURN line','
                CREATE (n:body {{
                    nodeId:line.id,nodeName:line.node_name,
                    snType:line.sn_type,
                    lastSiteNode:line.lastSiteNode,
                    bodySiteNodeId:line.bodySiteNodeId,
                    siteID:"{self.args.siteID}"
                }})
                return n
                ', {{batchSize:1000, iterateList:true, parallel:true}});'''
        #运行cypher,body_res记录返回结点n信息
        records,summary,keys = self.driver.execute_query(
                ins_cypher,
                database_="neo4j",
            )              
        ins_res = records[0].data()['total']
        logger.debug(f'导入实体节点:{ins_res},导入时间:{summary.result_consumed_after}ms')
        logger.info("导入节点成功")
        #记录导入节点信息
        self.result.node_num += ins_res 
        self.result.node_info.append(f'导入实体节点:{ins_res}')
        return self.result
        
    def load_relation(self) -> Result:
        instance_relation_filename = os.path.join(self.args["siteID"]+'/instance_relation.csv')
        #导入INSTANCE关系的cypher语句
        #导入INSTANCE关系的cypher语句
        instance_relation_cypher = f'''CALL apoc.periodic.iterate('
                    CALL apoc.load.csv("file:///{instance_relation_filename}" ,{{nullValues:["na"]}})
                    YIELD map AS line  RETURN line','\n''' + \
                   '''MATCH (from{nodeId:line.startId}),(to{nodeId:line.pid})\n''' + \
                   f'''WHERE from.siteID = "{self.args.siteID}" AND to.siteID = "{self.args.siteID}"''' + \
                   f'''call apoc.create.relationship(from,line.type,{{siteID:"{self.args.siteID}"}},to) yield rel\n''' + \
                   '''RETURN rel''' + \
                   f'''', {{batchSize:1000, iterateList:true, parallel:true}});'''
        #运行cypher,instance_relation_res记录返回关系r信息
        records,summary,keys = self.driver.execute_query(
                instance_relation_cypher,
                database_="neo4j",
            )
        instance_relation_res = records[0].data()['total']
        logger.info('导入实例关系成功')
        logger.debug(f'导入实例关系:{instance_relation_res},导入时间:{summary.result_consumed_after}ms')
        #记录导入关系信息
        self.result.relation_num += instance_relation_res
        self.result.relation_info.append(f'导入实例关系:{instance_relation_res}')
    
        return self.result
    




