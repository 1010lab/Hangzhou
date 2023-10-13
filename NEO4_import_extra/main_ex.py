import sys
sys.path.append('./NEO4_import_extra')
from NEO4_import_extra.get_node_relation import GetNodeRelation
from NEO4_import_extra.process import Processor
from NEO4_import_extra.load import Loader,Result
# from RDBS.db_utils import Mysql


def main_ex(args) -> Result:
    #定义mysql数据库实例，用于存储导入记录
    # mysql = Mysql()
    # getBodyNodeAndRelation or getInstanceNodeAndRelation
    #获取BODY和INSTANCE数据并处理为CSV文件
    gnr = GetNodeRelation(args)
    # 校验码：站点数据中没有snRelationList数据->10,否则->11
    gnr.save2csv()
    

    #Processor处理CSV文件
    p = Processor(args)
    #Loader导入数据
    loader = Loader(args)
        #返回虚拟树节点列表
    p.get_node()
    p.get_instance_relation()
    loader.load_node()
    r = loader.load_relation()
    return r.to_string()
        
