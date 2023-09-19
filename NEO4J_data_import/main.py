import sys
sys.path.append('./NEO4J_data_import')
from get_node_relation import GetNodeRelation
from process import Processor
from load import Loader,Result
from RDBS.db_utils import Mysql
import time
from datetime import datetime

def main(args) -> Result:
    #定义mysql数据库实例，用于存储导入记录
    mysql = Mysql()
    # getBodyNodeAndRelation or getInstanceNodeAndRelation
    #获取BODY和INSTANCE数据并处理为CSV文件
    # gnr = GetNodeRelation(args)
    # # 校验码：站点数据中没有snRelationList数据->10,否则->11
    # check_code1 = gnr.save2csv("getBodyNodeAndRelation")
    # check_code2 = gnr.save2csv("getInstanceNodeAndRelation")
    check_code1 = 11
    check_code2 = 11
    #Processor处理CSV文件
    p = Processor(args)
    #Loader导入数据
    loader = Loader(args)
    try:
        #返回虚拟树节点列表
        labelColObject,virtualTreeObeject,id = p.get_node()
        #若校验码大于20(即BODY和INSTANCE中有任一一个或两个关系数据),反之没有关系数据，不处理以及导入关系数据
        if check_code1+check_code2 > 20:
            p.get_body_relation() 
            p.get_instance_relation()
            loader.load_node()
            r = loader.load_relation()
        else:
            r = loader.load_node()
        loader.get_tree(labelColObject,id,'create_label_col')
        loader.get_tree(virtualTreeObeject,id,'create_tree')
    except Exception as e:
            print(f"Error delete data: {e}")
    load_time = datetime.strptime(time.ctime(), "%a %b %d %H:%M:%S %Y")
    mysql.insert(args["siteID"],str(load_time),'user')
    return r.to_string()
        

