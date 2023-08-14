import sys
sys.path.append('./NEO4J_data_import')
from get_node_relation import GetNodeRelation
from process import Processor
from load import Loader,Result


def main(args) -> Result:
    # getBodyNodeAndRelation or getInstanceNodeAndRelation
    #获取BODY和INSTANCE数据并处理为CSV文件
    gnr = GetNodeRelation(args)
    #校验码：站点数据中没有snRelationList数据->10,否则->11
    check_code1 = gnr.save2csv("getBodyNodeAndRelation")
    check_code2 = gnr.save2csv("getInstanceNodeAndRelation")
    #Processor处理CSV文件
    p = Processor(args)
    #Loader导入数据
    loader = Loader(args)
    p.get_node()
    #若校验码大于20(即BODY和INSTANCE中有任一一个或两个关系数据),反之没有关系数据，不处理以及导入关系数据
    if check_code1+check_code2 > 20:
        p.get_body_relation()
        p.get_instance_relation()
        r1 = loader.load_node()
        r2 = loader.load_relation()
        # 合并结果
        r = r1.merge(r2)
    else:
        r = loader.load_node()
    return r
        

# if __name__ == "__main__":
#     #解析命令行参数：usage: main.py [-h] --siteID SITEID [-n NEO4J_PATH] [-d DATA_PATH] [-c NEO4J_DATA_PATH]
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--siteID', help='站点名称', type=str, required=True)
#     parser.add_argument('-n','--NEO4J_PATH', help='neo4j目录',
#                         default= r'D:\All Apps\neo4j-community-5.4.0',
#                         type=str, required=False)
#     parser.add_argument('-d','--DATA_PATH',help='接受站点数据时所存储的文件路径',
#                         default= r'data',
#                         type=str, required=False)
#     parser.add_argument('-c','--NEO4J_DATA_PATH',help='生成neo4j数据的本文件下的文件路径',
#                         default= r'neo4j_data',
#                         type=str, required=False)
#     args = parser.parse_args()
#     main(args)