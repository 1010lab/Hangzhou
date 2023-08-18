import sys
sys.path.append('./NEO4J_data_import')
from get_node_relation import GetNodeRelation
from process import Processor
from load import Loader,Result


def main(args) -> Result:
    # getBodyNodeAndRelation or getInstanceNodeAndRelation
    #获取BODY和INSTANCE数据并处理为CSV文件
    # gnr = GetNodeRelation(args)
    #校验码：站点数据中没有snRelationList数据->10,否则->11
    # check_code1 = gnr.save2csv("getBodyNodeAndRelation")
    # check_code2 = gnr.save2csv("getInstanceNodeAndRelation")
    check_code1 = 11
    check_code2 = 11
    #Processor处理CSV文件
    p = Processor(args)
    #Loader导入数据
    loader = Loader(args)
    tree_list = p.get_node()
    for tree in tree_list:
        print(tree.__dict__)
        tree._create()
    #若校验码大于20(即BODY和INSTANCE中有任一一个或两个关系数据),反之没有关系数据，不处理以及导入关系数据
    if check_code1+check_code2 > 20:
        p.get_body_relation()
        p.get_instance_relation()
        loader.load_node()
        r = loader.load_relation()
    else:
        r = loader.load_node()
    return r.to_string()
        

