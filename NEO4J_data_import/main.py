import sys
sys.path.append('./NEO4J_data_import')
from get_node_relation import GetNodeRelation
from process import Processor
from load import Loader,Result


def main(args) -> Result:
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
    # for tree in label_tree:
    #     r = loader.tree_relation(tree,"labelCollection")
    # for tree in tree_list:
    #     r = loader.tree_relation(tree,"virtualTree")
    loader.set_siteId()
    return r.to_string()
        

