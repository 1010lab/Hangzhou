from tree import *

def get_tree(virtualTreeObject,nodeId,nodeName,func_name): 
    tree_list=[]
    tree_id_list = []
    for row,id,name in zip(virtualTreeObject,nodeId,nodeName):
        for item in row:
            #保证创建的虚拟树是唯一的，但其中的值不能更新
            if  item['id'] not in tree_id_list:
                if func_name == 'create_tree':
                    tree,treeId = create_tree(item,id,name)
                else:
                    tree,treeId = create_label_col(item,id,name)
                tree_list.append(tree)
                tree_id_list.append(treeId)
            #若存在此树，则添加节点id和节点name
            else:
                #用于指向需要添加的TreeNode
                index = tree_id_list.index(item['id'])
                tree_list[index].add_nodeIdLists(id)
                tree_list[index].add_nodeNameLists(name)
    return tree_list

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
                tree.create_relation(body_node,'is_root',tree_node,self.graph)

        #建立标签与节点间的关系
        if(type == 'labelCollection'):
            for id in tree.nodeIdLists:
                cypher = f'''MATCH (n) WHERE n.nodeId = "{id}" RETURN n'''
                node = self.graph.run(cypher).data()[0]['n']
                tree.create_relation(tree_node,'is_label',node,self.graph)
        return self.result