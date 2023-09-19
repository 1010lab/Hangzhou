from py2neo import Graph
# from neo4j_query.utils import generate_id
from neo4j import GraphDatabase

class Query():
    def __init__(self) -> None:
        URI = "neo4j://localhost"
        AUTH = ("neo4j", "123")

        with GraphDatabase.driver(URI, auth=AUTH) as self.driver:
            self.driver.verify_connectivity()
    
    def _run(self,cypher):
        records,summary,keys = self.driver.execute_query(
                cypher,
                database_="neo4j",
            )            
        return list(records)

    #查找虚拟根节点
    def find_vroot(self,siteID):
        rootId = siteID + "root"
        cypher = f'''MATCH (root:body)
                WHERE root.nodeId = '{rootId}'
                return root'''
        records = self._run(cypher)
        return records if len(self._run(cypher)) != 0 else None

    #查找虚拟树中的根节点
    def find_root(self,treeId):
        cypher = f'''MATCH (root)-[:is_root]->(vir:virtualTree)
                WHERE vir.nodeId = '{treeId}'
                return root'''
        return self._run(cypher)

    #用于寻找虚拟树根节点以及孤立节点
    #!存在的问题：当多站点进行图查询时，虚拟的root节点会被认为是孤立节点
    #出现问题的原因，即当一个站点进行图查询时，会创建虚拟root节点，而当另一个站点的数据导入时，因为load语句会为所有的节点以及关系创建对应的siteID属性，
    #如果再进行图查询，之前的虚拟root节点会被认为时孤立节点，并于当前的虚拟root节点产生关系
    def create_root(self,siteID,page_size,page_num):
        count = page_size*(page_num-1)
        rootId = str(page_num) +"root"
        cypher = f'''MATCH (start:body)
                WHERE start.siteID = '{siteID}'
                OPTIONAL MATCH (start:body)-[r:belong_to]->(end)
                WHERE start.siteID = '{siteID}'
                WITH start,r,end
                SKIP {count}
                LIMIT {page_size}
                WITH COLLECT(DISTINCT start) AS allStarts, COLLECT(DISTINCT end) AS allEnds
                MATCH (n:body)
                WHERE NOT (n)-[:belong_to]->() AND (n in allStarts OR n in allEnds)''' +\
                '''merge (root:body{{nodeId :'{nodeId}'}})'''.format(nodeId=rootId) +\
                '''with  root,n
                CREATE (n)-[r:vir_root]->(root)
                RETURN n,r,root'''
        return self._run(cypher)

    def delete_root(self,rootId):
        cypher = f'''MATCH (n:body)-[r]-() WHERE n.nodeId= "{rootId}"
                    DELETE n,r'''
        self._run(cypher)

    #用于查找虚拟树下的所以关系
    def find_root_relation(self,rootId):
        cypher = f'''MATCH (s)-[r:belong_to]->(rt:body)
                WHERE rt.nodeId = '{rootId}'
                RETURN s,r,rt'''
        return self._run(cypher)

    def find_vir_name(self,nodeId):
        cypher = f'''MATCH (n:virtualTree)
                WHERE  n.nodeId = '{nodeId}'
                RETURN n.nodeName as nodeName'''
        records = self._run(cypher)
        return records[0]['nodeName']

    #查找虚拟树根节点在表结构内部
    def  is_inner(self,nodeId,stuctureId):
        cypher = f'''MATCH  p=(m)-[r]->(n)
        WHERE '{stuctureId}' IN  m.structureList AND '{stuctureId}' IN  n.structureList
        WITH NODES(p) AS nodes
        MATCH (node)
        WHERE node IN nodes AND node.nodeId = '{nodeId}'
        RETURN node'''
        records = self._run(cypher)
        return True if len(records) > 0  else False

    #查找具有回路的表结构中的边
    def circle_relation(self,stuctureId):
        cypher = f'''MATCH (m:body)-[r1:belong_to]->(n), (n)-[r2:belong_to]->(m)
                    WITH r1,r2
                    MATCH p=(n:body)-[r:belong_to]->(root)
                    WHERE (r = r1 or r =r2) AND ('{stuctureId}' IN n.structureList AND '{stuctureId}' IN root.structureList AND '{stuctureId}' IN r.structureList)
                    return collect(DISTINCT r) as line'''
        records = self._run(cypher)
        return records[0]['line'][0].__dict__['_properties']['relationId'] if len(records[0]['line']) > 0 else None
        
      

    ###############################################################################################################
    def graph_query(self,label,siteID):
        if label =='body':
            cypher = f'''MATCH (start:{label})
                    WHERE start.siteID = '{siteID}'
                    OPTIONAL MATCH (start:{label})-[r:belong_to]->(end)
                    WHERE start.siteID = '{siteID}'
                    RETURN start, r, end '''
        if label =='instance':
            cypher = f'''MATCH (start:{label})
                    WHERE start.siteID = '{siteID}'
                    OPTIONAL MATCH (start:{label})-[r:belong_to]->(end:{label})
                    WHERE start.siteID = '{siteID}'
                    RETURN start, r, end ''' 
        if label is None:
            cypher =f'''MATCH (start)
                    WHERE start.siteID = '{siteID}' AND LABELS(start) in [['body'],['instance']]
                    OPTIONAL MATCH (start)-[r]->(end)
                    WHERE start.siteID = '{siteID}' AND TYPE(r) in ['belong_to','is_instance']
                    RETURN start, r, end '''
        return self._run(cypher)

    #本体/实体图查询(一个站点)
    #定义一个带有分页功能的图查询
    def graph_query_with_page(self,label,siteID,page_size,page_num):
        count = page_size*(page_num-1)
        cypher = f'''MATCH (start:{label})
                    WHERE start.siteID = '{siteID}'
                    WITH start
                    SKIP {count}
                    LIMIT {page_size}
                    OPTIONAL MATCH (start:{label})-[r:belong_to]->(end)
                    WHERE start.siteID = '{siteID}'
                    RETURN start, r, end '''
        records = self._run(cypher)
        return records

    def tree_query(self,label,treeId,siteID,page_size):
        pages = {}
        count = 0
        while True:

            if label == 'body':
                cypher = '''MATCH (start:{label}{{siteID:'{siteID}'}})\n'''.format(label=label,siteID=siteID)+\
                        '''WHERE "{treeId}"  IN start.virtualTreeList\n'''.format(treeId=treeId)+\
                        '''OPTIONAL MATCH (start)-[r:belong_to]->(end:body)\n'''.format(label=label,siteID=siteID)+\
                        '''WHERE "{treeId}" in end.virtualTreeList AND "{treeId}" in r.treeId\n'''.format(treeId=treeId)+\
                        '''RETURN start,r,end\n'''+\
                        '''SKIP {count}\n'''.format(count= count)+\
                        '''LIMIT {page_size}'''.format(page_size= page_size)
            else:
                cypher = '''MATCH (m:body{{siteID:'{siteID}'}})-[r1:belong_to]->(n:body)\n'''.format(label=label,siteID=siteID)+\
                        '''WHERE "{treeId}"  IN m.virtualTreeList AND "{treeId}" in n.virtualTreeList AND "{treeId}" in r1.treeId\n'''.format(treeId=treeId)+\
                        '''WITH COLLECT(r1.relationId) AS rel\n'''+\
                        '''MATCH (start)-[r:belong_to]-(end)\n'''+\
                        '''WHERE r.bodyRelationId in rel\n'''+\
                        '''RETURN start,r,end\n'''+\
                        '''SKIP {count}\n'''.format(count= count)+\
                        '''LIMIT {page_size}'''.format(page_size= page_size)
            records = self._run(cypher)
            if(len(records) == 0): break   
            pages[int(count/page_size)] = records
            count += page_size
        return pages
        
    def set_default_color(self,nodeId,color,remark):
        cypher = f'''MATCH (m) 
                    WHERE m.nodeId IN {nodeId}
                    SET m.defaultColor = '{color}'
                    SET m.remark = '{remark}'
                    RETURN m'''
        return self._run(cypher)
        
    #查找本体下的实例
    def get_instance(self,nodeId):
        cypher = f'''MATCH (ins:instance)-[:is_instance]->(m)
                    WHERE m.nodeId = "{nodeId}"
                    RETURN ins'''
        return self._run(cypher)

    #基于固定属性查询所有结点
    def by_attribute_query(self,attributeKey,attributeValue,label):
        cypher = f'''MATCH (n{":"+label if label else ""})
                     WHERE n.{attributeKey} = '{attributeValue}'
                     return n '''
        return self.graph.run(cypher).data()

    #基于固定属性值查询所有实体，并排序
    def order_by_attribute_query(self,attributeKey,attributeValue,label,order_by):
        result= self.by_attribute_query(attributeKey,attributeValue,label)
        nodes = [node['n'] for node in result]
        return sorted(nodes, key=lambda node: node[order_by])

    #个数统计查询
    def count_query(self,label,siteID):
        cypher =  f'''MATCH (n:{label})
                    WHERE n.siteID = "{siteID}"
                    RETURN  count(n) AS count'''
        records = self._run(cypher)
        return records[0]['count']

    #body/instance一跳关系查询
    def one_hop_query(self,nodeId):
        #label冗余对接是否保留
        cypher =  f'''MATCH (start)-[r]-(end)
                      WHERE start.nodeId = '{nodeId}' AND TYPE(r) in ["belong_to","is_instance"]
                      AND EXISTS(end.nodeName)
                      RETURN start,r, end'''
        return self._run(cypher)

    def  type_query(self,nodeId,type):
        if type == 1:
            cypher =  f'''MATCH (start)-[r]-(end)
                        WHERE start.nodeId = '{nodeId}' AND  start.snType = '2' AND end.snType = '2'
                        RETURN start,r, end'''
        else:
            cypher =  f'''MATCH (start)-[r]-(end)
                        WHERE start.nodeId = '{nodeId}' AND end.snType <> '2' AND TYPE(r) in ["belong_to","is_instance"]
                        RETURN start,r, end'''
        return self._run(cypher)

    #三跳关系查询，不区分body和instance
    def three_hop_query(self,nodeId,label):
        cypher =  f'''MATCH (startNode{":"+label if label else ""})-[*1..3]->(endNode)
                    WHERE startNode.nodeId =  '{nodeId}' 
                    RETURN startNode, endNode'''
        return self.graph.run(cypher).data()

    def shortest_path_query(self,startNodeId,endNodeId):
        cypher = f'''MATCH path = shortestPath((startNode)-[r*]-(endNode)) 
                WHERE startNode.nodeId= '{startNodeId}' AND endNode.nodeId = '{endNodeId}'
                WITH NODES(path) AS allNodes, RELATIONSHIPS(path) AS allRelationships
                MATCH (start)-[r]->(end) 
                WHERE  start IN allNodes AND end IN allNodes AND r IN allRelationships
                RETURN  start,r,end'''
        return self._run(cypher)

    def delete_graph(self,siteID):
        cypher =f'''MATCH (n)
                WHERE n.siteID = "{siteID}"
                OPTIONAL MATCH (n)-[r]-()
                WHERE r.siteID = "{siteID}"
                DELETE n, r'''
        records,summary,keys = self.driver.execute_query(
                cypher,
                database_="neo4j",
            )
        return summary        

    '''
        表结构组查询:
        表内结构查询，查询表结构路径上的所有本体实体以及各自关系
        structure_query:查询表结构关系
        sc_instance_query:查询表结构本体对应实体
        sc_ins_realtion_query:查询表结构对应实体以及关系
    '''
    #根据表结构id查询表结构中的节点以及关系
    def structure_query(self,structureId):
        cypher =f'''MATCH p = (start)-[r]->(end)
                    WHERE '{structureId}' IN  start.structureList AND '{structureId}' IN  end.structureList
                    RETURN  start,r,end'''
        return self._run(cypher)
    
    #查找表结构下本体对应实体要素以及本体与实体间的关系
    def sc_instance_query(self,structureId):
        cypher =f'''MATCH p = (start1)-[r1]->(end1)
                    WHERE '{structureId}' IN  start1.structureList AND '{structureId}' IN  end1.structureList
                    WITH COLLECT(start1) + COLLECT(end1) AS combinedNodes
                    MATCH (start)-[r:is_instance]->(end)
                    WHERE end IN combinedNodes
                    RETURN start,r,end'''
        return self._run(cypher)

    #查找表结构本体下的实体以及实体之间的关系
    def sc_ins_realtion_query(self,structureId):
        cypher =f'''MATCH p = (start1)-[r1]->(end1)
                    WHERE '{structureId}' IN  start1.structureList AND '{structureId}' IN  end1.structureList
                    WITH r1.relationId AS rid
                    MATCH (start)-[r:belong_to]->(end) 
                    WHERE r.bodyRelationId = rid
                    RETURN start,r,end'''
        return self._run(cypher)
    
    '''
        表外查询：查询表结构中虚拟树的数据，表结构的根到虚拟树的根
        find_vir_list:查找出该表结构下包含的虚拟树
        find_str_root:查找出表结构根节点
        find_vir_root:查找虚拟树根节点
    '''
    #统计数的节点树
    def count_tree(self,nodeId,type):
        cypher = f'''MATCH (m)
                WHERE '{nodeId}' IN  m.{type} 
                RETURN count(m) AS count'''
        return self._run(cypher)[0].get('count')

    #查找出该表结构下包含的虚拟树,需要对不存在关系的虚拟树根节点进行判断
    def find_vir_list(self,structureId):
        cypher = f'''MATCH (m)-[r]->(n)
                    WHERE '{structureId}' IN  m.structureList AND '{structureId}' IN  n.structureList AND '{structureId}' IN  r.structureList
                    WITH  COLLECT(m.virtualTreeList) + COLLECT(n.virtualTreeList) AS treeList
                    UNWIND treeList AS item
                    RETURN collect(DISTINCT item) AS uniqueTreeList'''
        #获取虚拟树Id列表
        records = self._run(cypher)[0].get('uniqueTreeList')
        # return list(set(item for sublist in records for item in sublist))
        return records

    def find_str_root(self,structureId):
        count = self.count_tree(structureId,"structureList")
        if count == 1:
            cypher = f'''MATCH (root:body) 
                    WHERE '{structureId}' IN  root.structureList
                    AND NOT (root)-[:belong_to]->()
                    RETURN root.nodeId AS id'''
        else:
            cypher = f'''MATCH p=(n:body)-[:belong_to*]->(root)
                        WHERE ALL(rel IN relationships(p) WHERE '{structureId}' IN rel.structureList) AND 
                        ('{structureId}' IN n.structureList AND '{structureId}' IN root.structureList)
                        RETURN root.nodeId AS id
                        LIMIT 1'''
        records = self._run(cypher)
        return records[0].get("id") if len(records) > 0  else None
      

    #查找虚拟树根节点,返回根节点id
    def  find_vir_root(self,virId):
        # count = self.count_tree(virId,"virtualTreeList")
        # if count == 1:
        #     f'''MATCH (root:body) 
        #             WHERE '{virId}' IN  root.virtualTreeList
        #             AND NOT (root)-[:belong_to]->()
        #             RETURN root.nodeId AS id'''
        # else:
        #     cypher = f'''MATCH p=(n:body)-[:belong_to*]->(root)
        #                 WHERE '{virId}' IN  n.virtualTreeList AND '{virId}' IN root.virtualTreeList
        #                 RETURN root.nodeId AS id
        #                 LIMIT 1'''
        cypher = f'''MATCH (n:body)
            WHERE "{virId}" IN n.virtualTreeList
            OPTIONAL MATCH (n)-[r]->(m)
            WHERE "{virId}" IN m.virtualTreeList AND "{virId}" IN r.treeId
            WITH n, r, m
            WHERE n IS NOT NULL AND r IS NULL AND m IS NULL
            RETURN n.nodeId AS id'''
        records = self._run(cypher)
        return records[0].get("id") if len(records) > 0  else None

    '''
        找出虚拟树根节点到表结构根节点的路径@1
        @1:找到节点间的本体以及本体关系
        virRoot:虚拟树根节点nodeId 
        strRoot:表结构根节点nodeId
    '''
    #此处逻辑可能存在漏洞,即在查找最短路径的过程中可能出现不属于当前虚拟树的路径，会造成返回出错
    def outer_query(self,virRoot,strRoot):
        cypher = f'''MATCH path = shortestPath((startNode)-[r*]->(endNode))
                WHERE startNode.nodeId = "{strRoot}" AND  endNode.nodeId = "{virRoot}"  
                WITH nodes(path) AS allNodes, relationships(path) AS allRelationships
                MATCH (start)-[r]->(end) 
                WHERE  start IN allNodes AND end IN allNodes AND TYPE(r) IN ["belong_to"] 
                return  start,r,end'''
        return self._run(cypher)

    #@2:查找路径上的本体与实体以及关系 
    def  outer_instance_query(self,virRoot,strRoot):
        cypher = f'''MATCH path = shortestPath((startNode)-[r*]->(endNode))
                WHERE startNode.nodeId = "{strRoot}" AND  endNode.nodeId = "{virRoot}" 
                WITH NODES(path) AS allNodes, RELATIONSHIPS(path) AS allRelationships
                MATCH (start)-[r]-(end) 
                WHERE  start IN allNodes AND TYPE(r) IN ["is_instance"] 
                RETURN  start,r,end'''
        return self._run(cypher)

    #@3:查找路径上实体之间的关系
    def outer_ins_relation_query(self,virRoot,strRoot):
        tmp_cypher = f'''MATCH path = shortestPath((startNode)-[r*]->(endNode))
                WHERE startNode.nodeId = "{strRoot}" AND  endNode.nodeId = "{virRoot}" 
                WITH RELATIONSHIPS(path) AS allRelationships
                UNWIND allRelationships AS rel
                MATCH (start)-[r]->(end)
                WHERE r.bodyRelationId = rel.relationId
                RETURN start,r,end'''
        records = self._run(tmp_cypher)
        return records
        
    '''
        最小子图查询：
        @para node_list:节点ID列表
    '''
    def minimal_graph_query(self,node_list):
        cypher = f'''MATCH (start)
            WHERE start.nodeId IN {node_list}
            MATCH (allowlist)
            WHERE allowlist.nodeId IN node_list
            WITH start, collect(allowlist) AS allowlistNodes
            CALL apoc.path.subgraphAll(start, {{
                relationshipFilter: "belong_to|is_instance",
                labelFilter: "+body|instance",
                minLevel: 1,
                whitelistNodes: allowlistNodes
            }})
            YIELD nodes, relationships
            return nodes,relationships
            '''

if __name__ == "__main__":
    q = Query()
    res = q.outer_ins_relation_query("64e309dff53c3715653685cb/64e30a69f53c3715653685eb","64e309dff53c3715653685cb/64e30a69f53c3715653685f0")
    print(res)