from py2neo import Graph
import time
from neo4j import GraphDatabase

class Query():
    def __init__(self) -> None:
        URI = "neo4j://localhost"
        AUTH = ("neo4j", "123")

        with GraphDatabase.driver(URI, auth=AUTH) as self.driver:
            self.driver.verify_connectivity()

    #本体/实体图查询(一个站点)
    def graph_query(self,label):
        if label:
            cypher = f'''MATCH (start:{label})
                        OPTIONAL MATCH (start:{label})-[r:belong_to]->(end)
                        RETURN start, r, end 
                        '''
        #若label为None,则查询所有节点
        else:
            cypher = f'''MATCH (start)
                        WHERE start:body OR start:instance
                        OPTIONAL MATCH (start)-[r:belong_to]->(end)
                        RETURN start, r, end '''

        records,summary,keys = self.driver.execute_query(
                cypher,
                database_="neo4j",
            )               
        return list(records)

    def tree_query(self,label,treeId):
        if label:
            cypher = f'''MATCH (start:{label})-[r:belong_to]->(end:{label})
                        WHERE "{treeId}"  IN start.virtualTreeList AND "{treeId}" in end.virtualTreeList
                        RETURN start,r,end'''
        else:
            cypher = f'''MATCH (start)-[r:belong_to]->(end)
                        WHERE "{treeId}"  IN start.virtualTreeList AND "{treeId}" in end.virtualTreeList
                        And start:body OR start:instance
                        RETURN start,r,end'''

        records,summary,keys = self.driver.execute_query(
                cypher,
                database_="neo4j",
            )               
        return list(records)
    
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
    def count_query(self,label):
        cypher =  '''MATCH (n:{label}) RETURN  count(n) AS count'''.format(label = label)
        if isinstance(label,list):
            cypher =  '''MATCH (n) RETURN  count(n) AS count'''   
        return self.graph.run(cypher).data()[0]

    #body/instance一跳关系查询
    def one_hop_query(self,nodeId,label):
        cypher =  f'''MATCH (startNode{":"+label if label else ""})-[r]->(endNode)
                      WHERE startNode.nodeId = '{nodeId}' 
                      RETURN startNode, r, endNode'''
        return self.graph.run(cypher).data()

    #三跳关系查询，不区分body和instance
    def three_hop_query(self,nodeId,label):
        cypher =  f'''MATCH (startNode{":"+label if label else ""})-[*1..3]->(endNode)
                    WHERE startNode.nodeId =  '{nodeId}' 
                    RETURN startNode, endNode'''
        return self.graph.run(cypher).data()


    def shortest_path_query(self,startNodeId,endNodeId):
        cypher = '''MATCH (startNode {{nodeId: '{startNodeId}'}}), (endNode {{nodeId: '{endNodeId}'}})'''.format(startNodeId=startNodeId,endNodeId=endNodeId)+\
                '''MATCH shortestPath = shortestPath((startNode)-[*]-(endNode)) '''+\
                '''RETURN shortestPath'''
        return self.graph.run(cypher).data()[0]

