from py2neo import Graph

class Query():
    def __init__(self) -> None:
        self.graph = Graph("http://localhost:7474/", auth=("neo4j", "lml2000326"),name = "neo4j")

    #基于固定属性查询所有结点
    def by_attribute_query(self,attributeKey,attributeValue,type=['body','instance']):
        pass

    #个数统计查询
    def count_query(self,label):
        cypher =  '''MATCH (n:{label}) RETURN  count(n) AS count'''.format(label = label)
        return self.graph.run(cypher).data()[0]

    #一跳关系查询，不区分body和instance
    def one_hop_query(self,nodeId):
        cypher =  '''MATCH (startNode)-[r]->(endNode)'''+\
                '''WHERE startNode.nodeId = '{startNodeId}' '''.format(startNodeId = nodeId)+\
                '''RETURN startNode, r, endNode'''
        return self.graph.run(cypher).data()[0]

    #三跳关系查询，不区分body和instance
    def three_hop_query(self,startNodeId):
        cypher =  '''MATCH (startNode)-[*1..3]->(endNode)''' +\
                '''WHERE startNode.nodeId =  '{startNodeId}' '''.format(startNodeId=startNodeId) +\
                '''RETURN startNode, endNode'''
        return self.graph.run(cypher).data()[0]


    def shortest_path_query(self,startNodeId,endNodeId):
        cypher =  '''MATCH (startNode {{nodeId: '{startNodeId}'}}), (endNode {{nodeId: '{endNodeId}'}})'''.format(startNodeId=startNodeId,endNodeId=endNodeId)+\
                '''MATCH shortestPath = shortestPath((startNode)-[*]-(endNode)) '''+\
                '''RETURN shortestPath'''
        return self.graph.run(cypher).data()[0]

