# from neo4j import GraphDatabase
# import os
# from dotenv import load_dotenv

# path = os.path.join(os.getcwd(),'.env')
# load_dotenv(path)
# NEO4J_USER = os.environ.get("NEO4J_USER")
# NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")

# #资料包图谱neo4j配置信息
# NEO4J_USER_EX = os.environ.get("NEO4J_USER_EX")
# NEO4J_PASSWORD_EX = os.environ.get("NEO4J_PASSWORD_EX")

# class PackageQuery():
#     def __init__(self) -> None:
#         URI = "bolt://10.215.28.242:7688"
#         AUTH = (NEO4J_USER_EX, NEO4J_PASSWORD_EX)

#         with GraphDatabase.driver(URI, auth=AUTH) as self.driver:
#             self.driver.verify_connectivity()
    
#     def _run(self,cypher):
#         records,summary,keys = self.driver.execute_query(
#                 cypher,
#                 database_="neo4j",
#             )            
#         return list(records)

#     def find_file(self,nodeId):
#         cypher = f'''MATCH (start:instance)
#                     WHERE start.nodeId= "{nodeId}"
#                     MATCH (end:instance)
#                     CALL apoc.path.expandConfig(start, {{
#                         relationshipFilter: ">package_search,>search_file",
#                         labelFilter: "+instance",
#                         minLevel: 2,
#                         maxLevel: 2,
#                         endNodes: [end]
#                     }})
#                     YIELD path
#                     RETURN nodes(path) AS nodes,relationships(path) AS relations,length(path) AS hops
#                     ORDER BY hops'''
                    
#         return self._run(cypher)
