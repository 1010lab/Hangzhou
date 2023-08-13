from query import Query


q = Query()
print(q.count_query('instance'))
res = q.one_hop_query("64c376111cce3d54abc0ee8d")
for key,value in res.items(): 
    print(key,value)
print(q.shortest_path_query('64c37e691cce3d54abc0f2fd','64c37e3f1cce3d54abc0f2d9'))
print(q.three_hop_query('64c37e3f1cce3d54abc0f2d9'))