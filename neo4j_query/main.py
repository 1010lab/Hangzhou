from query import Query


q = Query()
print(q.count_query('instance'))

print(q.by_attribute_query('type','自建'))