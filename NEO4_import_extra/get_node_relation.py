import requests
import json
import os
import csv


'''
    获取接口数据并保存为csv文件
'''
class GetNodeRelation():
    def __init__(self,args) -> None:
        self.args = args
        #用于存储站点转CSV的文件目录
        self.data_dir = os.path.join(args["DATA_PATH"],args["siteID"])
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
       


    def _save_csv(self,data,csv_type):
        #通过调用函数的type来分别存储BODY文件和INSTANCE文件
        save = os.path.join(self.data_dir,csv_type)
        if not os.path.exists(save):
            os.makedirs(save)
        with open(os.path.join(save,"instance.csv"), "w", newline="", encoding="utf-8") as csv_file:
            all_keys = set().union(*(d.keys() for d in data))
            writer = csv.DictWriter(csv_file, fieldnames=all_keys)
            # 写入CSV文件的表头
            writer.writeheader()
            # 逐行写入数据
            for row in data:
                writer.writerow(row)

    def reponse(self):
          #站点数据接口
        url = 'http://10.215.28.242:3032/sfm/structuralData/getDocPackageRelation'
        # url = "https://deepctest.hdec.com/deep/site/Knowledge/" + type
        # 构建请求的参数
        data = {
            "siteId": self.args["siteID"]
        }
        # 将参数转换为 JSON 格式
        payload = json.dumps(data)

        # 设置请求的头部信息
        headers = {
            "Request-Origion" : "SwaggerBootstrapUi",
            "accept" : "*/*",
            "Authorization" : "6",
            "Content-Type" : "application/json"
        }
        response = requests.post(url, data=payload, headers=headers)
        print(response.text)
        assert  response.status_code== 200,f"接受返回码错误:{response.status_code}"
        # 获取响应结果
        result = response.json()
        print(result)
    
        save_path = os.path.join(self.data_dir,"instance.json")
        json_result = json.dumps(result,indent=4,ensure_ascii=False)
        # if not os.path.exists(save_path):
        f = open(save_path,'w',encoding='utf-8')
        f.write(json_result)
        return result

    def save2csv(self):
        result = self.reponse()
        #获取JSON数据中的siteNodeList
        node_list = result["result"]["siteNodeList"]
        #获取JSON数据中的snRelationList
        relation_list = result["result"]["snRelationList"]
        #若站点不存在siteNodeList或siteNodeList为空列表，则抛出assert异常
        assert node_list is not None or node_list != [],'未发现该站点下需导入的Node节点'
        #保存NODE数据至CSV
        self._save_csv(node_list,'nodes')
        if relation_list is None or relation_list == []:
            print(f'WARNINNG:{self.args["siteID"]}站点中节点不存在关系')
            raise BaseException 
        else:
            self._save_csv(relation_list,'relations')
