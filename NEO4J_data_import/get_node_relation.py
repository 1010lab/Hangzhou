import requests
import json
import os
import csv

NO_RELATION = 10
HAVA_RELATION = 11


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
        #解析字典
        self.parse_dict = {'getBodyNodeAndRelation':'Body',
                           'getInstanceNodeAndRelation':'Instance'}


    def _save_csv(self,data,type,csv_type):
        #通过调用函数的type来分别存储BODY文件和INSTANCE文件
        save = os.path.join(self.data_dir,csv_type)
        if not os.path.exists(save):
            os.makedirs(save)
        with open(os.path.join(save,type+".csv"), "w", newline="", encoding="utf-8") as csv_file:
            all_keys = set().union(*(d.keys() for d in data))
            writer = csv.DictWriter(csv_file, fieldnames=all_keys)
            # 写入CSV文件的表头
            writer.writeheader()
            # 逐行写入数据
            for row in data:
                writer.writerow(row)

    def reponse(self,type):
        #站点数据接口
        url = 'http://10.215.28.201/deep/site/Knowledge/' + type
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
            "Authorization" : "eyJhbGciOiJIUzUxMiIsInppcCI6IkRFRiJ9.eNocjE0LwjAQRP_Lnlsw2WyS9ih4EBQv3sRD02ylfrShqaKI_92lMKeZN-8L-RmgBmtadBg6jKvoW8ONwUiaFRTQNzPUylbKkrWOpMhZHpE5lfN446HMPL14EvQ697J0hBiUjuRjMI5a79j7wB3aSkJRQH6nxakr0osz8fQQ6-lcwH289MPxk1hU-8N6u9vA7w8AAP__.2XqU1n5t52Z_yCTu3ntj5gxUj7JJaZxYORz24XamSoMnF7p4ht1_yKPadEvCrkaELvzCEewRPFk1EvJuCUCKxw",
            "Content-Type" : "application/json"
        }
        # 发送 POST 请求
        response = requests.post(url, data=payload, headers=headers)
        assert  response.status_code== 200,f"接受返回码错误:{response.status_code}"
        # 获取响应结果
        result = response.json()
      
        # 处理响应结果，存为json文件
        type = self.parse_dict[type]
        save_path = os.path.join(self.data_dir,type+"_data.json")
        json_result = json.dumps(result,indent=4,ensure_ascii=False)
        if not os.path.exists(save_path):
            f = open(save_path,'w',encoding='utf-8')
            f.write(json_result)
        return result

    def save2csv(self,type):
        result = self.reponse(type)
        #获取JSON数据中的siteNodeList
        node_list = result["result"]["siteNodeList"]
        #获取JSON数据中的snRelationList
        relation_list = result["result"]["snRelationList"]
        type = self.parse_dict[type]
        #若站点不存在siteNodeList或siteNodeList为空列表，则抛出assert异常
        assert node_list is not None or node_list != [],'未发现该站点下需导入的Node节点'
        #保存NODE数据至CSV
        self._save_csv(node_list,type,'nodes')
        if relation_list is None or relation_list == []:
            print(f'WARNINNG:{self.args["siteID"]}站点中节点不存在关系')
            return NO_RELATION
        else:
            self._save_csv(relation_list,type,'relations')
            return HAVA_RELATION
