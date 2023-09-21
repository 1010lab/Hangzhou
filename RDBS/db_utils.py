import mysql.connector
import os
from dotenv import load_dotenv
#加载配置文件
path = os.path.join(os.getcwd(),'.env')
load_dotenv(path)
mysql_user = os.environ.get("MYSQL_USER")
mysql_pwd = os.environ.get("MYSQL_PASSWORD")


#加载mysql
class Mysql():
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Mysql, cls).__new__(cls)
            cls._instance.config()
        return cls._instance

    def config(self):
        self.db = mysql.connector.connect(
        host="localhost",
        user=mysql_user,
        password=mysql_pwd,
        database="hangzhou"
        )
        self.cursor = self.db.cursor()   
   
    def create_import_table(self):
        sql = f'''CREATE TABLE IF NOT EXISTS import_info(
        `id` int NOT NULL AUTO_INCREMENT,
        `siteID` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '导入站点ID',
        `import_time` datetime DEFAULT NULL,
        `import_user` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE KEY `siteID` (`siteID`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;'''
        self.cursor.execute(sql)

    def insert(self,*args):
        sql = f'''INSERT INTO `hangzhou`.`import_info` ( `siteID`, `import_time`, `import_user`) VALUES (%s, %s, %s)'''
         # 检查参数是否包含足够的元素
        if len(args) != 3:
            print("Invalid number of arguments. You should provide values for siteID, import_time, and import_user.")
            return
        try:
            self.cursor.execute(sql, args)
            self.db.commit()  # 提交更改
            print("Data inserted successfully.")
        except Exception as e:
            print(f"Error inserting data: {e}")

    def delete(self,*arg):
        sql =f'''DELETE FROM `hangzhou`.`import_info` WHERE `siteID` = %s'''
        try:
            self.cursor.execute(sql,arg)
            self.db.commit()  # 提交更改
            print("Data deleted successfully.")
        except Exception as e:
            print(f"Error delete data: {e}")

