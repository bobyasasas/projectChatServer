import os
import json
import pymysql

base_path = os.path.dirname(os.path.abspath(__file__))

shell_str = """
#!/bin/bash

mysql_jdbc="jdbc:mysql://{0}:{1}/{2}?characterEncoding=UTF-8&serverTimezone=Asia/Shanghai"
mysql_user="{3}"
# mysql_passwd=""

sqoop import -D mapreduce.map.memory.mb={6} \
--connect $mysql_jdbc \
--username $mysql_user \
--password '{4}' \
--table {5} \
--target-dir {7} \
--delete-target-dir \
--num-mappers {8} \
--as-parquetfile
"""


# 获取配置文件，配置文件放在该python脚本的同级目录的config文件下
def get_config(file_name):
    with open(os.path.join(base_path, 'config/{}'.format(file_name))) as file:
        conf = json.load(file)
    return conf


# 获取mysql连接
def get_mysql_connect(host, port, user, password, db):
    conn = pymysql.connect(
        host=host, port=port,
        user=user, password=password,
        db=db, charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn


# 通过mysql元数据获取所有子表，这里子表指表名前缀相同的表。
def get_mysql_table(host, port, user, password, db):
    # 获取mysql连接
    conn = get_mysql_connect(host, port, user, password, db)
    # 查询语句，这里是查询表前缀为‘tb_test’的表
    query = "select table_schema,table_name from information_schema.tables where substring(table_name,1,7) = 'tb_test'"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            return data
    except Exception as e:
        raise e
    finally:
        conn.commit()
        conn.close()


def get_shell_script(file_name):
    # 获取配置文件
    conf = get_config(file_name)
    # 获取配置文件中mysql的host、port、user、password
    host = conf["mysql"]["host"]
    port = conf["mysql"]["port"]
    user = conf["mysql"]["user"]
    password = conf["mysql"]["password"]
    # 获取配置文件中sqoop脚本相关参数
    map_memory = conf["map_memory"]
    target_dir = conf["target_dir"]
    num_mappers = conf["num_mappers"]

    # 获取目标表列表
    data = get_mysql_table(host, port, user, passport, "information_schema")

    for db_table in data:
        t_db = db_table.get("table_schema")
        t_table = db_table.get("table_name")
        t_target_dir = target_dir + t_table

        shell_script = shell_str.format(host, port, t_db, user, passport, t_table, map_memory, t_target_dir,
                                        num_mappers)
        # 通过python调sqoop脚本
        subprocess.run(shell_script, shell=True)

        time.sleep(10)


if __name__ == '__main__':
    config_name = 'sqoop_sync.json'
    get_shell_script(config_name)