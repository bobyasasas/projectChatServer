import subprocess
import time


def import_data():
    host = "localhost"
    database = "chat"
    username = "chat"
    password = "EmzPYQEXDwp7S4pd"
    table = "users"
    target_dir = "/sqoop/backup/"+time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())

    # 构造Sqoop命令
    sqoop_cmd = f"/usr/local/sqoop/sqoop/bin/sqoop import \
                --connect jdbc:mysql://{host}:3307/{database} \
                --username {username} \
                --password {password} \
                --table {table} \
                --target-dir {target_dir} \
                --bindir /usr/local/sqoop/sqoop"
    print(sqoop_cmd)

    try:
        # 执行Sqoop命令
        subprocess.run(sqoop_cmd, shell=True, check=True)
        print("数据导入成功！")
    except subprocess.CalledProcessError as e:
        print("数据导入失败：", e)
