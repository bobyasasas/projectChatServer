import hashlib
import pymysql.cursors
from datetime import datetime

from pymysql import Error


class Mydb:

    def __init__(self):
        self.db = pymysql.connect(host='119.188.240.140',
                                  user='chat',
                                  password='EmzPYQEXDwp7S4pd',
                                  database='chat',
                                  port=33077,
                                  charset='utf8')

    def add_user(self, username, passwd):
        with self.db.cursor() as cursor:
            check_sql = "SELECT * FROM users WHERE username = %s"
            cursor.execute(check_sql, (username,))
            check_result = cursor.fetchall()
            if len(check_result) == 0:
                passwd_sha = hashlib.sha256(passwd.encode('utf-8')).hexdigest()
                time = datetime.now().timestamp()
                sql = "INSERT INTO users(username, passwd, last_login) VALUES (%s, %s, %s)"
                cursor.execute(sql, (username, passwd_sha, time))
                self.db.commit()
                print("用户", username, "注册成功!")
                return 1
            else:
                print("该账号已经注册过了!")
                return 0

    def check_passwd(self, username, passwd):
        with self.db.cursor() as cursor:
            check_sql = "SELECT * FROM users WHERE username = %s"
            cursor.execute(check_sql, (username,))
            check_result = cursor.fetchone()  # 使用 fetchone 而不是 fetchall，因为我们只需要第一个结果
            if check_result:  # 如果找到了匹配的用户
                stored_passwd_hash = check_result[2]  # 假设密码哈希在查询结果的某个列中，你需要用正确的列索引替换它
                given_passwd_hash = hashlib.sha256(passwd.encode('utf-8')).hexdigest()
                if stored_passwd_hash == given_passwd_hash:
                    return True
            return False

    def add_contact(self, username, username_contact):
        try:
            with self.db.cursor() as cursor:
                # 检查是否存在相同的记录
                check_sql = ("SELECT 1 FROM contacts WHERE (username=%s AND username_contact=%s) OR (username=%s AND "
                             "username_contact=%s)")
                cursor.execute(check_sql, (username, username_contact, username_contact, username))
                if cursor.fetchone() is None:
                    # 不存在相同的记录，执行插入操作
                    insert_sql = "INSERT INTO contacts(username, username_contact) VALUES (%s, %s)"
                    cursor.execute(insert_sql, (username, username_contact))
                    self.db.commit()
                    # 如果需要双向插入（请谨慎处理）
                    cursor.execute(insert_sql, (username_contact, username))

                    # 提交事务以确保更改被保存
                    self.db.commit()
                    print("Contact added successfully.")
                    return True
                else:
                    print("Contact already exists.")
                    return False
        except (Exception, self.db.Error) as error:
            # 打印出错误信息，并在需要时回滚事务
            print("Error while connecting to MySQL", error)
            if self.db:
                self.db.rollback()  # 回滚所有未提交的更改
            return False

    def get_contact(self, username):
        try:
            with self.db.cursor() as cursor:
                # 查询语句，找到指定用户的所有联系人
                query = "SELECT username_contact FROM contacts WHERE username = %s"
                cursor.execute(query, (username,))
                contacts = cursor.fetchall()
                contact_list = [contact[0] for contact in contacts]
                if not contact_list:
                    return []
                self.db.commit()
                return contact_list

        except Error as e:
            print(f"Error while executing query: {e}")
            return False

    def upload(self, filename, filesize):
        units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
        power = 0
        if filesize == 0:
            return f"0{units[0]}"

            # 计算文件大小并选择合适的单位
        while filesize > 1024:
            filesize /= 1024.0
            power += 1

        try:
            with self.db.cursor() as cursor:
                insert_sql = "INSERT INTO files(file_name, file_size) VALUES (%s, %s)"
                cursor.execute(insert_sql, (filename, f"{filesize:.2f}{units[power]}"))
                self.db.commit()
                print("file upload successfully.")
                return True
        except Error as e:
            print(f"Error while executing query: {e}")
            return False

    def get_files(self):
        try:
            with self.db.cursor() as cursor:
                # 查询语句，找到指定用户的所有联系人
                query = "SELECT * FROM files"
                cursor.execute(query)
                files = cursor.fetchall()
                if not files:
                    return []
                self.db.commit()
                return files

        except Error as e:
            print(f"Error while executing query: {e}")
            return False

    def ans(self, username, contact):
        # 获取当前时间
        now = datetime.now()

        # 获取当前的年份、月份、日子、小时，并将它们转换为整数类型
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour
        try:
            with self.db.cursor() as cursor:
                insert_sql = "INSERT INTO ans(username, contact,year,month,day,hour) VALUES (%s, %s,%s,%s,%s,%s)"
                cursor.execute(insert_sql, (username, contact, year, month, day, hour))
                self.db.commit()
                print("ans add successfully.")
                return True
        except Error as e:
            print(f"Error while executing query: {e}")
            return False

    def close(self):
        self.db.close()
