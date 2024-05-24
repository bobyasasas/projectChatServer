import hashlib
import pymysql.cursors
from datetime import datetime


class Mydb:

    def __init__(self):
        self.db = pymysql.connect(host='154.204.60.18',
                                  user='chat_user',
                                  password='6JE4Rs6GXwk57GGa',
                                  database='chat_user',
                                  port=3307,
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
        with self.db.cursor() as cursor:
            sql = "INSERT INTO contacts(username, username_contact) VALUES (%s, %s)"
            cursor.execute(sql, (username, username_contact))
            cursor.execute(sql, (username_contact, username))

    def close(self):
        self.db.close()
