import json
import os
import sqlite3
import datetime

import time

import requests
from flask import Flask, request, session, jsonify, send_file

import myutil.mysql
from myutil import sqoop_util

app = Flask(__name__)
app.config['SECRET_KEY'] = "wk6666"
app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(days=7)

BASE_URL = '/chat/'


def get_post(sender, receiver, message):
    flume_http_url = "http://localhost:50020"
    body = f'{sender} send to {receiver} a message : {message}'
    timestamp = time.time()
    data = [{"headers": {"timestamp": timestamp
                         },
             "body": body
             }]
    json_data = json.dumps(data)
    response = requests.post(flume_http_url, data=json_data)

    # 检查响应状态码
    if response.ok:
        print("数据发送成功")
    else:
        print(f"数据发送失败，状态码：{response.status_code}")


def mem_con_add_message(sender, receiver, message):
    get_post(sender, receiver, message)
    cur.execute('insert into message values(?, ?, ?,?)', (sender, receiver, message, datetime.datetime.now()))
    print(sender, " said to ", receiver, " ", message)
    mysql = myutil.mysql.Mydb()
    mysql.ans(sender, receiver)


def mem_con_get_message(sender, receiver):
    cur.execute('select * from message where sender=? and receiver=?', (sender, receiver))
    rows = cur.fetchall()
    cur.execute('delete from message where sender=? and receiver=?', (sender, receiver))
    return rows


def mem_con_check_message(receiver):
    cur.execute('select sender from message where receiver=?', (receiver,))
    names = cur.fetchall()
    unique_names = list(set(names))
    return unique_names


@app.route(BASE_URL + 'post/login', methods=['POST'])
def login():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data["username"]
    passwd = json_data["passwd"]
    mysql = myutil.mysql.Mydb()
    login_result = mysql.add_user(username, passwd)
    if login_result == 0:
        result = {
            'msg': "false"
        }
    else:
        result = {
            'msg': "true"
        }
    session['username'] = username
    session.permanent = True
    result_json = json.dumps(result)
    return result_json


@app.route(BASE_URL + 'post/signin', methods=['POST'])
def signin():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data["username"]
    passwd = json_data["passwd"]
    mysql = myutil.mysql.Mydb()
    if mysql.check_passwd(username, passwd):
        result = {
            'msg': "true"
        }
        session['username'] = username
        session.permanent = True
    else:
        result = {
            'msg': "false"
        }
    return json.dumps(result)


@app.route(BASE_URL + 'post/add_contacts', methods=['POST'])
def add_contact():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data["username"]
    add_name = json_data["add_name"]
    mysql = myutil.mysql.Mydb()
    if mysql.add_contact(username, add_name):
        result = {
            'msg': "true"
        }
    else:
        result = {
            'msg': "false"
        }
    return json.dumps(result)


@app.route(BASE_URL + 'post/get_contacts', methods=['POST'])
def get_contact():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data["username"]
    mysql = myutil.mysql.Mydb()
    result = mysql.get_contact(username)
    json_result = json.dumps(result)
    if not result:
        json_result = json.dumps([])
    return json.dumps(json_result)


@app.route(BASE_URL + 'post/send_message', methods=['POST'])
def send_messages():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data["username"]
    contact_name = json_data["contact_name"]
    message = json_data["message"]
    mem_con_add_message(username, contact_name, message)
    result = {
        "msg": "true"
    }
    return json.dumps(result)


@app.route(BASE_URL + 'post/get_message', methods=['POST'])
def get_messages():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data["username"]
    contact_name = json_data["contact_name"]
    return json.dumps(mem_con_get_message(username, contact_name))


@app.route(BASE_URL + 'post/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'msg': 'error'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'msg': 'error'}), 400

    if file:
        # 设置文件存储路径
        file_size = len(file.read())
        file.seek(0)
        upload_path = os.path.join('file/uploads', file.filename)
        mysql = myutil.mysql.Mydb()
        mysql.upload(file.filename, file_size)
        if not os.path.exists(os.path.dirname(upload_path)):
            os.makedirs(os.path.dirname(upload_path))

        # 存储文件
        file.save(upload_path)
        return jsonify({'msg': 'true'}), 200


@app.route(BASE_URL + 'post/get_files', methods=['POST'])
def get_files():
    mysql = myutil.mysql.Mydb()
    files = mysql.get_files()
    return json.dumps(files)


@app.route('/chat/download/<filename>')
def download_file(filename):
    # 指定文件的存储路径（确保这个路径是安全的）
    file_path = f'./file/uploads/{filename}'
    # 使用 send_file 函数发送文件
    return send_file(file_path, as_attachment=True)


@app.route(BASE_URL + 'post/check_message', methods=['POST'])
def check_messages():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    username = json_data["username"]
    result = mem_con_check_message(username)
    return json.dumps(result)


@app.route(BASE_URL + 'post/sqoop', methods=['POST'])
def sqoop_backup():
    request.get_data()
    sqoop_util.import_data()
    return jsonify({'msg': 'true'}), 200


if __name__ == '__main__':
    con = sqlite3.connect("memory", check_same_thread=False)
    cur = con.cursor()
    cur.execute('create table IF NOT EXISTS message (sender char(64), receiver char(64), time char(64), message char('
                '512));')
    app.run(debug=False, host='0.0.0.0', port=22255)
