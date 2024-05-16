import json
from datetime import timedelta

from flask import Flask, request, session

import myutil.mysql

app = Flask(__name__)
app.config['SECRET_KEY'] = "wk6666"
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)

BASE_URL = '/chat/'


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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
