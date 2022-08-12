from flask import Flask,request,jsonify
# from flask_mysqldb import MySQL,MySQLdb
# from flask_restful import Api, Resource
import waitress
from waitress import serve
import mysql.connector
import json
from main import flutch_table_updater
search_keyword=''

app = Flask(__name__)

# app.config['MYSQL_HOST']= 'ls-2d68fc7fe891f66db76054864a0b99dbae55e9b5.cxktmwagp6v8.us-east-1.rds.amazonaws.com'
# app.config['MYSQL_USER']= "dbmasteruser"
# app.config['MYSQL_PASSWORD']= "baron321E!"
# app.config['MYSQL_DB']= "flutchdatabase"
# app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
# mysql = MySQL(app)

try:
    db = mysql.connector.connect(
        host = 'ls-2d68fc7fe891f66db76054864a0b99dbae55e9b5.cxktmwagp6v8.us-east-1.rds.amazonaws.com',
        user = "dbmasteruser",
        passwd = "baron321E!",
        database = "flutchdatabase"

    # flutchDatabase

    )
    mycursor = db.cursor()
except Exception as err:
        print("No internet connection.")
        # logger.exception(err)

mycursor = db.cursor()

# api = Api(app)
# class Connector(Resource):
#     def get(self):
#         return {'data':'hello'}

# api.add_resource(Connector, "/")

@app.route('/')
def index():
    return 'Hello!'
    # return render_template(index.html)


@app.route('/main_table/', methods=["GET"])
def fetch_flutch_table():

    mycursor = db.cursor()
    mycursor.execute("SELECT * FROM flutch_table")
    temp= mycursor.fetchall()
    final_temp = [i[0] for i in temp]
    list1= []
    content= {}
    for result in temp:
        content= {'id': result[0], 'Channel name': result[2], 'Link': result[3], 'Language': result[4], 'Category': result[5]}
        list1.append(content)
        content= {}
    return jsonify(list1)
# ,methods = ['POST']

@app.route('/alter_main_table/', methods=["GET", "POST"])
def alter_flutch_table():
    if request.method == "GET":
        "1"
        # return redirect(url_for('main_table'))
    if request.method == "POST":
        received_data = request.form['search_keyword']






@app.route('/yt_statistics/',  methods=["GET"])
def stats():
    # cur  = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    mycursor = db.cursor()
    mycursor.execute("SELECT * FROM flutch_yt_historic_data")
    temp= mycursor.fetchall()

    list1= []
    content= {}
    for result in temp:
        content= {'id': result[1], 'Date': result[2], 'Subscribers': result[3], 'Avg Views': result[4], 'Avg Likes': result[5],'Avg Comments': result[6], 'Engagement rate': result[7]}
        list1.append(content)
        content= {}
    return jsonify(list1)



@app.route('/cost_table/',  methods=["GET"])
def cost_details():
    # cur  = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    mycursor = db.cursor()
    mycursor.execute("SELECT * FROM flutch_pricings")
    temp= mycursor.fetchall()
    final_temp = [i[0] for i in temp]
    list1= []
    content= {}
    for result in temp:
        content= {'id': result[0], 'Date': result[1], 'Cost per Integration': result[2], 'Cost per dedicated': result[3], 'CPV integration': result[4],'CPV dedicated': result[5]}
        list1.append(content)
        content= {}
    return jsonify(list1)


#
# @app.route('/search_keyword',methods = ['POST'])
# def get_search_keyword():
#     keyword = {'word': request.json['word']}
#     # print(request.json['name'])
#     global search_keyword
#     search_keyword= keyword['word']
#     return jsonify({'keyword':keyword})

# print(search_keyword)

# {'data':'hello'}



@app.route('/table_updater', methods=["GET", "POST"])
def table_updater():
    print("users endpoint reached...")
    # if request.method == "GET":
    #     with open("users.json", "r") as f:
    #         data = json.load(f)
    #         data.append({
    #             "username": "user4",
    #             "pets": ["hamster"]
    #         })
    #
    #         return Flask.jsonify(data)
    if request.method == "POST":
        received_data = request.get_json()
        print(f"received data: {received_data}")
        # received_data = request.form['search_keyword']
        message = received_data['data']
        flutch_table_updater(message)
        return_data = {
            "status": "success",
            "message": f"received: {message}"
        }
        return "Hello"
        # return Flask.Response(response=json.dumps(return_data), status=201)
        # # return redirect(url_for('main_table'))


@app.route('/profile_search/', methods=["GET","POST"])
def profile_search():
    if request.method == "POST":
        received_data = request.get_json()
        # input should be float
        message = received_data['data']

        # received_data = request.form['Enter minimum average view requirement']
        mycursor.execute("SELECT user_id FROM flutch_yt_historic_data where Avg_Views > message")
        temp = mycursor.fetchall()
        id_list = [i[0] for i in temp]
        list1 = []
        content = {}
        for id in id_list:
            mycursor.execute("SELECT * FROM flutch_table where id_bin = id")
            result1 = mycursor.fetchall()
            content = {'id': result1[0], 'Channel name': result1[2], 'Link': result1[3], 'Language': result1[4],
                       'Category': result1[5]}
            list1.append(content)
            content = {}
        return jsonify(list1)
# 44.207.129.53
# 172.26.7.242
#
# if __name__ == "__main__":
# serve(app, host='44.207.129.53', port=80)
#     app.run()
#     print(1)
try:
    app.run(host='0.0.0.0', port=8080,debug =True)
except:
    print('unable to open port')
