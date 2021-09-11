from flask import Flask, request, abort, session, jsonify, send_file, redirect, Response
from flask.templating import render_template
from kafka import KafkaProducer
import time
import json
import sys
import os
from kafka.admin import KafkaAdminClient, NewTopic


KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "parking"

class KafkaConnector(object):

    def __init__(self, kafka_broker, kafka_topic):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic

        # admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker, client_id='test')
        # topic_list = []
        # topic_list.append(NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1))
        # admin_client.create_topics(new_topics=topic_list, validate_only=False)

        
        try:
            self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        except Exception as e:
            print(f"ERROR --> {e}")                                                                                             
            # sys.exit(1)  




def create_app():
    app = Flask(__name__)


    kafka_connector = KafkaConnector(KAFKA_BROKER, KAFKA_TOPIC)

    @app.route('/', methods=["GET"])
    def home():
        month_options = ""
        for month in range(1, 13):
            month_options += f'<option value="{month}">'

        month = """
        <label> Choose Month: 
        <input list="month" name="month" /> </label>
        <datalist id="month">
        {options}
        </datalist>
        """.format(options=month_options)

        date_options = ""
        for date in range(1, 32):
            date_options += f'<option value="{date}">'

        date = """
        <label> Choose Date: 
        <input list="date" name="date" /> </label>
        <datalist id="date">
        {options}
        </datalist>
        """.format(options=date_options)

        hour_options = ""
        for hour in range(0, 24):
            hour_options += f'<option value="{hour}">'

        hour = """
        <label> Choose Hour: 
        <input list="hour" name="hour" /> </label>
        <datalist id="hour">
        {options}
        </datalist>
        """.format(options=hour_options)

        type_options = ""
        with open("/app/type_distinct.csv") as type_content:
            for type_detail in type_content.readlines():
                type_options += f'<option value="{type_detail.strip()}">'

        type_list = """
        <label> Choose Front In or Opposite: 
        <input list="type" name="type" /> </label>
        <datalist id="type">
        {options}
        </datalist>
        """.format(options=type_options)

        location_options = ""
        with open("/app/location_distinct.csv") as location_content:
            for location in location_content.readlines():
                location_options += f'<option value="{location.strip()}">'

        location_list = """
        <label> Choose Location: 
        <input list="location" name="location" /> </label>
        <datalist id="location">
        {options}
        </datalist>
        """.format(options=location_options)

        county_options = ""
        with open("/app/county_distinct.csv") as county_content:
            for county in county_content.readlines():
                county_options += f'<option value="{county.strip()}">'

        county_list = """
        <label> Choose County: 
        <input list="county" name="county" /> </label>
        <datalist id="county">
        {options}
        </datalist>
        """.format(options=county_options)

        code_options = ""
        with open("/app/code_distinct.csv") as code_content:
            for code in code_content.readlines():
                code_options += f'<option value="{code.strip()}">'

        code_list = """
        <label> Choose Violation Code: 
        <input list="code" name="code" /> </label>
        <datalist id="code">
        {options}
        </datalist>
        """.format(options=code_options)
        

        return render_template('index.html', message="THis is a message", month_list=month,
                                date_list=date, hour_list=hour, type_list=type_list, location_list=location_list,
                                county_list=county_list, code_list=code_list)


    @app.route('/predict', methods=["POST"])  
    def predict():
        month = request.form["month"]
        date = request.form["date"]
        hour = request.form["hour"]
        type1 = request.form["type"]
        location = request.form["location"]
        county = request.form["county"]
        code = request.form["code"]

        # Modify this as per requirement
        feature_dict = {'month': month, 'hour': hour, 'date': date, 'hour': hour, "type": type1, "location": location, "county": county, "code": code}
        feature_json = json.dumps(feature_dict)

        kafka_connector.kafka_producer.send(KAFKA_TOPIC, bytes(feature_json, encoding="utf8"))

        return feature_json
    
    return app



if __name__ == "__main__":
    time.sleep(20)
    app = create_app()
    app.run(host='0.0.0.0',port=4000, debug=True)
    # app.run(host='0.0.0.0',port=4000, debug=True)
