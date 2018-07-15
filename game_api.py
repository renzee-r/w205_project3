#!/usr/bin/env python
import json
import redis
from kafka import KafkaProducer
from flask import Flask, request
from datetime import datetime

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')
r = redis.Redis(host='redis', port='6379')

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

def validate_session_login(username, event):
    if username is None:
        event.update({'return_code': '1'})
        return 1

    user_info_raw = r.get(username)

    if user_info_raw is None:
        event.update({'return_code': '1'})
        return 2

    user_info = json.loads(user_info_raw)

    if user_info['session_datetime'] is None:
        event.update({'return_code': '1'})
        return 4

    last_session_datetime = datetime.strptime(user_info['session_datetime'], "%Y-%m-%d %H:%M:%S")

    if abs((datetime.now() - last_session_datetime).seconds) > 7200:
        event.update({'return_code': '1'})
        return 3

    if user_info['session_host'] != request.headers['Host']:
        event.update({'return_code': '1'})
        return 4


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/buy_sword")
def buy_sword():
    buy_sword_event = {'event_type': 'buy_sword'}
    vsl_result = validate_session_login(request.args.get('username'), buy_sword_event)

    if vsl_result == 1:
        return "Action Failed! Please provide a username.\n"
    if vsl_result == 2:
        return "Action Failed! This username does not exist. Please signup.\n"
    if vsl_result == 3:
        return "Action Failed! Your session has expired. Please login.\n"
    if vsl_result == 4:
        return "Action Failed! You do not have an active session on this machine. Please login.\n"

    log_to_kafka('events', buy_sword_event)
    return "Bought a Sword!\n"


@app.route("/login")
def login():
    login_event = {'event_type': 'login'}

    username = request.args.get('username')
    password = request.args.get('password')

    if username is None:
        login_event.update({'return_code': '1'})
        return "Login Failed! Please provide a username.\n"

    if password is None:
        login_event.update({'return_code': '1'})
        return "Login Failed! Please provide a password.\n"

    user_info_raw = r.get(username)

    if user_info_raw is None:
        login_event.update({'return_code': '1'})
        return "Login Failed! This username does not exist. Please signup.\n"

    user_info = json.loads(user_info_raw)

    if user_info['password'] != password:
        login_event.update({'return_code': '1'})
        return "Login Failed! This password is incorrect.\n"

    user_info['session_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_info['session_host'] = request.headers['Host']
    r.set(username, json.dumps(user_info))

    login_event.update({'return_code': '0'})
    log_to_kafka('events', login_event)
    return "Login successful! Welcome " + username + ".\n"


@app.route("/signup")
def signup():
    signup_event = {'event_type': 'signup'}

    username = request.args.get('username')
    password = request.args.get('password')

    if username is None:
        signup_event.update({'return_code': '1'})
        return "Signup Failed! Please provide a username.\n"

    if password is None:
        signup_event.update({'return_code': '1'})
        return "Signup Failed! Please provide a password.\n"

    user_info_raw = r.get(username)

    if user_info_raw is not None:
        signup_event.update({'return_code': '1'})
        return "Signup Failed! This username already exists.\n"

    new_user_info = {}
    new_user_info['username'] = username
    new_user_info['password'] = password
    new_user_info['session_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_user_info['session_host'] = request.headers['Host']
    r.set(username, json.dumps(new_user_info))

    log_to_kafka('events', signup_event)
    return "Signup successful! Welcome " + username + ".\n"