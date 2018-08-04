#!/usr/bin/env python
import json
import redis
from kafka import KafkaProducer
from flask import Flask, request
from datetime import datetime
from random import randint

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')
r = redis.Redis(host='redis', port='6379')

valid_items = ['sword', 'potion', 'shield']
used_session_ids = []

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
        return 1

    user_info = json.loads(user_info_raw)

    if user_info['session_datetime'] is None:
        event.update({'return_code': '1'})
        return 1

    last_session_datetime = datetime.strptime(user_info['session_datetime'], "%Y-%m-%d %H:%M:%S")

    if abs((datetime.now() - last_session_datetime).seconds) > 7200:
        event.update({'return_code': '1'})
        return 1


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)

    return "This is the default response!\n"


@app.route("/buy/<item>")
def buy_item(item):
    buy_item_event = {'event_type': 'buy_item'}

    session_id = request.cookies.get('session_id')
    username = r.get(session_id)

    # Validate the username and session
    vsl_result = validate_session_login(username, buy_item_event)
    if vsl_result == 1:
        return "Action Failed! You are not logged in. Please signup or login.\n"

    # If username and session is valid, update the event with the username
    buy_item_event.update({'username': username})

    # Validate if the item is valid
    if item not in valid_items:
        return 'The store does not have that item in stock.'

    # If item is valid, update the event with the item. Also
    # update the user's inventory.
    buy_item_event.update({'item_type': item})
    user_info = json.loads(r.get(username))
    user_info['inventory'].append(item)
    r.set(username, json.dumps(user_info))

    # Upadte the vent with the latest inventory info
    buy_item_event.update({'inventory': user_info['inventory']})

    # TODO: Randomly generate the item quality
    buy_item_event.update({'item_quality': 'common'})

    # TODO: Somehow attach the item quality to the items in 
    # user's inventory

    log_to_kafka('events', buy_item_event)
    return 'Bought a ' + item + '!\n'


@app.route("/join_guild")
def join_guild():
    join_guild_event = {'event_type': 'join_guild'}
    vsl_result = validate_session_login(request.args.get('username'), join_guild_event)

    if vsl_result == 1:
        return "Action Failed! Please provide a username.\n"
    if vsl_result == 2:
        return "Action Failed! This username does not exist. Please signup.\n"
    if vsl_result == 3:
        return "Action Failed! Your session has expired. Please login.\n"
    if vsl_result == 4:
        return "Action Failed! You do not have an active session on this machine. Please login.\n"

    join_guild_event.update({'username': request.args.get('username')})
    log_to_kafka('events', join_guild_event)
    return "Joined a Guild!\n"


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

    login_event.update({'user_info': user_info})
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

    # Build our response message
    response = app.make_response('Signup successful! Welcome ' + username + '.\n')
    
    # Generate the session ID. Ensure that the session ID is not already
    # in use
    session_id = randint(10000, 99999)
    while (session_id in used_session_ids):
        session_id = randint(10000, 99999)

    # Attach a cookie containing the session ID to the response
    response.set_cookie('session_id', str(session_id))

    # Insert a redis entry linking the session_id and the username
    r.set(session_id, username)

    # Create new user info dict
    new_user_info = {}
    new_user_info['username'] = username
    new_user_info['password'] = password
    new_user_info['session_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_user_info['session_host'] = request.headers['Host']
    new_user_info['inventory'] = []

    # Insert redis entry for the newly created user
    r.set(username, json.dumps(new_user_info))

    signup_event.update({'user_info': new_user_info})
    signup_event.update({'return_code': '0'})
    log_to_kafka('events', signup_event)
    return response
    #"Signup successful! Welcome " + username + ".\n"


@app.route("/status")
def status():
    status_event = {'event_type': 'status'}

    username = request.args.get('username')

    # Validate the username and session
    vsl_result = validate_session_login(username, status_event)
    if vsl_result == 1:
        return "Action Failed! Please provide a username.\n"
    if vsl_result == 2:
        return "Action Failed! This username does not exist. Please signup.\n"
    if vsl_result == 3:
        return "Action Failed! Your session has expired. Please login.\n"
    if vsl_result == 4:
        return "Action Failed! You do not have an active session on this machine. Please login.\n"

    status_event.update({'user_info': json.loads(r.get(username))})
    status_event.update({'return_code': '0'})
    log_to_kafka('events', status_event)
    return "Hello " + username + "!\n"