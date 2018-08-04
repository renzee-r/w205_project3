#!/usr/bin/env python
import json
import redis
from kafka import KafkaProducer
from flask import Flask, request
from functools import wraps
from datetime import datetime
from random import randint

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

# Instantiate redis. Redis will be used to store two types of entries:
# Entry 1 (Username : User Information)
#   Key:    The username of a registered user in our game
#   Value:  A dict of various user information fields:
#               password - The user's password for their account
#               session_datetime - The date and time of their last active session
#               inventory - A list of items that is in the user's inventory
#
# Entry 2 (SessionID : Username)
#   Key:    The session ID of a currently active session in our game
#   Value:  The username tied to the active session
r = redis.Redis(host='redis', port='6379')

# List of valid items within the game. This list is used to validate
# buy actions, since the user can technically provide any string as a
# parameter
valid_items = ['sword', 'potion', 'shield']

# List of used/stale session IDs. Used to ensure we do not assign
# the same session ID twice.
used_session_ids = []

# Sends the event dict to kafka
def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


# Decorator that valides that the incoming request has a 
# valid active session
def validate_session(function_to_protect):
    @wraps(function_to_protect)
    def wrapper(*args, **kwargs):
        # Get the session ID from the request cookies
        session_id = request.cookies.get('session_id')

        # Could not resolve the session ID. There was no valid cookie on the request.
        if session_id is None:
            return 'No active session found! Please login.\n'

        # Get the username associated with that session ID
        username = r.get(session_id)

        # Could not resolve username. The session to user mapping was invalid.
        if username is None:
            return 'Invalid session! Please login.\n'

        # Get the user information associated with that user
        user_info_raw = r.get(username)

        # The username does not belong to a registered account. This error
        # should never occur and would indicate some fatal condition within the game
        if user_info_raw is None:
            return 'Invalid username! Please login.\n'

        # Parse the raw user information into a dict
        user_info = json.loads(user_info_raw)

        # The user has never had an active session. This error should never
        # occur and would indicate some fatal condition within the game
        if user_info['session_datetime'] is None:
            return  'Invalid user session! Please login.\n'

        # The user was not active in the last 2 hours (or 7200 seconds). This
        # would require the user to login again.
        last_session_datetime = datetime.strptime(user_info['session_datetime'], "%Y-%m-%d %H:%M:%S")
        if abs((datetime.now() - last_session_datetime).seconds) > 7200:
            return 'Expired user session! Please login.\n'

        # Successfully passed all validation checks
        return function_to_protect(*args, **kwargs)


    return wrapper


# Default response, nothing special
@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)

    return "This is the default response!\n"


@app.route("/buy/<item>")
@validate_session
def buy_item(item):
    """Handles the purchasing of items for the user. First, purchasing items requires an
    active session, which is validates through our validate_session decorator. The item being 
    purchased is passed as a URL variable. If the item is not valid, the purchase will
    fail. If the session and item is valid, the function will append the item to the user's
    inventory. It will also refresh the user's session_datetime. Finally, log the event to kafka.
    """
    buy_item_event = {'event_type': 'buy_item'}

    # Get username from cookie and store it into the event. Don't
    # need validation checks as it is all handled in the decorator
    session_id = request.cookies.get('session_id')
    username = r.get(session_id)
    buy_item_event.update({'username': username})

    # Validate if the item is valid
    if item not in valid_items:
        return 'The store does not have that item in stock.'

    # If item is valid, update the event with the item. Also
    # update the user's inventory and session_datetime
    buy_item_event.update({'item_type': item})
    user_info = json.loads(r.get(username))
    user_info['inventory'].append(item)
    user_info['session_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    r.set(username, json.dumps(user_info))

    # Update the event with the latest inventory info
    buy_item_event.update({'inventory': user_info['inventory']})

    # TODO: Randomly generate the item quality
    buy_item_event.update({'item_quality': 'common'})

    # TODO: Somehow attach the item quality to the items in 
    # user's inventory

    # Send the even to kafka
    log_to_kafka('events', buy_item_event)

    # Return a success message
    return 'Bought a ' + item + '!\n'


@app.route("/join_guild/<guild_name>")
@validate_session
def join_guild(guild_name):
    """Handles the joining of guilds for the user. Joining a guild requires an
    active session, which is validated by our validate_session decorator. The guild
    being joined is passed as a URL variable. This action will refresh the user's 
    session. It will then log the event to kafka.
    """
    join_guild_event = {'event_type': 'join_guild'}

    # Get username from cookie and store it into the event. Don't
    # need validation checks as it is all handled in the decorator
    session_id = request.cookies.get('session_id')
    username = r.get(session_id)
    buy_item_event.update({'username': username})

    # Update the event with the name of the guild being joined
    join_guild_event = {'guild_name': guild_name}

    # Update the user's information with their new guild. Also
    # refresh the user's session.
    user_info = json.loads(r.get(username))
    user_info['guild'] = guild_name
    user_info['session_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    r.set(username, json.dumps(user_info))

    # Send the event to kafka
    log_to_kafka('events', join_guild_event)

    # Return a success message
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

    # TODO: Clear out any sessions currently tied to the user. Generate
    # a new session and assign it the user

    # Refresh the session datetime
    user_info['session_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    new_user_info['password'] = password
    new_user_info['session_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    users_str = 'Registered Users:\n'
    sessions_str = 'Game Sessions:\n'

    for key in r.keys():
        if key.isdigit():
            username = r.get(key)
            user_info = json.loads(r.get(username))
            if user_info['session_datetime'] is None:
                session_status = 'inactive'
            elif abs((datetime.now() - datetime.strptime(user_info['session_datetime'], "%Y-%m-%d %H:%M:%S")).seconds) > 7200:
                session_status = 'inactive'
            else:
                session_status = 'active'

            sessions_str += '\t' + key + ' : ' + username + ' (' + session_status + ')\n'
        else:
            users_str += '\t' + key + ' : ' + r.get(key) + '\n'

            

    
    status_str = users_str + '\n' + sessions_str
    return status_str