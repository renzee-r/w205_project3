# Project 3 Setup

- You're a data scientist at a game development company.  
- Your latest mobile game has two events you're interested in tracking: 
- `buy a sword` & `join guild`...
- Each has metadata

## Project 3 Task
- Your task: instrument your API server to catch and analyze event types.
- This task will be spread out over the last four assignments (9-12).

## Project 3 Task Options 

- All: Game shopping cart data used for homework 
- Advanced option 1: Generate and filter more types of items.
- Advanced option 2: Enhance the API to accept parameters for purchases (sword/item type) and filter
- Advanced option 3: Shopping cart data & track state (e.g., user's inventory) and filter


---


# Assignment 12

1) A summary type explanation of the example. 

* We spin up a cluster of containers for kafka, zookeeper, spark, mids, redis, presto, and cloudera images. On the mids container, we run our flask app. Our flask app acts as our game API server, accepting and processing game requests. The server has handlers for user events such as signing up and logging in, a handler for buying an item, a handler for joining a guild, and other administrative handlers. At this point, the game can handle any input of items or guild names passed as URL parameters. The user's in-game information is stored in Redis, keeping track of their guild and their inventory. The user's game session is tracked through client-side cookies, that are also maintained in Redis. After handling the in-game logic, the game API server will send the event data to our Kafka server. We then have spark scripts that initiates a read/write stream that reads the Kafka logs and writes them out to parquet files in HDFS. There is a spark script for each event type; buy events, guild events, and user events. Each spark script is responsible for defining the schema, filtering to only the specified event type, appending metadata, and writing the a specified HDFS directory. We can then declare the schema of the datatables in Hive and point them to our parquet files to make our data queryable from Presto. We test out this entire pipeline by looping a set of Apache Bench commands that constantly send buy requests to our game API server. We then run our spark streaming job to intake the requests coming into Kafka and write them into HDFS. With data in HDFS, we setup our Hive metastore containing the schema for our buy events and pointing to the parquet files. We finally test out some queries in Presto to ensure everything is working as expected. Happy with the results, we then spin everything down.


2) Your `docker-compose.yml`

    ---
    version: '2'
    services:
      redis:
        image: redis:latest
        expose:
          - "6379"
          
      zookeeper:
        image: confluentinc/cp-zookeeper:latest
        environment:
          ZOOKEEPER_CLIENT_PORT: 32181
          ZOOKEEPER_TICK_TIME: 2000
        expose:
          - "2181"
          - "2888"
          - "32181"
          - "3888"
        extra_hosts:
          - "moby:127.0.0.1"

      kafka:
        image: confluentinc/cp-kafka:latest
        depends_on:
          - zookeeper
        environment:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:32181
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
        expose:
          - "9092"
          - "29092"
        extra_hosts:
          - "moby:127.0.0.1"

      cloudera:
        image: midsw205/hadoop:0.0.2
        hostname: cloudera
        expose:
          - "8020" # nn
          - "8888" # hue
          - "9083" # hive thrift
          - "10000" # hive jdbc
          - "50070" # nn http
        ports:
          - "8888:8888"
        extra_hosts:
          - "moby:127.0.0.1"

      spark:
        image: midsw205/spark-python:0.0.6
        stdin_open: true
        tty: true
        volumes:
          - ~/w205:/w205
        expose:
          - "8888"
        ports:
          - "8889:8888" # 8888 conflicts with hue
        depends_on:
          - cloudera
        environment:
          HADOOP_NAMENODE: cloudera
          HIVE_THRIFTSERVER: cloudera:9083
        extra_hosts:
          - "moby:127.0.0.1"
        command: bash

      presto:
        image: midsw205/presto:0.0.1
        hostname: presto
        volumes:
          - ~/w205:/w205
        expose:
          - "8080"
        environment:
          HIVE_THRIFTSERVER: cloudera:9083
        extra_hosts:
          - "moby:127.0.0.1"

      mids:
        image: midsw205/base:0.1.9
        stdin_open: true
        tty: true
        volumes:
          - ~/w205:/w205
        expose:
          - "5000"
        ports:
          - "5000:5000"
        extra_hosts:
          - "moby:127.0.0.1"



3) Source code for the flask application(s) used.

  * game_api.py

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
            join_guild_event.update({'username': username})

            # Update the event with the name of the guild being joined
            join_guild_event.update({'guild_name': guild_name})

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
                return "Login Failed! Please provide a username.\n"

            if password is None:
                return "Login Failed! Please provide a password.\n"

            user_info_raw = r.get(username)

            if user_info_raw is None:
                return "Login Failed! This username does not exist. Please signup.\n"

            user_info = json.loads(user_info_raw)

            if user_info['password'] != password:
                return "Login Failed! This password is incorrect.\n"

            # TODO: Clear out any sessions currently tied to the user. Generate
            # a new session and assign it the user

            # Refresh the session datetime
            user_info['session_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            r.set(username, json.dumps(user_info))

            login_event.update({'user_info': user_info})
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
  
  * stream_buy_events.py

        #!/usr/bin/env python
        """Extract events from kafka and write them to hdfs
        """
        import json
        from pyspark.sql import SparkSession, Row
        from pyspark.sql.functions import udf, from_json
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType

        def buy_event_schema():
            """
            root
            |-- Accept: string (nullable = true)
            |-- Host: string (nullable = true)
            |-- User-Agent: string (nullable = true)
            |-- event_type: string (nullable = true)
            |-- username: string (nullable = true)
            |-- item_type: string (nullable = true)
            |-- Cookie: string (nullable = true)
            |-- item_quality: string (nullable = true)
            |-- user_info: Struct (nullable = true)
            |-- timestamp: string (nullable = true)
            """
            return StructType([
                StructField("Accept", StringType(), True),
                StructField("Host", StringType(), True),
                StructField("User-Agent", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("username", StringType(), True),
                StructField("item_type", StringType(), True),
                StructField("Cookie", StringType(), True),
                StructField("item_quality", StringType(), True),
                StructField("user_info", user_info_schema(), True),
            ])


        def user_info_schema():
            """
            root
            |-- password: string (nullable = true)
            |-- session_datetime: string (nullable = true)
            |-- inventory: array (nullable = true)
            """
            return StructType([
                StructField("password", StringType(), True),
                StructField("session_datetime", StringType(), True),
                StructField("inventory", ArrayType(StringType()), True),
            ])


        @udf('boolean')
        def is_buy_event(event_as_json):
            event = json.loads(event_as_json)
            if event['event_type'] == 'buy_item':
                return True
            return False


        def main():
            """main
            """
            spark = SparkSession \
                .builder \
                .appName("StreamBuyEventsJob") \
                .getOrCreate()

            raw_events = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "events") \
                .load()

            # Create a stream for buy events
            buy_events = raw_events \
                .filter(is_buy_event(raw_events.value.cast('string'))) \
                .select(raw_events.value.cast('string').alias('raw_event'),
                        raw_events.timestamp.cast('string'),
                        from_json(raw_events.value.cast('string'),
                                  buy_event_schema()).alias('json')) \
                .select('raw_event', 'timestamp', 'json.*')

            sink = buy_events \
                .writeStream \
                .format("parquet") \
                .option("checkpointLocation", "/tmp/checkpoints_buy_events") \
                .option("path", "/tmp/events_buy") \
                .trigger(processingTime="10 seconds") \
                .start()

            sink.awaitTermination()


        if __name__ == "__main__":
            main()

    * stream_guild_events.py

        #!/usr/bin/env python
        """Extract events from kafka and write them to hdfs
        """
        import json
        from pyspark.sql import SparkSession, Row
        from pyspark.sql.functions import udf, from_json
        from pyspark.sql.types import StructType, StructField, StringType


        def guild_event_schema():
            """
            root
            |-- Accept: string (nullable = true)
            |-- Host: string (nullable = true)
            |-- User-Agent: string (nullable = true)
            |-- event_type: string (nullable = true)
            |-- username: string (nullable = true)
            |-- Cookie: string (nullable = true)
            |-- guild_name: string (nullable = true)
            |-- timestamp: string (nullable = true)
            """
            return StructType([
                StructField("Accept", StringType(), True),
                StructField("Host", StringType(), True),
                StructField("User-Agent", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("username", StringType(), True),
                StructField("Cookie", StringType(), True),
                StructField("guild_name", StringType(), True),
            ])


        @udf('boolean')
        def is_guild_event(event_as_json):
            event = json.loads(event_as_json)
            if event['event_type'] == 'join_guild':
                return True
            return False


        def main():
            """main
            """
            spark = SparkSession \
                .builder \
                .appName("StreamGuildEventsJob") \
                .getOrCreate()

            raw_events = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "events") \
                .load()

            # Create a stream for guild events
            guild_events = raw_events \
                .filter(is_guild_event(raw_events.value.cast('string'))) \
                .select(raw_events.value.cast('string').alias('raw_event'),
                        raw_events.timestamp.cast('string'),
                        from_json(raw_events.value.cast('string'),
                                  guild_event_schema()).alias('json')) \
                .select('raw_event', 'timestamp', 'json.*')

            sink = guild_events \
                .writeStream \
                .format("parquet") \
                .option("checkpointLocation", "/tmp/checkpoints_guild_events") \
                .option("path", "/tmp/events_guild") \
                .trigger(processingTime="10 seconds") \
                .start()

            sink.awaitTermination()


        if __name__ == "__main__":
            main()

    * stream_user_events.py

        #!/usr/bin/env python
        """Extract events from kafka and write them to hdfs
        """
        import json
        from pyspark.sql import SparkSession, Row
        from pyspark.sql.functions import udf, from_json
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType


        def user_event_schema():
            """
            root
            |-- Accept: string (nullable = true)
            |-- Host: string (nullable = true)
            |-- User-Agent: string (nullable = true)
            |-- event_type: string (nullable = true)
            |-- user_info: Struct (nullable = true)
            |-- Cookie: string (nullable = true)
            |-- timestamp: string (nullable = true)
            """
            return StructType([
                StructField("Accept", StringType(), True),
                StructField("Host", StringType(), True),
                StructField("User-Agent", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("user_info", user_info_schema(), True),
                StructField("Cookie", StringType(), True),
            ])


        def user_info_schema():
            """
            root
            |-- password: string (nullable = true)
            |-- session_datetime: string (nullable = true)
            |-- inventory: array (nullable = true)
            """
            return StructType([
                StructField("password", StringType(), True),
                StructField("session_datetime", StringType(), True),
                StructField("inventory", ArrayType(StringType()), True),
            ])


        @udf('boolean')
        def is_user_event(event_as_json):
            event = json.loads(event_as_json)
            if (event['event_type'] == 'signup') or (event['event_type'] == 'login'):
                return True
            return False


        def main():
            """main
            """
            spark = SparkSession \
                .builder \
                .appName("ExtractEventsJob") \
                .getOrCreate()

            raw_events = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "events") \
                .load()

            # Create a stream for user events
            user_events = raw_events \
                .filter(is_user_event(raw_events.value.cast('string'))) \
                .select(raw_events.value.cast('string').alias('raw_event'),
                        raw_events.timestamp.cast('string'),
                        from_json(raw_events.value.cast('string'),
                                  user_event_schema()).alias('json')) \
                .select('raw_event', 'timestamp', 'json.*')

            sink = user_events \
                .writeStream \
                .format("parquet") \
                .option("checkpointLocation", "/tmp/checkpoints_user_events") \
                .option("path", "/tmp/events_user") \
                .trigger(processingTime="10 seconds") \
                .start()

            sink.awaitTermination()


        if __name__ == "__main__":
            main()



4) Each important step in the process:
	
## docker-compose up -d
Output:

	Creating network "assignment12renzeer_default" with the default driver
    Creating assignment12renzeer_cloudera_1
    Creating assignment12renzeer_redis_1
    Creating assignment12renzeer_mids_1
    Creating assignment12renzeer_zookeeper_1
    Creating assignment12renzeer_presto_1
    Creating assignment12renzeer_kafka_1
    Creating assignment12renzeer_spark_1

Result:  
This spins up our cluster of containers as specified by our docker-compose yaml file. This includes our kafka, zookeeper, spark, mids, cloudera, redis, and presto containers.

## docker-compose ps
Output:

    Name                            Command               State                                Ports                               
    -------------------------------------------------------------------------------------------------------------------------------------------
    assignment12renzeer_cloudera_1    /usr/bin/docker-entrypoint ...   Up      10000/tcp, 50070/tcp, 8020/tcp, 0.0.0.0:8888->8888/tcp, 9083/tcp 
    assignment12renzeer_kafka_1       /etc/confluent/docker/run        Up      29092/tcp, 9092/tcp                                              
    assignment12renzeer_mids_1        /bin/bash                        Up      0.0.0.0:5000->5000/tcp, 8888/tcp                                 
    assignment12renzeer_presto_1      /usr/bin/docker-entrypoint ...   Up      8080/tcp                                                         
    assignment12renzeer_redis_1       docker-entrypoint.sh redis ...   Up      6379/tcp                                                         
    assignment12renzeer_spark_1       docker-entrypoint.sh bash        Up      0.0.0.0:8889->8888/tcp                                           
    assignment12renzeer_zookeeper_1   /etc/confluent/docker/run        Up      2181/tcp, 2888/tcp, 32181/tcp, 3888/tcp   

Result:  
Confirming that all of our containers are up and running.

## docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
Output:

    Created topic "events".

Result:  
Create a topic named events within our kafka server. Specify that this topic only have 1 partition, use a replication factor of 1, and create only if it does not already exist. Also specify the zookeeper connection. 

## docker-compose exec mids bash -c 'pip install redis'
Output:

    Collecting redis
      Downloading https://files.pythonhosted.org/packages/3b/f6/7a76333cf0b9251ecf49efff635015171843d9b977e4ffcf59f9c4428052/redis-2.10.6-py2.py3-none-any.whl (64kB)
        100% |################################| 71kB 4.1MB/s 
    Installing collected packages: redis
    Successfully installed redis-2.10.6

Result:  
Our latest game server code utilizes redis to store game data. Redis is not installed by default on our mids container, so we will need to install it. 

## docker-compose exec mids env FLASK_APP=/w205/assignment-12-renzee-r/game_api.py flask run --host 0.0.0.0
Output:

	* Serving Flask app "game_api"
	* Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)

Result:  
Run the flask app on the mids container. Use the code from game_api.py, which contains our server handlers for the default response, buying items, joining a guild, and user actions. There have been various changes to the games server code, which will be covered in the following commands that send requests to the flask app.

## curl -c /tmp/cookies -b /tmp/cookies 'http://localhost:5000/signup?username=user1&password=pw1'
Client Output:

	Signup successful! Welcome user1.

API Server Output:

	172.18.0.1 - - [12/Aug/2018 23:22:06] "GET /signup?username=user1&password=pw1 HTTP/1.1" 200 -

Result:  
Send a URL request to our API server for /signup on the mids container. This creates the user user1 inside of our game. User data is stored as a redis entry. A new change is that this action will also generate a session ID and tie it to the newly created user. The session ID is also stored as a cookie on the requesting client as a way to maintain active game sessions. All in-game actions require an active session cookie. Otherwise, the game will prompt the user to login.

## curl -c /tmp/cookies -b /tmp/cookies http://localhost:5000/status
Client Output:

    Registered Users:
        user1 : {"password": "pw1", "session_datetime": "2018-08-13 02:16:20", "inventory": []}

    Game Sessions:
        68231 : user1 (active)

API Server Output:

    172.18.0.1 - - [13/Aug/2018 02:16:28] "GET /status HTTP/1.1" 200 -

Result:  
Use a custom /status request that prints out ingame data to retrieve the session ID tied to user1. This session ID is needed so that we can spoof the cookies on our Apache Bench commands.

## while true; do docker-compose exec mids ab -n 2 -C "session_id=68231" -H "Host: user1.comcast.com" http://localhost:5000/buy/sword && docker-compose exec mids ab -n 2 -C "session_id=68231" -H "Host: user1.comcast.com" http://localhost:5000/buy/potion &&  docker-compose exec mids ab -n 2 -C "session_id=68231" -H "Host: user1.comcast.com" http://localhost:5000/buy/shield; sleep 3; done
Client Output:

    ...
    Document Path:          /buy/sword
    Document Length:        16 bytes

    Concurrency Level:      1
    Time taken for tests:   0.014 seconds
    Complete requests:      2
    Failed requests:        0
    Total transferred:      342 bytes
    HTML transferred:       32 bytes
    Requests per second:    145.89 [#/sec] (mean)
    Time per request:       6.855 [ms] (mean)
    Time per request:       6.855 [ms] (mean, across all concurrent requests)
    Transfer rate:          24.36 [Kbytes/sec] received
    ...
    Document Path:          /buy/potion
    Document Length:        17 bytes

    Concurrency Level:      1
    Time taken for tests:   0.016 seconds
    Complete requests:      2
    Failed requests:        0
    Total transferred:      344 bytes
    HTML transferred:       34 bytes
    Requests per second:    123.84 [#/sec] (mean)
    Time per request:       8.075 [ms] (mean)
    Time per request:       8.075 [ms] (mean, across all concurrent requests)
    Transfer rate:          20.80 [Kbytes/sec] received
    ...
    Document Path:          /buy/shield
    Document Length:        17 bytes

    Concurrency Level:      1
    Time taken for tests:   0.018 seconds
    Complete requests:      2
    Failed requests:        0
    Total transferred:      344 bytes
    HTML transferred:       34 bytes
    Requests per second:    112.29 [#/sec] (mean)
    Time per request:       8.905 [ms] (mean)
    Time per request:       8.905 [ms] (mean, across all concurrent requests)
    Transfer rate:          18.86 [Kbytes/sec] received

API Server Output:

    127.0.0.1 - - [13/Aug/2018 00:23:45] "GET /buy/sword HTTP/1.0" 200 -
    127.0.0.1 - - [13/Aug/2018 00:23:45] "GET /buy/sword HTTP/1.0" 200 -
    127.0.0.1 - - [13/Aug/2018 00:23:46] "GET /buy/potion HTTP/1.0" 200 -
    127.0.0.1 - - [13/Aug/2018 00:23:46] "GET /buy/potion HTTP/1.0" 200 -
    127.0.0.1 - - [13/Aug/2018 00:23:46] "GET /buy/shield HTTP/1.0" 200 -
    127.0.0.1 - - [13/Aug/2018 00:23:46] "GET /buy/shield HTTP/1.0" 200 -

Result:  
In an infinite loop, use Apache Bench to send multiple buy requests to our server. Include the valid session cookie for user1 in the Apache Bench requests. A change since the last version is that the item being purchased is now specified as a URL parameter. This means that user can attempt to purchase any item. However, the game's server code only accepts certain item strings as valid for purchase. For non-valid items, no event will be logged and the user will be given an appropriate message.

## docker-compose exec mids bash -c "kafkacat -C -b kafka:29092 -t events -o beginning -e"
Output:

    ...
    {"username": "user1", "event_type": "buy_item", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "item_type": "shield", "Host": "user1.comcast.com", "Cookie": "session_id=68231", "inventory": ["sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield"], "item_quality": "common"}
    {"username": "user1", "event_type": "buy_item", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "item_type": "sword", "Host": "user1.comcast.com", "Cookie": "session_id=68231", "inventory": ["sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword"], "item_quality": "common"}
    {"username": "user1", "event_type": "buy_item", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "item_type": "sword", "Host": "user1.comcast.com", "Cookie": "session_id=68231", "inventory": ["sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword", "potion", "potion", "shield", "shield", "sword", "sword"], "item_quality": "common"}
    % Reached end of topic events [0] at offset 33: exiting

Result:  
Verify that the events have been logged to our events topic on our kafka server. We do this by using kafkacat on our mids container to output the contents of our topic from beginning to end. The results shows a large inventory of items from the continous stream of buy requests.

## docker-compose exec spark spark-submit /w205/assignment-12-renzee-r/stream_buy_events.py
Output:

    ...
    18/08/13 02:18:31 INFO StreamExecution: Streaming query made progress: {
      "id" : "65780b5a-24d3-4b4c-a239-0a643e8bfa8c",
      "runId" : "b6de77ef-67fc-4450-8390-daa11029cdfe",
      "name" : null,
      "timestamp" : "2018-08-13T02:18:30.778Z",
      "numInputRows" : 6,
      "inputRowsPerSecond" : 0.6528835690968444,
      "processedRowsPerSecond" : 11.450381679389313,
      "durationMs" : {
        "addBatch" : 320,
        "getBatch" : 14,
        "getOffset" : 9,
        "queryPlanning" : 18,
        "triggerExecution" : 524,
        "walCommit" : 150
      },
      "stateOperators" : [ ],
      "sources" : [ {
        "description" : "KafkaSource[Subscribe[events]]",
        "startOffset" : {
          "events" : {
            "0" : 85
          }
        },
        "endOffset" : {
          "events" : {
            "0" : 91
          }
        },
        "numInputRows" : 6,
        "inputRowsPerSecond" : 0.6528835690968444,
        "processedRowsPerSecond" : 11.450381679389313
      } ],
      "sink" : {
        "description" : "FileSink[/tmp/events_buy]"
      }
    }
    ...

Result:  
Execute our spark write stream for buy events. This file will parse any buy events in our Kafka log and write them to a parquet file stored in HDFS. This job will parse all previously logged events and as Apache Bench loops, we will see those events processed in the stream file batches. Along with the data fields within the game event themselves, the spark job will also append a timestamp and the raw event data. Currently, the job is setup to process every 10 seconds.

## docker-compose exec cloudera hadoop fs -ls /tmp/
Output:

    Found 5 items
    drwxrwxrwt   - root   supergroup          0 2018-08-13 02:18 /tmp/checkpoints_buy_events
    drwxr-xr-x   - root   supergroup          0 2018-08-13 02:19 /tmp/events_buy
    drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
    drwx-wx-wx   - hive   supergroup          0 2018-08-13 02:16 /tmp/hive
    drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs

Result:  
We do a directory list of our hadoop file server to confirm that the parquet files were written out successfully. 

## docker-compose exec cloudera hive
Output:  
None

Result:  
Open up the hive shell so we can establish the Hive metastore

## drop table default.events_buy; create external table default.events_buy (raw_event string, Accept string, Host string, User_Agent string, event_type string, timestamp string, username string, item_type string, Cookie string, item_quality string, user_info struct<password:string, session_datetime:string, inventory:array<string>>) stored as parquet location '/tmp/events_buy' tblproperties ("parquet.compress"="SNAPPY");

Output:

    OK
    Time taken: 0.505 seconds
    OK
    Time taken: 0.074 seconds

Result:  
Create a table called events_buy. Execute a drop statement first to clear out any exiting tables. Specify the schema to match the data fields within our buy events with the additional timestamp and raw_event fields appended. Point this table to where our write stream is outputting the parquet files for the buy events. With the table created, we can now query our data through Presto.

## docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
Output:  
None

Result:  
Open up the presto shell so that we can start interacting with our newly created table.

## show tables;
Output:

       Table    
    ------------
     events_buy 
    (1 row)

    Query 20180813_021953_00001_7qgfe, FINISHED, 1 node
    Splits: 2 total, 2 done (100.00%)
    0:04 [1 rows, 35B] [0 rows/s, 9B/s]

Result:  
Perform a simple `show tables` to ensure that the table was successfully created.

## describe events_buy;
Output:

        Column    |                                   Type                                    | Comment 
    --------------+---------------------------------------------------------------------------+---------
     raw_event    | varchar                                                                   |         
     accept       | varchar                                                                   |         
     host         | varchar                                                                   |         
     user_agent   | varchar                                                                   |         
     event_type   | varchar                                                                   |         
     timestamp    | varchar                                                                   |         
     username     | varchar                                                                   |         
     item_type    | varchar                                                                   |         
     cookie       | varchar                                                                   |         
     item_quality | varchar                                                                   |         
     user_info    | row(password varchar, session_datetime varchar, inventory array(varchar)) |         
    (11 rows)

    Query 20180813_023353_00023_4i9if, FINISHED, 1 node
    Splits: 2 total, 1 done (50.00%)
    0:00 [11 rows, 816B] [66 rows/s, 4.79KB/s]

Result:  
Describe our events_buy table to ensure that its schema matches what was specified in our create statement in Hive.

## select count(*) from events_buy where item_type = 'sword';
Output:

     _col0 
    -------
       296 
    (1 row)

    Query 20180813_023331_00022_4i9if, FINISHED, 1 node
    Splits: 79 total, 55 done (69.62%)
    0:02 [616 rows, 421KB] [326 rows/s, 223KB/s]

Result:  
Perform an actual query on our dataset. Select a count of events where the item_type is equal to 'sword'. The result is a table with a single column and row containing that count.

## docker-compose down
Output:

    Stopping assignment12renzeer_kafka_1 ... done
    Stopping assignment12renzeer_spark_1 ... done
    Stopping assignment12renzeer_redis_1 ... done
    Stopping assignment12renzeer_presto_1 ... done
    Stopping assignment12renzeer_mids_1 ... done
    Stopping assignment12renzeer_zookeeper_1 ... done
    Stopping assignment12renzeer_cloudera_1 ... done
    Removing assignment12renzeer_kafka_1 ... done
    Removing assignment12renzeer_spark_1 ... done
    Removing assignment12renzeer_redis_1 ... done
    Removing assignment12renzeer_presto_1 ... done
    Removing assignment12renzeer_mids_1 ... done
    Removing assignment12renzeer_zookeeper_1 ... done
    Removing assignment12renzeer_cloudera_1 ... done
    Removing network assignment12renzeer_default

Result:  
After we are done, we bring down all of our containers




