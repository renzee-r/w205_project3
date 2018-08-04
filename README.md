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

* We spin up a cluster of containers for kafka, zookeeper, spark, and our mids image. Then, on the mids container, we run our flask app. A change in the API server from last week is that the schema across different events is no longer standard. For example, 'buy' events now have a 'quality' column that is unique to those events. Another change is that we now use ApacheBench to generate our events. This gives us more flexibility in the event data we send over. To verify the data transfer, we use kafkacat on our mids container to read out the contents of our events topic. Similar to last week, we interact and write out the data through a spark script. The important modification since last week is to accomodate the varying schemas between different even types. So for this week, we filter the data to event type groups with similar schemas and write them out separately. We confirm that the files have been written to HDFS by performing a list directory. To ensure that the contents of the files are correct, we open a Jupyter notebook to inspect the parquet files. Happy with the results, we then spin down our cluster and cleanup the containers.


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
        image: midsw205/cdh-minimal:latest
        expose:
          - "8020" # nn
          - "50070" # nn http
          - "8888" # hue
        #ports:
        #- "8888:8888"
        extra_hosts:
          - "moby:127.0.0.1"

      spark:
        image: midsw205/spark-python:0.0.5
        stdin_open: true
        tty: true
        expose:
          - "8888"
        ports:
          - "8888:8888"
        volumes:
          - "~/w205:/w205"
        command: bash
        depends_on:
          - cloudera
        environment:
          HADOOP_NAMENODE: cloudera
        extra_hosts:
          - "moby:127.0.0.1"

      mids:
        image: midsw205/base:0.1.9
        stdin_open: true
        tty: true
        expose:
          - "5000"
        ports:
          - "5000:5000"
        volumes:
          - "~/w205:/w205"
        extra_hosts:
          - "moby:127.0.0.1"


3) Source code for the flask application(s) used.

  * game_api.py

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
            buy_sword_event.update({'item_quality': 'common'})
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
            new_user_info['inventory'] = {'sword': 0, 'potion': 0, 'shield': 0}
            r.set(username, json.dumps(new_user_info))

            log_to_kafka('events', signup_event)
            return "Signup successful! Welcome " + username + ".\n"
  
  * extract_events.py

        #!/usr/bin/env python
        """Extract events from kafka and write them to hdfs
        """
        import json
        from pyspark.sql import SparkSession, Row
        from pyspark.sql.functions import udf


        @udf('string')
        def munge_event(event_as_json):
            event = json.loads(event_as_json)
            event['Cache-Control'] = "no-cache"
            return json.dumps(event)


        def main():
            """main
            """
            spark = SparkSession \
                .builder \
                .appName("ExtractEventsJob") \
                .getOrCreate()

            raw_events = spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "events") \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()

            munged_events = raw_events \
                .select(raw_events.value.cast('string').alias('raw'),
                        raw_events.timestamp.cast('string')) \
                .withColumn('munged', munge_event('raw'))

            extracted_events = munged_events \
                .rdd \
                .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
                .toDF()

            extracted_events \
                .write \
                .parquet("/tmp/events_all")

            # Create a separate parquet file for item-based events
            item_events = extracted_events \
                .filter(extracted_events.event_type == 'buy_sword')
            item_events.show()

            item_events \
                .write \
                .mode("overwrite") \
                .parquet("/tmp/events_item")

            # Create a parquet file for guild-based events
            guild_events = extracted_events \
                .filter(extracted_events.event_type == 'join_guild')
            guild_events.show()

            guild_events \
                .write \
                .mode("overwrite") \
                .parquet("/tmp/events_guild")

            # Create a parquet file for user-based events
            user_events = extracted_events \
                .filter(extracted_events.event_type == 'signup' or extracted_events.event_type == 'login')
            user_events.show()

            user_events \
                .write \
                .mode("overwrite") \
                .parquet("/tmp/events_user")


        if __name__ == "__main__":
            main()


4) Each important step in the process. For each step, include:
	
* Command: `docker-compose up -d`
    * Output:

	    	Creating network "assignment11renzeer_default" with the default driver
            Creating assignment11renzeer_mids_1
            Creating assignment11renzeer_cloudera_1
            Creating assignment11renzeer_redis_1
            Creating assignment11renzeer_zookeeper_1
            Creating assignment11renzeer_kafka_1
            Creating assignment11renzeer_spark_1

    * Result: This spins up our cluster of containers as specified by our docker-compose yaml file. This includes our kafka, zookeeper, spark, and mids containers.

* Command: `docker-compose ps`
    * Output:

            Name                            Command               State                                         Ports                                        
            -------------------------------------------------------------------------------------------------------------------------------------------------------------
            assignment11renzeer_cloudera_1    cdh_startup_script.sh            Up      11000/tcp, 11443/tcp, 19888/tcp, 50070/tcp, 8020/tcp, 8088/tcp, 8888/tcp, 9090/tcp 
            assignment11renzeer_kafka_1       /etc/confluent/docker/run        Up      29092/tcp, 9092/tcp                                                                
            assignment11renzeer_mids_1        /bin/bash                        Up      0.0.0.0:5000->5000/tcp, 8888/tcp                                                   
            assignment11renzeer_redis_1       docker-entrypoint.sh redis ...   Up      6379/tcp                                                                           
            assignment11renzeer_spark_1       docker-entrypoint.sh bash        Up      0.0.0.0:8888->8888/tcp                                                             
            assignment11renzeer_zookeeper_1   /etc/confluent/docker/run        Up      2181/tcp, 2888/tcp, 32181/tcp, 3888/tcp   

    * Result: Confirming that all of our containers are up and running.

* Command: `docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181`
    * Output:

    		Created topic "events".

    * Result: Create a topic named events within our kafka server. Specify that this topic only have 1 partition, use a replication factor of 1, and create only if it does not already exist. Also specify the zookeeper connection. 

* Command: `docker-compose exec mids env FLASK_APP=/w205/full-stack/game_api.py flask run --host 0.0.0.0`
    * Output:

			* Serving Flask app "game_api"
			* Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)

    * Result: Run the flask app on the mids container. Use the code from game_api.py, which contains our server handlers for the default response, /buy_sword, /join_guild, /signup, and /login. With the addition of tracking user sessions, all actions now require the request to specify the user and for that user to be logged in. These responses will send a message to our kafka server and print out a success message. 

* Command: `docker-compose exec mids curl http://localhost:5000/`
    * Client Output:

    		This is the default response!

    * API Server Output:

    		127.0.0.1 - - [19/Jul/2018 20:31:40] "GET / HTTP/1.1" 200 -

    * Result: Send a URL request to our API server for the default response on the mids container. This will print out a success message on the client and also send the event to our kafka server. We should also see the incoming request in our API server logs.

* Command: `docker-compose exec mids curl http://localhost:5000/buy_sword?username=renzeer`
    * Client Output:

    		Action Failed! This username does not exist. Please signup.

    * API Server Output:

    		127.0.0.1 - - [19/Jul/2018 20:31:43] "GET /buy_sword?username=renzeer HTTP/1.1" 200 -

    * Result: Send a URL request to our API server for /buy_sword on the mids container. Since I have not registered the username renzeer in our game, the action fails. No event is logged to kafka in the event of a failed action.

* Command: `docker-compose exec mids curl 'http://localhost:5000/signup?username=user1&password=pw1'`
    * Client Output:

            Signup successful! Welcome renzeer.

    * API Server Output:

            127.0.0.1 - - [19/Jul/2018 20:32:10] "GET /signup?username=renzeer&password=pw123 HTTP/1.1" 200 -

    * Result: Send a URL request to our API server for /signup on the mids container. This creates the user renzeer inside of our game. User data is stored as a redis entry. Upon signup, the game automatically logs you in and creates an active session.

* Command: `docker-compose exec mids curl http://localhost:5000/buy_sword?username=renzeer`
    * Client Output:

            Bought a Sword!

    * API Server Output:

            127.0.0.1 - - [19/Jul/2018 20:32:22] "GET /buy_sword?username=renzeer HTTP/1.1" 200 -

    * Result: Send a URL request to our API server for /buy_sword on the mids container. Since we have now registered the username renzeer and have an active session, the action succeeds, printing a message to shell and logging an event to kafka.

* Command: `docker-compose exec mids curl http://localhost:5000/join_guild?username=renzeer`
    * Output:

    		Joined a Guild!

    * API Server Output:

    		127.0.0.1 - - [19/Jul/2018 20:32:35] "GET /join_guild?username=renzeer HTTP/1.1" 200 -

    * Result: Send a URL request to our API server for /join_guild on the mids container. Since our username and session is valid, the event succeeds, printing a message and logging an event to kafka

* Command: `docker-compose exec mids bash -c "kafkacat -C -b kafka:29092 -t events -o beginning -e"`
    * Output:

    		{"Host": "localhost:5000", "event_type": "default", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
            {"Host": "localhost:5000", "event_type": "signup", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
            {"Host": "localhost:5000", "event_type": "buy_sword", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
            {"Host": "localhost:5000", "event_type": "join_guild", "Accept": "*/*", "User-Agent": "curl/7.47.0"}
            % Reached end of topic events [0] at offset 4: exiting

    * Result: Verify that the events have been logged to our events topic on our kafka server. We do this by using kafkacat on our mids container to output the contents of our topic from beginning to end. The results show that the first buy_sword event was not logged since we were not logged in. However, all other events have been successfully logged.

* Command: `docker-compose exec spark spark-submit /w205/assignment-11-renzee-r/extract_events.py`
    * Output (excluding kafka config):

            18/07/19 21:44:31 INFO CodeGenerator: Code generated in 24.80991 ms
            +------+-------------+--------------+-----------+----------+--------------------+
            |Accept|Cache-Control|          Host| User-Agent|event_type|           timestamp|
            +------+-------------+--------------+-----------+----------+--------------------+
            |   */*|     no-cache|localhost:5000|curl/7.47.0|   default|2018-07-19 20:31:...|
            |   */*|     no-cache|localhost:5000|curl/7.47.0|    signup|2018-07-19 20:32:...|
            |   */*|     no-cache|localhost:5000|curl/7.47.0| buy_sword|2018-07-19 20:32:...|
            |   */*|     no-cache|localhost:5000|curl/7.47.0|join_guild|2018-07-19 20:32:...|
            +------+-------------+--------------+-----------+----------+--------------------+

            ...

            18/07/19 21:44:34 INFO DAGScheduler: Job 3 finished: showString at NativeMethodAccessorImpl.java:0, took 0.270423 s
            +------+-------------+--------------+-----------+----------+--------------------+
            |Accept|Cache-Control|          Host| User-Agent|event_type|           timestamp|
            +------+-------------+--------------+-----------+----------+--------------------+
            |   */*|     no-cache|localhost:5000|curl/7.47.0| buy_sword|2018-07-19 20:32:...|
            +------+-------------+--------------+-----------+----------+--------------------+

            ...

            18/07/19 21:44:35 INFO DAGScheduler: Job 5 finished: showString at NativeMethodAccessorImpl.java:0, took 0.280870 s
            +------+-------------+--------------+-----------+----------+--------------------+
            |Accept|Cache-Control|          Host| User-Agent|event_type|           timestamp|
            +------+-------------+--------------+-----------+----------+--------------------+
            |   */*|     no-cache|localhost:5000|curl/7.47.0|join_guild|2018-07-19 20:32:...|
            +------+-------------+--------------+-----------+----------+--------------------+

            ...

            18/07/19 21:44:36 INFO DAGScheduler: Job 7 finished: showString at NativeMethodAccessorImpl.java:0, took 0.263270 s
            +------+-------------+--------------+-----------+----------+--------------------+
            |Accept|Cache-Control|          Host| User-Agent|event_type|           timestamp|
            +------+-------------+--------------+-----------+----------+--------------------+
            |   */*|     no-cache|localhost:5000|curl/7.47.0|    signup|2018-07-19 20:32:...|
            +------+-------------+--------------+-----------+----------+--------------------+


    * Result: Submit our extraction code to spark for it to run. The code transforms the data to add the 'Cache-Control' and 'timestamp' fields. The extraction code also filters the data into 3 subsets of events; item events, guild events, and user events. The entire event log as well as these event subsets are written out to parquet.

* Command: `docker-compose exec cloudera hadoop fs -ls /tmp/`
    * Output:

            Found 6 items
            drwxr-xr-x   - root   supergroup          0 2018-07-19 21:44 /tmp/events_all
            drwxr-xr-x   - root   supergroup          0 2018-07-19 21:44 /tmp/events_guild
            drwxr-xr-x   - root   supergroup          0 2018-07-19 21:44 /tmp/events_item
            drwxr-xr-x   - root   supergroup          0 2018-07-19 21:44 /tmp/events_user
            drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
            drwx-wx-wx   - root   supergroup          0 2018-07-19 20:30 /tmp/hive

    * Result: We do a directory list of our hadoop file server to confirm that the parquet files were written out successfully. 

* Command: `docker-compose down`
    * Output:

    		Stopping assignment11renzeer_spark_1 ... done
            Stopping assignment11renzeer_kafka_1 ... done
            Stopping assignment11renzeer_zookeeper_1 ... done
            Stopping assignment11renzeer_mids_1 ... done
            Stopping assignment11renzeer_redis_1 ... done
            Stopping assignment11renzeer_cloudera_1 ... done
            Removing assignment11renzeer_spark_1 ... done
            Removing assignment11renzeer_kafka_1 ... done
            Removing assignment11renzeer_zookeeper_1 ... done
            Removing assignment11renzeer_mids_1 ... done
            Removing assignment11renzeer_redis_1 ... done
            Removing assignment11renzeer_cloudera_1 ... done
            Removing network assignment11renzeer_default

    * Result: After we are done, we bring down all of our containers





