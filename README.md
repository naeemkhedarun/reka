ReKa
====

A Restful interface to Apache Kafka
----------------------------------------------

This is a work in progress RESTful interface to push messages to [Apache Kafka](https://kafka.apache.org/) and retrieve information and messages from it. It is built on the [Play!](http://www.playframework.com/) framework using Scala.

Please note this has been developed against a single kafka instance and may not work correctly with a cluster.

Contribute
----------

Suggestions, raised issues and pull requests are extremely welcome.

[@naeemkhedarun](https://twitter.com/naeemkhedarun)

Getting Started
---------------

Update the configuration in conf/application.conf

    # Kafka Settings
    metadata.broker.list=["ubuntu:9092"]
    zookeeper.connect="ubuntu:2181"

Run play from within the repository root.

    play

Start the application.

    run

The development site will default to http://localhost:9000/ and you can use something like [postman](https://chrome.google.com/webstore/detail/postman-rest-client/fdmmgilgnpjigdojojpjoooidkmcomcm?hl=en) to make requests.


Supported Operations
--------------------

###Get a list of topics###

####Request####

    GET  /

####Response####

    {
        "Ok": true,
        "topics": [
            "yourtopic1",
            "yourtopic2"
        ]
    }

    
###Get a list of partitions###

####Request####
    GET  /:topic
    
####Response####

    {
        "topic": "mytopic",
        "partitions": [
            0,
            1
        ]
    }

###Get a list of messages####

####Request####

    GET  /:topic/:partition
    
####Response####

    [
        {
            "message1": {
                "data": {}
            }
        },
        {
            "message2": {
                "otherdata": {}
            }
        }
        ...
    ]
    
###Push a message into a partitioned topic###

####Request####
    
    POST /:topic/:partition
    
    {
       "message1": { "data": {...} }
    }
    
####Response####

    200 OK
    


