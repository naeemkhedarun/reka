ReKa
====

A Restful interface to Apache Kafka
----------------------------------------------

This is a work in progress RESTful interface to push messages to [Apache Kafka](https://kafka.apache.org/) and retrieve information and messages from it. It is built on the [Play!](http://www.playframework.com/) framework using Scala.

Supported Operations
-----------------------------

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
    
###Put a message into a partitioned topic###

####Request####
    
    POST /:topic/:partition
    
    {
       "message1": { "data": {...} }
    }
    
####Response####

    200 OK
    


