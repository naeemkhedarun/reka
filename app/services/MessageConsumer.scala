package services

import kafka.api.FetchRequestBuilder
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import java.util.{Properties, List}
import scala.collection.JavaConversions._
import java.util
import kafka.javaapi
import play.api.Play
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import play.api.Play.current

trait MessageConsumer {
  def get(topic: String, key: String, groupId: String)
  def send(topic: String, key: String, message: String)
}

trait MessageConsumerComponent {

  val messageConsumer: MessageConsumer

  class KafkaMessageConsumer extends MessageConsumer  {
    private val m_replicaBrokers: util.List[String] = new util.ArrayList[String]()

    def getLastOffset(consumer: SimpleConsumer, topic: String, partition: Int, whichTime: Long, clientName: String): Long = {
      val topicAndPartition = new TopicAndPartition(topic, partition)
      val requestInfo = new util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1))
      val request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion,
        clientName)
      val response = consumer.getOffsetsBefore(request)
      if (response.hasError) {
        println("Error fetching data Offset Data the Broker. Reason: " +
          response.errorCode(topic, partition))
        return 0
      }
      val offsets = response.offsets(topic, partition)
      offsets(0)
    }


    private def findNewLeader(a_oldLeader: String,
                              a_topic: String,
                              a_partition: Int,
                              a_port: Int): String = {
      for (i <- 0 until 3) {
        var goToSleep = false
        val metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition)
        if (metadata == null) {
          goToSleep = true
        } else if (metadata.leader == null) {
          goToSleep = true
        } else if (a_oldLeader.equalsIgnoreCase(metadata.leader.host) &&
          i == 0) {
          goToSleep = true
        } else {
          return metadata.leader.host
        }
        if (goToSleep) {
          try {
            Thread.sleep(1000)
          } catch {
            case ie: InterruptedException =>
          }
        }
      }
      println("Unable to find new leader after Broker failure. Exiting")
      throw new Exception("Unable to find new leader after Broker failure. Exiting")
    }

    private def findLeader(a_seedBrokers: List[String],
                           a_port: Int,
                           a_topic: String,
                           a_partition: Int): javaapi.PartitionMetadata = {
      var returnMetaData: javaapi.PartitionMetadata = null
      for (seed <- a_seedBrokers) {
        var consumer: SimpleConsumer = null
        try {
          consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup")
          val topics = new util.ArrayList[String]()
          topics.add(a_topic)
          val req = new TopicMetadataRequest(topics)
          val resp = consumer.send(req)
          val metaData = resp.topicsMetadata
          for (item <- metaData; part <- item.partitionsMetadata if part.partitionId == a_partition) {
            returnMetaData = part
            //break
          }
        } catch {
          case e: Exception => println("Error communicating with Broker [" + seed + "] to find Leader for [" +
            a_topic +
            ", " +
            a_partition +
            "] Reason: " +
            e)
        } finally {
          if (consumer != null) consumer.close()
        }
      }
      if (returnMetaData != null) {
        m_replicaBrokers.clear()
        for (replica <- returnMetaData.replicas) {
          m_replicaBrokers.add(replica.host)
        }
      }
      return returnMetaData
    }

    def get(topic: String, key: String, groupId: String) {

      val a_seedBrokers = Seq("ubuntu")
      val a_port = 9092
      val a_topic = topic
      val a_partition = 1
      var a_maxReads = 100

      val metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition)
      if (metadata == null) {
        throw new Exception("Can't find metadata for Topic and Partition. Exiting")
      }
      if (metadata.leader == null) {
        throw new Exception("Can't find Leader for Topic and Partition. Exiting")
      }
      var leadBroker = metadata.leader.host
      val clientName = "Client_" + a_topic + "_" + a_partition
      var consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName)
      var readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime,
        clientName)
      var numErrors = 0
      while (a_maxReads > 0) {
        if (consumer == null) {
          consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName)
        }
        val req = new FetchRequestBuilder().clientId(clientName).addFetch(a_topic, a_partition, readOffset,
          100000)
          .build()
        val fetchResponse = consumer.fetch(req)
        if (fetchResponse.hasError) {
          numErrors += 1
          val code = fetchResponse.errorCode(a_topic, a_partition)
          println("Error fetching data from the Broker:" + leadBroker +
            " Reason: " +
            code)
          if (numErrors > 5) //break
            if (code == ErrorMapping.OffsetOutOfRangeCode) {
              readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime,
                clientName)
              //continue
            }
          consumer.close()
          consumer = null
          leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port)
          //continue
        }
        numErrors = 0
        var numRead = 0
        for (messageAndOffset <- fetchResponse.messageSet(a_topic, a_partition)) {
          val currentOffset = messageAndOffset.offset
          if (currentOffset < readOffset) {
            println("Found an old offset: " + currentOffset + " Expecting: " +
              readOffset)
            //continue
          }
          readOffset = messageAndOffset.nextOffset
          val payload = messageAndOffset.message.payload
          val bytes = Array.ofDim[Byte](payload.limit())
          payload.get(bytes)
          println(String.valueOf(messageAndOffset.offset) + ": " + new String(bytes, "UTF-8"))
          numRead += 1
          a_maxReads -= 1
        }
        if (numRead == 0) {
          if (consumer != null) consumer.close()
          a_maxReads = 0
          return "done"
        }
      }
      if (consumer != null) consumer.close()

      return "passed"
    }

    def send(topic: String, key: String, message: String) {

      val props: Properties = new Properties()

      val setting: Option[String] = Play.configuration.getString("metadata.broker.list")
      props.put("metadata.broker.list", setting.get)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("request.required.acks", "1")

      val producer: Producer[String,String] = new Producer[String, String](new ProducerConfig(props))
      val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, key, message)

      producer.send(data)
      producer.close()
    }

  }
}