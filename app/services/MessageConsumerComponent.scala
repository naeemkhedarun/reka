package services

import kafka.api.FetchRequestBuilder
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import java.util.Properties
import scala.collection.JavaConversions._
import java.util
import kafka.javaapi
import play.api.Play
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import play.api.Play.current
import kafka.utils.{ZkUtils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient

trait MessageConsumerComponent {

  val messageConsumer: KafkaMessageConsumer

  class KafkaMessageConsumer {

    def listTopics() : Seq[String] = {
      val setting: Option[String] = Play.configuration.getString("zookeeper.connect")
      val zkClient = new ZkClient(setting.get, 30000, 30000, ZKStringSerializer)
      ZkUtils.getAllTopics(zkClient)
    }

    def listPartitions(topic: String) = {
      val setting: Option[String] = Play.configuration.getString("zookeeper.connect")
      val zkClient = new ZkClient(setting.get, 30000, 30000, ZKStringSerializer)
      ZkUtils.getPartitionsForTopics(zkClient, Seq(topic)).flatMap(f => f._2)
    }

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
                              topic: String,
                              partition: Int): String = {
      for (i <- 0 until 3) {
        var goToSleep = false
        val metadata = findLeader(m_replicaBrokers, topic, partition)
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

    private def findLeader(brokers: util.List[String],
                           topic: String,
                           partition: Int): javaapi.PartitionMetadata = {
      var returnMetaData: javaapi.PartitionMetadata = null
      for (seed <- brokers) {
        var consumer: SimpleConsumer = null
        try {
          val host = seed.split(":")(0)
          val port = seed.split(":")(1).toInt

          consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "leaderLookup")
          val topics = new util.ArrayList[String]()
          topics.add(topic)
          val req = new TopicMetadataRequest(topics)
          val resp = consumer.send(req)
          val metaData = resp.topicsMetadata
          for (item <- metaData; part <- item.partitionsMetadata if part.partitionId == partition) {
            returnMetaData = part
            //break
          }
        } catch {
          case e: Exception => println("Error communicating with Broker [" + seed + "] to find Leader for [" +
            topic +
            ", " +
            partition +
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
      returnMetaData
    }

    class TopicMessages {
      val messages: Seq[String] = Seq()
      val errors: Seq[String] = Seq()
    }

    def get(topic: String, partition: Int) : TopicMessages = {
      val result = new TopicMessages

      val brokers: util.List[String] = Play.configuration.getStringList("metadata.broker.list").get
      var maxReads = 100

      val metadata = findLeader(brokers, topic, partition)
      if (metadata == null) {
        result.errors :+ "Can't find metadata for Topic and Partition. Exiting"
        return result
      }
      if (metadata.leader == null) {
        result.errors :+ "Can't find Leader for Topic and Partition. Exiting"
        return result
      }

      var leadBroker = metadata.leader.host
      val port = metadata.leader.port

      val clientName = "Client_" + topic + "_" + partition
      var consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName)
      var readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime,
        clientName)
      var numErrors = 0
      while (maxReads > 0) {
        if (consumer == null) {
          consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName)
        }

        val req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset,
          100000)
          .build()
        val fetchResponse = consumer.fetch(req)
        if (fetchResponse.hasError) {
          numErrors += 1
          val code = fetchResponse.errorCode(topic, partition)
//          result.errors :+ ("Error fetching data from the Broker:" + leadBroker + " Reason: " + code)
          if (numErrors > 5) //break
            if (code == ErrorMapping.OffsetOutOfRangeCode) {
              readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime, clientName)
              //continue
            }
          consumer.close()
          consumer = null
          leadBroker = findNewLeader(leadBroker, topic, partition)

          //continue
        }
        numErrors = 0
        var numRead = 0
        for (messageAndOffset <- fetchResponse.messageSet(topic, partition)) {
          val currentOffset = messageAndOffset.offset
          if (currentOffset < readOffset) {
//            println("Found an old offset: " + currentOffset + " Expecting: " + readOffset)
            //continue
          }
          readOffset = messageAndOffset.nextOffset
          val payload = messageAndOffset.message.payload
          val bytes = Array.ofDim[Byte](payload.limit())
          payload.get(bytes)
          result.messages :+ (String.valueOf(messageAndOffset.offset) + ": " + new String(bytes, "UTF-8"))
          numRead += 1
          maxReads -= 1
        }
        if (numRead == 0) {
          if (consumer != null) consumer.close()
          maxReads = 0
        }
      }
      if (consumer != null) consumer.close()

      result
    }

    def send(topic: String, partition: Int, message: String) {

      val props: Properties = new Properties()

      val setting: Option[util.List[String]] = Play.configuration.getStringList("metadata.broker.list")
      props.put("metadata.broker.list", setting.get(0))
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("request.required.acks", "1")

      val producer: Producer[String,String] = new Producer[String, String](new ProducerConfig(props))
      val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, partition.toString, message)

      producer.send(data)
      producer.close()
    }

  }
}