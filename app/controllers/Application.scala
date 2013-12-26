package controllers

import play.api.mvc._
import services.MessageConsumerComponent
import play.api.libs.json.Json

object Application extends ApplicationController {
  val messageConsumer = new KafkaMessageConsumer
}

trait ApplicationController extends Controller with MessageConsumerComponent {

  def index() = Action {
    val topics = this.messageConsumer.listTopics()

    Ok(Json.obj(
      "Ok" -> true,
      "topics" -> Json.toJson(topics),
      "routes" -> Json.arr("/topic")
    ))
  }

  def getPartitions(topic: String) = Action {
    val partitions = this.messageConsumer.listPartitions(topic)
    Ok(Json.obj(
      "topic" -> topic,
      "partitions" -> Json.toJson(partitions)
    ))
  }

  def get(topic: String, partition: Int) = Action {
    val messages = this.messageConsumer.get(topic, partition)

    if(messages.errors.length > 0)
      NotFound(Json.obj("errors" -> Json.toJson(messages.errors)))

    Ok(Json.obj(
      "messages" -> Json.toJson(messages.messages)
    ))
  }

  def send(topic: String, partition: Int) = Action(parse.text) {
    request =>

      val body: String = request.body

      this.messageConsumer.send(topic, partition, body)
      Ok(Json.obj())
  }
}
