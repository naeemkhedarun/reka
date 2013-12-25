package controllers

import play.api.mvc._
import services.MessageConsumerComponent

object Application extends ApplicationController {
  val messageConsumer = new KafkaMessageConsumer
}

trait ApplicationController extends Controller with MessageConsumerComponent {

  def get(topic: String, key: String, groupId: String) = Action {
    this.messageConsumer.get(topic, key, groupId)
    Ok("done")
  }

  def send(topic: String, key: String) = Action(parse.text) {
    request =>

      val body: String = request.body

      this.messageConsumer.send(topic, key, body)

      Ok("received: " + topic + ", " + key + "," + body)
  }
}
