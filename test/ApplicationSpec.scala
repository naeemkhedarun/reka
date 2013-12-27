import controllers.ApplicationController
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import org.specs2.mock._

import play.api.test._
import play.api.test.Helpers._
import services.{TopicMessages, MessageConsumerComponent}

trait TestEnvironment extends MessageConsumerComponent with ApplicationController with Mockito
{
  val messageConsumer = mock[KafkaMessageConsumer]
}

@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends Specification with TestEnvironment {

  "getting by topic" should {

    messageConsumer.get("topic", 0) returns new TopicMessages

    "return list of messages" in {
      val result = this.get("topic",0)(FakeRequest())
      there was one(messageConsumer).get("topic", 0)
      status(result) must beEqualTo(OK)
    }
  }
}
