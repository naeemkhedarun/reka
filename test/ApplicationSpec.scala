import controllers.ApplicationController
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import org.specs2.mock._

import play.api.test._
import play.api.test.Helpers._
import services.MessageConsumerComponent

trait TestEnvironment extends MessageConsumerComponent with ApplicationController with Mockito
{
  val messageConsumer = mock[KafkaMessageConsumer]
}

@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends Specification with TestEnvironment {

  "getting by topic" should {

    "return list of messages" in {
      val result = this.get("topic","key","group")(FakeRequest())
      there was one(messageConsumer).get("topic", "key", "group")
      status(result) must beEqualTo(OK)
    }
  }
}
