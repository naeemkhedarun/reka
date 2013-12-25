import controllers.ApplicationController
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import org.specs2.mock._

import play.api.test._
import play.api.test.Helpers._
import services.{MessageConsumer}

@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends Specification with Mockito {

  var consumer = mock[MessageConsumer]

  object MockApplication extends ApplicationController { def messageConsumer = consumer }

  "getting by topic" should {

    "return list of messages" in {
      val result = MockApplication.get("topic","key","group")(FakeRequest())
      there was one(consumer).get("topic", "key", "group")
      status(result) must beEqualTo(OK)
    }
  }
}
