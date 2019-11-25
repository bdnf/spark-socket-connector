import DataModel.{Group, MeetupRSVGevent}
import net.liftweb.json.{JsonParser, Serialization, ShortTypeHints}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.internal.Logging
import scalawebsocket.WebSocket

object DataModel {
    case class MeetupRSVGevent(member: Option[Member], response: String, visibility: String, event: Option[Event],
                               mtime: Long, guests: Int, rsvp_id: Long, group: Group, venue: Option[Venue])
    case class Member( member_name: String, photo: Option[String], member_id: String)
    case class Venue( lon: Double, venue_name: String, venue_id: Int, lat: Double)
    case class Group( group_name: String, group_city: String, group_lat: Double, group_urlname: String, group_id: String,
                      group_country: String, group_lon: Double, group_topics: List[Group_topics] )
    case class Event( time:  Option[Long], event_url: String, event_id: String, event_name: String)
    case class Group_topics( urlkey: String, topic_name: String)
}


class WebSocketStreamReceiver (webSocketURL:String) extends Receiver[MeetupRSVGevent](StorageLevel.MEMORY_AND_DISK_2) with Logging{
    var webSocket : WebSocket = _

    override def onStart(): Unit = {
        logInfo(s"Starting ${getClass.getCanonicalName}")
        ///Start the thread that receives data over a connection
        new Thread("Socket Receiver") {
            override def run() {
                receive()
            }
        }.start()
    }

    override def onStop(): Unit = {
    }

    private def receive(): Unit = {
        webSocket = WebSocket().open(webSocketURL)
        webSocket.onTextMessage(msg => {
            store(toMeetupRSVGevent(msg))
        })
    }

    //TODO it should be in the DataModel Object
    private def toMeetupRSVGevent(msg: String): MeetupRSVGevent ={
        implicit val formats = net.liftweb.json.DefaultFormats
        implicit val formats2 = Serialization.formats(ShortTypeHints(List(classOf[Group])))

        JsonParser.parse(msg).extract[MeetupRSVGevent]
    }
}

object SparkSocketConnector extends App {


    val url = "wss://stream.meetup.com/2/rsvps"

    Logger.getLogger(this.getClass.getName).setLevel(Level.WARN)


    val spark = SparkSession.builder.appName("Meetup app")
      .config("spark.master", "local[2]")
      .getOrCreate()

    //spark.sparkContext.setLogLevel("WARN")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(60))

    val lines = ssc.receiverStream(new WebSocketStreamReceiver(url))

    //  lines.map(event => event.member.get)
    lines.map(event => {
      //println(event.event.get.event_name)
      ((event.group.group_country, 1))
    }).countByValue().print()

    ssc.start()
    ssc.awaitTermination()

}