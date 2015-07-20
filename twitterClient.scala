import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import scala.collection.mutable.ArrayBuffer
import akka.routing.RoundRobinRouter
import java.security.MessageDigest
import java.util.UUID
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import scala.util.Random
import akka.actor.Scheduler
import scala.concurrent.ops._
import scala.concurrent.duration._
import akka.actor.Cancellable


sealed trait TwitterMessage
case object RequestWork extends TwitterMessage
case class GetWork extends TwitterMessage
case class Tweeting(userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
case class UserTimeline(userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
case class HomeTimeline(userId: Array[Int],tweetList:Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
case class TweetResult(result:Int, tweet:String) extends TwitterMessage
case class UserTimelineResult(id:Int,userId:Array[Int],ownList:Array[Array[String]], ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
case class HomeTimelineResult(id:Int,userId:Array[Int],tweetList:Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
case class Tweet(start: Int, nrOfElements: Int,userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
case class UserProfile(start: Int, nrOfElements: Int,userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
case class HomeProfile(start: Int, nrOfElements: Int,userId: Array[Int],tweetList:Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
case class TweetsGenerated(id:Int, tweet: String) extends TwitterMessage
case class UserTimelineGenerated(id:Int,userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
case class HomeTimelineGenerated(id:Int,userId: Array[Int],tweetList:Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
case class UpdateTweet(id:Int,tweet:String) extends TwitterMessage
case class UpdateUserTimeline(id:Int,userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
case class UpdateHomeTimeline(id:Int,userId: Array[Int],tweetList:Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
case class RemoteTweetMessage(id:Int, tweet:String) extends TwitterMessage
case class RemoteUpdateUserTimeline(id:Int,userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
case class RemoteUpdateHomeTimeline(id:Int,userId: Array[Int],tweetList:Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
case class GiveWork(userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int],tweetList:Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
case class PrintingUserTimeline(ownListId:Array[String], ownCountId:Int,ownRecentCountId:Int) extends TwitterMessage
case class PrintingHomeTimeline(tweetListId: Array[String],tweetCountId:Int,tweetRecentCountId:Int)extends TwitterMessage
case class DisplayingUserTimeline(ownListId:Array[String], ownCountId:Int,ownRecentCountId:Int) extends TwitterMessage
case class DisplayingHomeTimeline(tweetListId:Array[String], tweetCountId:Int,tweetRecentCountId:Int) extends TwitterMessage



object twitterClient {
  
  def main(args: Array[String]) {
   val hostname = InetAddress.getLocalHost.getHostName
   printf("hmmm")
    val config = ConfigFactory.parseString(
      """akka{
		  		actor{
		  			provider = "akka.remote.RemoteActorRefProvider"
		  		}
		  		remote{
                   enabled-transports = ["akka.remote.netty.tcp"]
		  			netty.tcp{
						hostname = "127.0.0.1"
						port = 0
					}
				}     
    	}""")
      var nrOfWorkers : Int = 4
      var n:Int=args(1).toInt
      var nrOfTweetMessages:Int = (0.3*n).toInt
      var nrOfTweetElements:Int = (0.5*n).toInt
      var nrOfUserTimelineMessages:Int = n
      var nrOfUserTimelineElements:Int = 3*n
      var nrOfHomeTimelineMessages:Int = 2*n
      var nrOfHomeTimelineElements:Int = 4*n
      //var totalRequests:Int = (nrOfTweetMessages*nrOfTweetElements)+(nrOfUserTimelineMessages*nrOfUserTimelineElements)+(nrOfHomeTimelineMessages*nrOfHomeTimelineElements)


    	implicit val system = ActorSystem("ClientBitCoinSystem", ConfigFactory.load(config))
    	printf("gone")
    	val resultSender = system.actorOf(Props(new ResultSender(nrOfWorkers,args(0))),name= "resultSender")
    	val listener = system.actorOf(Props(new Listener(resultSender:ActorRef)), name = "listener")
    	val master2= system.actorOf(Props(new Master2(nrOfWorkers, nrOfUserTimelineMessages,nrOfUserTimelineElements,listener)), name = "master2")
    	val master3= system.actorOf(Props(new Master3(nrOfWorkers, nrOfHomeTimelineMessages,nrOfHomeTimelineElements,listener)), name = "master3")
    	val master1= system.actorOf(Props(new Master1(nrOfWorkers, nrOfTweetMessages,nrOfTweetElements,listener)), name = "master1")
    	val workRequestActor =    system.actorOf(Props(new WorkRequestActor( args(0),master1,master2,master3)),name = "workRequestActor")
    	
    	workRequestActor ! RequestWork   
    	
    	
  }


class WorkRequestActor(serverAddress : String, master1: ActorRef, master2:ActorRef,master3:ActorRef) extends Actor
    	{
  
    	val remote = context.actorFor("akka.tcp://ServerBitCoinSystem@" + serverAddress + ":3000/user/RemoteActor")
    	def receive = {
    
    	case RequestWork =>
    	  printf("Requesting Server for Tweet")
    		remote ! GetWork

    	case GiveWork(userId, ownList, ownCount,ownRecentCount,tweetList,tweetCount,tweetRecentCount)=>
    	  println("work received from server :" )
    	  
    		master1 ! Tweeting(userId, ownList, ownCount,ownRecentCount) 
    		master2 ! UserTimeline(userId,ownList, ownCount,ownRecentCount)
    		master3 ! HomeTimeline(userId,tweetList,tweetCount,tweetRecentCount)

    	}
  }

class Master1(nrOfWorkers: Int, nrOfTweetMessages: Int, nrOfTweetElements: Int,listener:ActorRef) extends Actor {
  
  var nrOfResults: Int = _

    val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")
   def receive = {
    
     case Tweeting(userId, ownList, ownCount,ownRecentCount)  =>
         for (i <- 0 until nrOfTweetMessages) workerRouter ! Tweet(i * nrOfTweetElements, nrOfTweetElements,userId,ownList, ownCount,ownRecentCount)
         
       case TweetResult(id, tweet) =>
         
         listener ! TweetsGenerated(id, tweet)
         nrOfResults += 1
        
        if (nrOfResults == nrOfTweetMessages*nrOfTweetElements) {
  
          context.stop(self)
        } 
  }
}

class Master2(nrOfWorkers: Int, nrOfUserTimelineMessages: Int, nrOfUserTimelineElements: Int,listener:ActorRef) extends Actor {
  
  var nrOfResults: Int = _
  
  val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")

  
   def receive = {
    
     case UserTimeline(userId, ownList, ownCount,ownRecentCount)  =>
         for (i <- 0 until nrOfUserTimelineMessages) workerRouter ! UserProfile(i * nrOfUserTimelineElements, nrOfUserTimelineElements,userId,ownList, ownCount,ownRecentCount)
         
       case UserTimelineResult(id,userId,ownList, ownCount,ownRecentCount) =>
         
         listener ! UserTimelineGenerated(id,userId,ownList, ownCount,ownRecentCount)
         nrOfResults += 1
        
        if (nrOfResults == nrOfUserTimelineMessages*nrOfUserTimelineElements) {
   
          context.stop(self)
        } 
  }
}

class Master3(nrOfWorkers: Int, nrOfHomeTimelineMessages: Int, nrOfHomeTimelineElements: Int,listener:ActorRef) extends Actor {
  
  var nrOfResults: Int = _
  
  val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")

  
   def receive = {
    
     case HomeTimeline(userId,tweetList,tweetCount,tweetRecentCount) =>
         for (i <- 0 until nrOfHomeTimelineMessages) workerRouter ! HomeProfile(i * nrOfHomeTimelineElements, nrOfHomeTimelineElements,userId,tweetList,tweetCount,tweetRecentCount)
         
       case HomeTimelineResult(id,userId,tweetList,tweetCount,tweetRecentCount) =>
         
         listener ! HomeTimelineGenerated(id,userId,tweetList,tweetCount,tweetRecentCount)
         nrOfResults += 1
        
        if (nrOfResults == nrOfHomeTimelineMessages*nrOfHomeTimelineElements) {

          context.stop(self)
        } 
  }
}


class Worker extends Actor {
  
  
  var k:Long = 0
    def receive = {
        case Tweet(start, nrOfTweetElements, userId,ownList, ownCount,ownRecentCount) =>
        
        for(i<-start until start+nrOfTweetElements)
      {    
 
         val rand = new Random(System.currentTimeMillis());
         val random_index = rand.nextInt(userId.length);
         val id:Int = userId(random_index)
         val tweet: String =  UUID.randomUUID().toString()
         
         if(ownCount(id)>= 100 && ownRecentCount(id)<100)
         { ownList(id)(ownRecentCount(id))=tweet
           ownRecentCount(id)=ownRecentCount(id)+1  
         }
         else if(ownCount(id)>=100 && ownRecentCount(id)>=100 )
         {
          ownRecentCount(id)=0
          ownList(id)(ownRecentCount(id))=tweet
           ownRecentCount(id)=ownRecentCount(id)+1
         }
         else
         {
           ownList(id)(ownCount(id))=tweet
          ownCount(id) = ownCount(id)+1
         }
         
         
          
          sender ! TweetResult(id, tweet)         
    }
        
        case UserProfile(start, nrOfUserTimelineElements, userId,ownList, ownCount,ownRecentCount) =>
      //  printf("entered the work-printing")
        for(i<-start until start+nrOfUserTimelineElements)
      {    

         
         val rand = new Random(System.currentTimeMillis());
         val random_index = rand.nextInt(userId.length);
         val id:Int = userId(random_index)
   
         
         
          sender ! UserTimelineResult(id,userId,ownList, ownCount,ownRecentCount) 
  
       }
        
         case HomeProfile(start, nrOfHomeTimelineElements, userId,tweetList,tweetCount,tweetRecentCount) =>
       // printf("entered for timeline printing \n")
        for(i<-start until start+nrOfHomeTimelineElements)
      {    
         
         val rand = new Random(System.currentTimeMillis());
         val random_index = rand.nextInt(userId.length);
         val id:Int = userId(random_index)
 
         
          sender ! HomeTimelineResult(id,userId,tweetList,tweetCount,tweetRecentCount) 
  
       }
        
        
      case DisplayingUserTimeline(ownListId,ownCountId,ownRecentCountId)=>
 
      if(ownCountId<100)
      {var i:Int = ownCountId-1
        while(i>=0 )
        {
        // printf("the tweet is %s \n",ownListId(i))
          i=i-1
        }
      }
      else
      { var i:Int=ownRecentCountId
        var j:Int=99
        //var f:Int=0
        while(i>=0)
        {
         // printf("the tweet is %s", ownListId(i))
          i= i-1
        }
       
        while(j!=ownRecentCountId)
        { // printf("the tweet is %s", ownListId(j))
          j=j-1
        }      
      }
        
        case DisplayingHomeTimeline(tweetListId,tweetCountId,tweetRecentCountId)=>
       
      //  printf("final timeline printing")
      if(tweetCountId<100)
      {var i:Int = tweetRecentCountId
        while(i>=0)
        { 
         //printf("the timeline tweet is %s \n",tweetListId(i))
          i=i-1
        }
      }
      else
      { var i:Int=tweetRecentCountId
        var j:Int=99
        //var f:Int=0
        while(i>=0)
        {
        //  printf("the timeline tweet is %s", tweetListId(i))
          i= i-1
        }
       
        while(j!=tweetRecentCountId)
        { // printf("the timeline tweet is %s", tweetListId(j))
          j=j-1
        }      
      }
        
    
}

}
class Listener(resultSender: ActorRef) extends Actor{
  def receive = {
    case TweetsGenerated(id, tweet) =>
      
        resultSender ! UpdateTweet(id,tweet)
      
      case UserTimelineGenerated(id,userId,ownList, ownCount,ownRecentCount) =>
      
        resultSender ! UpdateUserTimeline(id,userId,ownList, ownCount,ownRecentCount)
        
        case HomeTimelineGenerated(id,userId,tweetList,tweetCount,tweetRecentCount) =>
      
        resultSender ! UpdateHomeTimeline(id,userId,tweetList,tweetCount,tweetRecentCount)
      
  }
}

class ResultSender(nrOfWorkers: Int,serverAddress : String) extends Actor{
 //println("akka.tcp://ServerBitCoinSystem@" + serverAddress + ":3000/user/RemoteActor")
  // create the remote actor
  val remote = context.actorFor("akka.tcp://ServerBitCoinSystem@" + serverAddress + ":3000/user/RemoteActor")
  val workerRouter= context.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")
  def receive = {
   
    case UpdateTweet(id,tweet) =>
       remote ! RemoteTweetMessage(id, tweet)

    case UpdateUserTimeline(id,userId,ownList, ownCount,ownRecentCount) =>
       remote ! RemoteUpdateUserTimeline(id,userId,ownList, ownCount,ownRecentCount)

       
     case UpdateHomeTimeline(id,userId,tweetList,tweetCount,tweetRecentCount) =>
       remote ! RemoteUpdateHomeTimeline(id,userId,tweetList,tweetCount,tweetRecentCount)
      
    case PrintingUserTimeline(ownListId,ownCountId,ownRecentCountId) =>
       workerRouter!DisplayingUserTimeline(ownListId,ownCountId,ownRecentCountId) 
       
     case PrintingHomeTimeline(tweetListId,tweetCountId,tweetRecentCountId) =>
       workerRouter!DisplayingHomeTimeline(tweetListId,tweetCountId,tweetRecentCountId)
       
    case _ =>
      println("Unknown message in Proxyactor")
  }
}




}
