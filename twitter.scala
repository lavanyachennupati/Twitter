import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.routing.RoundRobinRouter
import java.security.MessageDigest
import scala.util.Random
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import akka.actor.Scheduler
import scala.concurrent.ops._
import scala.concurrent.duration._
import akka.actor.Cancellable



  sealed trait TwitterMessage
  case class RemoteTweetMessage(id:Int,tweet:String) extends TwitterMessage
  case class GetWork extends TwitterMessage
  case class RemoteUpdateUserTimeline(id:Int,userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends TwitterMessage
  case class RemoteUpdateHomeTimeline(id:Int,userId: Array[Int],tweetList: Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
  case class GiveWork(userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int],tweetList:Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int]) extends TwitterMessage
  case class UpdatingTweet(id: Int, tweet:String) extends TwitterMessage  
  case class UpdatingUserTimeline(id: Int,userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int],s:ActorRef) extends TwitterMessage
  case class UpdatingHomeTimeline(id: Int, userId: Array[Int],tweetList: Array[Array[String]],tweetCount:Array[Int],tweetRecentCount:Array[Int],s:ActorRef) extends TwitterMessage
  case class PrintingUserTimeline(ownListId:Array[String], ownCountId:Int,ownRecentCountId:Int)extends TwitterMessage
  case class PrintingHomeTimeline(tweetListId: Array[String],tweetCountId:Int,tweetRecentCountId:Int)extends TwitterMessage
  
  

//Sends messageToBeSent to receiverActor after initialDelayBeforeSending and then after each delayBetweenMessages



    
  
object twitter extends App {
  
  
    var n:Int=args(0).toInt
    var n1:Int=(0.82*n).toInt
    var n2:Int=(0.06*n).toInt
    var n3:Int=(0.09*n).toInt
    var p1:Int=(0.05*n).toInt
    var p2:Int=(0.1*n).toInt
    var p3:Int=(0.2*n).toInt
    var p4:Int=(0.5*n).toInt
    var userId: Array[Int] = new Array(n)
    var shuffled:Array[java.lang.Integer]=new Array(n)
    var followerIds= Array.ofDim[Array[Int]](n)
    var tweetList= Array.ofDim[Array[String]](n)
    var tweetCount: Array[Int]= new Array(n)
    var ownList= Array.ofDim[Array[String]](n)
    var ownCount: Array[Int]= new Array(n)
    var ownRecentCount: Array[Int]= new Array (n)
    var tweetRecentCount:Array[Int]= new Array(n)
    var nrOfWorkers : Int = 4
    val Tick:String = "tick"
    var totalNoOfRequests:Int= 0
 

   val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
      """ 
     akka{ 
    		actor{ 
    			provider = "akka.remote.RemoteActorRefProvider" 
    		} 
    		remote{ 
                enabled-transports = ["akka.remote.netty.tcp"] 
            netty.tcp{ 
    			hostname = "127.0.0.1" 
    			port = 3000 
    		} 
      }      
    }""")
   

 
    // Create an Akka system
    val system = ActorSystem("ServerBitCoinSystem", ConfigFactory.load(config))
     import system.dispatcher
     
     
     
       
    val remoteActor = system.actorOf(Props(new RemoteActor(userId,ownList,ownCount,ownRecentCount)), name = "RemoteActor")
   // remoteActor ! "The Server is Up & Running"
   
     val receiveActor = system.actorOf(Props(new ReceiveActor(cancellable)), name="ReceiveActor") 
    
     val cancellable: Cancellable = system.scheduler.schedule(25000 milliseconds,100 milliseconds,receiveActor, Tick)
    
    for(i:Int<-0 until n) 
    {
      tweetList(i)= Array.ofDim[String](n)
      ownList(i)= Array.ofDim[String](n)
    }
    
   for(i: Int<-0 until n)
   {
     userId(i) = i
     shuffled(i)=i
   }
    

   for (i<-0 until n1)
   { 
     java.util.Collections.shuffle(java.util.Arrays.asList(shuffled:_*))
     followerIds(i)= Array.ofDim[Int](p1)
     for (j<-0 until p1)
     {
       followerIds(i)(j)= shuffled(j)
     }
   }
  
    for (i<-n1 until n2)
    {
      java.util.Collections.shuffle(java.util.Arrays.asList(shuffled:_*))
      followerIds(i)= Array.ofDim[Int](p2)
      for (j<-0 until p2)
       {
       followerIds(i)(j)= shuffled(j)
       } 
        
    }
    
    for (i<-n2 until n3)
   { 
     java.util.Collections.shuffle(java.util.Arrays.asList(shuffled:_*))
     followerIds(i)= Array.ofDim[Int](p3)
     for (j<-0 until p3)
     {
       followerIds(i)(j)= shuffled(j)
     }
   }
    for (i<-n3 until n)
   { 
     java.util.Collections.shuffle(java.util.Arrays.asList(shuffled:_*))
     followerIds(i)= Array.ofDim[Int](p4)
     for (j<-0 until p4)
     {
       followerIds(i)(j)= shuffled(j)
     }
   }
    
  //def main(args: Array[String]) {}
  
  class ReceiveActor(cancellable:Cancellable) extends Actor
  { var i:Int=0
   var j:Int=0
    def receive=
    { 
      case Tick =>
       
        j= totalNoOfRequests-i
        i= totalNoOfRequests 
        
        printf("The number of requests in this one second is %d \n", j)
        
        if(j == 0)
        {cancellable.cancel()
          val stop: Long = System.currentTimeMillis
          printf("the time all the requests are processed is %d",stop)
          
          } 
       
  }
  }
  
  class RemoteActor(userId: Array[Int],ownList: Array[Array[String]],ownCount:Array[Int],ownRecentCount:Array[Int]) extends Actor 
  {
    
    val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")
     
  def receive = 
  {
    
    case GetWork => 
     val start: Long = System.currentTimeMillis
      printf("The starting time of the requests is %d",start )
       sender!GiveWork(userId, ownList, ownCount,ownRecentCount,tweetList,tweetCount,tweetRecentCount)
     printf("server assigning work")
    
    case RemoteTweetMessage(id, tweet) => 
        workerRouter ! UpdatingTweet(id,tweet)
        
    case RemoteUpdateUserTimeline(id,userId,ownList, ownCount,ownRecentCount) =>
        var s= sender
      workerRouter ! UpdatingUserTimeline(id,userId,ownList, ownCount,ownRecentCount,s)
        
      case RemoteUpdateHomeTimeline(id,userId,tweetList,tweetCount,tweetRecentCount) =>     
        var s= sender
      workerRouter ! UpdatingHomeTimeline(id,userId,tweetList,tweetCount,tweetRecentCount,s)
   
    case _ => 
      println("Unknown Mesasge Recevied at Server")
        
  } 
  }
      
   class Worker extends Actor {
  
    def receive = {
        case UpdatingTweet(id,tweet) =>  

       val k:Int= followerIds(id).length
       for(j<-0 until k)
       {
         val fid:Int=followerIds(id)(j)
         
         if(tweetCount(fid)>=100 && tweetRecentCount(fid)<100)
         {
           
           tweetList(fid)(tweetRecentCount(fid))=tweet
           tweetRecentCount(fid)=tweetRecentCount(fid)+1  
           
         }
         else if(tweetCount(fid)>=100 && tweetRecentCount(fid)>=100)
           
         {
           tweetRecentCount(fid)= 0
           tweetList(fid)(tweetRecentCount(fid))=tweet
           tweetRecentCount(fid)= tweetRecentCount(fid)+1
           
         }
         
         else
         {
           tweetList(fid)(tweetCount(fid)) = tweet
          tweetCount(fid)=tweetCount(fid)+1
        }
       
      }   
       totalNoOfRequests=totalNoOfRequests+1
        
      
    case UpdatingUserTimeline(id,userId,ownList, ownCount,ownRecentCount,s)=>
       var ownListId:Array[String]= ownList(id)
       var ownCountId:Int = ownCount(id)
       var ownRecentCountId:Int= ownRecentCount(id)
      s!PrintingUserTimeline(ownListId,ownCountId,ownRecentCountId)
      totalNoOfRequests=totalNoOfRequests+1

      
       case UpdatingHomeTimeline(id,userId,tweetList,tweetCount,tweetRecentCount,s)=>
         //printf("entered printtimeline \n")
       var tweetListId:Array[String]= tweetList(id)
       var tweetCountId:Int = tweetCount(id)
       var tweetRecentCountId:Int= tweetRecentCount(id)
      s!PrintingHomeTimeline(tweetListId,tweetCountId,tweetRecentCountId)
      totalNoOfRequests=totalNoOfRequests+1
      

         
  }
   }
}
   
   
   

   
     
   
    

    
    
    
    
