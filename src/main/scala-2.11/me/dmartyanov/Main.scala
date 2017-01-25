package me.dmartyanov

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by dmartyanov on 25/1/17.
  */
object Main extends App {
  implicit val system: ActorSystem = ActorSystem("test")
  implicit val ec: ExecutionContext = system.dispatcher

  val dataNodes = 10
  val iterations = 100
  //val delayConf = (0 until dataNodes).map(_ -> 200).toMap  //when all nodes have latency
  val delayConf = Map(1 -> 100)                               //when only one node has latency
  val actors = (0 until dataNodes).map { index =>
    index -> system.actorOf(
      delayConf.get(index).map(delay => Props(new DataDelayedActor(delay))).getOrElse(Props(new DataActor()))
    )
  } toMap

  val manager = system.actorOf(Props(new Producer(iterations, actors)))

  Thread.sleep(10000)
  system.terminate().onComplete(_ => println("Actor system is terminated"))
}

class Producer(iterations: Int, val actors: Map[Int, ActorRef]) extends Actor with ActorLogging {

  val counters = new scala.collection.mutable.HashMap[Int, Int]()
  val workersSet = actors.values.toSet
  workersSet.foreach(a => a ! SetupActor((workersSet - a).toSeq))
  Thread.sleep(1000)

  val rnd = new java.util.Random(System.currentTimeMillis())
  for (i <- 1 to iterations) {
    val index = rnd.nextInt(actors.size)
    actors.get(index).foreach(_ ! SetMessage(DataValue(i)))
    if(i == iterations) log.info(s"Iteration $i goes to $index")
    if(!counters.contains(index)) counters.put(index, 0)
    else counters.put(index, counters(index) + 1)
  }

  Thread.sleep(1000)
  context.self ! Print

  override def receive: Receive = {
    case Print =>
      actors.values.foreach(_ ! Print)
      Thread.sleep(300)
      log.info(counters.map { case (k, v) => s"$k : $v" } mkString ", ")
  }
}

class DataActor extends Actor with ActorLogging {

  var value = DataValue(0)
  var others: Seq[ActorRef] = Seq()

  override def receive: Receive = {
    case SetupActor(as) =>
      others = as
      log.info(s"Regular Actor ${context.self.path} is set up")
      context.become(active)
  }

  def active: Receive = {
    case SetMessage(dv) =>
      value = dv
      others.foreach(_ ! Replicate(dv))
    case Replicate(dv) =>
      value = dv
    case Print =>
      log.info(s"Actor ${context.self.path} has value { ${value.value} }")
  }
}


class DataDelayedActor(val msDelay: Int) extends Actor with ActorLogging {
  val rnd = new java.util.Random(System.currentTimeMillis())
  var value = DataValue(0)
  var others: Seq[ActorRef] = Seq()

  import context.dispatcher

  override def receive: Receive = {
    case SetupActor(as) =>
      others = as
      log.info(s"Delayed Actor ${context.self.path} is set up with max delay ${msDelay}")
      context.become(active)
  }

  def active: Receive = {
    case SetMessage(dv) =>
      value = dv
    case ActualSetMessage(dv) =>
      value = dv
      others.foreach(replica =>
        context.system.scheduler.scheduleOnce(rnd.nextInt(msDelay) milliseconds)(replica ! Replicate(dv))
      )
    case Replicate(dv) =>
      context.system.scheduler.scheduleOnce(rnd.nextInt(msDelay) milliseconds)(context.self ! ActualSetMessage(dv))
    case Print =>
      log.info(s"Actor ${context.self.path} has value { ${value.value} }")
  }
}

case class SetupActor(others: Seq[ActorRef])

case class DataValue(value: Int)

case class SetMessage(dv: DataValue)

case class ActualSetMessage(dv: DataValue)
case class ActualReplication(dv: DataValue, a: ActorRef)

case class Replicate(dv: DataValue)

case object Print
