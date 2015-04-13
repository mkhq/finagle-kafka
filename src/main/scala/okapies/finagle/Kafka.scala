package okapies.finagle

import com.twitter.finagle.{Client, Name, Stack, Service, ServiceFactory, param}
import com.twitter.finagle.client.{Bridge, DefaultClient, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.stats.StatsReceiver

import okapies.finagle.kafka.protocol.{KafkaBatchClientPipelineFactory, KafkaStreamClientPipelineFactory, Request, Response}

trait KafkaRichClient { self: Client[Request, Response] =>

  def newRichClient(dest: String): kafka.Client = kafka.Client(newService(dest))

  def newRichClient(dest: Name, label: String): kafka.Client = kafka.Client(newService(dest, label))

}

object Kafka extends Client[Request, Response] with KafkaRichClient {


  object Client {
    val stack: Stack[ServiceFactory[Request, Response]] = StackClient.newStack

//      .replace(StackClient.Role.pool, new SingletonPool.module[Request, Response])
  }

  case class Client(
    stack: Stack[ServiceFactory[Request, Response]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams
  ) extends StdStackClient[Request, Response, Client] {

    protected type In = Request
    protected type Out = Response

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ) = copy(stack, params)

    protected def newTransporter(): Transporter[In, Out] = {
      Netty3Transporter(KafkaBatchClientPipelineFactory, params)
    }

    protected def newDispatcher(
      transport: Transport[Request, Response]
    ): Service[Request, Response] = {
      val param.Stats(sr) = params[param.Stats]
      val param.Label(name) = params[param.Label]
      new PipeliningDispatcher(transport)
    }

  }

  val client = Client()

  def newClient(dest: Name, label: String) = client.newClient(dest, label)
}
