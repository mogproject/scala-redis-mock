package com.github.mogproject.redismock

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global


class MockPoolSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val clients = TestUtil.getRedisClientPool

  override def beforeEach = {
  }

  override def afterEach = clients.withClient{
    client => client.flushdb
  }

  override def afterAll = {
    clients.withClient{ client => client.disconnect }
    clients.close
  }

  def lp(msgs: List[String]) = {
    clients.withClient {
      client => {
        msgs.foreach(client.lpush("list-l", _))
        client.llen("list-l")
      }
    }
  }

  def rp(msgs: List[String]) = {
    clients.withClient {
      client => {
        msgs.foreach(client.rpush("list-r", _))
        client.llen("list-r")
      }
    }
  }

  def set(msgs: List[String]) = {
    clients.withClient {
      client => {
        var i = 0
        msgs.foreach { v =>
          client.set("key-%d".format(i), v)
          i += 1
        }
        Some(1000L)
      }
    }
  }

  describe("pool test") {
    it("should distribute work amongst the clients") {
      val l = (0 until 5000).map(_.toString).toList
      val fns = List[List[String] => Option[Long]](lp, rp, set)
      val tasks = fns map (fn => Future { fn(l) })
      val results = Await.result(Future.sequence(tasks), 20.seconds)
      results should equal(List(Some(5000), Some(5000), Some(1000)))
    }

  }
}
