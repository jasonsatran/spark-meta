package com.jasonsatran.spark.meta.helper

import org.scalatest._
import Helper.{round,percentFill,isEmpty}

class HelperSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers {

  describe("Helper") {
    it("should round") {
      val x = 123.4567
      val actual =round(x)
      assert(actual === 123.5)
    }

    it("should execute percent fill"){
      assert(percentFill(7,9)===77.8)
    }

    describe ("isEmpty") {
      it("detects empty values") {
        case class test(input: String, expected: Boolean)
        val tests = List(
          test("someting", false)
          , test("", true)
          , test(null, true)
          ,test("   ", true)
        )
        tests.foreach { (x: test) =>
          val actual = isEmpty(x.input)
          actual == x.expected
          assert(actual===x.expected)
        }
      }
    }
  }
}