package com.jasonsatran.spark.meta.helper

import org.scalatest._
import Helper.{round, percentage, isEmpty, isNumeric}

class HelperSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers {

  describe("Helper") {
    it("should round") {
      val x = 123.4567
      val actual =round(x)
      assert(actual === 123.5)
    }

    it("should execute percent fill"){
      assert(percentage(7,9)===77.8)
    }

    describe ("isEmpty") {
      it("detects empty values") {
        case class test(input: String, expected: Boolean)
        val tests = List(
          test("someting", false)
          , test("",  true)
          , test(null, false)
          ,test("   ", true)
        )
        tests.foreach { (x: test) =>
          val actual = isEmpty(x.input)
          actual == x.expected
          assert(actual === x.expected)
        }
      }
    }

    describe ("isNumeric"){
      it("returns true if the value is numeric"){
        case class test(input: String, expected: Boolean)
        val tests = List(
          test("abc", false),
          test("1", true),
          test("-1", true),
          test(".001", true),
          test(".001a", false),
          test("10.123456789", true)
        )

        tests.foreach { (x: test) =>
          val actual = isNumeric(x.input)
          actual == x.expected
          assert(actual === x.expected)
        }
      }
    }

  }
}