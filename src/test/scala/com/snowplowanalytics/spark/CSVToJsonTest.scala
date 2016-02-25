/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */



package com.snowplowanalytics.spark

// Java
import java.io.{FileWriter, File}

// Scala
import scala.io.Source

// Google Guava
import com.google.common.io.Files

// Specs2
import org.specs2.mutable.Specification



class CSVToJsonTest extends Specification {

  "A Conversion job" should {

    "Parse correctly" in {

      val tempDir = Files.createTempDir()

      val inputFile = new File("input.csv").getAbsolutePath
      val outputDir = new File(tempDir, "output").getAbsolutePath

      CSVToJSON.execute(
        master = Some("local"),
        args = List(inputFile, outputDir, "true", "false")
      )

      val outputFile = new File(outputDir, "part-00000")
      val actual = Source.fromFile(outputFile).getLines.map(_.split("\n")).next()
      actual contains  "{\"bidderrate\":\"0\",\"price\":\"177.5\",\"bidtime\":\"2.230949\",\"openbid\":\"99\",\"auctionid\":\"1638893549\",\"bid\":\"175\",\"bidder\":\"schadenfreud\"}"
    }
  }

}
