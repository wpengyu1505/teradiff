package io.rainbow6.teradiff

import io.rainbow6.teradiff.runner.TeradiffRunner
import junit.framework.TestCase
import org.junit.Test;

class E2ETest extends TestCase {

  @Test
  def test(): Unit = {
    val source1 = "src/test/resources/data1_header.txt"
    val source2 = "src/test/resources/data2_header.txt"
    val sourceType = "csv"
    val propertyFilename = "src/test/resources/test.properties"
    val outputFile = "target/testSummary.txt"
    val mode = "local"
    var partitions = "1"

    val args = Array[String](source1, source2, sourceType, propertyFilename, outputFile, mode, partitions)

    TeradiffRunner.main(args)
  }
}
