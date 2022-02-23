package io.rainbow6.teradiff

import io.rainbow6.teradiff.runner.TeradiffRunner
import junit.framework.TestCase
import org.junit.Test;

class E2ETest extends TestCase {

  @Test
  def test(): Unit = {

    val args = Array[String](
      "--left", "src/test/resources/data1_header.txt",
      "--right", "src/test/resources/data2.json",
      "--sourceType1", "csv",
      "--sourceType2", "json",
      "--propertyFile", "src/test/resources/test.properties",
      "--outputFile", "target/testSummary.txt",
      "--runMode", "local",
      "--partitions", "1",
      "--leftDelimiter", "|",
      "--rightDelimiter", "|",
      "--leftKey", "id",
      "--leftValue", "id,col1,col2,col3",
      "--rightKey", "id",
      "--rightValue", "id,col1,col2,col3",
      "--leftIgnores", "col1,col2",
      "--rightIgnores", "col1,col2",
      "--rightWithHeader",
      "--leftWithHeader"
    )

    TeradiffRunner.main(args)
  }
}
