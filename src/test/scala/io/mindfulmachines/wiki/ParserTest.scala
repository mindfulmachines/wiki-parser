package io.mindfulmachines.wiki

import org.scalatest.FunSuite
import com.google.common.io.Resources
import io.mindfulmachines.generic.Spark

class ParserTest extends FunSuite {

  test("testParseInternalLinks") {
    val rawpges = Parser.readWikiDump(Spark.sc, "file://" + Resources.getResource("wikidump.xml").getPath)
    val pages = Parser.parsePages(rawpges)
    assert(Parser.parseInternalLinks(pages.values).count() == 8814)
  }

  test("testParsePages") {
    val rawpges = Parser.readWikiDump(Spark.sc, "file://" + Resources.getResource("wikidump.xml").getPath)
    assert(Parser.parsePages(rawpges).count() == 85)
  }

  test("testParseRedirects") {
    val rawpges = Parser.readWikiDump(Spark.sc, "file://" + Resources.getResource("wikidump.xml").getPath)
    val pages = Parser.parsePages(rawpges)
    val redirects = Parser.parseRedirects(pages.values)
    assert(redirects.count() == 67)
  }


  test("testReadWikiDump") {
    assert(Parser.readWikiDump(Spark.sc, "file://" + Resources.getResource("wikidump.xml").getPath).count() == 85)
  }

  test("testReadPageCounts") {
    assert(Parser.readPageCounts(Spark.sc, "file://" + Resources.getResource("pagecounts").getPath).count() == 100)
  }


  test("testReadClickSteam") {
    assert(Parser.readClickSteam(Spark.sc, "file://" + Resources.getResource("clickstream.tsv").getPath).count() == 99)
  }

}
