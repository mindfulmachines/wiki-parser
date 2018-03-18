package io.mndfulmachines.wiki

import java.io.ByteArrayInputStream

import info.bliki.wiki.dump.{IArticleFilter, Siteinfo, WikiArticle, WikiXMLParser}
import info.bliki.wiki.filter.WikipediaParser
import info.bliki.wiki.model.WikiModel
import io.mndfulmachines.input.XmlInputFormat
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.htmlcleaner.HtmlCleaner
import org.xml.sax.SAXException

object Parser {

  /**
    * A helper class that allows for a WikiArticle to be serialized and also pulled from the XML parser
    *
    * @param page The WikiArticle that is being wrapped
    */
  case class WrappedPage(var page: WikiArticle = new WikiArticle) {}

  /**
    * Represents a parsed Wikipedia page from the Wikipedia XML dump
    *
    * https://en.wikipedia.org/wiki/Wikipedia:Database_download
    * https://meta.wikimedia.org/wiki/Data_dump_torrents#enwiki
    *
    * @param title Title of the current page
    * @param text Text of the current page including markup
    * @param isCategory Is the page a category page, not perfectly accurate
    * @param isFile Is the page a file page, not perfectly accurate
    * @param isTemplate Is the page a template page, not perfectly accurate
    */
  case class Page(title: String, text: String, isCategory: Boolean , isFile: Boolean, isTemplate: Boolean)

  /**
    * Helper class for parsing wiki XML, parsed pages are set in wrappedPage
    *
    */
  class SetterArticleFilter(val wrappedPage: WrappedPage) extends IArticleFilter {
    @throws(classOf[SAXException])
    def process(page: WikiArticle, siteinfo: Siteinfo)  {
      wrappedPage.page = page
    }
  }

  /**
    * Represents a redirect from one wiki article title to another
    *
    * @param pageTitle Title of the current article
    * @param redirectTitle Title of the article being redirected to
    */
  case class Redirect(pageTitle: String, redirectTitle: String)

  /**
    * Represent a link from one wiki article to another
    *
    * @param pageTitle Title of the current article
    * @param linkTitle Title of the linked article
    * @param row Row the link shows on
    * @param col Column the link shows up on
    */
  case class Link(pageTitle: String, linkTitle: String, row: Int, col: Int)

  /**
    * Represents a click from one wiki article to another.
    * https://datahub.io/dataset/wikipedia-clickstream
    * https://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream
    *
    * @param prevId Id of the article click originated from if any
    * @param currId Id of the article the click went to
    * @param n Number of clicks
    * @param prevTitle Title of the article click originated from if any
    * @param currTitle Title of the article the click went to
    * @param clickType Type of clicks, see documentation for more information
    */
  case class Clicks(prevId: String, currId: String, n: Long, prevTitle: String, currTitle: String, clickType: String)

  /**
    * Represents a page counts on a wikpiedia article
    * https://wikitech.wikimedia.org/wiki/Analytics/Data/Pagecounts-all-sites
    *
    * @param project The project the page view is for (ie: en, en.m, fr, etc.)
    * @param pageTitle The title of the page
    * @param views The number of views for the page in this project
    */
  case class PageCounts(project: String, pageTitle: String, views: Long)

  /**
    * Read page counts data from a directory
    * https://wikitech.wikimedia.org/wiki/Analytics/Data/Pagecounts-all-sites
    */
  def readPageCounts(sc: SparkContext, path: String): RDD[PageCounts] = {
    val rdd = sc.textFile(path + "/*")
    rdd.map(_.split(" ")).map(l => PageCounts(
      l(0),
      StringEscapeUtils.unescapeHtml4(l(1)),
      l(2).toLong
    )
    )
  }

  /**
    * Reads click stream data from a file
    * https://datahub.io/dataset/wikipedia-clickstream
    * https://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream
    */
  def readClickSteam(sc: SparkContext, file: String) : RDD[Clicks] = {
    val rdd = sc.textFile(file)
    rdd.zipWithIndex().filter(_._2 != 0).map(_._1)
      .repartition(10)
      .map(_.split('\t'))
      .map(l => Clicks(
        l(0),
        l(1),
        l(2).toLong,
        l(3).replace("_"," "), //Click stream uses _ for spaces while the dump parsing uses actual spaces
        l(4).replace("_"," "), //Click stream uses _ for spaces while the dump parsing uses actual spaces
        l(5))
      )
  }

  /**
    * Reads a wiki dump xml file, returning a single row for each <page>...</page>
    * https://en.wikipedia.org/wiki/Wikipedia:Database_download
    * https://meta.wikimedia.org/wiki/Data_dump_torrents#enwiki
    */
  def readWikiDump(sc: SparkContext, file: String) : RDD[(Long, String)] = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
    val rdd = sc.newAPIHadoopFile(file, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
    rdd.map{case (k,v) => (k.get(), new String(v.copyBytes()))}.repartition(100)
  }

  /**
    * Parses the raw page text produced by readWikiDump into Page objects
    */
  def parsePages(rdd: RDD[(Long, String)]): RDD[(Long, Page)] = {
    rdd.mapValues{
      text => {
        val wrappedPage = new WrappedPage
        //The parser occasionally exceptions out, we ignore these
        try {
          val parser = new WikiXMLParser(new ByteArrayInputStream(text.getBytes), new SetterArticleFilter(wrappedPage))
          parser.parse()
        } catch {
          case e: Exception =>
        }
        val page = wrappedPage.page
        if (page.getText != null && page.getTitle != null
          && page.getId != null && page.getRevisionId != null
          && page.getTimeStamp != null) {
          Some(Page(page.getTitle, page.getText, page.isCategory, page.isFile, page.isTemplate))
        } else {
          None
        }
      }
    }.flatMapValues(_.toSeq)
  }

  /**
    * Parses redirects out of the Page objects
    */
  def parseRedirects(rdd: RDD[Page]): RDD[Redirect] = {
    rdd.map {
      page =>
        val redirect =
          if (page.text != null && !page.isCategory && !page.isFile && !page.isTemplate) {
            val r =  WikipediaParser.parseRedirect(page.text, new WikiModel("", ""))
            if (r == null) {
              None
            } else {
              Some(Redirect(page.title,r))
            }
          } else {
            None
          }
        redirect
    }.filter(_.isDefined).map(_.get)
  }

  /**
    * Parses internal article links from a Page object, filtering out links that aren't to articles
   */
  def parseInternalLinks(rdd: RDD[Page]): RDD[Link] = {
    rdd.flatMap {
      page =>
        if (page.text != null) {
          try {
            val html = WikiModel.toHtml(page.text)
            val cleaner = new HtmlCleaner
            val rootNode = cleaner.clean(html)
            val elements = rootNode.getElementsByName("a", true)
            val out = for (
              elem <- elements;
              classType = elem.getAttributeByName("class");
              title = elem.getAttributeByName("title")
              if (
                title != null
                  && !title.startsWith("User:") && !title.startsWith("User talk:")
                  && (classType == null || !classType.contains("external"))
                )
            ) yield {
              Link(page.title, StringEscapeUtils.unescapeHtml4(title), elem.getRow, elem.getCol)
            }
            out.toList
          }  catch {
            case e: Exception => Nil
          }
        } else {
          Nil
        }
    }
  }
}
