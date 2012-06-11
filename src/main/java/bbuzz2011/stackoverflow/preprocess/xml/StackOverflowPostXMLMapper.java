/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bbuzz2011.stackoverflow.preprocess.xml;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.IOException;
import java.io.StringReader;

/**
 * Turns posts from a StackOverflow posts.xml file into the following output.
 *
 * Pairs of (post id, content)
 *
 * so they can be processed by {@link org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles}
 */
public class StackOverflowPostXMLMapper extends Mapper<LongWritable, Text, LongWritable, PostWritable> {

  public enum Counter {
    MISSING_TITLES, TITLES, QUESTIONS
  }

  private static final String QUESTION_TYPE = "1";

  public static String XPATH_ROW_BODY = "/row/@Body";
  public static String XPATH_TITLE = "/row/@Title";
  public static String XPATH_POST_TYPE = "/row/@PostTypeId";

  private XPathExpression postBodyXPath;
  private XPathExpression postTitleXPath;
  private XPathExpression postTypeXPath;

  private DocumentBuilder documentBuilder;
  private StackOverflowPostBodyHtmlParser parser;

  private LongWritable postKey = new LongWritable();
  private PostWritable postWritable = new PostWritable();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    try {
      initializeParsers();
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Could not initialize XPath", e);
    } catch (XPathExpressionException e) {
      throw new RuntimeException("Could not initialize XPath", e);
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    try {
      writePostBody(key, value, context);
    } catch (SAXException e) {
      throw new RuntimeException("Could not parse post", e);
    } catch (XPathExpressionException e) {
      throw new RuntimeException("Could not parse post", e);
    }
  }

  //========================================== Helper Methods ==========================================================

  private void initializeParsers() throws XPathExpressionException, ParserConfigurationException {
    XPathFactory factory = XPathFactory.newInstance();
    XPath xpath = factory.newXPath();
    postBodyXPath = xpath.compile(XPATH_ROW_BODY);
    postTitleXPath = xpath.compile(XPATH_TITLE);
    postTypeXPath = xpath.compile(XPATH_POST_TYPE);

    DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
    domFactory.setNamespaceAware(true);
    documentBuilder = domFactory.newDocumentBuilder();

    parser = new StackOverflowPostBodyHtmlParser();
  }

  private void writePostBody(LongWritable key, Text value, Context context) throws SAXException, IOException, XPathExpressionException, InterruptedException {
    context.getCounter(StackOverflowPostXMLMapper.Counter.TITLES).increment(1);

    Document doc = documentBuilder.parse(new InputSource(new StringReader(value.toString())));

    String title = (String) postTitleXPath.evaluate(doc, XPathConstants.STRING);
    if (title == null || title.equals("")) {
      context.getCounter(Counter.MISSING_TITLES).increment(1);
      return;
    }

    String postHtml = (String) postBodyXPath.evaluate(doc, XPathConstants.STRING);
    String content = parser.parsePostContent(postHtml);

    postKey.set((int) key.get());

    postWritable.setTitle(title);
    postWritable.setContent(content);

    if (isQuestion(doc)) {
      context.getCounter(Counter.QUESTIONS).increment(1);
      context.write(postKey, postWritable);
    }
  }

  private boolean isQuestion(Document doc) throws XPathExpressionException {
    return QUESTION_TYPE.equals(postTypeXPath.evaluate(doc, XPathConstants.STRING));
  }
}
