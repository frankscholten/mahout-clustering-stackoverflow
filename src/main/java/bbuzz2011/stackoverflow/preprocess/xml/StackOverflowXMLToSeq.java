package bbuzz2011.stackoverflow.preprocess.xml;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class StackOverflowXMLToSeq {

  private File input;
  private File output;

  public void run() throws FileNotFoundException {
    try {
      SAXParserFactory parserFact = SAXParserFactory.newInstance();
      SAXParser parser = parserFact.newSAXParser();

      System.out.println("XML Elements: ");

      DefaultHandler handler = new DefaultHandler() {
        public void startElement(String uri, String lName,
                                 String ele, Attributes attributes) throws SAXException {
          System.out.println(ele);
        }
      };
      parser.parse(new InputSource(new FileReader(input)), handler);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void setInput(File input) {
    this.input = input;
  }

  public void setOutput(File output) {
    this.output = output;
  }
}
