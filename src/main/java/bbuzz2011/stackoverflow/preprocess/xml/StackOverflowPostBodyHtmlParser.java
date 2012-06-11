package bbuzz2011.stackoverflow.preprocess.xml;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.PrototypicalNodeFactory;
import org.htmlparser.lexer.Lexer;
import org.htmlparser.nodes.TextNode;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import org.htmlparser.util.SimpleNodeIterator;

public class StackOverflowPostBodyHtmlParser {

  public String parsePostContent(String htmlBody) {
    try {
      PrototypicalNodeFactory factory = new PrototypicalNodeFactory();
      factory.registerTag(new CodeTag());

      Parser htmlParser = new Parser(new Lexer(htmlBody));
      htmlParser.setNodeFactory(factory);

      NodeList elements = htmlParser.parse(new NodeFilter() {
        @Override
        public boolean accept(Node node) {
          return isTextNode(node) && isNotCode(node);
        }
      });

      SimpleNodeIterator iterator = elements.elements();
      StringBuilder builder = new StringBuilder();
      while (iterator.hasMoreNodes()) {
        builder.append(iterator.nextNode().getText());
      }

      return builder.toString();
    } catch (ParserException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isTextNode(Node node) {
    return node instanceof TextNode;
  }

  private boolean isNotCode(Node node) {
    return !(node.getParent() instanceof CodeTag);
  }
}
