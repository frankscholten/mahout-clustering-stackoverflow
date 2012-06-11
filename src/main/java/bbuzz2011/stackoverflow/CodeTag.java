package bbuzz2011.stackoverflow;

import org.htmlparser.tags.CompositeTag;

public class CodeTag extends CompositeTag {

  private static final String[] ids = new String[]{"CODE"};

  public CodeTag() {
  }

  public String[] getIds() {
    return (ids);
  }

  public String[] getEnders() {
    return (ids);
  }

  public String[] getEndTagEnders() {
    return (new String[0]);
  }
}
