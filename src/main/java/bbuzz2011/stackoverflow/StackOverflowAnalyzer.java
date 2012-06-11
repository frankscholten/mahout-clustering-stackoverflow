package bbuzz2011.stackoverflow;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.util.Version;

import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class StackOverflowAnalyzer extends Analyzer {

  final List<String> stopWords = Arrays.asList(
          "what", "where", "how", "when", "why", "which", "were", "find", "myself", "these", "know", "anybody", "somebody", "differences",
          "good", "best", "much", "less", "more", "most", "been", "reading", "your", "mine", "with", "doing", "interested",
          "also", "from", "that", "like", "there", "would", "answer", "question", "need", "about",
          "have", "this", "using", "another", "difference", "between", "across"
  );

  @SuppressWarnings("unchecked")
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream tokenStream = new LetterTokenizer(Version.LUCENE_33, reader);
    tokenStream = new StandardFilter(Version.LUCENE_33, tokenStream);
    tokenStream = new LowerCaseFilter(Version.LUCENE_33, tokenStream);
    tokenStream = new LengthFilter(true, tokenStream, 4, 15);

    final Set stopSet = new CharArraySet(Version.LUCENE_33, stopWords.size(), true);
    stopSet.addAll(stopWords);
    stopSet.add(StopAnalyzer.ENGLISH_STOP_WORDS_SET);

    tokenStream = new StopFilter(Version.LUCENE_33, tokenStream, stopSet, true);

    return tokenStream;
  }
}