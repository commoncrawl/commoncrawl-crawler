package org.commoncrawl.util;

import info.bliki.commons.validator.routines.EmailValidator;
import info.bliki.htmlcleaner.ContentToken;
import info.bliki.htmlcleaner.EndTagToken;
import info.bliki.htmlcleaner.TagNode;
import info.bliki.htmlcleaner.TagToken;
import info.bliki.wiki.filter.AbstractParser;
import info.bliki.wiki.filter.Encoder;
import info.bliki.wiki.filter.TemplateParser;
import info.bliki.wiki.filter.Util;
import info.bliki.wiki.filter.WPList;
import info.bliki.wiki.filter.WPTable;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.DefaultEventListener;
import info.bliki.wiki.model.IEventListener;
import info.bliki.wiki.model.ITableOfContent;
import info.bliki.wiki.model.IWikiModel;
import info.bliki.wiki.tags.HTMLBlockTag;
import info.bliki.wiki.tags.HTMLTag;
import info.bliki.wiki.tags.HrTag;
import info.bliki.wiki.tags.PTag;
import info.bliki.wiki.tags.WPBoldItalicTag;
import info.bliki.wiki.tags.WPPreTag;
import info.bliki.wiki.tags.WPTag;
import info.bliki.wiki.tags.util.Attribute;
import info.bliki.wiki.tags.util.IBodyTag;
import info.bliki.wiki.tags.util.INoBodyParsingTag;
import info.bliki.wiki.tags.util.NodeAttribute;
import info.bliki.wiki.tags.util.TagStack;
import info.bliki.wiki.tags.util.WikiTagNode;

import java.util.List;

/**
 * A Wikipedia syntax parser for the second pass in the parsing of a Wikipedia
 * source text.
 * 
 * @see TemplateParser for the first pass
 */
public class WikipediaParser extends AbstractParser  {
    private static final String[] TOC_IDENTIFIERS = { "TOC", "NOTOC", "FORCETOC" };

    final static String HEADER_STRINGS[] = { "=", "==", "===", "====", "=====", "======" };

    final static int TokenNotFound = -2;

    final static int TokenIgnore = -1;

    final static int TokenSTART = 0;

    final static int TokenEOF = 1;

    final static int TokenBOLD = 3;

    final static int TokenITALIC = 4;

    final static int TokenBOLDITALIC = 5;

    final static HTMLTag BOLD = new WPTag("b");

    final static HTMLTag ITALIC = new WPTag("i");

    final static HTMLTag BOLDITALIC = new WPBoldItalicTag();

    final static HTMLTag STRONG = new WPTag("strong");

    final static HTMLTag EM = new WPTag("em");

    private ITableOfContent fTableOfContentTag = null;

    private int fHeadCounter = 0;

    /**
     * Enable HTML tags
     */
    private boolean fHtmlCodes = true;

    private boolean fNoToC = false;

    private boolean fRenderTemplate = false;

    private boolean fForceToC = false;

    private IEventListener fEventListener = null;

    public WikipediaParser(String stringSource, boolean renderTemplate) {
        this(stringSource, renderTemplate, null);
    }

    public WikipediaParser(String stringSource, boolean renderTemplate, IEventListener wikiListener) {
        super(stringSource);
        fRenderTemplate = renderTemplate;
        if (wikiListener == null) {
            fEventListener = DefaultEventListener.CONST;
        } else {
            fEventListener = wikiListener;
        }
    }

    /**
     * Copy the read ahead content in the resulting HTML text token.
     * 
     * @param diff
     *          subtract <code>diff</code> form the current parser position to get
     *          the HTML text token end position.
     */
    private void createContentToken(final int diff) {
        if (fWhiteStart) {
            try {
                final int whiteEndPosition = fCurrentPosition - diff;
                int count = whiteEndPosition - fWhiteStartPosition;
                if (count > 0) {
                    fWikiModel.append(new ContentToken(fStringSource.substring(fWhiteStartPosition, fWhiteStartPosition + count)));
                }
            } finally {
                fWhiteStart = false;
            }
        }
    }

    protected final boolean getNextChar(char testedChar) {
        int temp = fCurrentPosition;
        try {
            fCurrentCharacter = fSource[fCurrentPosition++];
            if (fCurrentCharacter != testedChar) {
                fCurrentPosition = temp;
                return false;
            }
            return true;

        } catch (IndexOutOfBoundsException e) {
            fCurrentPosition = temp;
            return false;
        }
    }

    protected final int getNextChar(char testedChar1, char testedChar2) {
        int temp = fCurrentPosition;
        try {
            int result;
            fCurrentCharacter = fSource[fCurrentPosition++];
            if (fCurrentCharacter == testedChar1)
                result = 0;
            else if (fCurrentCharacter == testedChar2)
                result = 1;
            else {
                fCurrentPosition = temp;
                return -1;
            }
            return result;
        } catch (IndexOutOfBoundsException e) {
            fCurrentPosition = temp;
            return -1;
        }
    }

    protected final boolean getNextCharAsDigit() {
        int temp = fCurrentPosition;
        try {
            fCurrentCharacter = fSource[fCurrentPosition++];
            if (!Character.isDigit(fCurrentCharacter)) {
                fCurrentPosition = temp;
                return false;
            }
            return true;
        } catch (IndexOutOfBoundsException e) {
            fCurrentPosition = temp;
            return false;
        }
    }

    protected final boolean getNextCharAsDigit(int radix) {

        int temp = fCurrentPosition;
        try {
            fCurrentCharacter = fSource[fCurrentPosition++];

            if (Character.digit(fCurrentCharacter, radix) == -1) {
                fCurrentPosition = temp;
                return false;
            }
            return true;
        } catch (IndexOutOfBoundsException e) {
            fCurrentPosition = temp;
            return false;
        }
    }

    protected final int getNumberOfChar(char testedChar) {
        int number = 0;
        try {
            while ((fCurrentCharacter = fSource[fCurrentPosition++]) == testedChar) {
                number++;
            }
        } catch (IndexOutOfBoundsException e) {

        }
        fCurrentPosition--;
        return number;
    }

    protected boolean getNextCharAsWikiPluginIdentifierPart() {
        int temp = fCurrentPosition;
        try {
            fCurrentCharacter = fSource[fCurrentPosition++];

            if (!Encoder.isWikiPluginIdentifierPart(fCurrentCharacter)) {
                fCurrentPosition = temp;
                return false;
            }
            return true;
        } catch (IndexOutOfBoundsException e) {
            fCurrentPosition = temp;
            return false;
        }
    }

    protected int getNextToken() // throws InvalidInputException
    {
        fWhiteStart = true;
        fWhiteStartPosition = fCurrentPosition;
        try {
            while (true) {
                fCurrentCharacter = fSource[fCurrentPosition++];

                // ---------Identify the next token-------------
                switch (fCurrentCharacter) {
                case '\n':
                    // check at the end of line, if there is open wiki bold or italic
                    // markup
                    reduceTokenStackBoldItalic();
                    break;
                case '{':
                    // dummy parsing of wikipedia templates for event listeners
                    if (parseTemplate()) {
                    } else {
                        // wikipedia table handling
                        if (parseTable()) {
                            continue;
                        }
                    }
                    break;
                case '_': // TOC identifiers __NOTOC__, __FORCETOC__ ...
                    if (parseSpecialIdentifiers()) {
                        continue;
                    }
                    break;
                case '=': // wikipedia header ?
                    if (parseSectionHeaders()) {
                        continue;
                    }
                    break;
                case WPList.DL_DD_CHAR: // start of <dl><dd> list
                case WPList.DL_DT_CHAR: // start of <dl><dt> list
                case WPList.OL_CHAR: // start of <ol> list
                case WPList.UL_CHAR: // start of <ul> list
                    if (parseLists()) {
                        continue;
                    }
                    break;
                // case ':':
                // if (parseSimpleDefinitionLists()) {
                // continue;
                // }
                // break;
                // case ';':
                // if (parseDefinitionLists()) {
                // continue;
                // }
                // break;
                case '-': // parse ---- as <hr>
                    if (parseHorizontalRuler()) {
                        continue;
                    }
                    break;
                case ' ': // pre-formatted text?
                case '\t':
                    if (parsePreformattedHorizontalRuler()) {
                        if (!fWhiteStart) {
                            fWhiteStart = true;
                            fWhiteStartPosition = fCurrentPosition;
                        } else {
                            createContentToken(1);
                        }
                        continue;
                    }
                    break;
                }

                if (isStartOfLine() && fWikiModel.getRecursionLevel() == 1) {
                    if (fWikiModel.stackSize() > 0 && (fWikiModel.peekNode() instanceof PTag) && isEmptyLine(1)) {
                        createContentToken(2);
                        reduceTokenStack(Configuration.HTML_PARAGRAPH_OPEN);
                    } else {
                        if (!isEmptyLine(1)) {
                            if (fWikiModel.stackSize() == 0) {
                                addParagraph();
                                // if (fWikiModel.getRecursionLevel() == 1) {
                                // addParagraph();
                                // } else {
                                // if (fCurrentPosition > 1) {
                                // addParagraph();
                                // }
                                // }
                            } else {
                                TagToken tag = fWikiModel.peekNode();
                                if (tag instanceof WPPreTag) {
                                    addPreformattedText();
                                    // } else if (tag instanceof PTag) {
                                    // createContentToken(fWhiteStart, fWhiteStartPosition, 2);
                                    // reduceTokenStack(Configuration.HTML_PARAGRAPH_OPEN);
                                } else {
                                    String allowedParents = Configuration.HTML_PARAGRAPH_OPEN.getParents();
                                    if (allowedParents != null) {

                                        int index = -1;
                                        index = allowedParents.indexOf("|" + tag.getName() + "|");
                                        if (index >= 0) {
                                            addParagraph();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // ---------Identify the next token-------------
                switch (fCurrentCharacter) {
                case '[':
                    if (parseWikiLink()) {
                        continue;
                    }
                    break;
                case '\'':
                    if (getNextChar('\'')) {
                        if (getNextChar('\'')) {
                            if (getNextChar('\'')) {
                                if (getNextChar('\'')) {
                                    createContentToken(5);
                                    return TokenBOLDITALIC;
                                }
                                fCurrentPosition -= 1;
                                fWhiteStart = true;
                                createContentToken(3);
                                return TokenBOLD;
                            }
                            createContentToken(3);
                            return TokenBOLD;
                        }
                        createContentToken(2);
                        return TokenITALIC;
                    }
                    break;
                case '<':
                    if (fHtmlCodes) {
                        int htmlStartPosition = fCurrentPosition;
                        // HTML tags are allowed
                        try {
                            switch (fStringSource.charAt(fCurrentPosition)) {
                            case '!': // <!-- HTML comment -->
                                if (parseHTMLCommentTags()) {
                                    continue;
                                }
                                break;
                            default:

                                if (fSource[fCurrentPosition] != '/') {
                                    // opening HTML tag
                                    WikiTagNode tagNode = parseTag(fCurrentPosition);
                                    if (tagNode != null) {
                                        String tagName = tagNode.getTagName();
                                        TagToken tag = fWikiModel.getTokenMap().get(tagName);
                                        if (tag != null) {
                                            tag = (TagToken) tag.clone();

                                            if (tag instanceof TagNode) {
                                                TagNode node = (TagNode) tag;
                                                List<NodeAttribute> attributes = tagNode.getAttributesEx();
                                                Attribute attr;
                                                for (int i = 1; i < attributes.size(); i++) {
                                                    attr = attributes.get(i);
                                                    node.addAttribute(attr.getName(), attr.getValue(), true);
                                                }
                                            }
                                            if (tag instanceof HTMLTag) {
                                                ((HTMLTag) tag).setTemplate(isTemplate());
                                            }

                                            createContentToken(1);

                                            fCurrentPosition = fScannerPosition;

                                            String allowedParents = tag.getParents();
                                            if (allowedParents != null) {
                                                reduceTokenStack(tag);
                                            }
                                            createTag(tag, tagNode, tagNode.getEndPosition());
                                            return TokenIgnore;

                                        }
                                        break;
                                    }
                                } else {
                                    // closing HTML tag
                                    WikiTagNode tagNode = parseTag(++fCurrentPosition);
                                    if (tagNode != null) {
                                        String tagName = tagNode.getTagName();
                                        TagToken tag = fWikiModel.getTokenMap().get(tagName);
                                        if (tag != null) {
                                            createContentToken(2);
                                            fCurrentPosition = fScannerPosition;

                                            if (fWikiModel.stackSize() > 0) {
                                                TagToken topToken = fWikiModel.peekNode();
                                                if (topToken.getName().equals(tag.getName())) {
                                                    fWikiModel.popNode();
                                                    return TokenIgnore;
                                                } else {
                                                    if (tag.isReduceTokenStack()) {
                                                        reduceStackUntilToken(tag);
                                                    }
                                                }
                                            } else {
                                            }
                                            return TokenIgnore;
                                        }
                                        break;
                                    }
                                }
                            }
                        } catch (IndexOutOfBoundsException e) {
                            // do nothing
                        }
                        fCurrentPosition = htmlStartPosition;
                    }
                    break;
                default:
                    if (Character.isLetter(fCurrentCharacter)) {
                        if (fCurrentCharacter == 'i' || fCurrentCharacter == 'I') {
                            // ISBN ?
                            if (parseISBNLinks()) {
                                continue;
                            }
                        }

                        if (parseURIScheme()) {
                            // a URI scheme registered in the wiki model (ftp, http,
                            // https,...)
                            continue;
                        }

                        if (fWikiModel.isCamelCaseEnabled() && Character.isUpperCase(fCurrentCharacter) && fWikiModel.getRecursionLevel() <= 1) {
                            if (parseCamelCaseLink()) {
                                continue;
                            }
                        }
                    }
                }

                if (!fWhiteStart) {
                    fWhiteStart = true;
                    fWhiteStartPosition = fCurrentPosition - 1;
                }

            }
            // -----------------end switch while try--------------------
        } catch (IndexOutOfBoundsException e) {
            // end of scanner text
        }
        try {
            createContentToken(1);
        } catch (IndexOutOfBoundsException e) {
            // end of scanner text
        }
        return TokenEOF;
    }

    private void addParagraph() {
        createContentToken(2);
        reduceTokenStack(Configuration.HTML_PARAGRAPH_OPEN);
        fWikiModel.pushNode(new PTag());
    }

    /**
     * Add the content of the wiki &lt;pre&gt; block. Trim the content at the
     * right side.
     */
    private void addPreformattedText() {
        if (fWhiteStart) {
            int currentPos = fCurrentPosition;
            int whiteEndPosition = fCurrentPosition - 2;
            while (whiteEndPosition > fWhiteStartPosition) {
                if (!Character.isWhitespace(fSource[whiteEndPosition])) {
                    whiteEndPosition++;
                    break;
                }
                whiteEndPosition--;
            }
            try {
                fCurrentPosition = whiteEndPosition;
                createContentToken(0);
            } finally {
                fCurrentPosition = currentPos;
            }
        }
        reduceTokenStack(Configuration.HTML_PARAGRAPH_OPEN);
        fWikiModel.pushNode(new PTag());
    }

    private boolean parseHTMLCommentTags() {
        int htmlStartPosition = fCurrentPosition;
        String htmlCommentString = fStringSource.substring(fCurrentPosition - 1, fCurrentPosition + 3);

        if (htmlCommentString.equals("<!--")) {
            fCurrentPosition += 3;
            if (readUntil("-->")) {
                String htmlCommentContent = fStringSource.substring(htmlStartPosition + 3, fCurrentPosition - 3);
                if (htmlCommentContent != null) {
                    createContentToken(fCurrentPosition - htmlStartPosition + 1);
                    return true;
                }
            }
        }
        return false;
    }

    private boolean parseISBNLinks() {
        int urlStartPosition = fCurrentPosition;
        boolean foundISBN = false;
        try {
            String urlString = fStringSource.substring(fCurrentPosition - 1, fCurrentPosition + 4);
            if (urlString.equalsIgnoreCase("isbn ")) {
                fCurrentPosition += 4;
                fCurrentCharacter = fSource[fCurrentPosition++];

                createContentToken(6);
                fWhiteStart = false;
                foundISBN = true;
                char ch;
                ch = fSource[fCurrentPosition++];
                while ((ch >= '0' && ch <= '9') || ch == '-') {
                    ch = fSource[fCurrentPosition++];
                }
            }
        } catch (IndexOutOfBoundsException e) {
            if (!foundISBN) {
                // rollback work :-)
                fCurrentPosition = urlStartPosition;
            }
        }
        if (foundISBN) {
            String urlString = fStringSource.substring(urlStartPosition - 1, fCurrentPosition - 1);
            fCurrentPosition--;
            fWikiModel.appendISBNLink(urlString);
            return true;
        }
        return false;
    }

    // private boolean parseFTPLinks() {
    // int urlStartPosition = fCurrentPosition;
    // boolean foundUrl = false;
    // try {
    // String urlString = fStringSource.substring(fCurrentPosition - 1,
    // fCurrentPosition + 2);
    // if (urlString.equalsIgnoreCase("ftp")) {
    // fCurrentPosition += 2;
    // fCurrentCharacter = fSource[fCurrentPosition++];
    //
    // if (fCurrentCharacter == ':' && fSource[fCurrentPosition++] == '/' &&
    // fSource[fCurrentPosition++] == '/') {
    // createContentToken(fWhiteStart, fWhiteStartPosition, 6);
    // fWhiteStart = false;
    // foundUrl = true;
    // while (Encoder.isUrlIdentifierPart(fSource[fCurrentPosition++])) {
    // }
    // }
    // }
    // } catch (IndexOutOfBoundsException e) {
    // if (!foundUrl) {
    // // rollback work :-)
    // fCurrentPosition = urlStartPosition;
    // }
    // }
    // if (foundUrl) {
    // String urlString = new String(fSource, urlStartPosition - 1,
    // fCurrentPosition - urlStartPosition);
    // fCurrentPosition--;
    // fWikiModel.appendExternalLink(urlString, urlString, true);
    // return true;
    // }
    // return false;
    // }

    // private boolean parseHTTPLinks() {
    // int urlStartPosition = fCurrentPosition;
    // boolean foundUrl = false;
    // try {
    // int diff = 7;
    // String urlString = fStringSource.substring(fCurrentPosition - 1,
    // fCurrentPosition + 3);
    // if (urlString.equalsIgnoreCase("http")) {
    // fCurrentPosition += 3;
    // fCurrentCharacter = fSource[fCurrentPosition++];
    // if (fCurrentCharacter == 's') { // optional
    // fCurrentCharacter = fSource[fCurrentPosition++];
    // diff++;
    // }
    //
    // if (fCurrentCharacter == ':' && fSource[fCurrentPosition++] == '/' &&
    // fSource[fCurrentPosition++] == '/') {
    // createContentToken(fWhiteStart, fWhiteStartPosition, diff);
    // fWhiteStart = false;
    // foundUrl = true;
    // while (Encoder.isUrlIdentifierPart(fSource[fCurrentPosition++])) {
    // }
    // }
    // }
    // } catch (IndexOutOfBoundsException e) {
    // if (!foundUrl) {
    // // rollback work :-)
    // fCurrentPosition = urlStartPosition;
    // }
    // }
    // if (foundUrl) {
    // String urlString = new String(fSource, urlStartPosition - 1,
    // fCurrentPosition - urlStartPosition);
    // fCurrentPosition--;
    // fWikiModel.appendExternalLink(urlString, urlString, true);
    // return true;
    // }
    // return false;
    // }

    private boolean parseMailtoLinks() {
        int urlStartPosition = fCurrentPosition;
        int tempPosition = fCurrentPosition;
        boolean foundUrl = false;
        try {
            String urlString = fStringSource.substring(fCurrentPosition - 1, fCurrentPosition + 6);
            if (urlString.equalsIgnoreCase("mailto:")) {
                tempPosition += 6;
                fCurrentCharacter = fSource[tempPosition++];

                foundUrl = true;
                while (!Character.isWhitespace(fSource[tempPosition++])) {
                }
            }
        } catch (IndexOutOfBoundsException e) {
        }
        if (foundUrl) {
            String urlString = fStringSource.substring(urlStartPosition - 1, tempPosition - 1);
            String email = urlString.substring(7);
            if (EmailValidator.getInstance().isValid(email)) {
                createContentToken(1);
                fWhiteStart = false;
                fCurrentPosition = tempPosition;
                fCurrentPosition--;
                fWikiModel.appendMailtoLink(urlString, urlString, true);
                return true;
            }

        }
        // rollback work :-)
        fCurrentPosition = urlStartPosition;
        return false;
    }

    /**
     * See <a href="http://en.wikipedia.org/wiki/URI_scheme">URI scheme</a>
     * 
     * @return <code>true</code> if a registered URI scheme was found in the wiki
     *         models configuration..
     */
    private boolean parseURIScheme() {
        if (fCurrentCharacter == 'm' || fCurrentCharacter == 'M') {
            // mailto ?
            if (parseMailtoLinks()) {
                return true;
            }
        }
        int urlStartPosition = fCurrentPosition;
        int tempPosition = fCurrentPosition;
        String uriSchemeName = "";
        int index = -1;
        boolean foundUrl = false;
        try {
            index = indexOfUntilNoLetter(':', fCurrentPosition);
            if (index > 0) {
                uriSchemeName = fStringSource.substring(fCurrentPosition - 1, index);
                if (fWikiModel.isValidUriScheme(uriSchemeName)) {
                    // found something like "ftp", "http", "https"
                    tempPosition += uriSchemeName.length() + 1;
                    fCurrentCharacter = fSource[tempPosition++];

                    createContentToken(1);
                    fWhiteStart = false;
                    foundUrl = true;
                    while (Encoder.isUrlIdentifierPart(fSource[tempPosition++])) {
                    }

                }
            }
        } catch (IndexOutOfBoundsException e) {
        }
        if (foundUrl) {
            String restString = fStringSource.substring(urlStartPosition - 1, tempPosition - 1);
            String uriSchemeSpecificPart = fStringSource.substring(index + 1, tempPosition - 1);
            if (fWikiModel.isValidUriSchemeSpecificPart(uriSchemeName, uriSchemeSpecificPart)) {
                fWhiteStart = false;
                fCurrentPosition = tempPosition;
                fCurrentPosition--;
                fWikiModel.appendExternalLink(uriSchemeName, restString, restString, true);
                return true;
            }

        }
        // rollback work :-)
        fCurrentPosition = urlStartPosition;
        return false;
    }

    private boolean parseCamelCaseLink() {
        int startLinkPosition = fCurrentPosition - 1;
        int temp = fCurrentPosition;
        boolean isCamelCase = false;
        try {
            char ch = fSource[temp++];
            while (Character.isLetterOrDigit(ch)) {
                if (Character.isUpperCase(ch)) {
                    // at least 2 upper case characters appear in the word
                    isCamelCase = true;
                }
                ch = fSource[temp++];
            }
        } catch (IndexOutOfBoundsException iobe) {
        }

        if (isCamelCase) {
            createContentToken(1);
            fWhiteStart = false;
            fCurrentPosition = temp - 1;

            String name = fStringSource.substring(startLinkPosition, fCurrentPosition);
            fWikiModel.appendInternalLink(name, null, name, null, false);
            return true;
        }

        return false;
    }

    /**
     * Parse a wiki section starting with a '[' character
     * 
     * @return <code>true</code> if a correct link was found
     */
    private boolean parseWikiLink() {
        int startLinkPosition = fCurrentPosition;
        if (getNextChar('[')) {
            return parseWikiTag();
        } else {
            createContentToken(1);
            fWhiteStart = false;

            if (readUntilCharOrStopAtEOL(']')) {
                String name = fStringSource.substring(startLinkPosition, fCurrentPosition - 1);

                // bbcode start
                if (fWikiModel.parseBBCodes() && name.length() > 0) {
                    // parse start tokens like phpBB forum syntax style (bbcode)
                    StringBuilder bbCode = new StringBuilder(name.length());
                    char ch = name.charAt(0);
                    if ('a' <= ch && ch <= 'z') {
                        // first character must be a letter
                        bbCode.append(ch);
                        if (parsePHPBBCode(name, bbCode)) {
                            return true;
                        }
                    }
                }
                // bbcode end

                if (handleHTTPLink(name)) {
                    return true;
                }
            }
            fCurrentPosition = startLinkPosition;
        }
        return false;
    }

    /**
     * Parse a wiki section starting with a '[[' sequence
     * 
     * @return <code>true</code> if a correct link was found
     */
    private boolean parseWikiTag() {
        int startLinkPosition = fCurrentPosition;
        int endLinkPosition;
        // wikipedia link style
        createContentToken(2);

        int temp = fCurrentPosition;
        if (findWikiLinkEnd()) {
            endLinkPosition = fCurrentPosition - 2;
            String name = fStringSource.substring(startLinkPosition, endLinkPosition);
            // test for a suffix string behind the Wiki link. Useful for plurals.
            // Example:
            // Dolphins are [[aquatic mammal]]s that are closely related to [[whale]]s
            // and [[porpoise]]s.
            temp = fCurrentPosition;
            StringBuilder suffixBuffer = new StringBuilder();

            try {
                while (true) {
                    fCurrentCharacter = fSource[fCurrentPosition++];
                    if (!Character.isLowerCase(fCurrentCharacter)) {
                        fCurrentPosition--;
                        break;
                    }
                    suffixBuffer.append(fCurrentCharacter);
                }
                String suffix = suffixBuffer.toString();
                fEventListener.onWikiLink(fSource, startLinkPosition, endLinkPosition, suffix);
                fWikiModel.appendRawWikipediaLink(name, suffix);
                return true;
            } catch (IndexOutOfBoundsException e) {
                fCurrentPosition = temp;
            }

            fEventListener.onWikiLink(fSource, startLinkPosition, endLinkPosition, "");
            fWikiModel.appendRawWikipediaLink(name, "");
            return true;
        } else {
            fWhiteStart = true;
            fWhiteStartPosition = startLinkPosition - 2;
            fCurrentPosition = temp + 1;
        }
        return false;
    }

    private boolean parsePreformattedHorizontalRuler() {
        if (isStartOfLine() && !isEmptyLine(1)) {
            // if (fWikiModel.stackSize() == 0 ||
            // !fWikiModel.peekNode().equals("pre")) {
            if (fWikiModel.stackSize() == 0 || !(fWikiModel.peekNode() instanceof HTMLBlockTag)
                    || (fWikiModel.peekNode() instanceof PTag)) {
                // !(fWikiModel.peekNode() instanceof PreTag || fWikiModel.peekNode()
                // instanceof WPPreTag)) {
                createContentToken(2);
                reduceTokenStack(Configuration.HTML_PRE_OPEN);

                // don't use Configuration.HTML_PRE_OPEN here
                // rendering differs between these tags!
                fWikiModel.pushNode(new WPPreTag());
            }
            return true;
        }
        return false;
    }

    /**
     * Parse <code>----</code> as &lt;hr&gt; tag
     * 
     * @return
     */
    private boolean parseHorizontalRuler() {
        if (isStartOfLine()) {
            int tempCurrPosition = fCurrentPosition;
            try {
                if (fSource[tempCurrPosition++] == '-' && fSource[tempCurrPosition++] == '-' && fSource[tempCurrPosition++] == '-') {
                    int pos = isEndOfLine('-', tempCurrPosition);
                    if (pos > 0) {
                        HrTag hr = new HrTag();
                        createContentToken(2);
                        reduceTokenStack(hr);
                        fCurrentPosition = pos;
                        fWikiModel.append(hr);
                        fWhiteStart = false;
                        return true;
                    }
                }
            } catch (IndexOutOfBoundsException e) {

            }
            fCurrentPosition = tempCurrPosition;
        }
        return false;
    }

    /**
     * Parse a wiki list <br/>
     * <br/>
     * Example:<br/>
     * 
     * <pre>
     * * first line
     * * second line
     * ** third line
     * </pre>
     * 
     * @return
     */
    private boolean parseLists() {
        // set scanner pointer to '\n' character:
        if (isStartOfLine()) {
            setPosition(fCurrentPosition - 2);
            WPList list = wpList();
            if (list != null && !list.isEmpty()) {
                createContentToken(1);
                reduceTokenStack(list);
                fCurrentPosition = getPosition() - 1;
                fWikiModel.append(list);
                return true;
            }
        }
        return false;
    }

    /**
     * Parses a wiki header line into &quot;h1, h2, h3, h4, h5, h6&quot; HTML
     * tags. <br/>
     * <br/>
     * Example wiki syntax header line: <br/>
     * <code>== Test header 2 ==</code>
     * 
     * @return <code>true</code> if a header line could be parsed correctly,
     *         <code>false</code> otherwise.
     */
    private boolean parseSectionHeaders() {
        if (isStartOfLine()) {
            int headerStartPosition = fCurrentPosition - 1;
            int endIndex = fStringSource.indexOf("\n", fCurrentPosition);
            if (endIndex < 0) {
                endIndex = fStringSource.length();
            }
            int headerEndPosition = endIndex;
            char ch;
            while (headerEndPosition > 0) {
                ch = fSource[--headerEndPosition];
                if (!Character.isWhitespace(ch)) {
                    break;
                }
            }
            if (headerEndPosition < 0 || headerEndPosition <= headerStartPosition) {
                return false;
            }
            int level = 0;
            int startPosition = headerStartPosition;
            int endPosition = headerEndPosition + 1;
            while (headerStartPosition < headerEndPosition) {
                if (fSource[headerStartPosition] == '=' && fSource[headerEndPosition] == '=') {
                    level++;
                    headerStartPosition++;
                    headerEndPosition--;
                } else {
                    headerEndPosition++;
                    break;
                }
            }
            if (level == 0) {
                return false;
            }
            if (level > 6) {
                level = 6;
            }
            createContentToken(1);
            reduceTokenStack();
            String head = "";
            if (headerEndPosition >= headerStartPosition) {
                if (headerEndPosition > headerStartPosition) {
                    head = fStringSource.substring(headerStartPosition, headerEndPosition);
                } else {
                    head = String.valueOf(fStringSource.charAt(headerStartPosition));
                }
            }
            fEventListener.onHeader(fSource, startPosition, endPosition, headerStartPosition, headerEndPosition, level);
            fCurrentPosition = endIndex;

            if (head != null) {
                fTableOfContentTag = fWikiModel.appendHead(head, level, fNoToC, ++fHeadCounter, startPosition, endPosition);
            }
            return true;
        }
        return false;
    }

    private boolean parseTable() {
        if (isStartOfLine()) {
            // wiki table ?
            setPosition(fCurrentPosition - 1);
            WPTable table = wpTable(fTableOfContentTag);
            if (table != null) {
                createContentToken(1);
                reduceTokenStack(table);
                // set pointer behind: "\n|}"
                fCurrentPosition = getPosition();
                fWikiModel.append(table);
                // table.filter(fSource, fWikiModel);
                return true;
            }
        }
        return false;
    }

    private boolean parseTemplate() {
        // dummy parsing of Wikipedia templates for event listeners
        // doesn't change fCurrentPosition
        if (fSource[fCurrentPosition] == '{') {
            int templateStartPosition = fCurrentPosition + 1;
            if (fSource[templateStartPosition] != '{') {
                int templateEndPosition = findNestedTemplateEnd(fSource, templateStartPosition);
                if (templateEndPosition > 0) {
                    fEventListener.onTemplate(fSource, templateStartPosition, templateEndPosition - 2);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Parse special identifiers like __TOC__, __NOTOC__, __FORCETOC__
     * 
     * @return
     */
    private boolean parseSpecialIdentifiers() {
        if (fSource[fCurrentPosition] == '_') {
            fCurrentPosition++;
            int tocEndPosition = fCurrentPosition;
            char ch;
            while (true) {
                ch = fSource[tocEndPosition++];
                if (ch >= 'A' && ch <= 'Z') {
                    continue;
                }
                break;
            }
            if (ch == '_' && fSource[tocEndPosition] == '_') {
                String tocIdent = fStringSource.substring(fCurrentPosition, tocEndPosition - 1);
                boolean tocRecognized = false;
                for (int i = 0; i < TOC_IDENTIFIERS.length; i++) {
                    if (TOC_IDENTIFIERS[i].equals(tocIdent)) {
                        createContentToken(2);
                        tocRecognized = true;
                        fCurrentPosition = tocEndPosition + 1;
                        switch (i) {
                        case 0: // TOC
                            fTableOfContentTag = fWikiModel.createTableOfContent(true);
                            fForceToC = true;
                            break;
                        case 1: // NOTOC
                            setNoToC(true);
                            break;
                        case 2: // FORCETOC
                            fForceToC = true;
                            break;
                        }
                        break;
                    }
                }
                if (tocRecognized) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if the scanners cursor position is at the beginning of a line.
     * 
     * @return <code>true</code> if the scanners cursor points to the beginning of
     *         a line, <code>false</code> otherwise.
     */
    private boolean isStartOfLine() {
        if (fCurrentPosition >= 2) {
            if (fSource[fCurrentPosition - 2] == '\n') {
                return true;
            }
        } else if (fCurrentPosition == 1) {
            return true;
        }
        return false;
    }

    private int isEndOfLine(char testChar, int currentPosition) {
        int tempPosition = currentPosition;
        try {
            char ch;
            while (true) {
                ch = fSource[tempPosition];
                if (ch != testChar) {
                    break;
                }
                tempPosition++;
            }
            while (true) {
                ch = fSource[tempPosition++];
                if (ch == '\n') {
                    return tempPosition;
                } else if (!Character.isWhitespace(ch)) {
                    return -1;
                }
            }
        } catch (IndexOutOfBoundsException e) {

        }
        return -1;
    }

    private void createTag(TagToken tag, WikiTagNode tagNode, int startMacroPosition) {
        String endTag;
        String macroBodyString = "";
        int index0;
        String command = tagNode.getTagName();
        if ((tag != null) && (tag instanceof IBodyTag) && (!tagNode.isEmptyXmlTag())) {
            endTag = command + '>';
            index0 = Util.indexOfIgnoreCase(fStringSource, "</", endTag, startMacroPosition);

            if (index0 >= 0) {
                macroBodyString = fStringSource.substring(startMacroPosition, index0);
                fCurrentPosition = index0 + endTag.length() + 2;
            } else {
                macroBodyString = fStringSource.substring(startMacroPosition, fSource.length);
                fCurrentPosition = fSource.length;
            }
        } else {
            macroBodyString = null;
            fCurrentPosition = startMacroPosition;
        }

        handleTag(tag, tagNode, macroBodyString);
    }

    private boolean handleHTTPLink(String name) {
        String urlString;
        String uriSchemeName = "";
        if (name != null) {
            boolean isEmail = false;
            urlString = name.trim();

            int index = -1;
            boolean foundUrl = false;
            try {
                index = urlString.indexOf(':', 1);
                if (index > 0) {
                    uriSchemeName = urlString.substring(0, index);
                    if (uriSchemeName.equalsIgnoreCase("mailto")) {
                        isEmail = true;
                        foundUrl = true;
                    } else {
                        if (fWikiModel.isValidUriScheme(uriSchemeName)) {
                            foundUrl = true;
                        }
                    }
                }
            } catch (IndexOutOfBoundsException e) {
            }

            if (foundUrl) {
                // Wikipedia link style: name separated by space?
                int pipeIndex = urlString.indexOf(' ');
                String alias = "";
                if (pipeIndex != (-1)) {
                    alias = urlString.substring(pipeIndex + 1);
                    urlString = urlString.substring(0, pipeIndex);
                } else {
                    alias = urlString;
                }

                if (isEmail) {
                    String email;
                    if (pipeIndex > 7) {
                        email = urlString.substring(7, pipeIndex);
                    } else {
                        email = urlString.substring(7);
                    }
                    if (EmailValidator.getInstance().isValid(email)) {
                        fWikiModel.appendMailtoLink(urlString, alias, false);
                        return true;
                    }
                } else {
                    parseURIScheme();
                    String uriSchemeSpecificPart = urlString.substring(index + 1);
                    if (fWikiModel.isValidUriSchemeSpecificPart(uriSchemeName, uriSchemeSpecificPart)) {
                        fWikiModel.appendExternalLink(uriSchemeName, urlString, alias, false);
                        return true;
                    }
                }

            }
        }
        return false;
    }

    private void handleTag(TagToken tag, WikiTagNode tagNode, String bodyString) {
        String command = tagNode.getTagName();
        try {
            if (tag instanceof EndTagToken) {
                fWikiModel.append(tag);
            } else {
                fWikiModel.pushNode(tag);
                if (null != bodyString) {
                    if (tag instanceof INoBodyParsingTag) {
                        ((TagNode) tag).addChild(new ContentToken(bodyString));
                    } else {
                        // recursively filter tags within the tags body string
                        WikipediaParser.parseRecursive(bodyString.trim(), fWikiModel, false, true);
                    }
                }
                if (tag instanceof IBodyTag) {
                    fWikiModel.popNode();
                }
            }
        } catch (IllegalArgumentException e) {
            TagNode divTagNode = new TagNode("div");
            divTagNode.addAttribute("class", "error", true);
            divTagNode.addChild(new ContentToken("IllegalArgumentException: " + command + " - " + e.getMessage()));
            fWikiModel.append(divTagNode);
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
            TagNode divTagNode = new TagNode("div");
            divTagNode.addAttribute("class", "error", true);
            divTagNode.addChild(new ContentToken(command + ": " + e.getMessage()));
            fWikiModel.append(divTagNode);
            e.printStackTrace();
        }
    }

    public void runParser() {
        int token = TokenSTART;
        while ((token = getNextToken()) != TokenEOF) {
            switch (token) {
            case TokenBOLDITALIC:
                if (fWikiModel.stackSize() > 0 && fWikiModel.peekNode().equals(BOLDITALIC)) {
                    fWikiModel.popNode();
                    // fResultBuffer.append("</i></b>");
                } else if (fWikiModel.stackSize() > 1 && fWikiModel.peekNode().equals(BOLD)
                        && fWikiModel.getNode(fWikiModel.stackSize() - 2).equals(ITALIC)) {
                    fWikiModel.popNode();
                    fWikiModel.popNode();
                    // fResultBuffer.append("</b></i>");
                } else if (fWikiModel.stackSize() > 1 && fWikiModel.peekNode().equals(ITALIC)
                        && fWikiModel.getNode(fWikiModel.stackSize() - 2).equals(BOLD)) {
                    fWikiModel.popNode();
                    fWikiModel.popNode();
                    // fResultBuffer.append("</i></b>");
                } else if (fWikiModel.stackSize() > 0 && fWikiModel.peekNode().equals(BOLD)) {
                    fWikiModel.popNode();
                    fWikiModel.pushNode(new WPTag("i"));
                } else if (fWikiModel.stackSize() > 0 && fWikiModel.peekNode().equals(ITALIC)) {
                    fWikiModel.popNode();
                    fWikiModel.pushNode(new WPTag("b"));
                } else {
                    fWikiModel.pushNode(new WPBoldItalicTag());
                    // fResultBuffer.append("<b><i>");
                }
                break;
            case TokenBOLD:
                if (fWikiModel.stackSize() > 0 && fWikiModel.peekNode().equals(BOLDITALIC)) {
                    fWikiModel.popNode();
                    fWikiModel.pushNode(new WPTag("i"));
                    // fResultBuffer.append("</b>");
                } else if (fWikiModel.stackSize() > 0 && fWikiModel.peekNode().equals(BOLD)) {
                    fWikiModel.popNode();
                    // fResultBuffer.append("</b>");
                } else {
                    fWikiModel.pushNode(new WPTag("b"));
                    // fResultBuffer.append("<b>");
                }
                break;
            case TokenITALIC:
                if (fWikiModel.stackSize() > 0 && fWikiModel.peekNode().equals(BOLDITALIC)) {
                    fWikiModel.popNode();
                    fWikiModel.pushNode(new WPTag("b"));
                    // fResultBuffer.append("</i>");
                } else if (fWikiModel.stackSize() > 0 && fWikiModel.peekNode().equals(ITALIC)) {
                    fWikiModel.popNode();
                    // fResultBuffer.append("</i>");
                } else {
                    fWikiModel.pushNode(new WPTag("i"));
                    // fResultBuffer.append("<i>");
                }
                break;
            }
        }
        reduceTokenStack();

        if (!fNoToC && fTableOfContentTag != null) {
            if (fHeadCounter > 3 || fForceToC) {
                fTableOfContentTag.setShowToC(true);
            }
        }

    }

    /**
     * Reduce the current token stack completely
     */
    private void reduceTokenStack() {
        while (fWikiModel.stackSize() > 0) {
            fWikiModel.popNode();
        }
    }

    private void reduceTokenStackBoldItalic() {
        boolean found = false;
        while (fWikiModel.stackSize() > 0) {
            TagToken token = fWikiModel.peekNode();//
            if (token.equals(BOLD) || token.equals(ITALIC) || token.equals(BOLDITALIC)) {
                if (fWhiteStart) {
                    found = true;
                    createContentToken(1);
                }
                fWikiModel.popNode();
            } else {
                return;
            }
        }
        if (found) {
            fWhiteStart = true;
            fWhiteStartPosition = fCurrentPosition;
        }
    }

    /**
     * Reduce the current token stack until an allowed parent is at the top of the
     * stack
     */
    private void reduceTokenStack(TagToken node) {
        String allowedParents = node.getParents();
        if (allowedParents != null) {
            TagToken tag;
            int index = -1;

            while (fWikiModel.stackSize() > 0) {
                tag = fWikiModel.peekNode();
                index = allowedParents.indexOf("|" + tag.getName() + "|");
                if (index < 0) {
                    fWikiModel.popNode();
                    if (tag.getName().equals(node.getName())) {
                        // for wrong nested HTML tags like <table> <tr><td>number
                        // 1<tr><td>number 2</table>
                        break;
                    }
                } else {
                    break;
                }
            }
        } else {
            while (fWikiModel.stackSize() > 0) {
                fWikiModel.popNode();
            }
        }
    }

    /**
     * Reduce the current token stack until the given nodes name is at the top of
     * the stack. Useful for closing HTML tags.
     */
    private void reduceStackUntilToken(TagToken node) {
        TagToken tag;
        int index = -1;
        String allowedParents = node.getParents();
        while (fWikiModel.stackSize() > 0) {
            tag = fWikiModel.peekNode();
            if (node.getName().equals(tag.getName())) {
                fWikiModel.popNode();
                break;
            }
            if (allowedParents == null) {
                fWikiModel.popNode();
            } else {
                index = allowedParents.indexOf("|" + tag.getName() + "|");
                if (index < 0) {
                    fWikiModel.popNode();
                } else {
                    break;
                }
            }
        }
    }

    public boolean isNoToC() {
        return fNoToC;
    }

    public void setNoToC(boolean noToC) {
        fNoToC = noToC;
    }

    /**
     * Call the parser on the first recursion level, where the text can contain a
     * table of contents (TOC).
     * 
     * <br/>
     * <br/>
     * <b>Note:</b> in this level the wiki model will call the
     * <code>setUp()</code> method before parsing and the <code>tearDown()</code>
     * method after the parser has finished.
     * 
     * @param rawWikitext
     *          the raw text of the article
     * @param wikiModel
     *          a suitable wiki model for the given wiki article text
     * @param parseTemplates
     *          parse the template expansion step
     * @param templateParserBuffer
     *          if the <code>templateParserBuffer != null</code> the
     *          <code>templateParserBuffer</code> will be used to append the
     *          result of the template expansion step
     * 
     */
    public static void parse(String rawWikiText, IWikiModel wikiModel, boolean parseTemplates, Appendable templateParserBuffer) {
        try {
            // initialize the wiki model
            wikiModel.setUp();

            Appendable buf;
            if (templateParserBuffer != null) {
                buf = templateParserBuffer;
            } else {
                buf = new StringBuilder(rawWikiText.length() + rawWikiText.length() / 10);
            }
            if (parseTemplates) {
                String pass1Text = null;
                try {
                    TemplateParser.parse(rawWikiText, wikiModel, buf, wikiModel.isTemplateTopic());
                    pass1Text = buf.toString();
                } catch (Exception ioe) {
                    ioe.printStackTrace();
                    pass1Text = "<span class=\"error\">TemplateParser exception: " + ioe.getClass().getSimpleName() + "</span>";
                }
                String redirectedLink = AbstractParser.parseRedirect(pass1Text, wikiModel);
                if (redirectedLink == null) {
                    parseRecursive(pass1Text, wikiModel, false, false);
                }
            } else {
                if (AbstractParser.parseRedirect(rawWikiText, wikiModel) == null) {
                    parseRecursive(rawWikiText, wikiModel, false, false);
                }
            }
        } finally {
            // clean up wiki model if necessary
            wikiModel.tearDown();
        }
    }

    /**
     * Call the parser on the subsequent recursion levels, where the subtexts (of
     * templates, table cells, list items or image captions) don't contain a table
     * of contents (TOC)
     * 
     * <b>Note:</b> the wiki model doesn't call the <code>setUp()</code> or
     * <code>tearDown()</code> methods for the subsequent recursive parser steps.
     * 
     * @param rawWikitext
     * @param wikiModel
     * @return
     */
    public static void parseRecursive(String rawWikitext, IWikiModel wikiModel) {
        parseRecursive(rawWikitext, wikiModel, false, true);
    }

    /**
     * Call the parser on the subsequent recursion levels, where the subtexts (of
     * templates, table cells, list items or image captions) don't contain a table
     * of contents (TOC)
     * 
     * <b>Note:</b> the wiki model doesn't call the <code>setUp()</code> or
     * <code>tearDown()</code> methods for the subsequent recursive parser steps.
     * 
     * @param rawWikitext
     * @param wikiModel
     * @param noTOC
     * @param appendStack
     * @return
     * @return
     */
    public static TagStack parseRecursive(String rawWikitext, IWikiModel wikiModel, boolean createOnlyLocalStack, boolean noTOC) {
        AbstractParser parser = wikiModel.createNewInstance(rawWikitext);
        return parser.parseRecursiveInternal(wikiModel, createOnlyLocalStack, noTOC);
    }

    /**
     * Determine if the currently parsed wiki text is a template text.
     * 
     * @return <code>true</code> if the currently parsed wiki text is a template
     */
    public boolean isTemplate() {
        return fRenderTemplate;
    }

}