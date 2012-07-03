/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/
package org.commoncrawl.service.parser.server;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.commoncrawl.io.NIOHttpHeaders;
import org.commoncrawl.service.parser.Link;
import org.commoncrawl.service.parser.Meta;
import org.commoncrawl.service.parser.ParseResult;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.CharsetUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.HttpHeaderUtils;
import org.commoncrawl.util.MimeTypeFilter;
import org.commoncrawl.util.HttpHeaderUtils.ContentTypeAndCharset;
import org.commoncrawl.util.MimeTypeFilter.MimeTypeDisposition;
import org.commoncrawl.util.Tuples.Pair;
import org.w3c.dom.Document;

import com.dappit.Dapper.parser.DocumentBuilder;
import com.dappit.Dapper.parser.InstructionsPool;
import com.dappit.Dapper.parser.MozillaParser;
import com.dappit.Dapper.parser.ParserInitializationException;
import com.dappit.Dapper.parser.ParserInstruction;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteProcessor;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.gson.JsonObject;

/**
 * 
 * @author rana
 *
 */
public class ParseWorker implements DocumentBuilder {

  private static final Log LOG = LogFactory.getLog(ParserSlaveServer.class);

  URL baseURL = null;
  
  ImmutableMap<String,String> linkTypeToSrcMap
  
    = new ImmutableMap.Builder<String,String>()
        .put("a", "href")
        .put("area", "href")
        .put("frame", "src")
        .put("iframe", "src")
        .put("script", "src")
        .put("link", "href")
        .put("img", "src").build();
  
  ImmutableSet<String> ignoreTextTagSet 
    = new ImmutableSet.Builder<String>()
      .add("noscript")
      .build();

  class LinkUnderConstruction { 
    public String linkURL = null;
    public String type    = null;
    public JsonObject jsonObject = new JsonObject();
    public String linkText = "";
    
    public LinkUnderConstruction(String linkType,BlockObjectInContext blockInContext) {
      type = linkType;
      jsonObject.addProperty("type", linkType);
      /*
      if (blockInContext != null) {
        JsonObject blockJSONObject = new JsonObject();
        blockJSONObject.addProperty("type", blockInContext.type);
        blockJSONObject.addProperty("oid", blockInContext.id);
        if (blockInContext.htmlId != null)
          blockJSONObject.addProperty("id", blockInContext.htmlId);
        if (blockInContext.classId != null)
          blockJSONObject.addProperty("class", blockInContext.classId);
        if (blockInContext.type.equals("table")) { 
          blockJSONObject.addProperty("t_row", Math.max(0,blockInContext.rowNumber));
          blockJSONObject.addProperty("t_cell", Math.max(0,blockInContext.cellNumber));
        }
        if (blockInContext.parent != null) { 
          blockJSONObject.addProperty("p_oid", blockInContext.parent.id);
//          if (blockInContext.parent.htmlId != null)
//            blockJSONObject.addProperty("p_html_id", blockInContext.parent.htmlId);
//          if (blockInContext.parent.classId != null)
//            blockJSONObject.addProperty("p_class", blockInContext.parent.classId);
          
        }
        jsonObject.add("context", blockJSONObject);
      }
      */
    }
    
    public Link buildLink() {
      if (linkURL != null && linkURL.length() != 0 && !linkURL.startsWith("#")) { 
        try {
          URL url = new URL(baseURL,linkURL);

          Link link = new Link();
          link.setUrl(url.toString());
          jsonObject.addProperty("text", linkText);
          link.setAttributes(jsonObject.toString());
          
          return link;

        } catch (MalformedURLException e) {
          //LOG.error(CCStringUtils.stringifyException(e));
        }
      }
      return null;

    }
  }
  
  private ParseResult activeParseResult;
  
  public void parsePartialHTMLDocument(ParseResult parseResultOut,URL baseURL,String content)throws IOException { 
    parseResultOut.setParseSuccessful(false);
    this.baseURL = baseURL;
    try {
      // init parser ... 
      MozillaParser.init(null, "/usr/local/lib");
      
      MozillaParser parser;
      
      try {
        parser = new MozillaParser(this);
        activeParseResult = parseResultOut;
        //LOG.info("Parsing Document");
        parser.parse(content.getBytes(Charset.forName("UTF-8")),"utf-8",null);
        activeParseResult = null;
        // set content type ... 
        parseResultOut.setContentType("text/html");
        parseResultOut.setText(textAccumulator.toString().replaceAll("[\\s]+", " "));
        parseResultOut.setParseSuccessful(true);
      } catch (ParserInitializationException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        parseResultOut.setParseFailureReason("Parser Initialization Failed!");
      }
      catch (Exception e) {
        parseResultOut.setParseFailureReason(CCStringUtils.stringifyException(e));
        LOG.error(parseResultOut);
      }      
    }
    catch (ParserInitializationException e) {
      parseResultOut.setParseFailureReason("Parser Initialization Failed!");
      LOG.error(CCStringUtils.stringifyException(e));
      throw new IOException(e);
    }
  }
  
  public void parseDocument(ParseResult parseResultOut,long domainId,long documentId,URL baseURL,String rawHeaders, FlexBuffer data)throws IOException {
    
    parseResultOut.setParseSuccessful(false);
    
    this.baseURL = baseURL;
    
    if (data.getCount() != 0) { 
      try {
        // init parser ... 
        MozillaParser.init(null, "/usr/local/lib");
        // load headers ... 
        NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(rawHeaders);
        // detect content type ... 
        ContentTypeAndCharset contentTypeInfo = new ContentTypeAndCharset();
        HttpHeaderUtils.parseContentType(headers, contentTypeInfo);
        //LOG.info("ContentType:" + contentTypeInfo._contentType + " Charset:" + contentTypeInfo._charset);
        // ok now extract charset if possible ... 
        Pair<Integer,Charset> charsetTuple = CharsetUtils.bestEffortDetectCharset(rawHeaders,data.get(),data.getOffset(),data.getCount());
        if (charsetTuple == null) { 
          charsetTuple = new Pair<Integer,Charset>(CharsetUtils.CHARSET_SRC_NO_MATCH,Charset.forName("ISO-8859-1"));
        }
        // decode bytes ... and convert to utf-8
        ByteBuffer utf8Bytes = null;
        try {
          if (charsetTuple.e1.toString().equalsIgnoreCase("utf-8")) { 
            //LOG.info("Input Charset is utf-8, transposing source bytes to dest bytes");
            if (data.getOffset() == 0) { 
              utf8Bytes = ByteBuffer.wrap(data.get(), 0, data.getCount());
            }
            else { 
              byte[] buffer = new byte[data.getCount()];
              System.arraycopy(data.get(), data.getOffset(),buffer, 0, data.getCount());
              utf8Bytes = ByteBuffer.wrap(buffer);
            }
          }
          else { 
            CharBuffer ucs2Chars = charsetTuple.e1.decode(ByteBuffer.wrap(data.get(),data.getOffset(),data.getCount()));
            utf8Bytes = Charset.forName("UTF-8").encode(ucs2Chars);
          }
        } catch (Exception e) {
          LOG.error(CCStringUtils.stringifyException(e));
          parseResultOut.setParseFailureReason(CCStringUtils.stringifyException(e));
          // this should not have happened... we consider this unrecoverable
          throw new IOException(e);
        }
        if (utf8Bytes == null || utf8Bytes.remaining() == 0) {
          parseResultOut.setParseFailureReason("Invalid UTF-8 bytes detected for doc:" + baseURL + " detector:" + charsetTuple.e0 + " Charset:" + charsetTuple.e1);
          throw new IOException(parseResultOut.getParseFailureReason());
        }
        //LOG.info("UTF-8 Data Length:" + utf8Bytes.remaining());
        MimeTypeDisposition disposition = MimeTypeFilter.checkMimeTypeDisposition(contentTypeInfo._contentType);
        //LOG.info("MimeType Disposition:"+ disposition);
        if (disposition == MimeTypeDisposition.ACCEPT_HTML) { 
          // ok ready to send to mozilla ... 
          MozillaParser parser;
          try {
            parser = new MozillaParser(this);
            activeParseResult = parseResultOut;
            //LOG.info("Parsing Document");
            parser.parse(utf8Bytes.array(),"utf-8",null);
            activeParseResult = null;
            // set content type ... 
            parseResultOut.setContentType(contentTypeInfo._contentType);
            parseResultOut.setText(textAccumulator.toString().replaceAll("[\\s]+", " "));
            parseResultOut.setParseSuccessful(true);
          } catch (ParserInitializationException e) {
            LOG.error(CCStringUtils.stringifyException(e));
            parseResultOut.setParseFailureReason("Parser Initialization Failed!");
          }
          catch (Exception e) {
            parseResultOut.setParseFailureReason(CCStringUtils.stringifyException(e));
            LOG.error(parseResultOut);
          }
        }
        else if (disposition == MimeTypeDisposition.ACCEPT_OTHER) {         
          
        }
        else { 
          parseResultOut.setParseFailureReason("Unsupported ContentType:" + contentTypeInfo._contentType);
        }
        
      } catch (ParserInitializationException e) {
        parseResultOut.setParseFailureReason("Parser Initialization Failed!");
        LOG.error(CCStringUtils.stringifyException(e));
        throw new IOException(e);
      }
    }
  }
    
  public static void main(String[] args) throws IOException {
    String baseURL = "http://unknown.com/";
    if (args.length != 0) { 
      baseURL = args[0];
    }
    URL baseURLObj;
    try {
      baseURLObj = new URL(baseURL);
    } catch (MalformedURLException e2) {
      throw new IOException("Invalid Base Link");
    }
    final DataOutputBuffer headerBuffer = new DataOutputBuffer();
    final DataOutputBuffer contentBuffer = new DataOutputBuffer();
    
    try {
      ByteStreams.readBytes(
          new InputSupplier<InputStream>() {

            @Override
            public InputStream getInput() throws IOException {
              return System.in;
            }
          }
          ,new ByteProcessor<Long>() {

        @Override
        public Long getResult() {
          return 0L;
        }

        int currLineCharCount = 0;
        boolean processingHeaders = true;
        @Override
        public boolean processBytes(byte[] buf, int start, int length)
            throws IOException {
          
          if (processingHeaders) { 
            int current = start;
            int end   = current + length;
            while (processingHeaders && current != end) {
              if (buf[current] != '\r' && buf[current] != '\n') { 
                currLineCharCount++;
              }
              else if (buf[current] == '\n') { 
                if (currLineCharCount == 0){ 
                  headerBuffer.write(buf,start,current - start + 1);
                  processingHeaders = false;
                }
                currLineCharCount = 0;
              }
              current++;
            }
            if (processingHeaders) { 
              headerBuffer.write(buf,start,length);
            }
            else { 
              length -= current-start;
              start = current;
            }
          }
          if (!processingHeaders) { 
            contentBuffer.write(buf,start,length);
          }
          return true;
        }
      });
      
      //LOG.info("HEADER LEN:" + headerBuffer.getLength());
      // System.out.println(new String(headerBuffer.getData(),0,headerBuffer.getLength(),Charset.forName("UTF-8")));
      //LOG.info("CONTENT LEN:" + contentBuffer.getLength());
      //System.out.println(new String(contentBuffer.getData(),0,contentBuffer.getLength(),Charset.forName("UTF-8")));
      // decode header bytes ... 
      String header = "";
      if (headerBuffer.getLength() != 0) { 
        try { 
          header = new String(headerBuffer.getData(),0,headerBuffer.getLength(),Charset.forName("UTF-8"));
        }
        catch (Exception e) { 
          LOG.warn(CCStringUtils.stringifyException(e));
          header = new String(headerBuffer.getData(),0,headerBuffer.getLength(),Charset.forName("ASCII"));
        }
      }
      
      //LOG.info("Parsing Document");
      ParseWorker worker = new ParseWorker();
      ParseResult result = new ParseResult();
      worker.parseDocument(result,0L,0L,baseURLObj,header,new FlexBuffer(contentBuffer.getData(),0,contentBuffer.getLength()));
      //LOG.info("Parse Result:" + result.getParseSuccessful()); 
      //LOG.info("Parse Data:" + result.toString());  
      
      
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    /*
    List<String> lines;
    try {
      lines = IOUtils.readLines(System.in, "UTF-8");
      for (String line : lines){ 
        System.out.println(line);
      }

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }*/
  }

  int inHeadTag = 0;
  int inBase    = 0;
  int blockId   = 0;
  int inTable   = 0;
  
  LinkUnderConstruction activeLink = null;
  BlockObjectInContext blockInConstruction = null;
  LinkedList<LinkUnderConstruction> linksUnderConstruction = new LinkedList<LinkUnderConstruction>();
  StringBuffer textAccumulator = new StringBuffer();

  static class BlockObjectInContext {
    public BlockObjectInContext parent;
    public String type = "";
    public int id;
    public int rowNumber=-1;
    public int cellNumber=-1;
    public String classId=null;
    public String htmlId = null;
    
    public BlockObjectInContext(BlockObjectInContext parentObject,String type,int id) {
      this.parent= parentObject;
      this.type = type;
      this.id = id;
    }
  }
  
  static ImmutableSet<String> validMetaTagKeys = new ImmutableSet.Builder<String>() 
    .add("name")
    .add("property")
    .add("http-equiv")
    .build();
    
  @Override
  public Document buildDocument(InstructionsPool instructionsPool,FileOutputStream optionalOutputStream) throws IOException {
    
    //LOG.info("Build Document Called");
    List<Integer> operations = instructionsPool.operations;
    List<String> arguments = instructionsPool.arguments;
    LinkedList<Integer> nodeStack = new LinkedList<Integer>();
    LinkedList<BlockObjectInContext> blockStack = new LinkedList<BlockObjectInContext>();
    Meta meta = null;
    
    for (int i=0; i<operations.size(); i++)
    {
        int domOperation = operations.get(i);
        String domArgument = arguments.get(i);
        //System.out.println("Operation :" + ParserInstruction.getOperationString(domOperation)+" Arg:~" + domArgument+"~");
        switch (domOperation)
        {
        // Open node :
        case ParserInstruction.OpenNode:
        case ParserInstruction.AddLeaf: {           
          activeLink = null;
          blockInConstruction = null;
          String nodeName = domArgument.toLowerCase();
          if (nodeName.equals("meta")) { 
            meta = new Meta();
          }
          else if (linkTypeToSrcMap.containsKey(nodeName)) {
            //LOG.info("Node:" + nodeName + " is of type Link. Adding to LinksUnderConst");
            activeLink = new LinkUnderConstruction(nodeName,blockStack.peek());
            linksUnderConstruction.push(activeLink);
          }
          else if (nodeName.equals("head")) { 
            inHeadTag++;
          }
          else if (nodeName.equals("base")) { 
            if (inHeadTag != 0) { 
              inBase++;
            }
          }
          else if (nodeName.equals("table") || nodeName.equals("div")) {
            blockInConstruction = new BlockObjectInContext(blockStack.peek(),nodeName,++blockId); 
            blockStack.push(blockInConstruction);
          }
          else if (nodeName.equals("tr") || nodeName.equals("th")) { 
            BlockObjectInContext table = blockStack.peek();
            if (table != null) { 
              table.rowNumber++;
              table.cellNumber = -1;
            }
          }
          else if (nodeName.equals("td")) { 
            BlockObjectInContext table = blockStack.peek();
            if (table != null) { 
              table.cellNumber++;
            }
          }
          nodeStack.push(i);
        }
        break;
        // Close node :
        case ParserInstruction.CloseNode:
        case ParserInstruction.CloseLeaf: {
          int arguementPos = nodeStack.pop();
          String nodeName = arguments.get(arguementPos).toLowerCase();
          //LOG.info("Close Node Called on Node:" + nodeName);
          if (nodeName.equals("head")) { 
            inHeadTag--;
          }
          else if (nodeName.equals("base")) { 
            if (inHeadTag != 0) { 
              inBase--;
            }
          }
          else if (linkTypeToSrcMap.containsKey(nodeName)){
            //LOG.info("Node:" + nodeName + " is a Link Type");
            LinkUnderConstruction linkPartial = linksUnderConstruction.pop();
            if (linkPartial != null) {
              //LOG.info("POPed a partial LinkObject of type:" + linkPartial.type);
              Link link = linkPartial.buildLink();
              if (link != null) { 
                activeParseResult.getExtractedLinks().add(link);
              }
            }
          }
          else if (nodeName.equals("table") || nodeName.equals("div")) {
            blockStack.pop();
          }
          else if (nodeName.equals("meta")) {
            if (meta != null) { 
              activeParseResult.getMetaTags().add(meta);
              meta = null;
            }
          }
          if (textAccumulator.length() != 0 
              && !Character.isWhitespace(textAccumulator.charAt(textAccumulator.length() - 1))) {  
            textAccumulator.append(" ");
          }
          
        }
        break;
        case ParserInstruction.AddText: {
          Integer arguementPos = nodeStack.peek();
          String nodeName = (arguementPos != null) ? arguments.get(arguementPos).toLowerCase() :null;          
          LinkUnderConstruction link = linksUnderConstruction.peek();
        
          if (link != null) {
            if (link.linkText.length() != 0)
              link.linkText += " ";
            link.linkText += domArgument.trim();
          }
          if (nodeName == null || !ignoreTextTagSet.contains(nodeName.toLowerCase())) {                 
            textAccumulator.append(domArgument);
          }
          
        }break;
//        case ParserInstruction.AddContent:
//          System.out.println("AddContent:"+domArgument);
//          break;
        
       case ParserInstruction.WriteAttributeKey: {
         
          // add an attribute with the next lookahead operation :
          String key = domArgument.toLowerCase();

          i++;
          domOperation = operations.get(i); 
          String value = arguments.get(i); 
          
          if (meta != null) {
            if (key.equalsIgnoreCase("content")) {
              meta.setValue(value);
            }
            else if (validMetaTagKeys.contains(key)) { 
              meta.setName(value);
            }
          }

          if(key.equals("href") && inBase != 0) { 
            if (value.length() != 0) { 
              try { 
                baseURL = new URL(value);
              }
              catch (Exception e) { 
                LOG.error(CCStringUtils.stringifyException(e));
                throw new IOException(e);
              }
            }
          }
          else if (activeLink != null) {
            if (linkTypeToSrcMap.get(activeLink.type).equalsIgnoreCase(key)) { 
              activeLink.linkURL = value;
            }
            else { 
              activeLink.jsonObject.addProperty(key, value);
            }
          }
          else if (blockInConstruction != null){ 
            if (key.equals("class")) { 
              blockInConstruction.classId = value;
            }
            else if (key.equals("id")) { 
              blockInConstruction.htmlId = value;
            }
          }
        }
        break;
        
       case ParserInstruction.SetTitle: { 
         activeParseResult.setTitle(domArgument);
       }
       break;
//        case ParserInstruction.AddEntity:
//          System.out.println("AddEntity:" + domArgument);
//            break;
//        case ParserInstruction.AddComment:
//          System.out.println("AddComment:" + domArgument); 
//            break;        case ParserInstruction.SetTitle:
//          System.out.println("SetTitle:" + domArgument);
//            break;
//        }
        }
      }
    return null;
  }
}
