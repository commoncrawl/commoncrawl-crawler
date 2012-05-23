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

package org.commoncrawl.util;

import gnu.xml.libxmlj.dom.GnomeDocument;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.hadoop.io.WritableUtils;
import org.commoncrawl.util.BitUtils.BitStream;
import org.commoncrawl.util.BitUtils.BitStreamReader;
import org.commoncrawl.util.RiceCoding.RiceCodeReader;
import org.commoncrawl.util.Tuples.Pair;
import org.w3c.dom.Attr;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Comment;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.dappit.Dapper.parser.InstructionsPool;
import com.dappit.Dapper.parser.ParserInstruction;
import com.google.common.collect.ImmutableList;

/**
 * 
 * @author rana
 *
 */
public class HTMLDomUtils {
  
  static class TextTokenData {
    
    public TextTokenData(String tokenValue) { 
      _token = tokenValue;
    }
    
    public String _token = null;  // the actual token value ...  
    public int _tokenId = -1;     // token id assoicated with this token 
    public int _tokenInstanceCount = 0; // number of times token appears in document ...    
  }
    
  static class HTMLTagSupport { 
    
    ImmutableList<TextTokenData> _htmlTagList;
    ImmutableList<TextTokenData> _htmlAttributeList;
    Map<String,TextTokenData> _htmlTagMap;
    Map<String,TextTokenData> _htmlAttributeTagMap;
    
    
    void initializeHTMLTagMaps() { 
    
      ImmutableList.Builder<TextTokenData> builder = new ImmutableList.Builder<TextTokenData>();
  
      builder.add(new TextTokenData("href"));
      builder.add(new TextTokenData("class"));
      builder.add(new TextTokenData("rel"));
      builder.add(new TextTokenData("src"));
      builder.add(new TextTokenData("id"));
      builder.add(new TextTokenData("type"));
      builder.add(new TextTokenData("title"));
      builder.add(new TextTokenData("style"));
      builder.add(new TextTokenData("width"));
      builder.add(new TextTokenData("height"));
      builder.add(new TextTokenData("alt"));
      
      builder.add(new TextTokenData("abbr"));
      builder.add(new TextTokenData("accept"));
      builder.add(new TextTokenData("accesskey"));
      builder.add(new TextTokenData("action"));
      builder.add(new TextTokenData("add"));
      builder.add(new TextTokenData("additional"));
      builder.add(new TextTokenData("align"));
      builder.add(new TextTokenData("alink"));
      builder.add(new TextTokenData("archive"));
      builder.add(new TextTokenData("autobuffer"));
      builder.add(new TextTokenData("autocomplete"));
      builder.add(new TextTokenData("autofocus"));
      builder.add(new TextTokenData("autoplay"));
      builder.add(new TextTokenData("autosave"));
      builder.add(new TextTokenData("axis"));
      builder.add(new TextTokenData("background"));
      builder.add(new TextTokenData("behavior"));
      builder.add(new TextTokenData("bgcolor"));
      builder.add(new TextTokenData("bgproperties"));
      builder.add(new TextTokenData("border"));
      builder.add(new TextTokenData("bordercolor"));
      builder.add(new TextTokenData("cellborder"));
      builder.add(new TextTokenData("cellpadding"));
      builder.add(new TextTokenData("cellspacing"));
      builder.add(new TextTokenData("challenge"));
      builder.add(new TextTokenData("char"));
      builder.add(new TextTokenData("charoff"));
      builder.add(new TextTokenData("charset"));
      builder.add(new TextTokenData("checked"));
      builder.add(new TextTokenData("cite"));
      builder.add(new TextTokenData("classid"));
      builder.add(new TextTokenData("clear"));
      builder.add(new TextTokenData("code"));
      builder.add(new TextTokenData("codebase"));
      builder.add(new TextTokenData("codetype"));
      builder.add(new TextTokenData("color"));
      builder.add(new TextTokenData("cols"));
      builder.add(new TextTokenData("colspan"));
      builder.add(new TextTokenData("compact"));
      builder.add(new TextTokenData("composite"));
      builder.add(new TextTokenData("content"));
      builder.add(new TextTokenData("contenteditable"));
      builder.add(new TextTokenData("controls"));
      builder.add(new TextTokenData("coords"));
      builder.add(new TextTokenData("create"));
      builder.add(new TextTokenData("curr"));
      builder.add(new TextTokenData("data"));
      builder.add(new TextTokenData("datetime"));
      builder.add(new TextTokenData("declare"));
      builder.add(new TextTokenData("defer"));
      builder.add(new TextTokenData("dir"));
      builder.add(new TextTokenData("direction"));
      builder.add(new TextTokenData("disabled"));
      builder.add(new TextTokenData("enctype"));
      builder.add(new TextTokenData("equiv"));
      builder.add(new TextTokenData("face"));
      builder.add(new TextTokenData("for"));
      builder.add(new TextTokenData("frame"));
      builder.add(new TextTokenData("frameborder"));
      builder.add(new TextTokenData("get"));
      builder.add(new TextTokenData("has"));
      builder.add(new TextTokenData("headers"));
      builder.add(new TextTokenData("hidden"));
      builder.add(new TextTokenData("hreflang"));
      builder.add(new TextTokenData("hspace"));
      builder.add(new TextTokenData("incremental"));
      builder.add(new TextTokenData("insert"));
      builder.add(new TextTokenData("ismap"));
      builder.add(new TextTokenData("keytype"));
      builder.add(new TextTokenData("label"));
      builder.add(new TextTokenData("lang"));
      builder.add(new TextTokenData("language"));
      builder.add(new TextTokenData("leftmargin"));
      builder.add(new TextTokenData("link"));
      builder.add(new TextTokenData("longdesc"));
      builder.add(new TextTokenData("loop"));
      builder.add(new TextTokenData("lowsrc"));
      builder.add(new TextTokenData("manifest"));
      builder.add(new TextTokenData("mapped"));
      builder.add(new TextTokenData("marginheight"));
      builder.add(new TextTokenData("marginwidth"));
      builder.add(new TextTokenData("max"));
      builder.add(new TextTokenData("maxlength"));
      builder.add(new TextTokenData("mayscript"));
      builder.add(new TextTokenData("media"));
      builder.add(new TextTokenData("method"));
      builder.add(new TextTokenData("min"));
      builder.add(new TextTokenData("multiple"));
      builder.add(new TextTokenData("name"));
      builder.add(new TextTokenData("nohref"));
      builder.add(new TextTokenData("noresize"));
      builder.add(new TextTokenData("noshade"));
      builder.add(new TextTokenData("nowrap"));
      builder.add(new TextTokenData("object"));
      builder.add(new TextTokenData("onabort"));
      builder.add(new TextTokenData("onbeforecopy"));
      builder.add(new TextTokenData("onbeforecut"));
      builder.add(new TextTokenData("onbeforepaste"));
      builder.add(new TextTokenData("onbeforeunload"));
      builder.add(new TextTokenData("onblur"));
      builder.add(new TextTokenData("onchange"));
      builder.add(new TextTokenData("onclick"));
      builder.add(new TextTokenData("oncontextmenu"));
      builder.add(new TextTokenData("oncopy"));
      builder.add(new TextTokenData("oncut"));
      builder.add(new TextTokenData("ondblclick"));
      builder.add(new TextTokenData("ondrag"));
      builder.add(new TextTokenData("ondragend"));
      builder.add(new TextTokenData("ondragenter"));
      builder.add(new TextTokenData("ondragleave"));
      builder.add(new TextTokenData("ondragover"));
      builder.add(new TextTokenData("ondragstart"));
      builder.add(new TextTokenData("ondrop"));
      builder.add(new TextTokenData("onerror"));
      builder.add(new TextTokenData("onfocus"));
      builder.add(new TextTokenData("oninput"));
      builder.add(new TextTokenData("onkeydown"));
      builder.add(new TextTokenData("onkeypress"));
      builder.add(new TextTokenData("onkeyup"));
      builder.add(new TextTokenData("onload"));
      builder.add(new TextTokenData("onmousedown"));
      builder.add(new TextTokenData("onmousemove"));
      builder.add(new TextTokenData("onmouseout"));
      builder.add(new TextTokenData("onmouseover"));
      builder.add(new TextTokenData("onmouseup"));
      builder.add(new TextTokenData("onmousewheel"));
      builder.add(new TextTokenData("onpaste"));
      builder.add(new TextTokenData("onreset"));
      builder.add(new TextTokenData("onresize"));
      builder.add(new TextTokenData("onscroll"));
      builder.add(new TextTokenData("onsearch"));
      builder.add(new TextTokenData("onselect"));
      builder.add(new TextTokenData("onselectstart"));
      builder.add(new TextTokenData("onstorage"));
      builder.add(new TextTokenData("onsubmit"));
      builder.add(new TextTokenData("onunload"));
      builder.add(new TextTokenData("poster"));
      builder.add(new TextTokenData("precision"));
      builder.add(new TextTokenData("process"));
      builder.add(new TextTokenData("profile"));
      builder.add(new TextTokenData("prompt"));
      builder.add(new TextTokenData("readonly"));
      builder.add(new TextTokenData("remove"));
      builder.add(new TextTokenData("results"));
      builder.add(new TextTokenData("rev"));
      builder.add(new TextTokenData("rows"));
      builder.add(new TextTokenData("rowspan"));
      builder.add(new TextTokenData("rules"));
      builder.add(new TextTokenData("scheme"));
      builder.add(new TextTokenData("scope"));
      builder.add(new TextTokenData("scrollamount"));
      builder.add(new TextTokenData("scrolldelay"));
      builder.add(new TextTokenData("scrolling"));
      builder.add(new TextTokenData("selected"));
      builder.add(new TextTokenData("set"));
      builder.add(new TextTokenData("shape"));
      builder.add(new TextTokenData("size"));
      builder.add(new TextTokenData("source"));
      builder.add(new TextTokenData("span"));
      builder.add(new TextTokenData("standby"));
      builder.add(new TextTokenData("start"));
      builder.add(new TextTokenData("summary"));
      builder.add(new TextTokenData("tabindex"));
      builder.add(new TextTokenData("tableborder"));
      builder.add(new TextTokenData("target"));
      builder.add(new TextTokenData("text"));
      builder.add(new TextTokenData("topmargin"));
      builder.add(new TextTokenData("truespeed"));
      builder.add(new TextTokenData("usemap"));
      builder.add(new TextTokenData("valign"));
      builder.add(new TextTokenData("value"));
      builder.add(new TextTokenData("valuetype"));
      builder.add(new TextTokenData("version"));
      builder.add(new TextTokenData("viewsource"));
      builder.add(new TextTokenData("vlink"));
      builder.add(new TextTokenData("vspace"));
      builder.add(new TextTokenData("wrap"));
      
      _htmlAttributeList = builder.build();
      _htmlAttributeTagMap = new HashMap<String, TextTokenData>();
      for (TextTokenData token : _htmlAttributeList) { 
        _htmlAttributeTagMap.put(token._token, token);
      }
      
      
      builder = new ImmutableList.Builder<TextTokenData>();
  
      builder.add(new TextTokenData("div"));
      builder.add(new TextTokenData("a"));
      builder.add(new TextTokenData("li"));
      builder.add(new TextTokenData("script"));
      builder.add(new TextTokenData("p"));
      builder.add(new TextTokenData("img"));
      builder.add(new TextTokenData("ul"));
      builder.add(new TextTokenData("br"));
      builder.add(new TextTokenData("address"));
      builder.add(new TextTokenData("abbr"));
      builder.add(new TextTokenData("area"));
      builder.add(new TextTokenData("article"));
      builder.add(new TextTokenData("aside"));
      builder.add(new TextTokenData("audio"));
      builder.add(new TextTokenData("b"));
      builder.add(new TextTokenData("base"));
      builder.add(new TextTokenData("bb"));
      builder.add(new TextTokenData("bdo"));
      builder.add(new TextTokenData("blockquote"));
      builder.add(new TextTokenData("body"));
      builder.add(new TextTokenData("button"));
      builder.add(new TextTokenData("canvas"));
      builder.add(new TextTokenData("caption"));
      builder.add(new TextTokenData("cite"));
      builder.add(new TextTokenData("code"));
      builder.add(new TextTokenData("col"));
      builder.add(new TextTokenData("colgroup"));
      builder.add(new TextTokenData("command"));
      builder.add(new TextTokenData("datagrid"));
      builder.add(new TextTokenData("datalist"));
      builder.add(new TextTokenData("dd"));
      builder.add(new TextTokenData("del"));
      builder.add(new TextTokenData("details"));
      builder.add(new TextTokenData("dialog"));
      builder.add(new TextTokenData("dfn"));
      builder.add(new TextTokenData("dl"));
      builder.add(new TextTokenData("dt"));
      builder.add(new TextTokenData("em"));
      builder.add(new TextTokenData("embed"));
      builder.add(new TextTokenData("eventsource"));
      builder.add(new TextTokenData("fieldset"));
      builder.add(new TextTokenData("figure"));
      builder.add(new TextTokenData("footer"));
      builder.add(new TextTokenData("form"));
      builder.add(new TextTokenData("h1"));
      builder.add(new TextTokenData("h2"));
      builder.add(new TextTokenData("h3"));
      builder.add(new TextTokenData("h4"));
      builder.add(new TextTokenData("h5"));
      builder.add(new TextTokenData("h6"));
      builder.add(new TextTokenData("head"));
      builder.add(new TextTokenData("header"));
      builder.add(new TextTokenData("hr"));
      builder.add(new TextTokenData("html"));
      builder.add(new TextTokenData("i"));
      builder.add(new TextTokenData("iframe"));
      builder.add(new TextTokenData("input"));
      builder.add(new TextTokenData("ins"));
      builder.add(new TextTokenData("kbd"));
      builder.add(new TextTokenData("label"));
      builder.add(new TextTokenData("legend"));
      builder.add(new TextTokenData("link"));
      builder.add(new TextTokenData("mark"));
      builder.add(new TextTokenData("map"));
      builder.add(new TextTokenData("menu"));
      builder.add(new TextTokenData("meta"));
      builder.add(new TextTokenData("meter"));
      builder.add(new TextTokenData("nav"));
      builder.add(new TextTokenData("noscript"));
      builder.add(new TextTokenData("object"));
      builder.add(new TextTokenData("ol"));
      builder.add(new TextTokenData("optgroup"));
      builder.add(new TextTokenData("option"));
      builder.add(new TextTokenData("output"));
      builder.add(new TextTokenData("param"));
      builder.add(new TextTokenData("pre"));
      builder.add(new TextTokenData("progress"));
      builder.add(new TextTokenData("q"));
      builder.add(new TextTokenData("ruby"));
      builder.add(new TextTokenData("rp"));
      builder.add(new TextTokenData("rt"));
      builder.add(new TextTokenData("samp"));
      builder.add(new TextTokenData("section"));
      builder.add(new TextTokenData("select"));
      builder.add(new TextTokenData("small"));
      builder.add(new TextTokenData("source"));
      builder.add(new TextTokenData("span"));
      builder.add(new TextTokenData("strong"));
      builder.add(new TextTokenData("style"));
      builder.add(new TextTokenData("sub"));
      builder.add(new TextTokenData("sup"));
      builder.add(new TextTokenData("table"));
      builder.add(new TextTokenData("tbody"));
      builder.add(new TextTokenData("td"));
      builder.add(new TextTokenData("textarea"));
      builder.add(new TextTokenData("tfoot"));
      builder.add(new TextTokenData("th"));
      builder.add(new TextTokenData("thead"));
      builder.add(new TextTokenData("time"));
      builder.add(new TextTokenData("title"));
      builder.add(new TextTokenData("tr"));
      builder.add(new TextTokenData("var"));
      builder.add(new TextTokenData("video"));
      
      _htmlTagList = builder.build();
      
      _htmlTagMap = new HashMap<String, TextTokenData>();
      for (TextTokenData token : _htmlTagList) { 
        _htmlTagMap.put(token._token, token);
      }
    }
  }

  
  public static class HTMLEncodedDocumentReader extends HTMLTagSupport {
    
    public static int MIN_SUPPORTED_VERSION = 1;
    public static int MAX_SUPPORTED_VERSION = 1;
    
    private ContentHandler _contentHandler;
    
    private RiceCodeReader _textTokenLengthsCoder;
    private RiceCodeReader _textTokenStreamCoder;
    private RiceCodeReader _hrefTokenLengthsCoder;
    private RiceCodeReader _hrefTokenStreamCoder;
    private RiceCodeReader _attributesTokenStreamCoder;
    private RiceCodeReader _attributesTokenLengthsCoder;

    //private  BitStreamReader _elementTokenStream;
    private  ByteArrayInputStream _elementInputStream;
    private  DataInputStream      _elementDataStream;
    
    private  BitStreamReader _htmlTagMaskStream;
    private  BitStreamReader _htmlAttributeMaskStream;
    
    private Map<Integer,String> _htmlTokenToTagMap;
    private Map<Integer,String> _htmlAttributeTokenToTagMap;
    
    private  Map<Integer,String> _textTokenMap;
    private  Map<Integer,String> _hrefTokenMap;
    private  Map<Integer,String> _attributesTokenMap;
    
    /**
     * initialize a compressed document reader 
     * 
     * @param contentHandler - a SAX1 Content Handler Interface 
     */
    public HTMLEncodedDocumentReader(ContentHandler contentHandler) { 
      _contentHandler = contentHandler;
    }
    
    
    /** 
     * Read an encoded html document. Generate SAX events via content handler as appropriate 
     * @param inputStream
     * @throws IOException
     */
    public void readDocument(InputStream inputStream) throws IOException { 
      // clear state ... 
      resetState();
      
      long timeStart = System.currentTimeMillis();
      // 
      DataInputStream fileDataInputStream = new DataInputStream(inputStream);
      // read version 
      int version = inputStream.read();
      if (version < MIN_SUPPORTED_VERSION || version > MAX_SUPPORTED_VERSION) { 
        throw new IOException("Unsupported File Version!");
      }
      // read crc 
      long crcValue = WritableUtils.readVLong(fileDataInputStream);
      // and buffer length ... 
      int  bufferLength = (int) WritableUtils.readVLong(fileDataInputStream);
      System.out.println("CRCValue is:" + crcValue + " DataBufferLen:" + bufferLength);
      // allocate buffer 
      byte[] dataBuffer = new byte[bufferLength];
      // read it in
      CRC32 crcCalculator = new CRC32();
      CheckedInputStream crcInputStream = new CheckedInputStream(inputStream,crcCalculator);
      if (crcInputStream.read(dataBuffer) != dataBuffer.length) { 
        throw new IOException("Underflow Error!");
      }
      // validate crc
      if (crcCalculator.getValue() != crcValue) { 
        System.out.println("StreamCRC:" + crcValue + " CalculatedCRC:" + crcCalculator.getValue());
        throw new IOException("CRC Mismatch Detected!");
      }
      // ok we move further 
      ByteArrayInputStream byteStream = new ByteArrayInputStream(dataBuffer);
      DataInputStream      byteDataStream = new DataInputStream(byteStream);
            
      // read mask stream sizes
      int htmlTagsMaskBits = WritableUtils.readVInt(byteDataStream);
      int attributeTagMaskBits = WritableUtils.readVInt(byteDataStream);
      // allocate buffers 
      byte[] htmlTagMaskBuffer = new byte[(htmlTagsMaskBits + 7)/8];
      byte[] attributesMaskBuffer = new byte[(attributeTagMaskBits + 7)/8];
      // read buffers
      byteStream.read(htmlTagMaskBuffer);
      byteStream.read(attributesMaskBuffer);
      System.out.println("Initilize Tag Maps");
      // initialize mask streams 
      _htmlTagMaskStream = new BitUtils.BitStreamReader(new BitStream(htmlTagMaskBuffer,htmlTagsMaskBits));
      _htmlAttributeMaskStream = new BitUtils.BitStreamReader(new BitStream(attributesMaskBuffer,attributeTagMaskBits));
      System.out.println("Assigning Token Ids");
      // assign html tag ids 
      assignHTMLTokenIds();
      // ok read in text buffer lengths 
      int textDataBufferLen       = WritableUtils.readVInt(byteDataStream);
      int hrefDataBufferLen       = WritableUtils.readVInt(byteDataStream);
      int attributeDataBufferLen  = WritableUtils.readVInt(byteDataStream);
      int compressedBufferLen     = WritableUtils.readVInt(byteDataStream);
      // allocate buffer 
      byte [] compressedTextBuffer = new byte[compressedBufferLen];
      System.out.println("Reading Compressed Text Buffer");
      // read
      byteStream.read(compressedTextBuffer);
      // uncompress
      byte[] uncompressedBuffer = GZIPUtils.unzip(compressedTextBuffer);
      // ok, we have to read the token length streams to figure out how to assign token ids to them
      _textTokenLengthsCoder = initializeRiceCodeReader(byteDataStream);
      _hrefTokenLengthsCoder = initializeRiceCodeReader(byteDataStream);
      _attributesTokenLengthsCoder = initializeRiceCodeReader(byteDataStream);
      // build token maps 
      System.out.println("Reading Text Tokens");
      _textTokenMap = buildTokenIdToTextMap(uncompressedBuffer,0,textDataBufferLen,_textTokenLengthsCoder);
      System.out.println("Reading HREF Text Tokens");
      _hrefTokenMap = buildTokenIdToTextMap(uncompressedBuffer,textDataBufferLen,hrefDataBufferLen,_hrefTokenLengthsCoder);
      System.out.println("Reading Attribute Text Tokens");
      _attributesTokenMap = buildTokenIdToTextMap(uncompressedBuffer,textDataBufferLen+hrefDataBufferLen,attributeDataBufferLen,_attributesTokenLengthsCoder);
      // and initialize token stream readers
      System.out.println("Initializing Token Stream Readers");
      _textTokenStreamCoder = initializeRiceCodeReader(byteDataStream);
      _hrefTokenStreamCoder = initializeRiceCodeReader(byteDataStream);
      _attributesTokenStreamCoder = initializeRiceCodeReader(byteDataStream);
      // read element stream data 
      int elementStreamComprssedSize = WritableUtils.readVInt(byteDataStream);
      // int elementStreamNBits         = WritableUtils.readVInt(byteDataStream);
      byte [] compressedElementStream = new byte[elementStreamComprssedSize];
      
      System.out.println("Reading Compressed Element Stream - Size:" + elementStreamComprssedSize);
      byteStream.read(compressedElementStream);
      // decompress ... 
      byte[] decompElementStream = GZIPUtils.unzip(compressedElementStream);
      System.out.println("Initializing Element Stream Reader-Bytes In:" + decompElementStream.length);
      // allocate a reader 
      // _elementTokenStream = new BitUtils.BitStreamReader(new BitStream(decompElementStream,elementStreamNBits));
      _elementInputStream = new ByteArrayInputStream(decompElementStream);
      _elementDataStream = new DataInputStream(_elementInputStream);
      
      long timeEnd = System.currentTimeMillis();
      
      // ok read to roll ... 
      System.out.println("Ready to Decode Document - Setup Took:" + (timeEnd-timeStart));
      
      timeStart = System.currentTimeMillis();
      runDecodeLoop();
      timeEnd   = System.currentTimeMillis();
      
      System.out.println("Decode Loop Took:" + (timeEnd-timeStart));
    }
    
    public String getHTMLAttributeToken(String tokenName) {  
    	for (String token : _htmlAttributeTokenToTagMap.values()) { 
    		if (token.equals(tokenName)) { 
    			return token;
    		}
    	}
    	return null;
    }
    
    private void runDecodeLoop() throws IOException{ 
      List<String> elementStack = new ArrayList<String>();
      try { 
        _contentHandler.startDocument();
        int availableBytes = _elementInputStream.available();
        System.out.println("Available Bytes at ElementInputStream:" + availableBytes);
        while (_elementInputStream.available() != 0) {
          
            int elementToken = nextElementStreamInt();
            
            switch (elementToken) { 
          
              case HTMLDocumentEncoder.ELEMENT_START_WO_ATTRIBUTES:
              case HTMLDocumentEncoder.CUSTOM_ELEMENT_START_WO_ATTRIBUTES: 
              case HTMLDocumentEncoder.ELEMENT_START_W_ATTRIBUTES:
              case HTMLDocumentEncoder.CUSTOM_ELEMENT_START_W_ATTRIBUTES: {
                
                AttributesImpl attributes = new AttributesImpl();
                String qName = null;
                if (elementToken == HTMLDocumentEncoder.CUSTOM_ELEMENT_START_WO_ATTRIBUTES || elementToken == HTMLDocumentEncoder.CUSTOM_ELEMENT_START_W_ATTRIBUTES) { 
                  qName = _textTokenMap.get((int)_textTokenStreamCoder.nextValue());
                }
                else { 
                  qName = _htmlTokenToTagMap.get((int)nextElementStreamInt());
                }
                
                int attributeCount = 0;
                
                if (elementToken == HTMLDocumentEncoder.ELEMENT_START_W_ATTRIBUTES || elementToken == HTMLDocumentEncoder.CUSTOM_ELEMENT_START_W_ATTRIBUTES) { 
                  attributeCount = nextElementStreamInt();
                }
                
                for (int i=0;i<attributeCount;++i) {
                  
                  String attrQName = null;
                  String attrValue = "";
                  
                  int attributeToken = nextElementStreamInt();
                  
                  if (attributeToken == HTMLDocumentEncoder.CUSTOM_ATTRIBUTE_START_WO_VALUES || attributeToken == HTMLDocumentEncoder.CUSTOM_ATTRIBUTE_START_W_VALUES) { 
                    attrQName = _textTokenMap.get((int)_textTokenStreamCoder.nextValue());
                    // System.out.println("*** CUSTOM ATTRIBUTE:" + attrQName);
                  }
                  else {
                    int tokenId = nextElementStreamInt();
                    attrQName = _htmlAttributeTokenToTagMap.get((int)tokenId);
                    // System.out.println("*** TokenId:" + tokenId + " Resolved to:" + attrQName);
                  }
                  // ok now process values ... 
                  if (attributeToken == HTMLDocumentEncoder.ATTRIBUTE_START_W_VALUES || attributeToken == HTMLDocumentEncoder.CUSTOM_ATTRIBUTE_START_W_VALUES) { 
                    if (attrQName.equals("href") || attrQName.equals("src") || attrQName.equals("url")) { 
                      attrValue = _hrefTokenMap.get((int)_hrefTokenStreamCoder.nextValue());
                    }
                    else { 
                      attrValue = _attributesTokenMap.get((int)_attributesTokenStreamCoder.nextValue());
                    }
                  }
                  attributes.addAttribute("", "", attrQName,"" , attrValue);
                }
                // System.out.println("Calling contentHandler startElement with qName:" + qName);
                _contentHandler.startElement("", "", qName, attributes);
                elementStack.add(qName);
              }
              break;
                
              case HTMLDocumentEncoder.ELEMENT_END: { 
                _contentHandler.endElement(null, null, elementStack.remove(elementStack.size()-1));
              }
              break;
              
              case HTMLDocumentEncoder.TEXT_NODE: {
                String text = _textTokenMap.get((int)_textTokenStreamCoder.nextValue()); 
                _contentHandler.characters(text.toCharArray(), 0, text.length());
              }
              break;
              
              case HTMLDocumentEncoder.COMMENT_NODE: { 
                throw new IOException("Unexpected Comment Node!");
              }
              
              case HTMLDocumentEncoder.CDATA_NODE: { 
                throw new IOException("Unexpected CData Node!");
              }
          }
        }
        _contentHandler.endDocument();
      }
      catch (SAXException e) { 
        e.printStackTrace();
        throw new IOException(e);
      }
    }
    
    private Map<Integer,String> buildTokenIdToTextMap(byte[] dataBuffer,int offset,int length,RiceCodeReader tokenLengthsReader) {
      
      Map<Integer,String> mapOut = new HashMap<Integer,String>();
      
      CharBuffer buffer = Charset.forName("UTF8").decode(ByteBuffer.wrap(dataBuffer,offset,length));
      
      int currentLength = 0;
      int currentOffset = 0;
      int nextTokenId = 1;
      
      while (tokenLengthsReader.hasNext()) { 
        // read the next length (delta)
        currentLength += tokenLengthsReader.nextValue();
        // subtract 1 (rice coding limitation)
        currentLength -= 1;
        // create a subsequence ...
        mapOut.put(nextTokenId, buffer.subSequence(currentOffset,currentOffset+currentLength).toString());
        // increment offset 
        currentOffset += currentLength;
        // increment token id 
        nextTokenId++;
      }
      return mapOut;
    }
    
    private RiceCodeReader initializeRiceCodeReader(DataInputStream stream) throws IOException { 
      int mValue = stream.read();
      int numbits = WritableUtils.readVInt(stream);
      byte[] dataArray = new byte[(numbits + 7) / 8];
      stream.read(dataArray);
      return new RiceCodeReader(mValue,numbits,dataArray,0);      
    }
    
    private void resetState() { 
      _textTokenLengthsCoder = null;
      _textTokenStreamCoder = null;
      _hrefTokenLengthsCoder = null;
      _hrefTokenStreamCoder = null;
      _attributesTokenStreamCoder = null;
      _attributesTokenLengthsCoder = null;
      // _elementTokenStream = null;
      _elementInputStream = null;
      _elementDataStream = null;
      _htmlTagMaskStream = null;
      _htmlAttributeMaskStream = null;
      _textTokenMap = null;
      _hrefTokenMap = null;
      _attributesTokenMap = null;
      _htmlTokenToTagMap = new HashMap<Integer,String>();
      _htmlAttributeTokenToTagMap = new HashMap<Integer,String>();
      

      initializeHTMLTagMaps();
    }
    
    private void assignHTMLTokenIds() { 
      assignTokenIdsToSet(_htmlTagList,_htmlTagMaskStream,_htmlTokenToTagMap);
      assignTokenIdsToSet(_htmlAttributeList,_htmlAttributeMaskStream,_htmlAttributeTokenToTagMap);
    }
    
    private void assignTokenIdsToSet(ImmutableList<TextTokenData> tokenSet,BitStreamReader maskStream,Map<Integer,String> tokenToTagMap) { 
      int nextTokenId=0;
      // iterate token set in proper order 
      for (TextTokenData token : tokenSet) {
        // get next mask value in bit strem
        int bitValue = maskStream.getbit();
        // if 'on', then assign a token id to this tag 
        if (bitValue == 1) { 
          token._tokenId = nextTokenId++;
          tokenToTagMap.put(token._tokenId,token._token);
          //System.out.println("Assigned TokenId:" + token._tokenId + " to Token:" + token._token);
        }
      }
    }
    
    private int nextElementStreamInt()throws IOException {
      /*
      int nibbleNumber = 0;
      int bitNumber = 0;
      
      int result    = 0;
      boolean done = false;
      
      while (!done) { 
        int bitValue = _elementTokenStream.getbit();
        // if last bit in the nibble 
        if (bitNumber == 3) { 
          if (bitValue == 0) { 
            done = true;
          }
          else { 
            bitNumber = 0;
            nibbleNumber++;
          }
        }
        else {
          if (bitValue != 0) { 
            result |= 1 << (bitNumber + (nibbleNumber * 3));
          }
          bitNumber++;
        }
        
      }
      return result;
      */
      return WritableUtils.readVInt(_elementDataStream);
    }
    
  }
  
  public static class HTMLDocumentEncoder extends HTMLTagSupport { 
    
    public static final int ENCODER_VERSION = 1;
    
    public static final int ELEMENT_START_WO_ATTRIBUTES = 0;
    public static final int ELEMENT_START_W_ATTRIBUTES  = 1;
    public static final int ATTRIBUTE_START_WO_VALUES    = 2;
    public static final int ATTRIBUTE_START_W_VALUES     = 3;
    public static final int ELEMENT_END                  = 4;
    public static final int TEXT_NODE              = 5;
    public static final int COMMENT_NODE           = 6;
    public static final int CDATA_NODE             = 7;
    public static final int CUSTOM_ATTRIBUTE_START_WO_VALUES    = 8;
    public static final int CUSTOM_ATTRIBUTE_START_W_VALUES     = 9;
    public static final int CUSTOM_ELEMENT_START_WO_ATTRIBUTES = 10;
    public static final int CUSTOM_ELEMENT_START_W_ATTRIBUTES = 11;
    
    
    TreeMap<String,TextTokenData> _textTokenMap = new TreeMap<String,TextTokenData>();
    Vector<TextTokenData>         _sortedTextTokenArray = new Vector<TextTokenData>();
    TextTokenIdData               _textTokenIdData;
    
    TreeMap<String,TextTokenData> _hrefTokenMap = new TreeMap<String,TextTokenData>();
    Vector<TextTokenData>         _sortedHRefTokenArray = new Vector<TextTokenData>();
    TextTokenIdData               _hrefTokenIdData;

    TreeMap<String,TextTokenData> _attributeTokenMap = new TreeMap<String,TextTokenData>();
    Vector<TextTokenData>         _sortedAttributeTokenArray = new Vector<TextTokenData>();
    TextTokenIdData               _attributeTokenIdData;
    
    
    double                    _averageElementTagId;
    int                       _totalElementTags;
    int                       _elementStreamMValue;
    
    
    public RiceCoding _textTokenLengthsCoder;
    public RiceCoding _textTokenStreamCoder;
    public RiceCoding _hrefTokenLengthsCoder;
    public RiceCoding _hrefTokenStreamCoder;
    public RiceCoding _attributesTokenStreamCoder;
    public RiceCoding _attributesTokenLengthsCoder;
    
    
    public  ByteArrayOutputStream _elementTokenStream;
    public  DataOutputStream      _elementDataStream;
    
    public  BitStream _htmlTagMaskStream;
    public  BitStream _htmlAttributeMaskStream;
    
   
    public int    _encodedTextTokensCount = 0;
    public int    _encodedHREFTokensCount = 0;
    public int    _encodedAttributeTokensCount = 0;
    public int    _encodedElementTokensCount = 0;
    public int    _encodedIntegersCount = 0;

    
    public HTMLDocumentEncoder() { 

    }
    
    private void initializeEncoder() { 
      _textTokenMap = new TreeMap<String,TextTokenData>();
      _sortedTextTokenArray = new Vector<TextTokenData>();
      _textTokenIdData = null;
      
      _hrefTokenMap = new TreeMap<String,TextTokenData>();
      _sortedHRefTokenArray = new Vector<TextTokenData>();
      _hrefTokenIdData = null;

      _attributeTokenMap = new TreeMap<String,TextTokenData>();
      _sortedAttributeTokenArray = new Vector<TextTokenData>();
      _attributeTokenIdData = null;
      
      
      _htmlTagList = null;
      _htmlAttributeList = null;
      _htmlTagMap = null;
      _htmlAttributeTagMap = null;
      
      
      _textTokenLengthsCoder = null;
      _textTokenStreamCoder= null;
      _hrefTokenLengthsCoder= null;
      _hrefTokenStreamCoder= null;
      _attributesTokenStreamCoder= null;
      _attributesTokenLengthsCoder= null;
      
      
      // _elementTokenStream = new BitStream();
      _elementTokenStream = new ByteArrayOutputStream();
      _elementDataStream  = new DataOutputStream(_elementTokenStream);
      _htmlTagMaskStream  = new BitStream();
      _htmlAttributeMaskStream  = new BitStream();
      
      _encodedTextTokensCount = 0;
      _encodedHREFTokensCount = 0;
      _encodedAttributeTokensCount = 0;
      _encodedElementTokensCount = 0;
      _encodedIntegersCount = 0;    
      
      _averageElementTagId = 0.0;
      _totalElementTags = 0;
      _elementStreamMValue = 0;

    }
    
    public void encodeHTMLDocument(InstructionsPool instructions,OutputStream finalOutputStream)throws IOException { 
      preEncodeDocument();
      collectTokens(instructions);
      postEncodeDocument(instructions, finalOutputStream);
    }
    
    
    private void collectTokens(InstructionsPool instructionsPool)throws IOException { 
      List<Integer> operations = instructionsPool.operations;
      List<String> arguments = instructionsPool.arguments;

      for (int i=0; i<operations.size(); i++)
      {
          int domOperation = operations.get(i);
          String domArgument = arguments.get(i);
          //System.out.println("Operation :" + ParserInstruction.getOperationString(domOperation)+" Arg:~" + domArgument+"~");
          switch (domOperation)
          {
          // Open node :
          case ParserInstruction.AddLeaf:
          case ParserInstruction.OpenNode: { 
            String tagName = domArgument.toLowerCase();
            
            TextTokenData tokenData = _htmlTagMap.get(tagName);
            
            // if tag is well know tag, increase its instance count ... 
            if (tokenData != null) { 
              tokenData._tokenInstanceCount++;
            }
            // otherwsie add the token as a text token
            else { 
              addTextToken(tagName);
            }
          }break;
          // Close node :
          case ParserInstruction.CloseNode:
              break;

          case ParserInstruction.SetTitle:
          case ParserInstruction.AddText: {
            
            if (domOperation == ParserInstruction.SetTitle) { 
              _htmlTagMap.get("title")._tokenInstanceCount++;
            }
            // get the token's text data ... 
            String text = domArgument;

            text = text.replaceAll("\\s{2,}"," ");
            text = text.trim();
            
            if (text.length() != 0) { 
              addTextToken(text);
            }
          }
          break;
          
          case ParserInstruction.AddContent:
            break;
          case ParserInstruction.WriteAttributeKey: { 
            String attributeName = domArgument.toLowerCase();
            // if a pre-defined attribute name ... increment the associated token's instance count
            TextTokenData tokenData = _htmlAttributeTagMap.get(attributeName);
            if (tokenData != null) { 
              tokenData._tokenInstanceCount++;
            }
            // otherwsie add the token as a text token
            else { 
              addTextToken(attributeName);
            }
            // advance to value 
            i++;
            operations.get(i);
            domArgument = arguments.get(i);
            // always add value as a text token for now ... 
            if (domArgument.length() != 0) {
              if (attributeName.equalsIgnoreCase("href") || attributeName.equals("src") || attributeName.equals("url")) { 
                addHREFTextToken(domArgument);
              }
              else { 
                addAttributeTextToken(domArgument);            
              }
            }
          }
          break;
          case ParserInstruction.CloseLeaf:
              break;
          case ParserInstruction.AddEntity:
              break;
          case ParserInstruction.AddComment:
              break;
        }
      }      
    }
    
    private void preEncodeDocument() throws IOException { 
      // init encoder 
      initializeEncoder();
      // init html tags ... 
      initializeHTMLTagMaps();
    }
    
    private void postEncodeDocument(Object documentOrPool,OutputStream finalOutputStream) throws IOException {
      // assign ids ... 
      assignHTMLTokenIds();
      
      _textTokenIdData = assignTextTokenIds(_textTokenMap,_sortedTextTokenArray);
      _hrefTokenIdData = assignTextTokenIds(_hrefTokenMap,_sortedHRefTokenArray);
      _attributeTokenIdData = assignTextTokenIds(_attributeTokenMap,_sortedAttributeTokenArray);
      
      // create token stream encoder
      _textTokenStreamCoder = new RiceCoding(_textTokenIdData._textTokenMValue);
      // create text token length encoder
      _textTokenLengthsCoder = new RiceCoding(_textTokenIdData._textTokenLenMValue);
      
      // create token stream encoder
      _hrefTokenStreamCoder = new RiceCoding(_hrefTokenIdData._textTokenMValue);
      // create text token length encoder
      _hrefTokenLengthsCoder = new RiceCoding(_hrefTokenIdData._textTokenLenMValue);

      // create token stream encoder
      _attributesTokenStreamCoder = new RiceCoding(_attributeTokenIdData._textTokenMValue);
      // create text token length encoder
      _attributesTokenLengthsCoder = new RiceCoding(_attributeTokenIdData._textTokenLenMValue);
      
      // create text buffer 
      byte[] textData = encodeTextData(_sortedTextTokenArray,_textTokenLengthsCoder);
      // create href buffer 
      byte[] hrefData = encodeTextData(_sortedHRefTokenArray,_hrefTokenLengthsCoder);
      // create attributes buffer 
      byte[] attributesData = encodeTextData(_sortedAttributeTokenArray,_attributesTokenLengthsCoder);
      
      // create combined buffer 
      byte[] combinedBuffer = new byte[textData.length + hrefData.length + attributesData.length];
      System.arraycopy(textData, 0, combinedBuffer, 0, textData.length);
      System.arraycopy(hrefData, 0, combinedBuffer, textData.length, hrefData.length);
      System.arraycopy(attributesData, 0, combinedBuffer, hrefData.length + textData.length, attributesData.length);
      
      
      // compress 
      byte[] compressedTextData = GZIPUtils.zip(textData);
      byte[] compressedHRefData = GZIPUtils.zip(hrefData);
      byte[] compressedAttributesData = GZIPUtils.zip(attributesData);
      byte[] combinedCompressedData = GZIPUtils.zip(combinedBuffer);
      
      // put out stats 
      System.out.println("Text Buffer Original Size:" + textData.length + " Compressed Size:" + compressedTextData.length);
      System.out.println("HREF Buffer Original Size:" + hrefData.length + " Compressed Size:" + compressedHRefData.length);
      System.out.println("Attributes Buffer Original Size:" + attributesData.length + " Compressed Size:" + compressedAttributesData.length);
      System.out.println("Combined Buffer Original Size:" + combinedBuffer.length + " Compressed Size:" + combinedCompressedData.length);
      // stats 
      System.out.println("Text Length Buffer:" + (_textTokenLengthsCoder.getNumBits() + 7)/8);
      System.out.println("HREF Length Buffer:" + (_hrefTokenLengthsCoder.getNumBits() + 7)/8);
      System.out.println("Attributes Length Buffer:" + (_attributesTokenLengthsCoder.getNumBits() + 7)/8);

      // encode the dom 
      if (documentOrPool instanceof GnomeDocument) { 
        encodeDOM((GnomeDocument)documentOrPool);
      }
      else { 
        encodeDOM((InstructionsPool)documentOrPool);
      }
      // some stats ... 
      System.out.println("Text Token Stream Size:" + (_textTokenStreamCoder.getNumBits() + 7) / 8);
      System.out.println("HREF Token Stream Size:" + (_hrefTokenStreamCoder.getNumBits() + 7) / 8);
      System.out.println("Element Stream Size:" + _elementTokenStream.size());
      System.out.println("Element Token Count:" + _encodedElementTokensCount);
      System.out.println("Text Token Instance Count:" + _encodedTextTokensCount);
      System.out.println("HREF Token Instance Count:" + _encodedHREFTokensCount);
      System.out.println("Integer Token Instance Count:" + _encodedIntegersCount);
      
      
      byte elementStreamBuffer[] = _elementTokenStream.toByteArray();
      byte elementStreamBufferCompressed[] = GZIPUtils.zip(elementStreamBuffer);
      System.out.println("Compression of Element Stream Buffer Yielded Buffer of Size:" + elementStreamBufferCompressed.length);
      
      // allocate a 32 bit checksum object 
      CRC32 checksum = new CRC32();
      // allocate a byte stream 
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      // checksum the bytes 
      CheckedOutputStream   checkedOutputStream = new CheckedOutputStream(byteStream,checksum);
      // and allocate a data output stream
      DataOutputStream intermediateDataOutput = new DataOutputStream(checkedOutputStream);
      
      // write out html,href and attribute tag mask streams 
      WritableUtils.writeVInt(intermediateDataOutput,_htmlTagMaskStream.nbits);
      WritableUtils.writeVInt(intermediateDataOutput,_htmlAttributeMaskStream.nbits);
      checkedOutputStream.write(_htmlTagMaskStream.bits,0,(_htmlTagMaskStream.nbits + 7) /8);
      checkedOutputStream.write(_htmlAttributeMaskStream.bits,0,(_htmlAttributeMaskStream.nbits + 7) /8);

      
      // write out the indivdual text buffer lengths ... 
      WritableUtils.writeVInt(intermediateDataOutput,textData.length);
      WritableUtils.writeVInt(intermediateDataOutput,hrefData.length);
      WritableUtils.writeVInt(intermediateDataOutput,attributesData.length);
      // write out compressed buffer length 
      WritableUtils.writeVInt(intermediateDataOutput,combinedCompressedData.length);
      // write out the consolidated, compressed text buffer 
      checkedOutputStream.write(combinedCompressedData);
      // write out the lengths streams bitcount...
      intermediateDataOutput.writeByte(_textTokenLengthsCoder.getMValue());
      WritableUtils.writeVInt(intermediateDataOutput,_textTokenLengthsCoder.getNumBits());
      checkedOutputStream.write(_textTokenLengthsCoder.getBits(),0,(_textTokenLengthsCoder.getNumBits() + 7) / 8);
      
      intermediateDataOutput.writeByte(_hrefTokenLengthsCoder.getMValue());
      WritableUtils.writeVInt(intermediateDataOutput,_hrefTokenLengthsCoder.getNumBits());
      checkedOutputStream.write(_hrefTokenLengthsCoder.getBits(),0,(_hrefTokenLengthsCoder.getNumBits() + 7) / 8);
      
      intermediateDataOutput.writeByte(_attributesTokenLengthsCoder.getMValue());
      WritableUtils.writeVInt(intermediateDataOutput,_attributesTokenLengthsCoder.getNumBits());
      checkedOutputStream.write(_attributesTokenLengthsCoder.getBits(),0,(_attributesTokenLengthsCoder.getNumBits() + 7) / 8);
      
      // write out token streams lengths
      intermediateDataOutput.writeByte(_textTokenStreamCoder.getMValue());
      WritableUtils.writeVInt(intermediateDataOutput,_textTokenStreamCoder.getNumBits());
      checkedOutputStream.write(_textTokenStreamCoder.getBits(), 0,(_textTokenStreamCoder.getNumBits() + 7) / 8);

      intermediateDataOutput.writeByte(_hrefTokenStreamCoder.getMValue());
      WritableUtils.writeVInt(intermediateDataOutput,_hrefTokenStreamCoder.getNumBits());
      checkedOutputStream.write(_hrefTokenStreamCoder.getBits(),0,(_hrefTokenStreamCoder.getNumBits() + 7) / 8);
      
      
      intermediateDataOutput.writeByte(_attributesTokenStreamCoder.getMValue());
      WritableUtils.writeVInt(intermediateDataOutput,_attributesTokenStreamCoder.getNumBits());
      checkedOutputStream.write(_attributesTokenStreamCoder.getBits(),0,(_attributesTokenStreamCoder.getNumBits() + 7) / 8);
      
      // write out element stream ... compressed length, number of bits, and bits  
      WritableUtils.writeVInt(intermediateDataOutput,elementStreamBufferCompressed.length);
      //WritableUtils.writeVInt(intermediateDataOutput,_elementTokenStream.nbits);
      checkedOutputStream.write(elementStreamBufferCompressed,0,elementStreamBufferCompressed.length);
      
      System.out.println("Total Compressed Buffer Size:" + byteStream.size());
      
      // now write out final stream .. 
      DataOutputStream finalStreamDataOut = new DataOutputStream(finalOutputStream);
      // write out version 
      finalStreamDataOut.writeByte(ENCODER_VERSION);
      // write out crc 
      WritableUtils.writeVLong(finalStreamDataOut,checksum.getValue());
      
      // get byte buffer  
      byte[] intermediateBuffer = byteStream.toByteArray();
      
      // write size 
      WritableUtils.writeVLong(finalStreamDataOut,intermediateBuffer.length);
      // then bits
      finalOutputStream.write(intermediateBuffer);      
    }
    
    public void encodeHTMLDocument(GnomeDocument document,OutputStream finalOutputStream) throws IOException { 
      // 
      preEncodeDocument();
      // collect token from docucment ... 
      collectTokens(document);
      //
      postEncodeDocument(document,finalOutputStream);
    }
    
    private byte[] encodeTextData(Vector<TextTokenData> tokenArray,RiceCoding lengthsCoder) { 
      StringBuffer buffer = new StringBuffer();
      int lastTokenLength = 0;
      for (TextTokenData token : tokenArray) { 
        buffer.append(token._token);
        if (lastTokenLength == 0) { 
          lengthsCoder.addItem(token._token.length() + 1);
        }
        else { 
          lengthsCoder.addItem((token._token.length() - lastTokenLength) + 1);
        }
        lastTokenLength = token._token.length();
      }
      return buffer.toString().getBytes(Charset.forName("UTF8"));
    }

    private static double lg(double value) { 
      return Math.log(value)/Math.log(2.0);
    }
    
    private static int bitsToEncode(int value) { 
      int bitsToEncode = 0;
      
      while (value != 0) { 
        bitsToEncode++;
        value >>= 1;
      }
      return bitsToEncode;
    }
    
    private void encodeInteger(int value, BitStream bitStream,int bitsPerWord) {
      
      int bitsToEncode = bitsToEncode(value);
      int bitsCounter = 0;
      while (bitsToEncode != 0) { 
        bitStream.addbit((value & 0x1) != 0 ? 1 : 0);
        value >>= 1;
        bitsCounter++;
        bitsToEncode--;
        if (bitsCounter == bitsPerWord) {
          bitStream.addbit((bitsToEncode != 0) ? 1: 0);
          bitsCounter = 0;
        }
      }
      if (bitsCounter > 0) { 
        while (bitsCounter++ <= bitsPerWord) { 
          bitStream.addbit(0);
        }
      }
    }    
    
    
    
    private void encodeIntegerIntoTokenStream(int integer)throws IOException {
      _encodedIntegersCount++;
      // encodeInteger(integer,_elementTokenStream,3);
      // System.out.println("encoding Integer into token stream:" + integer);
      WritableUtils.writeVInt(_elementDataStream, integer);
    }
    
    private void encodeElementToken(int tokenId) throws IOException {
      _encodedElementTokensCount++;
      encodeIntegerIntoTokenStream(tokenId);
    }
    
    
    private void encodeTextTokenId(int tokenId) {
      _encodedTextTokensCount++;
      _textTokenStreamCoder.addItem(tokenId);
    }

    private void encodeHRefTokenId(int tokenId) {
      _encodedHREFTokensCount++;
      _hrefTokenStreamCoder.addItem(tokenId);
    }

    private void encodeAttributeTokenId(int tokenId) {
      _encodedAttributeTokensCount++;
      _attributesTokenStreamCoder.addItem(tokenId);
    }

    private void assignHTMLTokenIds() { 
      assignTokenIdsToSet(_htmlTagList,_htmlTagMaskStream);
      assignTokenIdsToSet(_htmlAttributeList,_htmlAttributeMaskStream);
      // recompute average 
      _averageElementTagId /= (double)_totalElementTags;
      _elementStreamMValue = (int) Math.max(0,Math.floor(lg(_averageElementTagId)));
      System.out.println("Average Element Tag Id:" + _averageElementTagId + " MValue:" + _elementStreamMValue);
    }
    
    private void assignTokenIdsToSet(ImmutableList<TextTokenData> tokenSet,BitStream maskStream) { 
      int nextTokenId=0;
      for (TextTokenData token : tokenSet) { 
        if (token._tokenInstanceCount != 0) { 
          token._tokenId = nextTokenId++;
          _averageElementTagId += token._tokenId * token._tokenInstanceCount;
          _totalElementTags += token._tokenInstanceCount;
          maskStream.addbit(1);
          // System.out.println("Token:" + token._token + " Id:" + token._tokenId);
        }
        else { 
          maskStream.addbit(0);
        }
      }
      
      // sort set by instance count ... 
      Vector<TextTokenData> instanceCountSet = new Vector<TextTokenData>();
      instanceCountSet.addAll(tokenSet);
      Collections.sort(instanceCountSet, new Comparator<TextTokenData>() {

        @Override
        public int compare(TextTokenData o1, TextTokenData o2) {
          return o1._tokenInstanceCount - o2._tokenInstanceCount;
        } 
        
      });
      
      /*
      System.out.println("** tokens by freq **");
      for (TextTokenData token : Iterables.reverse(instanceCountSet)) { 
        
        if (token._tokenInstanceCount != 0)
          System.out.println("Token:" + token._token + " Hits:" + token._tokenInstanceCount);
      }
      */
    }
    
    private static class TextTokenIdData { 
      public double _averageTokenId;
      public double _averageTokenLengthDelta;
      public int _textTokenMValue;
      public int _textTokenLenMValue;
    }
    
    private TextTokenIdData assignTextTokenIds(TreeMap<String,TextTokenData> tokenMap,Vector<TextTokenData> sortedTokenArray) { 
      
      double tokenIdValueTotal = 0.0;
      double tokenLengthDeltaTotal  = 0.0;
      int    totalTokenInstanceCount = 0;
      
      // add tokens to list
      sortedTokenArray.addAll(tokenMap.values());
      // sort them by token lengths... 
      Collections.sort(sortedTokenArray,new Comparator<TextTokenData>() {

        @Override
        public int compare(TextTokenData o1, TextTokenData o2) {
          return o1._token.length() - o2._token.length();
        } 
        
      });
      // assign token ids 
      int nextTokenId = 1; // start with a token id of one ...
      int lastTokenLength = 0;
      int deltaCount = 0;
      for (TextTokenData token : sortedTokenArray) { 
        token._tokenId = nextTokenId++;
        tokenIdValueTotal += token._tokenId * token._tokenInstanceCount;
        if (lastTokenLength != 0) {
          int delta  = (token._token.length() - lastTokenLength);
          tokenLengthDeltaTotal += delta;
          deltaCount++;
        }
        lastTokenLength = token._token.length();
        
        totalTokenInstanceCount += token._tokenInstanceCount;
        //System.out.println("Assigned TokenId:" + token._tokenId + " to Token:" + token._token);
      }
      
      TextTokenIdData tokenIdData = new TextTokenIdData();
      
      tokenIdData._averageTokenId = tokenIdValueTotal / (double)totalTokenInstanceCount;
      tokenIdData._averageTokenLengthDelta = tokenLengthDeltaTotal / (double)deltaCount;
      tokenIdData._textTokenMValue = (int) Math.max(0,Math.floor(lg(tokenIdData._averageTokenId)));
      tokenIdData._textTokenLenMValue = (int) Math.max(0,Math.floor(lg(tokenIdData._averageTokenLengthDelta)));
      
      System.out.println("Average Token Id is:" + tokenIdData._averageTokenId + "m for RiceCoding Purposes is:" + tokenIdData._textTokenMValue);
      System.out.println("Average Token Length Delta is:" + tokenIdData._averageTokenLengthDelta+ "m for RiceCoding Purposes is:" + tokenIdData._textTokenLenMValue);
      
      return tokenIdData;
    }
    
    private void collectTokens(GnomeDocument document) { 
      collectTokensFromNode(document);
    }
    
    private void encodeDOM(GnomeDocument document)throws IOException { 
      encodeNode(document);
    }
    
    static class NodeInConstruction { 
      String nodeName;
      ArrayList<Pair<String,String>> attributes = null;
      
      NodeInConstruction(String nodeName) { 
        this.nodeName = nodeName;
      }
      
      void addAttribute(String attributeName,String attributeValue) { 
        if (attributes == null)
          attributes = new ArrayList<Tuples.Pair<String,String>>();
        attributes.add(new Pair<String, String>(attributeName,attributeValue));
      }
    }
    
    private void encodeAttributeInNodeInConstruction(Pair<String,String> attribute) throws IOException { 
      // get the attribute name ... 
      String attributeName = attribute.e0;
      // check to see if this is a known attribute 
      TextTokenData tokenData = _htmlAttributeTagMap.get(attributeName);
      // if custom attribute name
      if (tokenData == null) { 
        encodeElementToken((attribute.e1 == null || attribute.e1.length() ==0) ? CUSTOM_ATTRIBUTE_START_WO_VALUES : CUSTOM_ATTRIBUTE_START_W_VALUES);
        encodeTextTokenId(_textTokenMap.get(attributeName)._tokenId);
      }
      // normal case .. known attribute name ... 
      else { 
        if (tokenData._tokenId == -1) { 
          throw new RuntimeException("UnNumbered Token:" + tokenData._token + " Encountered!");
        }
        encodeElementToken((attribute.e1 == null || attribute.e1.length() ==0) ? ATTRIBUTE_START_WO_VALUES : ATTRIBUTE_START_W_VALUES);
        encodeIntegerIntoTokenStream(tokenData._tokenId);
      }
      
      if (attribute.e1 != null && attribute.e1.length() !=0) {
        if (attributeName.equals("href") || attributeName.equals("src") || attributeName.equals("url")) { 
          encodeHRefTokenId(_hrefTokenMap.get(attribute.e1)._tokenId);
        }
        else { 
          encodeAttributeTokenId(_attributeTokenMap.get(attribute.e1)._tokenId);
        }
      }
    }
    
    private void processNodeInConstruction(NodeInConstruction nodeInConstruction) throws IOException { 
      // get the element name 
      String elementName = nodeInConstruction.nodeName;
      // lookit up in the html tag map 
      TextTokenData tokenData = _htmlTagMap.get(elementName);
      // if not present, this is a custom element
      if (tokenData == null) { 
        encodeElementToken((nodeInConstruction.attributes != null && nodeInConstruction.attributes.size()  != 0) ? CUSTOM_ELEMENT_START_W_ATTRIBUTES : CUSTOM_ELEMENT_START_WO_ATTRIBUTES);
        encodeTextTokenId(_textTokenMap.get(elementName)._tokenId);
      }
      // normal case ... known element 
      else { 
        if (tokenData._tokenId == -1) { 
          throw new RuntimeException("UnNumbered Token:" + tokenData._token + " Encountered!");
        }
        encodeElementToken((nodeInConstruction.attributes != null && nodeInConstruction.attributes.size()  != 0) ? ELEMENT_START_W_ATTRIBUTES : ELEMENT_START_WO_ATTRIBUTES);
        encodeIntegerIntoTokenStream(tokenData._tokenId);
      }
      
      // encode attributes ...
      if (nodeInConstruction.attributes != null &&  nodeInConstruction.attributes.size()  != 0) { 
        encodeIntegerIntoTokenStream(nodeInConstruction.attributes.size());
        for (int i = 0; i < nodeInConstruction.attributes.size(); i++) { 
          encodeAttributeInNodeInConstruction(nodeInConstruction.attributes.get(i));
        }
      }
      
    }
    
    private void encodeDOM(InstructionsPool instructionsPool) throws IOException { 
      
      List<Integer> operations = instructionsPool.operations;
      List<String> arguments = instructionsPool.arguments;
      NodeInConstruction nodeInConstruction = null;

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
            if (nodeInConstruction != null) { 
              processNodeInConstruction(nodeInConstruction);
              nodeInConstruction = null;
            }
            nodeInConstruction = new NodeInConstruction(domArgument.toLowerCase());
          }break;
          // Close node :
          case ParserInstruction.CloseNode:
          case ParserInstruction.CloseLeaf: { 
            if (nodeInConstruction != null) { 
              processNodeInConstruction(nodeInConstruction);
              nodeInConstruction = null;
            }
            encodeElementToken(ELEMENT_END);
          }
          break;
          case ParserInstruction.AddText: { 
            if (nodeInConstruction != null) { 
              processNodeInConstruction(nodeInConstruction);
              nodeInConstruction = null;
            }
            String text = domArgument;

            text = text.replaceAll("\\s{2,}"," ");
            text = text.trim();
            
            if (text.length() != 0) { 
              encodeElementToken(TEXT_NODE);
              encodeTextTokenId(_textTokenMap.get(text)._tokenId);
            }
            System.out.println("AddText:"+domArgument);
          }
          break;
          
          case ParserInstruction.AddContent:
            if (nodeInConstruction != null) { 
              processNodeInConstruction(nodeInConstruction);
              nodeInConstruction = null;
            }
            /*

            String text = domArgument;

            text = text.replaceAll("\\s{2,}"," ");
            text = text.trim();
            
            if (text.length() != 0) { 
              encodeElementToken(CDATA_NODE);
              encodeTextTokenId(_textTokenMap.get(text)._tokenId);
            }
            */
            break;
          case ParserInstruction.WriteAttributeKey: { 
            // get the attribute name ... 
            String attributeName = domArgument.toLowerCase();
            // get value 
            ++i;
            //
            operations.get(i);
            String attributeValue = arguments.get(i);
            
            if (nodeInConstruction != null) { 
              nodeInConstruction.addAttribute(attributeName, attributeValue);
            }
          }
          break;
          case ParserInstruction.AddEntity:
            if (nodeInConstruction != null) { 
              processNodeInConstruction(nodeInConstruction);
              nodeInConstruction = null;
            }
            System.out.println("AddEntity:" + domArgument);
            break;
          case ParserInstruction.AddComment:
            if (nodeInConstruction != null) { 
              processNodeInConstruction(nodeInConstruction);
              nodeInConstruction = null;
            }
            System.out.println("AddComment:" + domArgument); 
            break;
          case ParserInstruction.SetTitle: 
            if (nodeInConstruction != null) { 
              processNodeInConstruction(nodeInConstruction);
              nodeInConstruction = null;
            }
            NodeInConstruction node = new NodeInConstruction("title");
            processNodeInConstruction(node);
            String title = domArgument;
            title = title.replaceAll("\\s{2,}"," ");
            title = title.trim();
            
            if (title.length() != 0) { 
              encodeElementToken(TEXT_NODE);
              encodeTextTokenId(_textTokenMap.get(title)._tokenId);
            }
            encodeElementToken(ELEMENT_END);

            System.out.println("SetTitle:" + domArgument);
            break;
          }
      }
    }
    
    private void encodeNode(Node node) throws IOException { 
      
      switch (node.getNodeType()) {
      
        case Node.ATTRIBUTE_NODE: {
          // get the attribute name ... 
          String attributeName = ((Attr)node).getName().toLowerCase();
          // check to see if this is a known attribute 
          TextTokenData tokenData = _htmlAttributeTagMap.get(attributeName);
          // if custom attribute name
          if (tokenData == null) { 
            encodeElementToken((node.getNodeValue().length() ==0) ? CUSTOM_ATTRIBUTE_START_WO_VALUES : CUSTOM_ATTRIBUTE_START_W_VALUES);
            encodeTextTokenId(_textTokenMap.get(((Attr)node).getName())._tokenId);
          }
          // normal case .. known attribute name ... 
          else { 
            if (tokenData._tokenId == -1) { 
              throw new RuntimeException("UnNumbered Token:" + tokenData._token + " Encountered!");
            }
            encodeElementToken((node.getNodeValue().length() ==0) ? ATTRIBUTE_START_WO_VALUES : ATTRIBUTE_START_W_VALUES);
            encodeIntegerIntoTokenStream(tokenData._tokenId);
          }
          
          if (node.getNodeValue().length() != 0) {
            if (attributeName.equals("href") || attributeName.equals("src") || attributeName.equals("url")) { 
              encodeHRefTokenId(_hrefTokenMap.get(node.getNodeValue())._tokenId);
            }
            else { 
              encodeAttributeTokenId(_attributeTokenMap.get(node.getNodeValue())._tokenId);
            }
          }
        /*
        if (node.getNodeName().equals("href") || node.getNodeName().equals("src")) { 
          if (node.getNodeValue().length() != 0) { 
            tokenSet.add(node.getNodeValue());
          }
        }    
        else { 
          if (node.getNodeValue() != null && node.getNodeValue().length() != 0) {
            String tokens[] = node.getNodeValue().split("[ \n\t\r]"); 
            for (String token : tokens) {
              if (token.length() != 0) { 
                tokenSet.add(token);
              }
            }
          }
        }
        */
      }
      break;
      
      case Node.DOCUMENT_NODE:
      case Node.DOCUMENT_FRAGMENT_NODE:
      case Node.ELEMENT_NODE: { 
        
        if (node.getNodeType() == Node.ELEMENT_NODE) {
          
          NamedNodeMap nm = node.getAttributes();
          
          // get the element name 
          String elementName = ((Element)node).getTagName().toLowerCase();
          // lookit up in the html tag map 
          TextTokenData tokenData = _htmlTagMap.get(elementName);
          // if not present, this is a custom element
          if (tokenData == null) { 
            encodeElementToken((nm.getLength() != 0) ? CUSTOM_ELEMENT_START_W_ATTRIBUTES : CUSTOM_ELEMENT_START_WO_ATTRIBUTES);
            encodeTextTokenId(_textTokenMap.get((((Element)node).getTagName()))._tokenId);
          }
          // normal case ... known element 
          else { 
            if (tokenData._tokenId == -1) { 
              throw new RuntimeException("UnNumbered Token:" + tokenData._token + " Encountered!");
            }
            encodeElementToken((nm.getLength() != 0) ? ELEMENT_START_W_ATTRIBUTES : ELEMENT_START_WO_ATTRIBUTES);
            encodeIntegerIntoTokenStream(tokenData._tokenId);
          }
          
          if (nm.getLength() != 0) { 
            encodeIntegerIntoTokenStream(nm.getLength());
            for (int i = 0; i < nm.getLength(); i++) { 
              encodeNode(nm.item(i));
            }
          }
        }
        
        // walk children ... 
        NodeList list = node.getChildNodes();
        
        for (int i = 0; i < list.getLength(); i++) { 
          encodeNode(list.item(i));
        }
        
        if (node.getNodeType() == Node.ELEMENT_NODE) {
          // close element 
          encodeElementToken(ELEMENT_END);
        }
        
      }
      break;
        
      /*
      case Node.CDATA_SECTION_NODE: { 
        String text = ((CDATASection)node).getData();

        text = text.replaceAll("\\s{2,}"," ");
        text = text.trim();
        
        if (text.length() != 0) { 
          encodeElementToken(CDATA_NODE);
          encodeTextTokenId(_textTokenMap.get(text)._tokenId);
        }
      }
      break;
      
      case Node.COMMENT_NODE: { 
        String text = ((Comment)node).getData();

        text = text.replaceAll("\\s{2,}"," ");
        text = text.trim();
        
        if (text.length() != 0) { 
          encodeElementToken(COMMENT_NODE);
          encodeTextTokenId(_textTokenMap.get(text)._tokenId);
        }
      }
      break;
      */
      
      case Node.TEXT_NODE: {
        String text = ((Text)node).getData();

        text = text.replaceAll("\\s{2,}"," ");
        text = text.trim();
        
        if (text.length() != 0) { 
          encodeElementToken(TEXT_NODE);
          encodeTextTokenId(_textTokenMap.get(text)._tokenId);
        }
        
        /*
        
        String parts[] = text.split("[ \n\t\r\\.-/\\(\\);\"\']");
        int nonEmptyPartCount = 0;
        for (String part : parts) {
          if (part.length() != 0) 
            nonEmptyPartCount++;
        }
        encodeElementToken(TEXT_NODE);
        encodeIntegerIntoTokenStream(nonEmptyPartCount);
        
        for (String part : parts) {
          if (part.length() != 0) { 
            encodeTextTokenId(_tokenMap.get(part)._tokenId);
          }
        }
        */
        
      }
      break;
      
      case Node.ENTITY_REFERENCE_NODE: { 
        System.out.println("*********Hit Entity Node!!!");
      }
      break;
      
      default:
        // System.out.println(indent + "Unknown node");
        break;
      }
    }          
    
    private void addTextToken(String token) { 
      TextTokenData tokenData = _textTokenMap.get(token);
      if (tokenData == null) { 
        tokenData = new TextTokenData(token);
        _textTokenMap.put(token,tokenData);
      }
      tokenData._tokenInstanceCount++;
    }

    private void addHREFTextToken(String token) { 
      TextTokenData tokenData = _hrefTokenMap.get(token);
      if (tokenData == null) { 
        tokenData = new TextTokenData(token);
        _hrefTokenMap.put(token,tokenData);
      }
      tokenData._tokenInstanceCount++;
    }

    private void addAttributeTextToken(String token) { 
      TextTokenData tokenData = _attributeTokenMap.get(token);
      if (tokenData == null) { 
        tokenData = new TextTokenData(token);
        _attributeTokenMap.put(token,tokenData);
      }
      tokenData._tokenInstanceCount++;
    }
    
    
    public int collectTokensFromNode(Node node) {
      int nodeCount = 1;
      
      switch (node.getNodeType()) {
      case Node.ATTRIBUTE_NODE: {
        
        String attributeName = ((Attr)node).getName().toLowerCase();
        
        // if a pre-defined attribute name ... increment the associated token's instance count
        TextTokenData tokenData = _htmlAttributeTagMap.get(attributeName);
        if (tokenData != null) { 
          tokenData._tokenInstanceCount++;
        }
        // otherwsie add the token as a text token
        else { 
          addTextToken(((Attr)node).getName());
        }
        
        // always add value as a text token for now ... 
        if (node.getNodeValue().length() != 0) {
          if (attributeName.equals("href") || attributeName.equals("src") || attributeName.equals("url")) { 
            addHREFTextToken(node.getNodeValue());
          }
          else { 
            addAttributeTextToken(node.getNodeValue());            
          }
        }
        
        /*
        if (node.getNodeName().equals("href") || node.getNodeName().equals("src")) { 
          if (node.getNodeValue().length() != 0) { 
            tokenSet.add(node.getNodeValue());
          }
        }    
        else { 
          if (node.getNodeValue() != null && node.getNodeValue().length() != 0) {
            String tokens[] = node.getNodeValue().split("[ \n\t\r]"); 
            for (String token : tokens) {
              if (token.length() != 0) { 
                tokenSet.add(token);
              }
            }
          }
        }
        */
      }
      break;
      case Node.DOCUMENT_NODE:
      case Node.DOCUMENT_FRAGMENT_NODE:
      case Node.ELEMENT_NODE:
        
        if (node.getNodeType() == Node.ELEMENT_NODE) { 
          
          String tagName = ((Element)node).getTagName().toLowerCase();
                    
          TextTokenData tokenData = _htmlTagMap.get(tagName);
          
          // if tag is well know tag, increase its instance count ... 
          if (tokenData != null) { 
            tokenData._tokenInstanceCount++;
          }
          // otherwsie add the token as a text token
          else { 
            addTextToken(((Element)node).getTagName());
          }
        }
        NamedNodeMap nm = node.getAttributes();
        for (int i = 0; i < nm.getLength(); i++)
          nodeCount += collectTokensFromNode(nm.item(i));
        
        NodeList list = node.getChildNodes();
        for (int i = 0; i < list.getLength(); i++)
          nodeCount += collectTokensFromNode(list.item(i));
        break;

      
      case Node.CDATA_SECTION_NODE: { 
        CDATASection section = (CDATASection)node;
        // get the token's text data ... 
        String text = section.getData();

        text = text.replaceAll("\\s{2,}"," ");
        text = text.trim();
        
        if (text.length() != 0) { 
          //System.out.println("***Skipping CDATA:" + text);
          //addTextToken(text);
        }
        //nodeCount += 1;
      }
      break;
        
      case Node.COMMENT_NODE: {
        // get the token's text data ... 
        String text = ((Comment)node).getData();

        text = text.replaceAll("\\s{2,}"," ");
        text = text.trim();
        
        if (text.length() != 0) { 
          //System.out.println("***Skipping COMMENT:" + text);
          //addTextToken(text);
        }
        //nodeCount += 1;
      }
      break;
      

      case Node.TEXT_NODE: {
        // get the token's text data ... 
        String text = ((Text)node).getData();

        text = text.replaceAll("\\s{2,}"," ");
        text = text.trim();
        
        if (text.length() != 0) { 
          addTextToken(text);
        }
        
        /*
        
        if (text.length() != 0) { 
          String parts[] = text.split("[ \n\t\r\\.-/\\(\\);\"\']");
          for (String part : parts) {
            if (part.length() != 0) { 
              addTextToken(part);
            }
          }
        }
        */
        
        nodeCount += 1;
      }
      break;
      default:
        // System.out.println(indent + "Unknown node");
        break;
      }
      
      return nodeCount;
    }    
  }
}
