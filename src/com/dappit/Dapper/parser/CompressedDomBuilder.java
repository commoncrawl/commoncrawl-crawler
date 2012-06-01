package com.dappit.Dapper.parser;

import java.io.FileOutputStream;
import java.io.IOException;

import org.commoncrawl.util.HTMLDomUtils.HTMLDocumentEncoder;
import org.w3c.dom.Document;

public class CompressedDomBuilder implements DocumentBuilder {
  
  HTMLDocumentEncoder encoder = new HTMLDocumentEncoder();
  
  public CompressedDomBuilder() { 
  }
  
  @Override
  public Document buildDocument(InstructionsPool instructionsPool,FileOutputStream outputStream)throws IOException {
    encoder.encodeHTMLDocument(instructionsPool, outputStream);
    return null;
  }

}
