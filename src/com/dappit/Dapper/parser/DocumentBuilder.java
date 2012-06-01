package com.dappit.Dapper.parser;

import java.io.FileOutputStream;
import java.io.IOException;

import org.w3c.dom.Document;

public interface DocumentBuilder {
  public Document buildDocument(InstructionsPool instructionsPool,FileOutputStream optionalOutputStream) throws IOException;
}
