package com.dappit.Dapper.parser;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DebugDocumentBuilder implements DocumentBuilder {

  @Override
  public Document buildDocument(InstructionsPool instructionsPool,FileOutputStream optionalOutputStream)
      throws IOException
  {

      System.out.println("iterating parse instructions");
      //Element currentElement = null;
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
          case ParserInstruction.OpenNode:
            System.out.println("OpenNode:" + domArgument);
            break;
          // Close node :
          case ParserInstruction.CloseNode:
            System.out.println("CloseNode");
              break;
          case ParserInstruction.AddText:
            System.out.println("AddText:"+domArgument);
            break;
          case ParserInstruction.AddContent:
            System.out.println("AddContent:"+domArgument);
            break;
          case ParserInstruction.AddLeaf:
            System.out.println("AddLeaf:"+domArgument);
            break;
          case ParserInstruction.WriteAttributeKey:
            System.out.println("WriteAttributeKey:"+domArgument);
            break;
          case ParserInstruction.WriteAttributeValue:
            System.out.println("WriteAttributeValue:"+domArgument);
            break;
          case ParserInstruction.CloseLeaf:
            System.out.println("CloseLeaf");
              break;
          case ParserInstruction.AddEntity:
            System.out.println("AddEntity:" + domArgument);
              break;
          case ParserInstruction.AddComment:
            System.out.println("AddComment:" + domArgument); 
              break;
          case ParserInstruction.SetTitle:
            System.out.println("SetTitle:" + domArgument);
              break;
          }
      }
      return null;
  }
}
