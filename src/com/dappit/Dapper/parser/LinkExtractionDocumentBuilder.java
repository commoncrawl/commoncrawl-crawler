package com.dappit.Dapper.parser;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.commoncrawl.util.Tuples;
import org.commoncrawl.util.Tuples.Pair;
import org.w3c.dom.Document;

import com.google.common.collect.Sets;

public class LinkExtractionDocumentBuilder implements DocumentBuilder {

  static abstract class Node {
    public NodeWithChildren parentNode;
    public int nodePosition = -1;
    
    public Node(NodeWithChildren parentNode) { 
      this.parentNode = parentNode;
    }
    public abstract boolean isTextNode();
    public boolean isLeafNode() { return true; }
    public abstract String nodeName();
    
    
  }
  
  static class TextNode extends Node {
    public TextNode(NodeWithChildren parentNode,String text) {
      super(parentNode);
      this.text = text; 
    }
    public String text;
    
    @Override
    public boolean isTextNode() {
      return true;
    }
    public String nodeName() { return "text"; }
    
    @Override
    public String toString() {
      return "textNode:" + text;
    };

  }
  
  static class NodeWithAttributes extends Node { 

    public NodeWithAttributes(NodeWithChildren parentNode,String nodeName) { 
      super(parentNode);
      this.nodeName = nodeName;
    }
    
    public String nodeName() { return nodeName; }
    
    public String nodeName;    
    public LinkedList<Pair<String,String>> attributes = null;

    public void addAttribute(String attributeName,String attributeValue) { 
      if (attributes == null)
        attributes = new LinkedList<Tuples.Pair<String,String>>();
      attributes.add(new Pair(attributeName,attributeValue));
    }
    
    public String getAttribute(String name) {
      if (attributes != null) { 
        for (Pair<String,String> attribute : attributes) { 
          if (attribute.e0.equals(name)) { 
            return attribute.e1;
          }
        }
      }
      return "";
    }

    @Override
    public boolean isTextNode() {
      return false;
    }
    
    @Override
    public String toString() {
      return "LeafNode:" + nodeName;
    }
  }
  
  static class NodeWithChildren extends NodeWithAttributes {

    public NodeWithChildren(NodeWithChildren parentNode,String nodeName) {
      super(parentNode,nodeName);
    }
    
    public ArrayList<Node> childNodes = null;
    public int anchorCount = 0;
    public int imgCount =0;
    public int brCount =0;
    
    public void addNode(Node node) { 
      if (childNodes == null)
        childNodes = new ArrayList<LinkExtractionDocumentBuilder.Node>();
      childNodes.add(node);
      node.nodePosition = childNodes.size() - 1;
      
      String nodeName = node.nodeName();
          
      if (nodeName.equals("br")) { 
        this.brCount++;
      }
      else if (nodeName.equals("a")) { 
        this.anchorCount++;
      }
      else if (nodeName.equals("img")) { 
        this.imgCount ++;
      }
    }

    @Override
    public boolean isLeafNode() {
      return false;
    }
    
    @Override
    public String toString() {
      return "BlockNode:" + nodeName;
    }

    
    public int nestingLevel = 0;
    public int leafsWithURLCount = 0;
    
  }
  
  public static class LinkExtractionContext { 
    NodeWithChildren activeExtractionNode = null;
    public NodeWithAttributes    activeLeafNode = null;    
  }
  
  
  static Set<String> trackedNonLeafNodes = Sets.newHashSet(
      "p","div","tr","td","th","table","a","li","dt","dd",
      "h1","h2","h3","h4","h5","h6"
      );
  static Set<String> trackedLeafNodes = Sets.newHashSet("br","link","img");
  
  @Override
  public Document buildDocument(InstructionsPool instructionsPool,FileOutputStream optionalOutputStream) throws IOException {
    
    System.out.println("iterating parse instructions");
    LinkExtractionContext context = new LinkExtractionContext();
    
    List<Integer> operations = instructionsPool.operations;
    List<String> arguments = instructionsPool.arguments;

    for (int i=0; i<operations.size(); i++) {
        int domOperation = operations.get(i);
        String domArgument = arguments.get(i);
        //System.out.println("Operation :" + ParserInstruction.getOperationString(domOperation)+" Arg:~" + domArgument+"~");
        switch (domOperation)
        {
          // Open node :
          case ParserInstruction.OpenNode: {
            String tagName = domArgument.toLowerCase();
            if (trackedNonLeafNodes.contains(tagName)) {
              System.out.println("Found TrackedNode:" + tagName);
              NodeWithChildren node = new NodeWithChildren(context.activeExtractionNode,tagName);
              if (context.activeExtractionNode != null)
                context.activeExtractionNode.addNode(node);
              context.activeExtractionNode = node;
            }
            else { 
              if (context.activeExtractionNode != null)
                context.activeExtractionNode.nestingLevel++;
            }
          }
          break;
          // Close node :
          case ParserInstruction.CloseNode:{
            NodeWithChildren activeNode = context.activeExtractionNode;
            if (activeNode != null) { 
              if (activeNode.nestingLevel != 0) {
                activeNode.nestingLevel--;
              }
              else { 
                //if (activeNode.leafsWithURLCount != 0) { 
                  extractLinksFromNode(activeNode);
                //}
                // pop stack 
                context.activeExtractionNode = activeNode.parentNode;
              }
            }
          }
          break;
          case ParserInstruction.AddText:
            if (context.activeExtractionNode != null){ 
              String text = domArgument;
  
              text = text.replaceAll("\\s{2,}"," ");
              text = text.trim();
              
              if (text.length() != 0) { 
                context.activeExtractionNode.addNode(new TextNode(context.activeExtractionNode, text));
              }
            }
            break;
          case ParserInstruction.AddContent:
            //System.out.println("AddContent:"+domArgument);
            break;
          case ParserInstruction.AddLeaf: {
            if (context.activeExtractionNode != null) { 
              String tagName = domArgument.toLowerCase();
              if (trackedLeafNodes.contains(tagName)) { 
                context.activeLeafNode = new NodeWithAttributes(context.activeExtractionNode, tagName);
              }
            }
          }break;
          case ParserInstruction.WriteAttributeKey: { 
            String key = domArgument.toLowerCase();
            ++i;
            operations.get(i);
            String value = arguments.get(i);
            
            if (context.activeLeafNode != null) { 
              context.activeLeafNode.addAttribute(key, value);
            }
            else if (context.activeExtractionNode != null) { 
              context.activeExtractionNode.addAttribute(key, value);
            }
          }
          break;
          case ParserInstruction.CloseLeaf: {
            if (context.activeLeafNode != null) { 
              context.activeExtractionNode.addNode(context.activeLeafNode);
              context.activeLeafNode = null;
            }
          }
          break;
          
          case ParserInstruction.AddEntity:
            System.out.println("AddEntity:" + domArgument);
              break;
          case ParserInstruction.AddComment:
            //System.out.println("AddComment:" + domArgument); 
              break;
          case ParserInstruction.SetTitle: {
          }
          break;
        }
    }
    return null;
  }

  void findTextAroundNode(Node anchorNode,NodeWithChildren parentNode,StringBuffer buffer) { 
    buffer.append("Parent Node:" + parentNode.nodeName + "\nContext Text:");
    if (parentNode.childNodes != null) { 
      for (Node child : parentNode.childNodes) {
        if (child == anchorNode) 
          break;
        if (child.isTextNode()) { 
          buffer.append(((TextNode)child).text);
        }
      }
    }
  }
  
  void extractLinksFromNode(NodeWithChildren node) {
    if (node.parentNode == null && node.nodeName.equals("a")){ 
      extractTextFromAnchorNode(node);
    }
    else { 
      if (node.childNodes != null) { 
        for (Node childNode : node.childNodes) { 
          if (childNode instanceof NodeWithChildren) { 
            if (((NodeWithChildren)childNode).nodeName.equalsIgnoreCase("a")) { 
              extractTextFromAnchorNode((NodeWithChildren)childNode);
            }
          }
        }
      }
    }
  }
  
  void extractTextFromNode(NodeWithChildren node,StringBuffer textBufferOut) { 
    if (node.childNodes != null) { 
      for (Node childNode : node.childNodes) { 
        if (childNode.isTextNode()) {
          textBufferOut.append(((TextNode)childNode).text + " ");
        }
        else if (!childNode.isLeafNode()) { 
          extractTextFromNode((NodeWithChildren)childNode,textBufferOut);
        }
      }
    }    
  }
  
  Node findPreviousAnchorOrBreakInParent(NodeWithChildren anchorNode) { 
    for (int i=anchorNode.nodePosition-1;i>=0;--i) { 
      Node prevNode = anchorNode.parentNode.childNodes.get(i);
      if (prevNode.nodeName().equals("a") || prevNode.nodeName().equals("br") || trackedNonLeafNodes.contains(prevNode.nodeName())) { 
        return prevNode;
      }
    }
    return null;
  }
  
  void extractTextAfterGivenNodeAndBeforeNextAnchorOrBreak(
      NodeWithChildren parent,
      Node previousAnchorOrBreak,
      NodeWithChildren currentAnchor,StringBuffer textBuffer) {
    int startIndex = (previousAnchorOrBreak != null) ? previousAnchorOrBreak.nodePosition : -1;
    for (int i=startIndex + 1;i<parent.childNodes.size();++i) { 
      Node nodeAtIndex = parent.childNodes.get(i);
      if (nodeAtIndex.isTextNode()) { 
        textBuffer.append(((TextNode)nodeAtIndex).text +" ");
      }
      else if (nodeAtIndex.isLeafNode()) { 
        if (nodeAtIndex.nodeName().equals("br")) { 
          break;
        }
      }
      else if (!nodeAtIndex.isLeafNode()) {
        if (nodeAtIndex.nodeName().equals("a") && nodeAtIndex != currentAnchor) { 
          break;
        }
        else { 
          extractTextFromNode((NodeWithChildren)nodeAtIndex,textBuffer);
        }
      }
    }
  }
        
  void extractTextFromAnchorNode(NodeWithChildren node){
    
    StringBuffer anchorText = new StringBuffer();
    
    // ok get parent .. first 
    NodeWithChildren parent = node.parentNode;
    // if parent present .. 
    if (parent != null) { 
      if (parent.brCount == 0 && parent.anchorCount == 1) { 
        extractTextFromNode(parent,anchorText);
      }
      else {
        System.out.println("Looking For Previous Break In Parent");
        Node previousAnchorOrBreak = findPreviousAnchorOrBreakInParent(node);
        System.out.println("Found:" + previousAnchorOrBreak);  
            
        System.out.println("Extracting Text Given Previous Node");
        extractTextAfterGivenNodeAndBeforeNextAnchorOrBreak(parent,previousAnchorOrBreak,node,anchorText);
      }
    }
    else { 
      extractTextFromNode(parent,anchorText); 
    }
    System.out.println("Anchor Text:" + anchorText.toString() + " href:" + node.getAttribute("href"));
  }
  
  
  
}
