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

import java.io.IOException;
import java.util.List;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author rana
 *
 * @param <MetadataType>
 */
public class NameTree<MetadataType> {

  private static final Log LOG = LogFactory.getLog(NameTree.class);

  
  private Node<MetadataType> _root = new Node<MetadataType>();
  private Stack<TreePosition> _iterationStack = new Stack<TreePosition>();
  private long numberOfNodes = 0;
  private long numberOfNodesRemoved = 0;
  
  
  private static final int searchMode_NodeChar = 1;
  private static final int searchMode_MultiNodeChar = 2;
  
  public NameTree() { 
    _iterationStack.ensureCapacity(1024);
  }
  
  public synchronized long getActiveNodeCount() { 
    return numberOfNodes - numberOfNodesRemoved;
  }
  
  
  public synchronized void clear() { 
    _root = new Node<MetadataType>();
    _iterationStack.clear();
    numberOfNodes = 0;
    numberOfNodesRemoved  = 0;
  }
  
  public static class DNSResult { 
    
    DNSResult(int ipAddress,long ipAddressTTL,String cName) { 
      _ipV4Address = ipAddress;
      _ttl = ipAddressTTL;
      _cName = cName;
    }
    
    public int getIPAddress() { 
      return _ipV4Address;
    }
    
    public long getTTL() { 
      return _ttl;
    }
    
    public String getCannonicalName() { 
      return _cName;
    }
    
    int       _ipV4Address;
    long    _ttl;
    String   _cName;
  }
  
  private static final class TreePosition { 
    
    TreePosition(Node node,int index) { 
      _node = node;
      _index = index;
    }
    
    public final Node getNode() { 
      return _node;
    }
    
    public final int getIndex() { 
      return _index;
    }
    
    public final void setIndex(int index) { 
      _index = index;
    }
    
    public final Node resolve() { 
      if (_index < _node.getChildCount()) { 
        return _node.getChildAt(_index);
      }
      return null;
    }
    
    private Node _node;
    private int    _index;
  }
  
    
  static int numberOfNodesChildEQ1 =0;
  static int numberOfNodesChildLTEQ4 =0;
  static int numberOfNodesChildLTEQ8 =0;
  static int numberOfNodesChildGT8 =0;
  
  
  public static class Node<MetadataType> { 

    private static final int GROWTH_FACTOR = 1;

    private char     _nodeChar;
    private char     _nodeCharArray[];
    private int      _flags = 0;
    private int      _count = 0;  
    private Object   _children;
    private Node<MetadataType>     _parent = null;
    private long     _lastTouched = -1;
    private long     _longData = 0;
    private MetadataType _metadata = null;
    
    /*
    private Object _metadata = null;
    */
    
    public static final short Flag_Is_RootNode        = 1 << 0; 
    public static final short Flag_Is_TerminalNode   	= 1 << 1;
    public static final short Flag_Is_SuperNode  			= 1 << 2;
    public static final short Flag_Is_MultiCharNode  	= 1 << 3;
    public static final short Flag_NEXT_AVAILABLE_BIT_POS  = 4;
    
    private Node() {
      _parent = null;
      _flags = Flag_Is_RootNode;
    }
    
    public boolean isRootNode() { 
      return (_flags & Flag_Is_RootNode) != 0;
    }
    
    public Node<MetadataType> getParentNode() { 
      return _parent;
    }
    
    public Node(Node<MetadataType> parent,char nodeChar,int flags) {
      
      numberOfNodesChildEQ1++;
      
      _nodeChar = nodeChar;
      _flags = (short)flags;
      _parent = parent;
    }
    
    public final char getNodeChar() { 
      return _nodeChar;
    }
    
    public final char[] getMultiCharArray() { 
      return _nodeCharArray;
    }
    
    public final boolean isTerminalNode() { 
      return (_flags & Flag_Is_TerminalNode) != 0;
    }
    
    public final void markAsTerminalNode() { 
      _flags |= Flag_Is_TerminalNode;
    }
    
    public final boolean isSuperNode() { 
      return (_flags & Flag_Is_SuperNode) != 0;
    }
    
    public final void markAsSuperNode() { 
      _flags |=Flag_Is_SuperNode;
    }
    
    public final boolean isMultiCharNode() { 
      return (_flags & Flag_Is_MultiCharNode) != 0;
    }
    
    public final void setFlag(short flag) { 
    	_flags |= flag;
    }

    public final void clearFlag(short flag) { 
    	_flags &= ~flag;
    }
    
    public final boolean isFlagSet(short flag) { 
      return (_flags & flag) != 0;
    }
        
    /** update last touched time **/
    public final void setLastTouchedTime(long timeInMilliseconds) { 
      _lastTouched = timeInMilliseconds;
    }
   
    /** get last touched time **/
    public final long getLastTouchedTime() { 
      return _lastTouched;
    }
       
    
    public final void setLongData(long data) { 
      _longData = data;
    }
    
    public final long getLongData() { 
      return _longData;
    }
    
    public final void setMetadata(MetadataType metadata) { 
    	_metadata = metadata;
    }
    
    public MetadataType  getMetadata() { 
    	return _metadata;
    }

    public String getFullName() { 
      StringBuffer nameOut = new StringBuffer();
      
      Node currentNode = this;
      
      while (!currentNode.isRootNode()) { 
        if (currentNode.isMultiCharNode()) { 
          nameOut.append(currentNode.getMultiCharArray());
        }
        nameOut.append(currentNode.getNodeChar());
        
        currentNode = currentNode.getParentNode();
      }
      return nameOut.toString();
    }
    
    public final int compareTo(char c) {
      if (_nodeChar < c)
        return -1;
      else if (_nodeChar > c)
        return 1;
      else
        return 0; 
    }
    
    public final int     getChildCount() { return _count; }
    public final Node<MetadataType>    getChildAt(int index) { return (_count == 1) ? (Node)_children : ((Node[])_children)[index]; }
    public final void    removeChildAt(int index) {
      if (index >= _count)
        throw new RuntimeException("Invalid Index" );
      if (_count == 1) { 
        _children = null;
      }
      else { 
        int rightOfIndexCount = _count - (index + 1);
        if (rightOfIndexCount > 0) {
          if (_count > 2)
            System.arraycopy(_children, index + 1, _children, index, rightOfIndexCount);
          else 
            _children = ((Node[])_children)[1];
        }
        else { 
          if (_count == 2) { 
            _children = ((Node[])_children)[0];
          }
        }
      }
      _count--;
    }
    
    public final Node<MetadataType>    findOrAddChild(NameTree<MetadataType> cacheObject,char nodeChar,boolean addChild) {

      int itemPosition= -1;
      
      if (_count == 1) { 
        if (((Node)_children).getNodeChar() == nodeChar) {
          itemPosition = 0;
        }
        else if (((Node)_children).getNodeChar() < nodeChar ) { 
          itemPosition = -2;
        }
      }
      else if (_count > 1) { 
        itemPosition = binarySearch((Node[])_children,0,_count,nodeChar);
      }
      
      // lt 0 means item was not found ... 
      if (itemPosition < 0 && addChild) { 

        // allocate a new Node
        Node newNode = new Node(this,nodeChar,0);
        // increment stats ... 
        cacheObject.numberOfNodes++;
        
        // normalize item positoin ... 
        itemPosition = Math.abs(itemPosition + 1);
        
        // check some bounding conditions.
        if (_count == 0) { 
          _children = newNode;
        }
        else { 
          // get some basic bounding information established ...  
          int leftCopyItems = itemPosition;
          int rightCopyItems = _count - itemPosition;
  
          Node copyArray[] = (_count == 1) ? null: (Node[]) _children;
          // if the array is full ... 
          if (_count ==1 || _count == ((Node[])_children).length) {
            if (_count == 1) { 
              numberOfNodesChildEQ1 --;
              numberOfNodesChildLTEQ4++;
            }
            else if (_count == 4) { 
              numberOfNodesChildLTEQ4 --;
              numberOfNodesChildLTEQ8++;
            }
            else if (_count == 8) { 
              numberOfNodesChildLTEQ8--;
              numberOfNodesChildGT8++;
            }
            // allocate a new array 
            int growAmount = ((_count / GROWTH_FACTOR) + 1) * GROWTH_FACTOR;
            copyArray = new Node[growAmount];
          }
        
          // copy lbound items only if copying into new array ... 
          if (leftCopyItems != 0 && copyArray != null) {   
            if (_count == 1)
              copyArray[0] = (Node)_children;
            else
              System.arraycopy(_children, 0, copyArray, 0, leftCopyItems);
          }
          // copy rbound items no matter what ... 
          if (rightCopyItems != 0) {
            if (_count == 1)
              copyArray[1] = (Node)_children;
            else
              System.arraycopy(_children, itemPosition, copyArray, itemPosition + 1, rightCopyItems);
          }
          
          // and assign copy array to children 
          _children = copyArray;
          // insert new item into array ... 
          ((Node[])_children)[itemPosition] = newNode;
        }
        // increment count 
        _count++;
      }
      if (itemPosition >=0) {
        return (_count == 1) ? (Node)_children : ((Node[])_children)[itemPosition];
      }
      return null;
    }
    
    
    // Like public version, but without range checks.
    private static int binarySearch(Node[] a, int fromIndex, int toIndex,char key) {
      
      int low = fromIndex;
      int high = toIndex - 1;

      while (low <= high) {
          int mid = (low + high) >>> 1;
          Node midVal = a[mid];
          
          int cmp = midVal.compareTo(key);
    
          if (cmp < 0)
            low = mid + 1;
          else if (cmp > 0)
            high = mid - 1;
          else
            return mid; // key found
      }
      return -(low + 1);  // key not found.
    }
    
    public void markAsMultiCharNode(char s[], int startOffset,int length) {
      if (length == 0) { 
        throw new RuntimeException();
      }
      _flags |= Flag_Is_MultiCharNode;
      _nodeCharArray = new char[length];
      System.arraycopy(s, startOffset, _nodeCharArray, 0, length);
    }
    
    public Node<MetadataType> splitMultiCharNodeAt(NameTree cacheObject,int splitIdx) {
    
      // allocate new intermediate node 
      Node newIntermediateNode = new Node(this,_nodeCharArray[splitIdx],0);
      
      
      //increment stats ... 
      cacheObject.numberOfNodes++;
      
      newIntermediateNode._longData = this._longData;
      newIntermediateNode._metadata = this._metadata;
      
      this._longData = 0;
      this._metadata = null;
      // this._metadata = null;
      newIntermediateNode._flags =(short) (this._flags & ~Flag_Is_MultiCharNode);
      this._flags = 0;
      
      int leftOfSplitLength = splitIdx;
      int rightOfSplitLength = _nodeCharArray.length - splitIdx - 1;
      if (leftOfSplitLength != 0) { 
        newIntermediateNode.markAsMultiCharNode(_nodeCharArray,0,leftOfSplitLength);
      }
      if (rightOfSplitLength != 0) { 
        char array[] = new char[rightOfSplitLength];
        System.arraycopy(_nodeCharArray,splitIdx+1 , array, 0, rightOfSplitLength);
        _nodeCharArray = array;
        _flags |= Flag_Is_MultiCharNode;
      }
      else { 
        _nodeCharArray = null;
        _flags &= ~Flag_Is_MultiCharNode;
      }
      
      // absorb the old node's children ... 
      newIntermediateNode._children = this._children;
      newIntermediateNode._count    = this._count;
      
      // and re-parent our children to point to new intermediate branch node 
      for (int i=0;i<newIntermediateNode.getChildCount();++i) { 
        newIntermediateNode.getChildAt(i)._parent = newIntermediateNode;
      }
      
      // make the new node the old node's only child ... 
      this._children         = newIntermediateNode;
      this._count             = 1;
      
      return newIntermediateNode;
    }
    
  }

  /** add a name node **/
  public Node<MetadataType> addNameNode(String nodeName) {
    return addNode(_root,nodeName);
  }
  
  /** underlying routine shared by name and ip lookup routines **/
  private Node<MetadataType> addNode(Node<MetadataType> rootNode,String path){
    // search super node in reverse order ... 
    String s = path.toLowerCase();
    Node<MetadataType> node = rootNode;
    
    if (s.length() > 0) {
      
      int multiNodeIdx = -1;
      int searchMode = searchMode_NodeChar;
      
      for (int i= s.length()-1; i >= 0; i--) {
        if (searchMode == searchMode_NodeChar) {
          // find the node for the next character ... 
          node= node.findOrAddChild(this,s.charAt(i),true);
          // update/set the node's ttl
          //node.setTimeToLive(Math.max(node.getTimeToLive(),ttl));
          // if the returned node is a multi-char node, search into the node ... 
          if (node.isMultiCharNode()) {
            searchMode = searchMode_MultiNodeChar;
            multiNodeIdx = node.getMultiCharArray().length - 1;
          }
          // otherwise... if the node has no children and the index is not zero and current char is not '.' token ... 
          else if (node.getChildCount() == 0 && i != 0 && s.charAt(i) != '.' && !node.isTerminalNode()) { 
            int multiNodeScanStart = i-1;
            int multiNodeCharEndPos = multiNodeScanStart;
            // walk backwards until either end of string is reached, or a '.' token is located ... 
            while (multiNodeCharEndPos >= 0 && s.charAt(multiNodeCharEndPos) != '.')
              multiNodeCharEndPos--;
            
            // if we actually accumulated something in the scan buffer ... 
            if (multiNodeScanStart - multiNodeCharEndPos != 0) { 
              // if scan terminated before the end of the string, then a '.' token terminated the scan ... 
              if (multiNodeCharEndPos != -1) {
                // gobble up the appropriate number of characters ... 
                node.markAsMultiCharNode(s.toCharArray(),multiNodeCharEndPos + 1,i-(multiNodeCharEndPos + 1));
                //set up i so that loop can continue ... 
                i = multiNodeCharEndPos + 1;
                // and break out of multi-char scan mode ... 
                searchMode = searchMode_NodeChar;
              }
              else { 
                // gobble up remaining characters and covert node to multi-char node ...  
                node.markAsMultiCharNode(s.toCharArray(),0,i);
                // and break out of loop... 
                break;
              }
            }
          }
        }
        else { 
          if (multiNodeIdx ==-1 || i == -1) { 
            throw new RuntimeException();
          }
          if (node.getMultiCharArray()[multiNodeIdx] != s.charAt(i)) { 
            node.splitMultiCharNodeAt(this,multiNodeIdx);
            i+=1;
            searchMode = searchMode_NodeChar;
          }
          else { 
            multiNodeIdx -= 1;
            if (multiNodeIdx < 0)
              searchMode = searchMode_NodeChar;
          }
        }
        
      }
      // finally, if we are still in multi-char search mode and multiNodeIdx != -1
      // this means that we terminated a successfull match int the MIDDLE of a multi-char node
      if (searchMode == searchMode_MultiNodeChar && multiNodeIdx != -1) { 
        // in this case, we have to split the multi-char node appropriately ... 
        node.splitMultiCharNodeAt(this,multiNodeIdx);
      }
      node.markAsTerminalNode();
      
      return node;
    }
    return null;
  }
  
  
  public Node<MetadataType> findNode(String nodeName) { 
    Node<MetadataType> nodeOut = _findNode(_root,nodeName);
    if (nodeOut != null && nodeOut.isTerminalNode()) { 
      nodeOut.setLastTouchedTime(System.currentTimeMillis());
    }
    return nodeOut;
  }
  
  private synchronized Node _findNode(Node rootNode,String nodeName) { 
    
    String s = nodeName.toLowerCase();
    
    Node node= rootNode;
    
    if (s.length() > 0) {
      
      Node lastSubTerminalNode = null;
      
      for (int i= s.length()-1; i >= 0 && node != null; i--) {
        
        if (s.charAt(i) == '.' && node != _root) {
          lastSubTerminalNode = node;
        }
        
        node= node.findOrAddChild(this,s.charAt(i),false);
        
        // now if this is a multi-char node ... 
        if (node != null && node.isMultiCharNode()) { 
          int innerScanPos = i-1;
          int multiCharArrayScanPos = node.getMultiCharArray().length -1;
          while (innerScanPos >= 0 &&  multiCharArrayScanPos >=0) { 
            if (s.charAt(innerScanPos) == node.getMultiCharArray()[multiCharArrayScanPos]) { 
              innerScanPos--;
              multiCharArrayScanPos--;
            }
            else { 
              break;
            }
          }
          
          // now first condition for a successfull match is than we completely scanned the mutli-char array ... 
          if (multiCharArrayScanPos == -1) { 
            // now if char scan also reached the beginning of the search string ... 
            if (innerScanPos == -1) { 
              // this is a true match ... 
              return (node.isTerminalNode()) ? node : null;
            }
            // otherwise ... check the character at new position ... 
            else { 
              // set i to inner scan pos + 1 (so that out loop will properly adjust value)
              i=innerScanPos+1;
            }
          }
          else { 
            node = null;
            break;
          }
        }
      }
      
      if (node == null && lastSubTerminalNode != null) {
        if (lastSubTerminalNode.isSuperNode()) {
          node = lastSubTerminalNode;
        }
      }
      if (node != null)
        return (node.isTerminalNode()) ? node : null;
    }
    return null;
  }
  
  private synchronized void collectTerminalNodes(Node node,List<Node> terminalNodeVector)throws IOException {
    if (node.isTerminalNode()) {
      terminalNodeVector.add(node);
    }
    for (int i=0;i<node.getChildCount();++i) { 
      collectTerminalNodes(node.getChildAt(i),terminalNodeVector);
    }
  }
  
  public synchronized void collectTerminalNodes(List<Node> terminalNodeVector){
    try { 
      collectTerminalNodes(_root,terminalNodeVector);
    }
    catch (IOException e) { 
      
    }
  } 
}
