/**
 * 
 */
package com.dappit.Dapper.parser;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.CDATASection;
import org.w3c.dom.Comment;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.EntityReference;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * @author Ohad Serfaty
 * 
 * A class for building DOM documents from mozilla's content sink instructions
 * 
 * supported operations are : OpenNode <tag name> CloseNode <tag name> AddText
 * <content> AddLeaf <tag name> WriteAttributeKey <key> - in pair with the next
 * op : WriteAttributeValue <value> CloseLead AddComment AddEntity
 * 
 * Unsupported ( fot the time being ) : AddInstruction AddTitle
 * 
 * 
 * Note that this class is reusable , you can use reset() to clear the content
 * of the dom.
 * 
 */
public class DomDocumentBuilder implements com.dappit.Dapper.parser.DocumentBuilder
{
	private static DocumentBuilder documentBuilder = null;

	private static char charMinusOne = (char) -1;

	public static String getCDATASection(String domArgument)
	{
		if (!domArgument.contains("CDATA"))
			return null;
		Pattern pat = Pattern.compile("(.*)\\<\\!(\\s*)\\[CDATA(.*)\\]\\]\\>(.*)", Pattern.DOTALL + Pattern.MULTILINE);
		Matcher mat = pat.matcher(domArgument);
		if (mat.find())
		{
			String group3 = mat.group(3);
			if (group3.startsWith("["))
				group3 = group3.replaceFirst("\\[", "");
			String result = mat.group(1) + group3 + mat.group(4);
			return result;
		}
		return null;
	}

	/**
	 * Finalize and build the dom document.
	 * 
	 * @return
	 */
	public Document buildDocument(InstructionsPool instructionsPool,FileOutputStream optionalStreamOut) throws IOException
	{
		if( documentBuilder == null )
		{
			try {
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      } catch (ParserConfigurationException e) {
        throw new IOException(e);
      }
		}

		// System.out.println("building document...");
		Document resultDocument = documentBuilder.newDocument();
		Element currentElement = null;
		List<Integer> operations = instructionsPool.operations;
		List<String> arguments = instructionsPool.arguments;
		boolean isInLeaf = false;
		boolean closeHtml = true;

		for (int i=0; i<operations.size(); i++)
		{
			int domOperation = operations.get(i);
			String domArgument = arguments.get(i);
			//System.out.println("Operation :" + ParserInstruction.getOperationString(domOperation)+" Arg:~" + domArgument+"~");
			String currentNamespaceURI = currentElement == null ? null : currentElement.getNamespaceURI();
			switch (domOperation)
			{
			// Open node :
			case ParserInstruction.OpenNode:
				closeHtml = true;
				Element childNode = createElementNS(resultDocument, currentNamespaceURI, domArgument.toLowerCase());
				if (currentElement == null)
				{
					resultDocument.appendChild(childNode);
					currentElement = childNode;
				}
				else
				{
					if (!domArgument.equalsIgnoreCase("html"))
					{
						currentElement.appendChild(childNode);
						currentElement = childNode;
					}
					else
						closeHtml = false;
				}
				break;
			// Close node :
			case ParserInstruction.CloseNode:
			{
				if (currentElement == null)
				{
					System.err.println("Error : Close Node where no OpenNode was called. trying to fix...");
					// this.dump();
				}
				else if (closeHtml)
				{
					Node parentNode = currentElement.getParentNode();
					if( parentNode == resultDocument )
					{
						currentElement = null;
					}
					else
					{
						currentElement = (Element) parentNode;
					}
				}

			}
				break;
			case ParserInstruction.AddText:
			case ParserInstruction.AddContent:
				// System.out.println(currentElement.getNodeName() +" : Adding
				// text :" + domArgument);
				// check : try and resolve this with a <newline> from mozilla
				// instead :
				boolean script = false;
				boolean style = false;

				if (currentElement.getNodeName().equalsIgnoreCase("script"))
					script = true;
				else if (currentElement.getNodeName().equalsIgnoreCase("style"))
					style = true;
				else
					domArgument = DomDocumentBuilder.fixText(domArgument);

				// System.out.println("Body content :" + domArgument);
//				 System.out.println("AddText "+domArgument.length());
				if (domArgument.length() >= 1)
				{
					if (!script && !style)
					{
						Text textNode = resultDocument.createTextNode(domArgument);
						currentElement.appendChild(textNode);
					}
					else
					{
						domArgument = domArgument.trim();
						String cdata = getCDATASection(domArgument);

						if (cdata != null)
						{
							if (script)
								cdata = DomDocumentBuilder.fixText(cdata);
							else
								cdata = DomDocumentBuilder.fixText(domArgument);
							CDATASection cdataSection = resultDocument.createCDATASection(cdata);
							currentElement.appendChild(cdataSection);
						}
						else
						{
							domArgument = DomDocumentBuilder.fixText(domArgument);
							Text textNode = resultDocument.createTextNode(domArgument);
							currentElement.appendChild(textNode);
						}
					}
				}
				break;
			case ParserInstruction.AddLeaf:
				Element leafElement = createElementNS(resultDocument, currentNamespaceURI, domArgument);
				currentElement.appendChild(leafElement);
				currentElement = leafElement;
				isInLeaf = true;
				break;
			case ParserInstruction.WriteAttributeKey:
				// add an attribute with the next lookahead operation :
				String key = domArgument;

				i++;
				domOperation = operations.get(i); // Fetch the next operation , must be WriteAttributeValue
				String value = arguments.get(i); // Feth the attributes value.

				if( key.trim().equalsIgnoreCase("xmlns") )
				{
					currentElement = (Element) resultDocument.renameNode( currentElement, value, currentElement.getNodeName() );
				}
				else if( !domArgument.trim().equalsIgnoreCase("_moz-userdefined") )
				{
					trySetAttribute( currentElement, domArgument.toLowerCase(), DomDocumentBuilder.fixText(value) );
				}
				break;
			case ParserInstruction.CloseLeaf:
				if (isInLeaf)
				{
					currentElement = (Element) currentElement.getParentNode();
					isInLeaf = false;
				}
				break;
			case ParserInstruction.AddEntity:
				EntityReference entity = resultDocument.createEntityReference(domArgument);
				// a bugfix for a c++ problem in the mozilla parser:
				if (!Character.isDigit(domArgument.charAt(0)))
					entity.appendChild(resultDocument.createTextNode("&" + domArgument + ";"));
				else
					entity.appendChild(resultDocument.createTextNode(""));
				currentElement.appendChild(entity);
				break;
			case ParserInstruction.AddComment:
				Comment comment = resultDocument.createComment(domArgument);
				currentElement.appendChild(comment);
				break;
			case ParserInstruction.SetTitle:
				Element titleNode = createElementNS(resultDocument, currentNamespaceURI, "title");
				titleNode.appendChild(resultDocument.createTextNode(fixText(domArgument)));
				NodeList headElements = resultDocument.getElementsByTagName("head");
				// Add the title with the new text :
				if (headElements.getLength() > 0)
					headElements.item(0).appendChild(titleNode);
				break;
			}
		}
		return resultDocument;
	}

	public static String fixText(String text)
	{
		return text;
//		StringBuilder fixedText = new StringBuilder(text.length()+6);
//		char[] charArray = text.toCharArray();
//		boolean textChanged = false; 
//		for (int i = 0; i < charArray.length; i++)
//		{
//			char ch = charArray[i];
//			if (ch == '&')
//			{
//				char ch2 = charArray.length >= i + 2 ? charArray[i + 1] : charMinusOne;
//				char ch3 = charArray.length >= i + 3 ? charArray[i + 2] : charMinusOne;
//				char ch4 = charArray.length >= i + 4 ? charArray[i + 3] : charMinusOne;
//				char ch5 = charArray.length >= i + 5 ? charArray[i + 4] : charMinusOne;
//				char ch6 = charArray.length >= i + 6 ? charArray[i + 5] : charMinusOne;
//				// char ch7 = charArray.length > i+7 ? charArray[i+6] :
//				// charMinusOne;
//				// System.out.println("ch3:" + ch3 +" ch7:" + ch6);
//				if (ch2 == '#')
//				{
//					if (ch3 == '1')
//					{
//						if ((ch4 == '0' && ch5 == ';'))
//							i = i + 4;
//					}
//					else if (ch3 == '9' && ch4 == ';')
//					{
//						i = i + 3;
//					}
//				}
//				else if (ch2 == 'l' && ch3 == 't' && ch4 == ';')
//				{
//					fixedText.append('>');
//					i = i + 2;
//				}
//				else if (ch2 == 'g' && ch3 == 't' && ch4 == ';')
//				{
//					fixedText.append('<');
//					i = i + 3;
//				}
//				else if (ch2 == 'a' && ch3 == 'm' && ch4 == 'p' && ch5 == ';')
//				{
//					fixedText.append('&');
//					i = i + 4;
//				}
//				else if (ch2 == 'q' && ch3 == 'u' && ch4 == 'o' && ch5 == 't' && ch6 == ';')
//				{
//					fixedText.append('"');
//					i = i + 5;
//				}
//				textChanged = true;
//			}
//			else if (ch == '\n')
//				fixedText.append(ch);
//			else if (ch == 32)
//			{
//				fixedText.append(' ');
//				textChanged = true;
//			}
//			else if (ch < 32 && ch > 0 && ch != 9)
//				textChanged = true;
//			else
//				fixedText.append(ch);
//		}
//		if (!textChanged)
//			return text;
//		return fixedText.toString();
	}
	
//  Old version of the fixText function :
//	private static final String String32 = new String(new byte[]{ 32 });
//	private static final String String0 = new String(new byte[]{ 0 });
//	private static final String String1 = new String(new byte[]{ 0x1 });
//	private static final String String14 = new String(new byte[]{ 0x14 });
//	private static final String String1d = new String(new byte[]{ 0x1d });
//	private static final String String0xf = new String(new byte[]{ 0xf });
//	private static final String String0x1A = new String(new byte[]{ 0x1A });
//	private static final String String0x12 = new String(new byte[]{ 0x12 });
//	private static final String String0x8 = new String(new byte[]{ 0x8 });
//	private static final String String0x1f = new String(new byte[]{ 0x1f });
//	private static final String String0x2 = new String(new byte[]{ 0x2 });
//	private static final String String0x7 = new String(new byte[]{ 0x7 });
//	private static final String String0x18 = new String(new byte[]{ 0x18 });
//	private static final String String0x19 = new String(new byte[]{ 0x19 });
//	private static final String String0x1B = new String(new byte[]{ 0x1B });
//	private static final String String0x1C = new String(new byte[]{ 0x1C });
//	private static final String String0x11 = new String(new byte[]{ 0x11 });
//	private static final String String0x10 = new String(new byte[]{ 0x10 });
//	private static final String String0x13 = new String(new byte[]{ 0x13 });
//	
//	private static String fixTextOld(String text)
//	{
//		String fixedText = new String(text);
//		fixedText = fixedText.replaceAll("&#10;", "");
//		fixedText = fixedText.replaceAll("&#9;", "");
//		fixedText = fixedText.replaceAll("&#160;", " ");
//		fixedText = fixedText.replaceAll("&#194;", "\"");
//		fixedText = fixedText.replaceAll("&quot;", "\"");
//		fixedText = fixedText.replaceAll("&lt;", "<");
//		fixedText = fixedText.replaceAll("&gt;", ">");
//		fixedText = fixedText.replaceAll("&amp;", "&");
//		fixedText = fixedText.replaceAll(String32, " ");
//		fixedText = fixedText.replaceAll("["+String0+String1+String14+String1d+String0xf + String0xf+
//				String0x1A + String0x12 + String0x8 + String0x1f+ String0x2+String0x7+String0x18+String0x19+String0x1B+String0x1C+
//				String0x11+String0x10+String0x13+"]" , "");
//		return fixedText;
//	}

	/**
	 * Try setting an attribute on a given Node, but don't throw any exceptions
	 * if the operation fails because of an invalid character
	 */
	private static void trySetAttribute( Element element, String name, String value )
	{
		try
		{
			element.setAttribute( name, value );
		}
		catch( DOMException e )
		{
			System.err.println( "Failed to set attribute: " + name + " => " + value + "; continuing anyway" );
		}
	}

	/**
	 * Wrapper around createElementNS that correctly handles a qualifiedName
	 * containing a prefix (<tt>&lt;foo:bar&gt;</tt>) with no namespaceURI
	 */
	private static Element createElementNS( Document doc, String namespaceURI, String qualifiedName )
	{
		if( namespaceURI == null )
		{
			return doc.createElement( qualifiedName );
		}
		else
		{
			return doc.createElementNS( namespaceURI, qualifiedName );
		}
	}
}
