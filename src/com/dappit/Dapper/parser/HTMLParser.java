package com.dappit.Dapper.parser;

import java.io.FileOutputStream;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import org.dom4j.DocumentException;
import org.w3c.dom.Document;

/**
 * An HTML parser that can take a stream of bytes and return a DOM Document
 * object.
 *
 * @author Tony Novak
 */
public interface HTMLParser
{
	/**
	 * Parse an html document, returning a DOM document.
	 *
	 * @param htmlBytes       Raw bytes to parse
	 * @param htmlEncoding    The character set (e.g. <tt>UTF-8</tt>) for
	 *                        decoding <i>htmlBytes</i>.  If <tt>null</tt>
	 *                        is passed, the default HTTP encoding
	 *                        (<tt>ISO-8559-1</tt>) is used, unless the
	 *                        character set is overridden by a
	 *                        <tt>&lt;META&gt;</tt> tag in the HTML body.
	 *                        If a non-null value is passed, any such
	 *                        <tt>&lt;META&gt;</tt> tag is ignored.
	 */
	public Document parse( byte[] htmlBytes, String htmlEncoding,FileOutputStream optinalOutputStream ) throws DocumentException, ParserException, ParserConfigurationException,IOException;
}
