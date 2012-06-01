package com.dappit.Dapper.parser;
/**
 * 
 */

/**
 * @author Ohad Serfaty
 *
 * An exception thrown for initialization problems.
 *
 */
public class ParserInitializationException extends ParserException {

	private static final long serialVersionUID = -4149559558547113570L;

	public ParserInitializationException(String string) {
		super(string);
	}

	public ParserInitializationException(String string, Throwable cause) {
		super(string, cause);
	}

	public ParserInitializationException(Throwable e) {
		super(e);
	}

}
