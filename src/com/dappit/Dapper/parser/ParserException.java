package com.dappit.Dapper.parser;
/**
 * @author Ohad Serfaty
 *
 * A Parser exception
 * 
 */
public class ParserException extends Exception {

	private static final long serialVersionUID = 3194008556698920289L;

	public ParserException(String string) {
		super(string);
	}

	public ParserException(String message, Throwable cause) {
		super(message, cause);
	}

	public ParserException(Throwable e) {
		super(e);
	}

}
