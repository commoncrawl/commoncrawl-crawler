/**
 * 
 */
package com.dappit.Dapper.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Ohad Serfaty
 *
 */
public class InstructionsPool
{
	public static final int DefaultPoolSize = 450;
	
	public List<Integer> operations;
	public List<String> arguments;
	private double currentParserVersion;
	
	public InstructionsPool()
	{
		this(DefaultPoolSize);
	}
	
	public InstructionsPool(int poolSize)
	{
		 operations = new ArrayList<Integer>();
		 arguments = new ArrayList<String>();
	}
	
	/**
	 *  reset the builder. can be reused after creating a document.
	 */
	public void reset()
	{
		operations.clear();
		arguments.clear();
	}
	
	/**
	 * Add a content sink instruction with an argument
	 * 
	 * @param domOperation
	 * @param domArgument
	 */
	public void addInstruction(int domOperation , String domArgument)
	{
		this.operations.add(domOperation);
		this.arguments.add(domArgument);
	}
	
	public void dump()
	{
		Iterator<Integer> i2 = this.operations.iterator();
		Iterator<String> j2 = this.arguments.iterator();
		while (i2.hasNext())
			System.err.println(i2.next() +" : " + j2.next());
	}
	
	/**
	 * @return
	 */
	public List<Integer> getInstructions() {
		return operations;
	}

	/**
	 * @param currentParserVersion
	 */
	public void setParserVersion(double currentParserVersion)
	{
		this.currentParserVersion = currentParserVersion;
	}
	
}
