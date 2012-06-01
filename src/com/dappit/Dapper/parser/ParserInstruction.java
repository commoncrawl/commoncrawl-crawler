/**
 * 
 */
package com.dappit.Dapper.parser;

/**
 * @author Ohad Serfaty
 *
 */
public class ParserInstruction
{
	
	public static final int OpenNode = 1;
	public static final int CloseNode = 2;
	public static final int WriteAttributeKey = 3;
	public static final int WriteAttributeValue = 4;
	public static final int AddText = 5;
	public static final int AddContent = 6;
	public static final int CloseLeaf = 7;
	public static final int AddLeaf = 8;
	public static final int AddEntity = 9;
	public static final int AddComment = 10;
	public static final int SetTitle = 11;
	public static final int AddProcessingInstruction = 12;
	public static final int AddDoctypeDecl = 13;
	/**
	 * @param domOperation
	 * @return
	 */
	public static String getOperationString(int domOperation)
	{
		switch (domOperation)
		{
			case OpenNode: return "OpenNode";
			case CloseNode: return "CloseNode";
			case WriteAttributeKey: return "WriteAttributeKey";
			case WriteAttributeValue: return "WriteAttributeValue";
			case AddText: return "AddText";
			case AddContent: return "AddContent";
			case CloseLeaf: return "CloseLeaf";
			case AddLeaf: return "AddLeaf";
			case AddEntity: return "AddEntity";
			case AddComment: return "AddComment";
			case SetTitle: return "SetTitle";
			case AddProcessingInstruction: return "AddProcessingInstruction";
			case AddDoctypeDecl: return "AddDoctypeDecl";
		}
		
		return "N/A";
	}
	

}
