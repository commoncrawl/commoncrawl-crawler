/**
 * 
 */
package com.dappit.Dapper.parser;

import java.io.File;

/**
 * @author Ohad Serfaty
 *
 */
public class EnviromentController 
{
	
	/**
	 * @author Ohad Serfaty
	 * An enumeration of the Operating system detected. 
	 * 
	 */
	public enum OS 
	{
		WIN , LINUX , MACOSX , UNKNOWN
	}
	
	private static OS OperatingSystem = null;
	
	public static String getOperatingSystemName() throws Exception{
		switch (detectOperatingSystem())
		{
		case WIN :
			return "win"; 
		case LINUX :
			return "lin";
		case MACOSX:
			return "macosx";
		}
		throw new Exception ("Could not detect Operating system.");
	}
	
	public static String getSharedLibraryExtension() throws Exception{
		switch (detectOperatingSystem())
		{
		case WIN :
			return ".dll"; 
		case LINUX :
			return ".so";
		case MACOSX:
			return ".jnilib";
		}
		throw new Exception ("Could not detect Operating system.");
	}
	
	public static OS detectOperatingSystem()
	{
		if (OperatingSystem != null)
			return OperatingSystem;
		String osName =  System.getProperty("os.name");
		System.out.println("Operating system : " +osName);
		
		if (osName.toLowerCase().contains("windows")){
			OperatingSystem = OS.WIN;
			return OS.WIN;
		}
		else
			if (osName.toLowerCase().contains("linux")){
				OperatingSystem = OS.LINUX;
				return OS.LINUX;
			}
			else
		if (osName.toLowerCase().contains("macosx") || (System.getProperty("mrj.version") != null)){
			OperatingSystem = OS.MACOSX;
			return OS.MACOSX;
		}
		return OS.UNKNOWN;
	}
	
	public static native void setenv(String variableName , String value);
	
	public static void addenv(String variableName , String valueToAdd)
	{
		String currentEnviromentVariable = System.getenv(variableName);
		EnviromentController.setenv(variableName,currentEnviromentVariable + valueToAdd);
	}
	
	public static void addenv(String variableName , String valueToAdd , String seperatorIfNotEmpty){
		String currentEnviromentVariable = EnviromentController.getenv(variableName);
		if (currentEnviromentVariable==null || currentEnviromentVariable.length()==0)
			addenv(variableName, valueToAdd);
		else
			addenv(variableName, seperatorIfNotEmpty + valueToAdd);
	}

	public static native String getenv(String variableName);
	
	public static void addLibraryDirectory(String directoryPath) throws Exception
	{
		switch (detectOperatingSystem())
		{
		case WIN :
			addenv("PATH" , directoryPath , File.pathSeparator);
			break;
		case LINUX :
			addenv("LD_LIBRARY_PATH" , directoryPath , File.pathSeparator);
			break;
		case MACOSX:
			addenv("DYLD_LIBRARY_PATH" , directoryPath , File.pathSeparator);
			break;
		case UNKNOWN: 
			throw new Exception ("Could not detect Operating system.");
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		System.load("C:\\dapper\\workspace\\Dapper\\java\\lib\\parser\\bin\\EnviromentController.dll");
		System.out.println(EnviromentController.detectOperatingSystem());
		System.out.println("BEFORE : PATH : " + EnviromentController.getenv("PATH"));
		EnviromentController.addLibraryDirectory("C:\\dapper\\mozilla\\dist\\bin");
		System.out.println("AFTER :PATH : " + System.getenv("PATH"));
		System.out.println("AFTER :PATH : " + EnviromentController.getenv("PATH"));
	}

}
