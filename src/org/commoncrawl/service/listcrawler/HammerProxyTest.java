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

package org.commoncrawl.service.listcrawler;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.nutch.util.GZIPUtils;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.async.Timer.Callback;
import org.commoncrawl.io.NIOBufferList;
import org.commoncrawl.io.NIOHttpConnection;
import org.commoncrawl.io.NIOHttpConnection.State;
import org.mortbay.util.UrlEncoded;

/** 
 * Some code to test proxy cache performance 
 * 
 * @author rana
 *
 */
public class HammerProxyTest implements NIOHttpConnection.Listener {

  private int       finishCount = 0;
  private int       successCount = 0;
  private int       failureCount  = 0;
  private int       connectionCount = 0;
  private int       resolvingCount = 0;

  private int maxCount = 0;
  private int socketMax = 0;
  private long loopCounter = 0;

  
  private EventLoop          eventLoop;;
  
  private ArrayList<String> urlList = null;
  NIOHttpConnection connections[] = null;
  private ArrayList<NIOHttpConnection> closedConnections = new ArrayList<NIOHttpConnection>();
  
 
  
  private HammerProxyTest(ArrayList<String> urlList,int socketMax,int maxCount) {

      eventLoop   = new EventLoop();
      
      this.maxCount = maxCount;
      this.socketMax = socketMax;
      this.connections = new NIOHttpConnection[socketMax];
      this.urlList = urlList;
            
  }

  
  private final void run() { 
    
    long startTime = System.currentTimeMillis();
    
    eventLoop.start();
    eventLoop.setTimer(new Timer(1000,true,new Callback() {

      public void timerFired(Timer timer) {
        
        if (urlList.size() == 0 && connectionCount == 0) { 
          eventLoop.stop();
        }
        else { 

          
          for (int j=0;j<connections.length;++j) { 
            
            if (connections[j] == null || connections[j].hasTimedOut()) { 

              if (connections[j] != null && connections[j].hasTimedOut()) { 
                connections[j].close();
                connections[j] = null;
                connectionCount--;
              }
              
              if (urlList.size() != 0) { 

                connectionCount ++;
                
                String url = urlList.remove(0);
                
                try { 
                  connections[j] = new  NIOHttpConnection(new URL(url),eventLoop.getSelector(),eventLoop.getResolver(),null);
                  connections[j].setDownloadMax(1024000);
                  connections[j].getRequestHeaders().set("Host", "www.commoncrawl.org");
                  connections[j].open();
                  connections[j].setListener(HammerProxyTest.this); 
                }
                catch (IOException e) { 
                  System.out.println("Error Opening Connection for URL:" + url);
                  connections[j] = null;
                  connectionCount--;
                }
              }
            }
          }
          
          loopCounter++;
          
          if (loopCounter % 1 == 0) { 

            System.out.format("%1$10s", "Index");
            System.out.format("%1$50.50s ", "URL");
            System.out.format("%1$10s ", "STATE\n");
            System.out.format("%1$25s\n", "DESC");


            for (int i=0;i<connections.length;++i) { 
              System.out.format("%1$10s ", i);
              if (connections[i] == null)
                System.out.print("null\n");
              else  {
                System.out.format("%1$50.50s ",connections[i].getURL().toString().substring(Math.max(0,connections[i].getURL().toString().length() - 50)));
                System.out.format("%1$10s ",connections[i].getState().toString());
                System.out.format("%1$10s\n",connections[i].getErrorDesc());
              }
            }
            System.out.print("\n");
          }
          
        }
      } 
      
    }));
    
    // wait on the event loop thread ... 
    try {
      eventLoop.getEventThread().join();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
   
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;
    
    System.out.print("\n");
    
    System.out.println("Stats:");
    
    System.out.format("%1$50.50s", "URL");
    System.out.format("%1$10s", "STATE");
    System.out.format(" %1$40.40s", "Result");
    System.out.format(" %1$10.10s", "HTTPCode");
    System.out.format("%1$10s", "Resolve");
    System.out.format("%1$10s", "Connect");
    System.out.format("%1$10s", "Upload");
    System.out.format("%1$10s", "Download");
    System.out.format("%1$20s\n", "Size");
    
    long totalBytes = 0;
    
    for (NIOHttpConnection connection : closedConnections) { 

      System.out.format("%1$50.50s", connection.getURL().toString().substring(Math.max(0,connection.getURL().toString().length() - 50)));
      System.out.format("%1$10s",    connection.getState());
      if (connection.getState() == State.ERROR) { 
        System.out.format(" %1$40.40s", formatException(connection.getLastException()));
        System.out.format(" %1$10.10s", "-1");

      }
      else {
        System.out.format(" %1$40.40s", connection.getResponseHeaders().getValue(0));
        System.out.format(" %1$10.10s", NIOHttpConnection.getHttpResponseCode(connection.getResponseHeaders()));

      }
      System.out.format("%1$10s",    connection.getResolveTime());
      System.out.format("%1$10s",    connection.getConnectTime());
      System.out.format("%1$10s",    connection.getUploadTime());
      System.out.format("%1$10s",    connection.getDownloadTime());
      System.out.format("%1$20s\n",  connection.getDownloadLength());
      
      if (connection.getState() == State.DONE) { 
        totalBytes += connection.getDownloadLength();
      }
    }
    
    System.out.println("\nFinal Stats:");
    System.out.println("Count:"+finishCount+" Success:"+successCount+" Failure:"+failureCount +" MS:"+ Long.toString(totalTime)+ " KBytes:"+Long.toString(totalBytes/1000)+ " KB/s:"+ Long.toString((totalBytes/1000) / (totalTime/1000)) );
    
  }
  
  private static String formatException(Exception e) { 
    if (e == null)
      return "";
    else { 
      String exceptionString = e.toString();
      return exceptionString.substring(exceptionString.lastIndexOf(".")+1);
    }
  }
  
  public void dumpContent(byte[] data) { 
    System.out.print(dumpAsHex(data));
    
  }
  
  public String dumpAsText(byte[] data) { 
    ByteBuffer bb = ByteBuffer.wrap(data);
    StringBuffer buf = new StringBuffer();
    buf.append(Charset.forName("ASCII").decode(bb));
    return buf.toString();
  }
  
  private static final int HEX_CHARS_PER_LINE  = 32;
  public String dumpAsHex(byte[] data) {
    
      StringBuffer buf = new StringBuffer(data.length << 1) ;
      int k = 0 ;
      int flen = data.length ;
      char hexBuffer[] = new char[HEX_CHARS_PER_LINE*2 + (HEX_CHARS_PER_LINE - 1)+ 2];
      char asciiBuffer[] = new char[HEX_CHARS_PER_LINE + 1];
      
      hexBuffer[hexBuffer.length-1]=0;
      asciiBuffer[asciiBuffer.length - 1]=0;

      for (int i = 0; i < flen ; i++) {
          int j = data[i] & 0xFF ;
          
          hexBuffer[k*3] = Character.forDigit((j >>> 4) , 16);
          hexBuffer[k*3+1] = Character.forDigit((j & 0x0F), 16);
          hexBuffer[k*3+2] = ' ';
          
          if (j<0x20)
            asciiBuffer[k] =  '.';
          else if (k < 0x78)
            asciiBuffer[k] = (char)j;
          else
            asciiBuffer[k] = '?';            
          k++ ;
          if (k % HEX_CHARS_PER_LINE == 0) {
              hexBuffer[hexBuffer.length-2] = 0;
              buf.append(hexBuffer);
              buf.append(" ");
              buf.append(asciiBuffer);
              buf.append('\n') ;
              k = 0 ;
          }
      }
      if (k  != 0) {
        hexBuffer[k*3 + 1] = 0;
        asciiBuffer[k] = 0;
        buf.append(hexBuffer);
        buf.append(" ");
        buf.append(asciiBuffer);
        buf.append('\n') ;
      }
      return buf.toString() ;
  }
  
  public void HttpConnectionStateChanged(NIOHttpConnection theConnection, State oldState, State state) {
    
    // System.out.println("State Changed from oldState:"+oldState.toString()+" to newState:"+state.toString());
    if (state == State.DONE || state == State.ERROR) {
      if (oldState == State.AWAITING_RESOLUTION) { 
        resolvingCount--;
      }
      finishCount++;
      
      if (state == State.DONE) { 
        successCount++;
        
        /*
        System.out.println("Connection:" + theConnection.getURL() + " State == DONE. Content Length:"+ theConnection.getContentLength() + 
            "Content Buffer Size:" + theConnection.getContentBuffer().available());
        */

        boolean dumpContent = false;
        if (dumpContent && theConnection.getContentBuffer().available() != 0) { 

          NIOBufferList contentBuffer = theConnection.getContentBuffer();
          try { 
            // now check headers to see if it is gzip encoded
            int keyIndex =  theConnection.getResponseHeaders().getKey("Content-Encoding");
            
            if (keyIndex != -1) { 

              String encoding = theConnection.getResponseHeaders().getValue(keyIndex);
            
              byte data[] = new byte[contentBuffer.available()]; 
              // and read it from the niobuffer 
              contentBuffer.read(data);
                
                if (encoding.equalsIgnoreCase("gzip")) { 
                  data = GZIPUtils.unzipBestEffort(data,1024000);
                  contentBuffer.reset();
                  contentBuffer.write(data, 0, data.length);
                  contentBuffer.flush();
                  
                  System.out.println("GUnzip Content Size:" + data.length);
                }
            }
            
            byte data[] = new byte[contentBuffer.available()];
            contentBuffer.read(data);
            
            dumpContent(data);
            
/*            
            BufferedReader reader = new BufferedReader(new NIOStreamDecoder(contentBuffer,Charset.forName("ASCII").newDecoder()));

            System.out.println("Dumping Content");
            
            String line;
            
            while ((line = reader.readLine()) != null) { 
              System.out.println(line);
            }
*/            
          }
          catch (IOException e) { 
            System.out.println(e);
            e.printStackTrace();
          }
          catch (Exception e) { 
            System.out.println(e);
            e.printStackTrace();
          }
        }
      }
      else if (state == State.ERROR) { 
        failureCount++;
      }
      for (int i=0;i<connections.length;++i) { 
        if (connections[i] == theConnection) { 
          connections[i] = null;
          connectionCount--;
          break;
          
        }
      }
      theConnection.getContentBuffer().reset();
      closedConnections.add(theConnection); 
    }
    else if (state == State.AWAITING_RESOLUTION) { 
      resolvingCount++;
    }
  }
    
  
  
  public static void main(String[] args) {

    // set the default ccbot user agent string 
    NIOHttpConnection.setDefaultUserAgentString("CCBot/1.0 (+http://www.commoncrawl.org/bot.html)");
    
    
    
    String usage = "Usage: --proxyIP <proxy ip address> --url <singleurl> --urls <urlsfile> --socketmax <max simulataneous sockets> --maxcount <max urls>";
    String urlFilePath = null;
    String singleURL = null;
    int socketMax = 100;
    int maxCount = -1;
    String proxyIPAddress = "38.103.63.52:80";
    ArrayList<String> urlList = new ArrayList<String>();
    
    if (args.length % 2 == 0) { 
      for (int i=0;i<args.length;i+=2) { 
        String argName = args[i];
        String value   = args[i+1];
  
        if (argName.equals("--urls")) { 
            
            urlFilePath = value;
          
            try { 
              
            System.out.println("Attempting to load URL List from Path:" + value);
            
            URL resourceURL = ClassLoader.getSystemResource(urlFilePath);
            
            InputStream stream = null;
            InputStream stream2 = null;
            
            long fileSize = -1;
            int  averageLineSize = -1;
            if (resourceURL != null) {
              stream = resourceURL.openStream();
              stream2 = resourceURL.openStream();
            }
            else { 
              File file = new File(urlFilePath);
              fileSize = file.length();
              stream = new FileInputStream(new File(urlFilePath));
              stream2 = new FileInputStream(new File(urlFilePath));
            }
            
            int urlCount = 0;
            
            try { 
            
              int lineCount = 0;
              
              if (fileSize != -1) { 
                
                int charCount = 0;
                
                BufferedReader reader1 = new BufferedReader(new InputStreamReader(stream));
                String line;
                while ((line = reader1.readLine()) != null) { 
                  charCount += line.length();
                  lineCount++;
                  if (lineCount == 100)
                    break;
                }
                averageLineSize = (charCount / lineCount);
                lineCount = (int)(fileSize / averageLineSize);
                System.out.println("Guessing Line Count To Be:" + lineCount);
              }
              else { 
                // figure out line count ... 
                BufferedReader reader1 = new BufferedReader(new InputStreamReader(stream));
                while (reader1.readLine() != null)
                  lineCount++;
              }
              
              System.out.println("There are:" + lineCount + " lines in the source file");
              int startOffset = 0;
              
              // figure out a random start offset if really large file ... 
              if (lineCount > maxCount) { 
                // figure out offset max 
                int offsetMax = lineCount - maxCount;
                // pick a random offset 
                startOffset = (int)(Math.random() * (double)offsetMax); 
              }
              System.out.println("Picked Random Start Offset as:" + startOffset + ". Seeking");
              
              
              BufferedReader reader2 = new BufferedReader(new InputStreamReader(stream2));
              
              if (fileSize != -1) { 
                stream2.skip(averageLineSize * startOffset);
                // skip potentially partial line
                reader2.readLine();
              }
              else { 
                int linePos = 0;
                // skip to offset ...
                while (linePos++ < startOffset) {
                  reader2.readLine();
                }
              }
                
              String line;
              while ((line = reader2.readLine()) != null) { 
                urlList.add("http://" + proxyIPAddress + "/proxy?renderAs=text&nocachenodice=1&url=" + UrlEncoded.encodeString(line));
                if (maxCount != -1 && ++urlCount == maxCount)
                  break;
              }
            }
            catch (IOException e) { 
              if (stream != null) { 
                stream.close();
              }
              if (stream2 != null) { 
                stream2.close();
              }
            }
            
            System.out.println("Loaded URLS from URL List. Count:"+urlCount);
          }
          catch(IOException e) { 
            System.out.println(e.toString());
            e.printStackTrace();
            System.exit(-1);
          }
          
        }
        else if (argName.equals("--url")) { 
          System.out.println("Adding Single URL to URL List:"+value);
          urlList.add("http://" + proxyIPAddress + "/proxy?renderAs=text&url=" + value);
        }
        else if (argName.equals("--socketmax")) { 
          socketMax = Math.max(1,Integer.parseInt(value));
        }
        else if (argName.equals("--maxcount")) { 
          maxCount = Math.max(1,Integer.parseInt(value));
        }
      }
    }
    
    if (urlList.size() == 0) { 
      System.out.println(usage);
      System.exit(-1);
    }
    else { 
      HammerProxyTest unitTest = new HammerProxyTest(urlList,socketMax,maxCount);
      unitTest.run();
      System.exit(0);
    }
  }


  public void HttpContentAvailable(NIOHttpConnection theConnection,NIOBufferList contentBuffer) {
    // NOOP
  }

}

