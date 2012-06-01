<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.commoncrawl.*"
  import="java.text.DateFormat"
  import="java.lang.Math"
  import="java.net.URLEncoder"
%>

<%
  CrawlerServer server = (CrawlerServer) application.getAttribute("commoncrawl.server");
%>

<html>
<head>
<meta HTTP-EQUIV="REFRESH" content="5"/>
<title>Crawler Server Detail for Crawler: <%= server.getHostName() %></title>
</head>
<body>
<table border=1> 
<tr><td><a href="showQueueDetail.jsp">Queue Details</a></td><td width=100%></td></tr>
</table>
<br>
<%
	server.dumpStats(out);	
%>	
</body>
</html>
