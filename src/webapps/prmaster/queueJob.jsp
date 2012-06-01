<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
	import="org.commoncrawl.crawl.pagerank.PageRankJobConfig"
	import="java.util.Vector"
%>
<%
  PageRankMaster server = (PageRankMaster) application.getAttribute("commoncrawl.server");

	
	response.sendRedirect("index.jsp");  

%>
  
 