<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
	import="org.commoncrawl.service.pagerank.PageRankJobConfig"
	import="java.util.Vector"
%>
<%

  PageRankMaster server = (PageRankMaster) application.getAttribute("commoncrawl.server");
  
  String valuePath = request.getParameter("input_values");
  String graphPath = request.getParameter("input_graph");
  String iterations = request.getParameter("iterations");
  String slaveCount = request.getParameter("slaveCount");

	server.createNewJob(valuePath,graphPath,Integer.parseInt(iterations),Integer.parseInt(slaveCount));  

  response.sendRedirect("index.jsp");  
  
%>
