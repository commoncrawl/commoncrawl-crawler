<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
	import="org.commoncrawl.crawl.pagerank.PageRankJobConfig"
	import="java.util.Vector"
%>
<%

  PageRankMaster server = (PageRankMaster) application.getAttribute("commoncrawl.server");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
	<STYLE type="text/css">
		@import "common.css";
	</STYLE>
	
	<title>Page Rank Master</title>
</head>
<body>
		<TABLE BORDER=1> 
			<!-- master status  -->
			<TR>
				<TD>Master Status
				<TD><IFRAME src="masterStatus.jsp" WIDTH=600 HEIGHT=200> </IFRAME></TD>
			</TR>
			<!-- slave status  -->
			<TR>
				<TD>Slave Status:
				<TD>
				<IFRAME src="slaveStatus.jsp" WIDTH=600 HEIGHT=600>
				</IFRAME>
			</TR>
		</TABLE>
	</body>
</html>
