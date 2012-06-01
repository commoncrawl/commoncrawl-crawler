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
	<meta http-equiv="refresh" content="5" />
	<STYLE type="text/css">
		@import "common.css";
	</STYLE>
	<title>Page Rank Master - Slave Status</title>
</head>
<body>
<TABLE width=100% BORDER=1>
	<TR><TD>Status:<TD><%=server.getMasterState()%></TR>
	<!-- active job -->
	<TR>
		<TD>Active Job:
		<TD>
			<%
			PageRankJobConfig job = server.getActiveJobConfig();
			
			if (job != null) { 
			%>
			<B>JobName:</B>job-<%=job.getJobId()%><BR/>
			<B>Current Iteration:</B><%=job.getIterationNumber()%><BR/>
			<B>Max Iterations:</B><%=job.getMaxIterationNumber()%><BR/>
			<B>Slaves:</B><%=job.getSlaveCount()%><BR/>
			<B>Values Path:</B><%=job.getInputValuesPath()%><BR/>
			<B>Outlinks Path:</B><%=job.getOutlinksDataPath()%><BR/>
			<B>Job Path:</B><%=job.getJobWorkPath()%><BR/>
			<%
			}
			%>
	</TR>
</TABLE>
</BODY>