#!/bin/bash -aeu 

hadoop fs -ls s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments | cut -d" " -f 17- | sort > /tmp/valid.txt
hadoop fs -ls s3n://aws-publicdatasets/common-crawl/parse-output/segment        | cut -d" " -f 17- | sort > /tmp/all.txt
sed -i "s/valid_segments/segment/" /tmp/valid.txt

echo "* "
echo "* List of Invalid Segments"
echo "* "
diff -b -w /tmp/all.txt valid.txt | fgrep "segment" | sed "s/< /hadoop fs -rmr s3n:\/\/aws-publicdatasets/"

