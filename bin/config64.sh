#!/bin/bash
set -e

if [ $# -ne 2 ]; then
	echo "need accesskey and secret!"
	exit 1;
fi
sudo mkdir /usr/local/cuda
sudo mkdir /usr/local/cuda/lib64
cd /home/hadoop
curl -o /home/hadoop/mozilla_64.tgz http://s3.amazonaws.com/commoncrawl-public/mozilla_64.tgz
cd /usr/local/cuda/lib64
sudo tar -xzf ~/mozilla_64.tgz
#sudo mkdir /usr/local/lib
cd /home/hadoop
curl -o /home/hadoop/local_lib64.tgz http://s3.amazonaws.com/commoncrawl-public/local_lib64.tgz
cd /usr/local/lib
sudo tar -xzf /home/hadoop/local_lib64.tgz
sudo ldconfig
#sudo apt-get install s3cmd
cd /home/hadoop/
wget http://s3.amazonaws.com/commoncrawl-public/ccJNIs64.tgz
cd /home/hadoop/native/Linux-amd64-64/
tar -xzf /home/hadoop/ccJNIs64.tgz
cd /home/hadoop/
echo "access_key = $1" >> /home/hadoop/.s3cfg
echo "secret_key = $2" >> /home/hadoop/.s3cfg
s3cmd get s3://commoncrawl-public/ccprod.tgz
mkdir /home/hadoop/ccprod
cd /home/hadoop/ccprod 
tar -xzf /home/hadoop/ccprod.tgz
cd /home/hadoop/ccprod/lib
rm commoncrawl-0.1.jar
s3cmd get s3://commoncrawl-public/commoncrawl-0.1.jar
cd /home/hadoop/ccprod/conf
cp * /home/hadoop/conf
cd /home/hadoop
s3cmd get s3://commoncrawl-public/gson-2.1.jar
rm /home/hadoop/lib/gson*.jar
cp /home/hadoop/gson-2.1.jar /home/hadoop/lib
# create swap partition
if [ -e /mnt3 ]; then
  swapDrive=/mnt3
else
  swapDrive=/mnt1
fi

# turn swap on  
sudo dd if=/dev/zero of=$swapDrive/swapfile bs=1M count=8092
sudo chmod 600 $swapDrive/swapfile
sudo mkswap $swapDrive/swapfile
echo $swapDrive/swapfile none swap defaults 0 0 | sudo tee -a /etc/fstab
sudo swapon -a
sudo bash -c "echo -e '\nhadoop -       nofile          200000' >> /etc/security/limits.conf"
sudo bash -c "echo -e '\nfs.file-max=200000' >> /etc/sysctl.conf"

