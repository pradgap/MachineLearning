#!/bin/sh

# Steps to install Spark on a VM
 
# installation of Oracle Java JDK.
sudo apt-get -y update
sudo apt-get -y install python-software-properties
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get -y update
sudo apt-get -y install oracle-java7-installer

# Installation of commonly used python scipy tools
sudo apt-get -y install python-numpy python-scipy python-matplotlib ipython ipython-notebook python-pandas python-sympy python-nose

# Download PreBuilt Spark and extract
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz
tar -xzvf spark-2.1.0-bin-hadoop2.7.tgz

# Run spark-shell
cd spark-2.1.0-bin-hadoop2.7/
./bin/spark-shell

#############################################################################################################
# Alternatively You can Build the spark 
# Installation of scala
wget http://www.scala-lang.org/files/archive/scala-2.11.1.deb
sudo dpkg -i scala-2.11.1.deb
sudo apt-get -y update
sudo apt-get -y install scala
# Installation of sbt
wget http://scalasbt.artifactoryonline.com/scalasbt/sbt-native-packages/org/scala-sbt/sbt//0.12.3/sbt.deb
sudo dpkg -i sbt.deb
sudo apt-get -y update
sudo apt-get -y install sbt
# Downloading spark
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.0.0.tgz
tar -zxf spark-1.0.0.tgz
cd spark-1.0.0
# Building spark
./sbt/sbt assembly
# Clean-up
rm scala-2.11.1.deb
rm sbt.deb
rm spark-1.0.0.tgz
rm install.sh

