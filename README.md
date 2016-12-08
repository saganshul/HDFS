# HDFS

A distributed Java-based file system for storing large volumes of data.

# Installation

```bash
bash install.sh
```
> **Note**: You have to provide correct root password in order to install it properly. If you already have mysql server, provide your root password when asked to create database and table.

# How to use

> **Note**: Don't forget to put your network interface in global.properties. For example: eth0, enp7s0.

##### Launch nameNode

```bash
bash nameNode.sh
```

##### Launch dataNode

```bash
bash dataNode.sh
```

> **Note**: In order to start dataNode, you have to provide your nameNode IP address in global.properties

##### Launch client

```bash
bash client.sh
```

> **Note**: In order to start client, you have to provide your nameNode IP address in global.properties
