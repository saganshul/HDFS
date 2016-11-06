package DataNode;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import ProtoBuf.HDFSProtoBuf.WriteBlockResponse;
import ProtoBuf.HDFSProtoBuf.WriteBlockRequest;
import ProtoBuf.HDFSProtoBuf.HeartBeatRequest;
import ProtoBuf.HDFSProtoBuf.HeartBeatResponse;
import ProtoBuf.HDFSProtoBuf.BlockReportRequest;
import ProtoBuf.HDFSProtoBuf.BlockReportResponse;
import ProtoBuf.HDFSProtoBuf.DataNodeLocation;

import NameNode.INameNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;

public class DataNode implements IDataNode {
	
	private static Integer dataNodeId;
	private static Statement stmt;
	private static Connection con;
	private static Integer heartBeatTimeout;
	private static Integer blockReportTimeout;
	private static String myIp;
	private static Integer myPort;
	
	public DataNode(){
		heartBeatTimeout = 1000;
		blockReportTimeout = 1000;
		myIp = "0.0.0.0";
		myPort = 1099;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/hdfs","root","njsirisgod");
			stmt = con.createStatement();
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) {
		
		if (args.length != 1) {
			System.err.println("USAGE: java DataNode.DataNode <serverID>");
			System.exit(-1);
		}

		dataNodeId = Integer.parseInt(args[0]);
		
        try {
            DataNode obj = new DataNode();
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("DataNode", stub);

            System.err.println("DataNode ready");
        } catch (Exception e) {
            System.err.println("DataNode exception: " + e.toString());
            e.printStackTrace();
        }
        
        new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					HeartBeatRequest.Builder heartBeatRequest = HeartBeatRequest.newBuilder();
					heartBeatRequest.setId(dataNodeId);
					INameNode nameNode = null;
					try {
						nameNode = (INameNode) LocateRegistry.getRegistry().lookup("NameNode");
					} catch (RemoteException | NotBoundException e) {
						e.printStackTrace();
					}
					byte[] serializedHeartBeatResponse = null;
					try {
						serializedHeartBeatResponse = nameNode.heartBeat(heartBeatRequest.build().toByteArray());
					} catch (RemoteException e) {
						e.printStackTrace();
					}
					HeartBeatResponse heartBeatResponse = null;
					try {
						heartBeatResponse = HeartBeatResponse.parseFrom(serializedHeartBeatResponse);
					} catch (InvalidProtocolBufferException e) {
						e.printStackTrace();
					}
					Integer heartBeatStatus = heartBeatResponse.getStatus();
					if (heartBeatStatus != 1 ) {
						System.out.println("Heart Beating...");
					} else {
						System.err.println("Heart not beating properly...");
						System.exit(-1);
					}
					try {
						Thread.sleep(heartBeatTimeout);
					} catch (InterruptedException e) {
						// nope
					}
				}
			}
		}).start();
        
        new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					INameNode nameNode = null;
					try {
						nameNode = (INameNode) LocateRegistry.getRegistry().lookup("NameNode");
					} catch (RemoteException | NotBoundException e) {
						e.printStackTrace();
					}
					
					BlockReportRequest.Builder blockReport = BlockReportRequest.newBuilder();
					ResultSet res = null;
					ArrayList<Integer> blockNumbers = new ArrayList<Integer>();
					DataNodeLocation.Builder dataNodeLocation = DataNodeLocation.newBuilder();
					dataNodeLocation.setIp(myIp);
					dataNodeLocation.setPort(myPort);
					
					blockReport.setId(dataNodeId);
					blockReport.setLocation(dataNodeLocation);
					
					try {
						res = stmt.executeQuery("select blocknum from datablock");
						while(res.next()) {
							blockNumbers.add(res.getInt(1));
						}
					} catch (SQLException e) {

						e.printStackTrace();
					}
					blockReport.addAllBlockNumbers(blockNumbers);
					byte[] serializedBlockReportResponse = null;
					try {
						serializedBlockReportResponse = nameNode.blockReport(blockReport.build().toByteArray());
					} catch (RemoteException e) {
						e.printStackTrace();
					}
					BlockReportResponse blockReportResponse = null;
					try {
						blockReportResponse = BlockReportResponse.parseFrom(serializedBlockReportResponse);
					} catch (InvalidProtocolBufferException e) {
						e.printStackTrace();
					}
					for (Integer tempStatus : blockReportResponse.getStatusList()) {
						if (tempStatus == 1) {
							System.err.println("Error in making Block Request Report");
							System.exit(-1);
						}
					}
					
					try {
						Thread.sleep(blockReportTimeout);
					} catch (InterruptedException e) {
						// nope
					}
				}
			}
		}).start();
    }

	public byte[] writeBlock(byte[] message) {
		WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
		try {
			stmt.executeUpdate("create database if not exists hdfs");
			stmt.execute("use hdfs");
			stmt.executeUpdate("create table if not exists datablock(blocknum int,data longtext,primary key(blocknum))");
			WriteBlockRequest writeBlockRequest;
			writeBlockRequest = WriteBlockRequest.parseFrom(message);
			String data = new String(ByteString.copyFrom(writeBlockRequest.getDataList()).toByteArray());
			PreparedStatement pstmt = con.prepareStatement("insert into datablock(blocknum,data) values(?,?)");
			pstmt.setInt(1, writeBlockRequest.getBlockInfo().getBlockNumber());
			pstmt.setString(2, data);
			pstmt.executeUpdate();
			response.setStatus(0);
		} catch (SQLException e) {
			response.setStatus(1);
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			response.setStatus(1);
			e.printStackTrace();
		}

		WriteBlockResponse finalRes=response.build();
		return finalRes.toByteArray();
	}
}
