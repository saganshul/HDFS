package NameNode;

import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import ProtoBuf.HDFSProtoBuf.OpenFileResponse;
import ProtoBuf.HDFSProtoBuf.OpenFileRequest;
import ProtoBuf.HDFSProtoBuf.CloseFileResponse;
import ProtoBuf.HDFSProtoBuf.CloseFileRequest;
import ProtoBuf.HDFSProtoBuf.AssignBlockRequest;
import ProtoBuf.HDFSProtoBuf.AssignBlockResponse;
import ProtoBuf.HDFSProtoBuf.DataNodeLocation;
import ProtoBuf.HDFSProtoBuf.BlockLocations;
import ProtoBuf.HDFSProtoBuf.HeartBeatRequest;
import ProtoBuf.HDFSProtoBuf.HeartBeatResponse;
import ProtoBuf.HDFSProtoBuf.BlockReportRequest;
import ProtoBuf.HDFSProtoBuf.BlockReportResponse;
import ProtoBuf.HDFSProtoBuf.BlockLocationRequest;
import ProtoBuf.HDFSProtoBuf.BlockLocationResponse;
import ProtoBuf.HDFSProtoBuf.ListFilesRequest;
import ProtoBuf.HDFSProtoBuf.ListFilesResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Enumeration;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Random;

import com.google.protobuf.InvalidProtocolBufferException;

public class NameNode implements INameNode {

	private static int handle = 0;
	private static int blocknu = 0;
	private static HashMap<String, Integer> handler;
	private static HashMap<Integer, ArrayList<Integer>> handleToBlocks;
	private static HashSet<Integer> aliveDataNode;
	private static HashMap<Integer, DataNodeLocation> dataNodeMap;
	private static HashMap<Integer, ArrayList<Integer>> idtoBlockMap;
	private static HashMap<Integer, HashSet<DataNodeLocation>> blockToDataNodeMap;
	private static Lock lock;
	private static Lock blockAssignLock;
	private static final String blockIDDelimiter = ",";
	private static final String fileNameDelimiter = "--";
	private static File dataFile;
	private static int commitTimeout = 10000;

	public NameNode() throws IOException {
		handler = new HashMap<String, Integer>();
		handleToBlocks = new HashMap<Integer, ArrayList<Integer>>();
		aliveDataNode = new HashSet<Integer>();
		lock = new ReentrantLock();
		blockAssignLock = new ReentrantLock();
		dataNodeMap = new HashMap<Integer, DataNodeLocation>();
		idtoBlockMap = new HashMap<Integer, ArrayList<Integer>>();
		blockToDataNodeMap = new HashMap<Integer, HashSet<DataNodeLocation>>();
		dataFile = new File("nameNode.txt");

		try {
			dataFile.createNewFile();
		} catch (IOException e) {

			e.printStackTrace();
		}

    	if (dataFile == null) {
			System.out.println("Data file missing...");
			System.exit(-1);
		}

    	loadData();
	}

	private static void commitData() throws IOException {

		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(blocknu);
		stringBuilder.append(System.lineSeparator());
		for (String tempfileNameString : handler.keySet()) {
			stringBuilder.append(tempfileNameString);
			stringBuilder.append(fileNameDelimiter);
			String separator = "";
			for (Integer tempInteger : handleToBlocks.get(handler.get(tempfileNameString))) {
				stringBuilder.append(separator).append(Integer.toString(tempInteger));
				separator = blockIDDelimiter;
			}
			stringBuilder.append(System.lineSeparator());
		}

		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dataFile));
		bufferedWriter.write(stringBuilder.toString());
		bufferedWriter.close();
	}

	private static void loadData() throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(dataFile));

		try {
			String tempLine1 = bufferedReader.readLine();
			blocknu = Integer.parseInt(tempLine1);
		} catch(NumberFormatException ex){
			System.out.println("Sorry! You persistant file is empty.Can't load anything");
			bufferedReader.close();
			return ;
		}

		for (String tempLine; (tempLine = bufferedReader.readLine()) != null;) {
			Integer temphandle = new Integer(++handle);
			String[] splitStrings = tempLine.split(fileNameDelimiter, 2);
			String fileName = splitStrings[0];
			ArrayList<Integer> blockNumbers = new ArrayList<Integer>();
			if (!splitStrings[1].equals("")) {
				for (String tempString : splitStrings[1].split(blockIDDelimiter)) {
					blockNumbers.add(Integer.parseInt(tempString));
				}
			}
			handler.put(fileName, temphandle);
			handleToBlocks.put(temphandle, blockNumbers);
		}
		bufferedReader.close();
	}


	public byte[] openFile(byte[] message){
		OpenFileRequest request;
		int responseHandle = 0;
		String fileName = "";
		boolean forRead = false;
		OpenFileResponse.Builder response = OpenFileResponse.newBuilder();

		try {
			request = OpenFileRequest.parseFrom(message);
			fileName = request.getFileName();
			forRead = request.getForRead();
		} catch (Exception e){
			System.err.println("Error msg is : " + e.toString());
		}

		/**
		* These are the instruction for read and write
		* For Read
		* If file is present in HashMap output set status = 0 otherwise 1;
		* If file is present, response will contain its handle(Integer)
		* If file is present, response will contain corresponding blocks for DataNode
		* For write
		* If file is present send error for now - we are not handling updates
		* If file is not present, Add entry in HashMaps and send empty blocks in response
		*/

		if(forRead) {
			if (handler.containsKey(fileName)) {
				System.err.println("File: " + fileName + "Found");
				responseHandle = handler.get(fileName);
				ArrayList<Integer> blocks = handleToBlocks.get(responseHandle);
				response.addAllBlockNums(blocks);
				response.setStatus(0);
				response.setHandle(responseHandle);
			} else {
				System.err.println("File: " + fileName + " - Not Found");
				response.setStatus(1);
			}
		} else {

			if (handler.containsKey(fileName)) {
				response.setStatus(1);
				System.err.println("File: " + fileName + " - File already created. Cannot create again");
			} else {
				System.err.println("File: " + fileName + " - Writing File");
				lock.lock();
				responseHandle = ++handle;
				handler.put(fileName, handle);
				handleToBlocks.put(handle, new ArrayList<Integer>());
				lock.unlock();

				response.setHandle(responseHandle);
				response.setStatus(0);
			}

		}
		OpenFileResponse encoded_response = response.build();
		return encoded_response.toByteArray();
	}

	public byte[] assignBlock(byte[] message){

		int Handle = 0;
		Random rand = new Random();
		AssignBlockResponse.Builder tempResponse = AssignBlockResponse.newBuilder();

		/**
		* These function is responsible for assigning blocks.
		* Block will be unique.
		* Locking will be used to assign ( to maintain uniqueness)
		* In response we will send status and BlockLocation which will contain block no. assigned and array of DataNode.
		* Array of DataNode is present here to maintain replication of Blocks.
		*/

		try {
			Handle = AssignBlockRequest.parseFrom(message).getHandle();
		} catch (InvalidProtocolBufferException e) {
			System.err.println("AssignBlock: Err in getting file handle");
			tempResponse.setStatus(1);
			return tempResponse.build().toByteArray();
		}

		/* Checking if DataNodes are available or not */
		if (aliveDataNode.isEmpty()){
			handler.values().remove(Handle);
			System.err.println("Sorry! No DataNode are alive. Cannot complete your request.");
			tempResponse.setStatus(1);
			return tempResponse.build().toByteArray();
		}

		blockAssignLock.lock();
		int blockNuToAssign = ++blocknu;
		blockAssignLock.unlock();

		handleToBlocks.get(Handle).add(blockNuToAssign);
		if (aliveDataNode.isEmpty()){
			System.err.println("Sorry! No DataNode are alive. Cannot complete your request.");
			tempResponse.setStatus(1);
			return tempResponse.build().toByteArray();
		}

		ArrayList<DataNodeLocation> dataNodeLocations =  new ArrayList<DataNodeLocation>();
		int  dnid1 = rand.nextInt((aliveDataNode.size()));
		if (aliveDataNode.size() > 1) {
			int  dnid2 = rand.nextInt((aliveDataNode.size()));
			while(dnid1 == dnid2)
			{
				dnid2 = rand.nextInt((aliveDataNode.size()));
			}
			int dn2 = (int) aliveDataNode.toArray()[dnid2];
			dataNodeLocations.add(dataNodeMap.get(dn2));
		}
		int dn1 = (int) aliveDataNode.toArray()[dnid1];
		dataNodeLocations.add(dataNodeMap.get(dn1));
		BlockLocations.Builder tempBlockLocations = BlockLocations.newBuilder();
		tempBlockLocations.setBlockNumber(blockNuToAssign);
		tempBlockLocations.addAllLocations(dataNodeLocations);

		tempResponse = AssignBlockResponse.newBuilder();
		tempResponse.setNewBlock(tempBlockLocations);
		tempResponse.setStatus(0);
		return tempResponse.build().toByteArray();
	}

    public static void main(String args[]) throws IOException {

        try {
            NameNode obj = new NameNode();
            INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("NameNode", stub);

            System.err.println("NameNode ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

    public byte[] heartBeat(byte[] message) {

    	Integer dataNodeId = -1;

		try {
			dataNodeId = HeartBeatRequest.parseFrom(message).getId();
			if (aliveDataNode.contains(dataNodeId)){
				System.out.println("DataNode : " + dataNodeId.toString() + " Is beating");
			} else {
				System.out.println("New DataNode Found... Awaiting BlockReport...");
			}
			return HeartBeatResponse.newBuilder().setStatus(0).build().toByteArray();
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
			return HeartBeatResponse.newBuilder().setStatus(1).build().toByteArray();
		}
	}

	public byte[] blockReport(byte[] message) {

		BlockReportRequest blockRequest = null;
		Integer dataNodeId = -1;
		DataNodeLocation dataNodeLocation = null;
		ArrayList<Integer> blockList = new ArrayList<Integer>();

		try {
			blockRequest = BlockReportRequest.parseFrom(message);
			dataNodeId = blockRequest.getId();
			dataNodeLocation = blockRequest.getLocation();
			if (blockRequest.getBlockNumbersCount() != 0) {
				blockList = new ArrayList<Integer>(blockRequest.getBlockNumbersList());
				for (Integer tempBlockID : blockRequest.getBlockNumbersList()) {
					if (blockToDataNodeMap.get(tempBlockID) == null) {
						blockToDataNodeMap.put(tempBlockID, new HashSet<DataNodeLocation>());
					}
					blockToDataNodeMap.get(tempBlockID).add(dataNodeLocation);
				}
			}
			aliveDataNode.add(dataNodeId);
			dataNodeMap.put(dataNodeId, dataNodeLocation);
			idtoBlockMap.put(dataNodeId, blockList);
			BlockReportResponse.Builder blockReportResonse = BlockReportResponse.newBuilder();
			for (Integer temporaryIndex = 0; temporaryIndex < blockRequest.getBlockNumbersCount(); temporaryIndex++) {
				blockReportResonse.addStatus(0);
			}
			return blockReportResonse.build().toByteArray();

		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			return BlockReportResponse.newBuilder().addStatus(0).build().toByteArray();
		}
	}

	public byte[] blockLocations(byte[] message) {
		BlockLocationRequest request = null;

		BlockLocationResponse.Builder encoded_response = BlockLocationResponse.newBuilder();

		try {
			request = BlockLocationRequest.parseFrom(message);
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
		}

		List<Integer> blocks = request.getBlockNumsList();
		for (Integer block : blocks) {
			BlockLocations.Builder temp = BlockLocations.newBuilder();
			temp.addAllLocations(blockToDataNodeMap.get(block));
			temp.setBlockNumber(block);
			encoded_response.addBlockLocations(temp);
		}

		encoded_response.setStatus(0);
		BlockLocationResponse finalResponse = encoded_response.build();
		return finalResponse.toByteArray();
	}

	public byte[] list(byte[] message) {
		return ListFilesResponse.newBuilder().setStatus(0).addAllFileNames(handler.keySet()).build().toByteArray();
	}

	public byte[] closeFile(byte[] message) {
		try {
			CloseFileRequest closeFileRequest = CloseFileRequest.parseFrom(message);
			if (handleToBlocks.get(closeFileRequest.getHandle()) != null) {
				commitData();
				return CloseFileResponse.newBuilder().setStatus(0).build().toByteArray();
			} else {
				return CloseFileResponse.newBuilder().setStatus(1).build().toByteArray();
			}
		} catch (IOException e) {
			e.printStackTrace();
			return CloseFileResponse.newBuilder().setStatus(1).build().toByteArray();
		}
	}


}
