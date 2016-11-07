package Client;

import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.NotBoundException;
import java.io.IOException;
import java.io.*;

import com.google.protobuf.ByteString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import NameNode.INameNode;
import DataNode.IDataNode;
import ProtoBuf.HDFSProtoBuf.OpenFileResponse;
import ProtoBuf.HDFSProtoBuf.OpenFileRequest;
import ProtoBuf.HDFSProtoBuf.AssignBlockResponse;
import ProtoBuf.HDFSProtoBuf.AssignBlockRequest;
import ProtoBuf.HDFSProtoBuf.DataNodeLocation;
import ProtoBuf.HDFSProtoBuf.WriteBlockRequest;
import ProtoBuf.HDFSProtoBuf.WriteBlockResponse;
import ProtoBuf.HDFSProtoBuf.ReadBlockRequest;
import ProtoBuf.HDFSProtoBuf.ReadBlockResponse;
import ProtoBuf.HDFSProtoBuf.BlockLocations;
import ProtoBuf.HDFSProtoBuf.BlockLocationRequest;
import ProtoBuf.HDFSProtoBuf.BlockLocationResponse;
import ProtoBuf.HDFSProtoBuf.ListFilesRequest;
import ProtoBuf.HDFSProtoBuf.ListFilesResponse;

public class Client {
	private static Registry registry = null;
	private static INameNode nameNode = null;
	private static String host = null; // It should contain the address of Namenode
	private static Integer blockSize = 3200000;
	
    private Client() {}

    public static void main(String[] args) throws NotBoundException, IOException {

        try {
            registry = LocateRegistry.getRegistry(host);
            nameNode = (INameNode) registry.lookup("NameNode");            
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
        
        Scanner scan = new Scanner(System.in);
		boolean quit = false;
		while(true){
			System.out.print("HDFS->");
			String input = scan.nextLine();
			String[] inputArray = input.split(" ");
			if(inputArray.length < 1){
				System.out.println("Please provide command");
				continue;
			}

			switch (inputArray[0]){
			case "get":
				if(inputArray.length <= 1 ) {
					quit = true;
					System.err.println("No Filename given");
					break;
				}
				getFile(inputArray[1]);
				break;
			case "put":
				if(inputArray.length <= 1 ) {
					quit = true;
					System.err.println("No Filename given");
					break;
				}
				putFile(inputArray[1]);
				break;
			case "list":
				listFile();
				break;
			case "quit":
				System.out.println("Going to quit :)");
				quit = true;
				break;
			default :
				System.out.println("Undefined command");
				break;
			}
			if(quit) break;
		}
    }
    
    public static void getFile(String fileName) throws NotBoundException, IOException {
		int handle = 0;
		byte[] encoded_response = null;
		OpenFileResponse response = null;
		byte[] encoded_locationResponse = null;
		BlockLocationResponse locationResponse = null;
		byte [] encoded_readBlockResponse = null;
		IDataNode dn = null;
		int status = -1;
		int locationStatus = -1;
		
		OpenFileRequest.Builder request = OpenFileRequest.newBuilder();
		request.setFileName(fileName);
		request.setForRead(true);
		OpenFileRequest encoded_req = request.build();
		encoded_response = nameNode.openFile(encoded_req.toByteArray());
		response = OpenFileResponse.parseFrom(encoded_response);

		status = response.getStatus();
		if(status != 0) {
			System.out.println("Some error occurred");
			return;
		}
		
		List<Integer> blocks = response.getBlockNumsList();
		handle = response.getHandle();
		BlockLocationRequest.Builder locationRequest = BlockLocationRequest.newBuilder();
		locationRequest.addAllBlockNums(blocks);
		BlockLocationRequest encoded_locationRequest = locationRequest.build();
		encoded_locationResponse = nameNode.blockLocations(encoded_locationRequest.toByteArray());
		
		locationResponse = BlockLocationResponse.parseFrom(encoded_locationResponse);

		locationStatus = locationResponse.getStatus();
		if(locationStatus != 0) {
			System.out.println("Some error occurred");
			return;
		}
		
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

		for (BlockLocations tempBlockLocations : locationResponse.getBlockLocationsList()) {
			ReadBlockResponse readBlockResponse = null;
			for (DataNodeLocation location : tempBlockLocations.getLocationsList()) {
				ReadBlockRequest.Builder readBlockRequest = ReadBlockRequest.newBuilder();
				readBlockRequest.setBlockNumber(tempBlockLocations.getBlockNumber());
				dn = (IDataNode) LocateRegistry.getRegistry(location.getIp(), location.getPort()).lookup("DataNode");
				encoded_readBlockResponse = dn.readBlock(readBlockRequest.build().toByteArray());
				
				readBlockResponse = ReadBlockResponse.parseFrom(encoded_readBlockResponse);
				if (readBlockResponse.getStatus() != 0) {
					System.err.println("Error in ReadBlockRequest... Trying next...");
					continue;
				} else {
					break;
				}
			}
			if (readBlockResponse.getStatus() != 0) {
				System.err.println("No more DataNodes... Failing...");
				return;
			}
			byteArrayOutputStream.write(ByteString.copyFrom(readBlockResponse.getDataList()).toByteArray());
		}

		Files.write(Paths.get(fileName), byteArrayOutputStream.toByteArray());
    }
    
    public static void putFile(String fileName) throws NotBoundException, IOException {
    	int handle = 0;
		byte[] encoded_response = null;
		OpenFileResponse response = null;
		int status = -1;
		
		int bytesRead = 0;
		InputStream fStream = new FileInputStream(fileName);
		byte[] fileChunk = new byte[blockSize];
		
		Path path = Paths.get(fileName);

		if(!Files.isReadable(path)) {
			System.err.println("Err: File not readable");
			return;
		}
		if(!Files.exists(path)) {
			System.err.println("Err: File doesn't exist");
			return;
		}
		if(Files.isDirectory(path)) {
			System.err.println("Err: It is a directory");
		}
		
		OpenFileRequest.Builder request = OpenFileRequest.newBuilder();
		request.setFileName(fileName);
		request.setForRead(false);
		OpenFileRequest encoded_req = request.build();
		encoded_response = nameNode.openFile(encoded_req.toByteArray());
		response = OpenFileResponse.parseFrom(encoded_response);

		status = response.getStatus();
		handle = response.getHandle();
		
		if(status != 0) {
			System.err.println("Some error occurred");
			return;
		}
		
		InputStream inputStream = Files.newInputStream(path);
		byte[] byteBuffer = new byte[blockSize];

		while( (bytesRead = fStream.read(fileChunk)) != -1) {
			AssignBlockRequest.Builder assignBlockRequest = AssignBlockRequest.newBuilder();
			assignBlockRequest.setHandle(handle);
			byte[] assignBlockResponse = nameNode.assignBlock(assignBlockRequest.build().toByteArray());

			if (AssignBlockResponse.parseFrom(assignBlockResponse).getStatus() != 0) {
				System.err.println("Err occurred");
				return;
			}

			BlockLocations blockLocations = AssignBlockResponse.parseFrom(assignBlockResponse).getNewBlock();
			ArrayList<DataNodeLocation> locationsToReplicate = new ArrayList<DataNodeLocation> (blockLocations.getLocationsList());
			WriteBlockRequest.Builder writeBlockRequest = WriteBlockRequest.newBuilder();
			writeBlockRequest.setBlockInfo(blockLocations);
			writeBlockRequest.addData(ByteString.copyFrom(bytesRead == blockSize ? fileChunk : Arrays.copyOf(fileChunk, bytesRead))); // Check the case when fileChunk is not full
			boolean gotDataNode = false;
			IDataNode dn = null;

			for (DataNodeLocation tempLocation : locationsToReplicate) {

				try {
					dn = (IDataNode) LocateRegistry.getRegistry(tempLocation.getIp(), tempLocation.getPort()).lookup("DataNode");
					gotDataNode = true;
				} catch (Exception e) {
					continue;
				}

				byte[] writeBlockResponse = dn.writeBlock(writeBlockRequest.build().toByteArray());
				if (WriteBlockResponse.parseFrom(writeBlockResponse).getStatus() != 0) {
					System.err.println("Err occurred");
					gotDataNode = false;
					continue;
				} else {
					break;
				}
			}

			if(!gotDataNode) {
				System.err.println("Some err occurred :(");
				return;
			}
		}

		fStream.close();
    }
    
    public static void listFile() throws NotBoundException, IOException {
    	
    	byte[] encodedListResponse = null;
    	ListFilesResponse listResponse = null;
    	
    	INameNode dn = null;
    	ListFilesRequest.Builder request = ListFilesRequest.newBuilder();
    	request.setDirName("HDFS");
    	
    	encodedListResponse = nameNode.list(request.build().toByteArray());
    	
    	listResponse = ListFilesResponse.parseFrom(encodedListResponse);
    	
    	int status = listResponse.getStatus();
		
		if(status != 0) {
			System.err.println("Some error occurred");
			return;
		}
		
    	for (String fileName : listResponse.getFileNamesList()) {
    		System.out.println(fileName);
		}
    }
    
    
}
