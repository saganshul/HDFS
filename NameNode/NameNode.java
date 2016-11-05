package NameNode;

import java.io.ByteArrayOutputStream;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import ProtoBuf.HDFSProtoBuf.OpenFileResponse;
import ProtoBuf.HDFSProtoBuf.OpenFileRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NameNode implements INameNode {
	
	private static int handle = 0;
	private static HashMap<String, Integer> handler;
	private static HashMap<Integer, ArrayList<Integer>> handleToBlocks;
	private static Lock lock;
	
	public NameNode() {
		handler = new HashMap<String, Integer>();
		handleToBlocks = new HashMap<Integer, ArrayList<Integer>>();
		lock = new ReentrantLock();
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
        
    public static void main(String args[]) {
        
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
}
