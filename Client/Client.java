package Client;

import java.util.Scanner;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.NotBoundException;
import java.io.IOException;
import java.io.*;
import com.google.protobuf.ByteString;

import NameNode.INameNode;
import ProtoBuf.HDFSProtoBuf.OpenFileResponse;
import ProtoBuf.HDFSProtoBuf.OpenFileRequest;

public class Client {
	private static Registry registry = null;
	private static INameNode nameNode = null;
	private static String host = null; // It should contain the address of Namenode
	
    private Client() {}

    public static void main(String[] args) throws NotBoundException, IOException {

        try {
            registry = LocateRegistry.getRegistry(host);
            nameNode = (INameNode) registry.lookup("NameNode");
            getFile("anshul");
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
        
        Scanner scan = new Scanner(System.in);
		boolean quit = false;
		while(true){
			System.out.print("~$>");
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
				break;
			case "list":
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
		int status = -1;

		OpenFileRequest.Builder request = OpenFileRequest.newBuilder();
		request.setFileName(fileName);
		request.setForRead(true);
		OpenFileRequest encoded_req = request.build();
		encoded_response = nameNode.openFile(encoded_req.toByteArray());
		response = OpenFileResponse.parseFrom(encoded_response);

		status = response.getStatus();
		System.out.println(status);
		if(status != 0) {
			System.out.println("Some error occurred");
			return;
		}
    }
}
