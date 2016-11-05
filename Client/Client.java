package Client;

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

    public static void main(String[] args) {

        try {
            registry = LocateRegistry.getRegistry(host);
            nameNode = (INameNode) registry.lookup("NameNode");
            getFile("anshul");
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
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
