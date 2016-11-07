package NameNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INameNode extends Remote {
	byte[] openFile(byte[] message) throws RemoteException;
	byte[] assignBlock(byte[] message) throws RemoteException;
	byte[] heartBeat(byte[] message) throws RemoteException;
	byte[] blockReport(byte[] message) throws RemoteException;
	byte[] blockLocations(byte[] message) throws RemoteException;
	byte[] list(byte[] serializedListFilesRequest) throws RemoteException;
}
