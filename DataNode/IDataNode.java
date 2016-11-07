package DataNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDataNode extends Remote {
	byte[] writeBlock(byte[] message) throws RemoteException;
	byte[] readBlock(byte[] message) throws RemoteException;
}
