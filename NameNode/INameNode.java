package NameNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INameNode extends Remote {
	byte[] openFile(byte[] message) throws RemoteException;
}
