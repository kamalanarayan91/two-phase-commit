/**
 *  Server: Starts the two phase commit operation.
 */
import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.io.*;


public class Server implements ProjectLib.CommitServing 
{

	public static final int COMMIT = 1;
	public static final int DECISION = -1;
	public static final int SERVERDECISION = -1;
	public static final int ACK = 3;
	
	public static AtomicInteger commitNumber = new AtomicInteger(1);
	public static ProjectLib PL;
	public static ConcurrentHashMap<Integer,Commit> commitMap = new ConcurrentHashMap<Integer,Commit>();

	public static Object SendLock = new Object();

	/**
	 * Start commit function. Called by a new thread for each commit.
	 */
	public void startCommit( String filename, byte[] img, String[] sources ) 
	{

		/*start two phase*/
		
		int currCommit = commitNumber.getAndIncrement();

		Commit commit = new Commit(filename,img,sources);
		
		commit.setCommitID(currCommit);

		commit.populateCommitInfo(true);

		commit.sendMessagesToUsers();

		commitMap.put(currCommit,commit);

		/*End- two phase*/

	}


	/**
	 * Serialization of the message Body to byte[] array
	 */
	public static byte[] serialize(Object obj) throws IOException
	{
    	ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    	ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
    	objectOutputStream.writeObject(obj);
    	return outputStream.toByteArray();
	}

	/**
	 * Deserialization of the message Body object
	 */
	public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException 
	{
    	ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    	ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
    	return objectInputStream.readObject();
	}
	
	/**
	 * main method for Server. Restores the server state based on log files
	 * and then starts receiving messages.
	 */
	public static void main ( String args[] ) throws Exception 
	{
		
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
			
		int count = 1;
		ServerStateRestorer.restore();

		// main loop
		while (true) 
		{

			ProjectLib.Message msg = PL.getMessage();
			
			MessageBody mb =(MessageBody)deserialize(msg.body);
			Commit commit = commitMap.get(mb.getCommitId());
			commit.handleUserMessage(msg);
		}

	}

}

