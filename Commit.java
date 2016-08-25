/**
 * Commit : Contains all the information for a particular commit.
 */
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
public class Commit
{
	public static final int timeout = 6500; 

	private static final int IRRELEVENT = -1;
	private static final int COMMIT = 1;
	private static final int DECISION = 2;
	private static final int SERVERDECISION = 3;
	private static final int ACK = 4;

	private static final String LOG_PREPARE = "PREPARE";
	private static final String LOG_ACK = "ACK";
	private static final String LOG_USERDECISION = "USERDECISION";
	private static final String LOG_SERVERDECISION = "SERVERDECISION";
	private static final String LOG_WRITTEN = "WRITTEN";

	private String collageName;
	private byte[] collageBytes;
	private String[] sources;


	private ArrayList<String> userNodeList = new ArrayList<String>();
	private ArrayList<Boolean> userDecisionList = new ArrayList<Boolean>();

	private ConcurrentHashMap<String,ArrayList<String>> userFileListHash = new ConcurrentHashMap<String,ArrayList<String>>();
	private ConcurrentHashMap<String,Boolean> userDecisionMap = new ConcurrentHashMap<String,Boolean>();
	private ConcurrentHashMap<String,Boolean> userAckMap = new ConcurrentHashMap<String,Boolean>();

	private int commitID;
	private int numberOfFiles;

	private AtomicInteger pendingDecisions;
	private AtomicInteger pendingAcks;

	private String fileName;
	private BufferedWriter bufferedWriter;

	Commit(String collageName, byte[] img, String[] sources)
	{

		this.collageName = collageName;
		this.collageBytes = img;
		this.sources = sources;

	}

	/* A few getters and setters*/
	public void setCommitID(int commitId)
	{
		this.commitID = commitId;
	}

	public int getNumUsers()
	{
		return userNodeList.size();
	}
	/* End - A few getters and setters*/

	/**
	 * Creates and writes to the commit log file.
	 * @param line 
	 */
	public void writeToFile(String line)
	{
		try
		{
			fileName = commitID+".log";
			File file = new File(fileName);
			FileOutputStream fileOut = new FileOutputStream(file);
			bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOut));
			bufferedWriter.write(line);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			Server.PL.fsync();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Appends a line to the log file
	 * @param line 
	 */

	public void appendLineToFile(String line)
	{
		try
		{
			bufferedWriter.write(line);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			Server.PL.fsync();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Writes a backup copy of the collage under a different name. So that
	 * when the server is killed, the byte array can be obtained from this file
	 */
	public void writeBackUpCollage()
	{
		try
		{
			String collageName = "Server_"+commitID; 
			FileOutputStream fos = new FileOutputStream(collageName);
			fos.write(collageBytes);
			fos.close();
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

	/**
	 * Effectively intializes everything required for the commit. 
	 * @param firstTime - this variable is used inorder to either create a new
	 * file or to append to an existing log file.
	 */
	public void populateCommitInfo(boolean firstTime)
	{
		StringBuilder sourcesStringBuilder = new StringBuilder();
		
		for( String src: sources)
		{
		

			String[] splits = src.split(":");
			if(userNodeList.contains(splits[0])==false)
			{
				userNodeList.add(splits[0]);
				ArrayList<String> fileList = new ArrayList<String>();
				fileList.add(splits[1]);
				userFileListHash.put(splits[0],fileList);
			}
			else
			{
				ArrayList<String> fileList = userFileListHash.get(splits[0]);
				fileList.add(splits[1]);
				userFileListHash.put(splits[0],fileList);
			}

			sourcesStringBuilder.append(src);
			sourcesStringBuilder.append(",");

		}

		String sourcesString = sourcesStringBuilder.toString();
		sourcesString = sourcesString.substring(0,sourcesString.length()-1);
		pendingDecisions = new AtomicInteger(userNodeList.size());
		pendingAcks = new AtomicInteger(userNodeList.size());

		
		if(firstTime==true)
		{
			writeToFile(collageName+'\t'+sourcesString);
			writeBackUpCollage();
		}
		else
		{
			try
			{
				fileName = commitID+".log";
				FileWriter file = new FileWriter(fileName,true);
				bufferedWriter = new BufferedWriter(file);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}

	}


	/**
	 * Serialization of the message Body to byte[] array
	 * @param  obj         
	 * @return             byte array
	 * @throws IOException 
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
	 * @param  data                   byteArray Data.
	 * @return                        Message Body object
	 */
	public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException 
	{
    	ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    	ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
    	return objectInputStream.readObject();
	}
	
	
	/**
	 * Sends PREPARE message to all the userNodes
	 */
	public void sendMessagesToUsers()
	{
		for(String userNode: userNodeList)
		{

			MessageBody messageBody = new MessageBody();
			// message body 
			messageBody.setMessageType(COMMIT);
			// commit ID
			messageBody.setCommitId(commitID);
			// collage filename
			messageBody.setFilename(collageName);
			// collageBytes
			messageBody.setCollageBytes(collageBytes);
			//files
			ArrayList<String> fileList = userFileListHash.get(userNode);
			String[] files = new String[fileList.size()];
			files = fileList.toArray(files);
			messageBody.setSources(files);

			try
			{

				ProjectLib.Message msg = new ProjectLib.Message(userNode,serialize(messageBody));
				String line = LOG_PREPARE+'\t'+ msg.addr; 
				appendLineToFile(line);
				Server.PL.sendMessage(msg);
				Thread t = new Thread(new VoteTimeOutThread(userNode));
				t.start();
				
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}


	}
	
	/**
	 * Take action based on type of user message
	 */
	public void handleUserMessage(ProjectLib.Message msg)
	{
		try
		{


			MessageBody messageBody = (MessageBody)deserialize(msg.body);

			if(messageBody.getMessageType() == DECISION)
			{
				String line = LOG_USERDECISION+'\t'+
						 msg.addr+'\t'+Boolean.toString(messageBody.getUserDecision()).toUpperCase(); 
				appendLineToFile(line);

				if(userDecisionMap.get(msg.addr) == null)
				{
					userDecisionList.add(messageBody.getUserDecision());

					userDecisionMap.put(msg.addr,messageBody.getUserDecision());
					
					int pending = pendingDecisions.decrementAndGet();

					if(pending == 0)
					{
						takeCommitDecision();
					}
				}
				else
				{

					//ignore it.

				}			

			}

			if(messageBody.getMessageType() == ACK)
			{
				String line = LOG_ACK+'\t'+ msg.addr;
				appendLineToFile(line);

				userAckMap.put(msg.addr,true);
				pendingAcks.decrementAndGet();
				
				
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

	/**
	 * Determine what to do for the commit. effectively the final decision is made through
	 * this method.
	 */
	public void takeCommitDecision()
	{
		
		boolean decision = false;
		if(!userDecisionList.contains(false))
		{
			
			writeCollage();
			decision = true;
		}

		sendServerDecision(decision);


	}


	/**
	 * Send ServerDecision : Sends the final decision of the commit to all the nodes.
	 */
	public void sendServerDecision(boolean decision)
	{
		for(String userNode:userNodeList)
		{
			MessageBody messageBody = new MessageBody();
			messageBody.setMessageType(SERVERDECISION);
			messageBody.setCommitId(commitID);
			messageBody.setServerDecision(decision);
			ArrayList<String> fileList = userFileListHash.get(userNode);
			String[] files = new String[fileList.size()];
			files = fileList.toArray(files);
			messageBody.setSources(files);
			try
			{

				ProjectLib.Message msg = new ProjectLib.Message(userNode,serialize(messageBody));
				String line = LOG_SERVERDECISION+'\t'+Boolean.toString(decision).toUpperCase(); 
				appendLineToFile(line);
				Server.PL.sendMessage(msg);
				Thread t = new Thread(new DistributeDecisionTimeOutThread (msg));
				t.start();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}



	/**
	 * writeCollage: writes the collage to a file
	 */
	public void writeCollage()
	{
		try
		{
			FileOutputStream fos = new FileOutputStream(collageName);
			fos.write(collageBytes);
			fos.close();
			String line = LOG_WRITTEN+'\t'+collageName; 
			appendLineToFile(line);	
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}



	/**
	 * Thread class that waits for a stipulated period of Time for the decision messages.
	 */
	
	public class VoteTimeOutThread implements Runnable
	{

		
		String userNodeName;

		VoteTimeOutThread(String userNodeName)
		{
			this.userNodeName = userNodeName;
		}

		@Override
		public void run()
		{
			try
			{
				Thread.sleep(timeout);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}


			if(userDecisionMap.get(userNodeName)==null)
			{
				//No decision had arrived within the timeout period. make it false.
				userDecisionMap.put(userNodeName,false);
				userDecisionList.add(false);
			
				int pending = pendingDecisions.decrementAndGet();

				if(pending == 0) // you either time out or you end up good.
				{
					takeCommitDecision();
				}

			}
			else
			{
				//nothing to do here, the reply had arrived on time.
				return;
			}
		}
	}

	/**
	 * Thread class for Distribute Decision. It will automatically retry sending that message until the
	 * ack is received.
	 */
	public class DistributeDecisionTimeOutThread implements Runnable
	{

		ProjectLib.Message message;
		DistributeDecisionTimeOutThread(ProjectLib.Message msg)
		{
			this.message = msg;
		}

		@Override 
		public void run()
		{
			boolean flag = true;

			while(flag)
			{
				try
				{
					Thread.sleep(timeout);
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}

				if(userAckMap.get(message.addr) != null)
				{
					return;
				}
				else
				{
					//timedout, send message
					Server.PL.sendMessage(message);
				}
			}	

		}

	}
	
}