/**
 *  UserNode: The 'client' node in the two phase commit protocol.
 */
import java.io.*;
import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.atomic.*;


public class UserNode implements ProjectLib.MessageHandling
{

	public final String myId;
	
	private static final int IRRELEVENT = -1;
	private static final int COMMIT = 1;
	private static final int DECISION = 2;
	private static final int SERVERDECISION = 3;
	private static final int ACK = 4;

	public static ProjectLib PL;
	public static ArrayList<String> inUseFileList = new ArrayList<String>();
	public static ArrayList<String> deletedList = new ArrayList<String>();

	public BufferedWriter bufferedWriter;


	public UserNode( String id )
	{
		myId = id;
		String fileName = id+".log";
		try
		{
			File file = new File(fileName);
			if(file.exists()==true)
			{
				FileWriter fileW = new FileWriter(fileName,true);
				bufferedWriter = new BufferedWriter(fileW);
			}
			else
			{
				FileWriter fileW = new FileWriter(fileName);
				bufferedWriter = new BufferedWriter(fileW);

			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
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
	 * Called whenever a message has been delivered to this node.
	 * @param  msg 
	 * @return     
	 */
	public  boolean deliverMessage( ProjectLib.Message msg ) 
	{
		
		try
		{
			handleMessage(msg);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return true;
	}


	/**
	 * Main routine for handling the message.
	 * @param  msg       
	 * @throws Exception 
	 */
	public  void handleMessage(ProjectLib.Message msg) throws Exception
	{
		MessageBody mb = (MessageBody)deserialize(msg.body);

		if(mb.getMessageType()==COMMIT)
		{
			String[] sources = mb.getSources();
			
			boolean alreadyInUseFlag = false;
			for(String file : sources)
			{
			
				synchronized(inUseFileList)
				{
					if(inUseFileList.contains(file) || deletedList.contains(file))
					{
						alreadyInUseFlag = true;
						
					}
					else
					{
						inUseFileList.add(file);
					}
					
				}
			}

			boolean decision = false;
			if(alreadyInUseFlag == true)
			{
				decision = false;
			
			}
			else
			{
				decision = PL.askUser(mb.getCollageBytes(),mb.getSources());
			
			}
			

			mb.setUserDecision(decision);
			mb.setMessageType(DECISION);
			msg.addr = "Server";
			msg.body = serialize(mb);

			PL.sendMessage(msg);

		}

		if(mb.getMessageType()==SERVERDECISION)
		{

			
			if(mb.getServerDecision()==true)
			{

				
				String[] sources = mb.getSources();
				try
				{
					for(String fileName: sources)
					{
						if(deletedList.contains(fileName))
						{
			
							continue;
						}

						File file = new File(fileName);
						file.delete();
						deletedList.add(fileName);

						bufferedWriter.write(fileName);
						bufferedWriter.newLine();
						bufferedWriter.flush();
						PL.fsync();

			
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
							
			}

			
			//remove from the list only if decision is false
			if(mb.getServerDecision()==false)
			{

				String[] sources = mb.getSources();
				for(String file: sources)
				{
			
					synchronized(inUseFileList)
					{
						inUseFileList.remove(file);
					}
				}
			}

			
			mb.setMessageType(ACK);
			msg.addr = "Server";
			msg.body = serialize(mb);
			PL.sendMessage(msg);

		}

		
	}

	/**
	 * main method: restores state from log, if applicable
	 */
	public static void main( String args[] ) throws Exception 
	{
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		

		//Restore state and then register with rmi
		restoreState(args[1]+".log");
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		
	}


	/**
	 * Add the already deleted files to the list. So that future commits will be
	 * denied.
	 */
	public static void restoreState(String filename)
	{
		File file = new File(filename);
		if(file.exists()==false)
		{
			return;
		}

		try
		{
			FileInputStream fileInputStream = new FileInputStream(filename);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
			String line;
			while ((line = bufferedReader.readLine()) != null) 
			{
				if(line.equals(""))
				{
					continue;
				}

				deletedList.add(line);
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}

