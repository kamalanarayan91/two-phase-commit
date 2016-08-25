/**
 * Reads all the server log files, one by one and restores 
 * the state of the server.
 */
import java.io.*;
import java.util.*;
public class ServerStateRestorer
{

	private static final String LOG_PREPARE = "PREPARE";
	private static final String LOG_ACK = "ACK";
	private static final String LOG_USERDECISION = "USERDECISION";
	private static final String LOG_SERVERDECISION = "SERVERDECISION";
	private static final String LOG_WRITTEN = "WRITTEN";
	private static final String LOG_FALSE = "FALSE";

	public static void restore()
	{
		for(int commitIndex = 1; commitIndex< Integer.MAX_VALUE; commitIndex++)
		{
			String filename = Integer.toString(commitIndex)+".log";
			String collageBytes = "Server_"+Integer.toString(commitIndex);
			File file = new File(filename);
			byte[] img = null;
			if(file.exists() == false)
			{
				//we're done. Exit
				return;
			}

			//Increment the commit number in the server		
			Server.commitNumber.getAndIncrement();

			try
			{
				RandomAccessFile raf = new RandomAccessFile(collageBytes,"r");
				img = new byte[(int)raf.length()];
				raf.read(img);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}

			// read the file line by line.
			// first line seperated by \t contains filename and sources
			int numSources=0;
			int numPendingPrepares=0;
			int numPendingAcks=0;
			int numPendingDecisions=0;
			int numPendingUserDecisions=0;

			String sourcesString = null;
			try
			{

				FileInputStream fileInputStream = new FileInputStream(filename);
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
				String line;
				ArrayList<String> userNodeList = new ArrayList<String>();

				//Read File Line By Line
				int lineCount = 1;
				String finalDecision = "NA";
				boolean written = false;
				Commit commit = null;
				while ((line = bufferedReader.readLine()) != null)   
				{

			  		if(line.equals(""))
			  		{
			  			continue;
			  		}
				  	// Print the content on the console
				  	String[] splits = line.split("\t");


				  	if(lineCount == 1)
				  	{
				  		String collageName = splits[0];
				  		String sources = splits[1];

				  		String[] sourceArray = sources.split(",");

				  		sourcesString = sources;//comma seperated string
				  		lineCount++;

				  		commit = new Commit(collageName,img,sourceArray);
				  		commit.setCommitID(commitIndex);
				  		commit.populateCommitInfo(false);
				  		Server.commitMap.put(commitIndex,commit);

				  		
				  		numSources = numPendingPrepares = numPendingUserDecisions = commit.getNumUsers();
				  		numPendingAcks = numPendingDecisions  = numPendingPrepares;

				  		
				  	}
				  	else
				  	{
				  		String type;
				  		String userNode;
				  		String decision;

				  		type = splits[0];
				  		userNode = splits[1];

				  		if(type.equals(LOG_PREPARE))
				  		{
				  			
				  			numPendingPrepares--;				  			
				  		}
				  		if(type.equals(LOG_ACK))
				  		{
				  			
				  			numPendingAcks--;
				  		}

				  		if(type.equals(LOG_USERDECISION))
				  		{
				  			
				  			decision = splits[2];
				  			numPendingUserDecisions--;
				  			if(decision.equals(LOG_FALSE))
				  			{
				  			
				  				finalDecision = LOG_FALSE;
				  			}
				  		}
				  		if(type.equals(LOG_SERVERDECISION))
				  		{
				  			
				  			numPendingDecisions--;
				  		}
				  		if(type.equals(LOG_WRITTEN))
				  		{
				  			
				  			written=true;
				  		}

				  	}

				}

				//Close the input stream
				bufferedReader.close();


				//All user nodes have sent the decision. 
				if(numPendingUserDecisions == 0)
				{
					//everything received.
					if(numPendingDecisions == 0 ) // Server decision has been sent to everyone.
					{
						
						if(numPendingAcks == 0)
						{
							
							continue;//we're all good.
						}
						
						else
						{
							// we need acks again.
							if(finalDecision.equals(LOG_FALSE))
							{
								commit.sendServerDecision(false);				
							}
							else
							{
								commit.sendServerDecision(true);				
							}
						}
						
					}
					else
					{

						// we need acks again.
						if(finalDecision.equals(LOG_FALSE))
						{
							commit.sendServerDecision(false);				
						}
						else
						{
							commit.sendServerDecision(true);				
						}

					}
				}
				else
				{ 
					// abort commit in all other cases
					commit.sendServerDecision(false);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}

		}
	}


}