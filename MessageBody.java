/**
 * Body of the message that is shared between the server
 * and client.
 */
import java.io.*;
public class MessageBody implements Serializable
{

	/**
	 * Member Variables
	 */
	private static final int IRRELEVENT = -1;
	private static final int COMMIT = 1;
	private static final int DECISION = 2;
	private static final int SERVERDECISION = 3;
	private static final int ACK = 4;


	private int messageType;

	private String filename;

	private String[] sources;

	private int commitId;
	
	private boolean userDecision;
	
	private int ackCommitId;

	private boolean serverDecision;

	private byte[] collageBytes;

	MessageBody()
	{
		messageType = IRRELEVENT;
		commitId = IRRELEVENT;
		filename = null;
		userDecision= false;
		ackCommitId= IRRELEVENT;	
		serverDecision = false;
		collageBytes = null;
		sources = null;

	}

	/* Start - Getters and setters*/
	public void setSources(String[] sources)
	{
		this.sources = sources;
	}

	public String[] getSources()
	{
		return this.sources;
	}
	public byte[] getCollageBytes()
	{
		return this.collageBytes;
	}

	public void setCollageBytes(byte[] bytes)
	{
		this.collageBytes = bytes;
	}

	public int getMessageType() 
	{
		return this.messageType;
	}

	public void setMessageType(int messageType) 
	{
		this.messageType = messageType;
	}


	public String getFilename() 
	{
		return filename;
	}

	public void setFilename(String filename) 
	{
		this.filename = filename;
	}

	public int getCommitId() 
	{
		return commitId;
	}

	public void setCommitId(int commitId) 
	{
		this.commitId = commitId;
	}


	public boolean getUserDecision() 
	{
		return userDecision;
	}

	public void setUserDecision(boolean userDecision)
	{
		this.userDecision = userDecision;
	}


	public int getAckCommitId() 
	{
		return ackCommitId;
	}

	public void setAckCommitId(int ackCommitId) 
	{
		this.ackCommitId = ackCommitId;
	}


	public boolean getServerDecision() 
	{
		return serverDecision;
	}

	public void setServerDecision(boolean serverDecision) 
	{
		this.serverDecision = serverDecision;
	}
	
	/* End - getters and setters*/	

}