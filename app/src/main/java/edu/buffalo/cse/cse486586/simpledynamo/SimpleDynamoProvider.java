package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import Messenger.DBHandler;

public class SimpleDynamoProvider extends ContentProvider {

	static final String[] REMOTE_PORTS = {"11124","11112","11108","11116","11120"};
	static final HashMap<String,String> hashInfo = new HashMap<String, String>();

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private DBHandler myDB;
	static String myHash = "";
	static String myPort = "";
	static final int SERVER_PORT = 10000;


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		// TODO Auto-generated method stub
		Log.d("Delete" , "Entered Delete");
		Log.d("Delete" , "Selection is " + selection);

		try{
			if(!(selection.equals("@") || selection.equals("*"))){

				Log.d("query", "Only single key is deleted");
				String hashKey = genHash(selection);
				String owner_port = getOwnerPort(hashKey);
				String port_list[] = getReplicatePorts(owner_port);

				for(int i=port_list.length-1;i>=0;i--)
				{
					if(port_list[i].equals(myPort))
					{
						deleteMyData(selection);
						continue;
					}
					SendDataToPort(port_list[i],"Delete###" + selection);
				}
			}

			else{
				if(selection.equals("@"))
					deleteMyData("*");
				else
					sendDeleteRequest();
			}
		}
		catch (Exception e){

		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		try {

			Log.d("Insert" , "Start the code");

			String key = values.get("key").toString();
			String hashkey = this.genHash(key);
			String value = values.get("value").toString();

			Log.d("Insert" , "key : " + key);
			Log.d("Insert" , "hashkey : " + hashkey);
			Log.d("Insert" , "value : " + value);

			String owner_port = getOwnerPort(hashkey);

			if(owner_port.equals(myPort))
			{
				insertIntoDB(key,value);
			}
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "ReplicateAndForward",key,value,owner_port);

			return uri;

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Pratibha Alert No Such Algo Exception");
			return null;
		}
	}

	private String getOwnerPort(String hashKey){

		Log.d("getOwnerPort" , "Entered the code");
		int len = REMOTE_PORTS.length;
		Log.d("getOwnerPort" , "len is " + len);
		if((hashKey.compareTo(hashInfo.get(REMOTE_PORTS[0]))<=0) || (hashKey.compareTo(hashInfo.get(REMOTE_PORTS[len-1]))>0))
		{
			Log.d("getOwnerPort" , "First check correct, 0th port");
			return REMOTE_PORTS[0];
		}

		for(int i=1;i<len;i++)
		{
			Log.d("Insert" , "checking for " + REMOTE_PORTS[i]);
			if(valueBetween(hashInfo.get(REMOTE_PORTS[i-1]),hashInfo.get(REMOTE_PORTS[i]),hashKey))
				return REMOTE_PORTS[i];
		}
		return null;
	}

	private boolean valueBetween(String start, String end, String value) {
		Log.d("valueBetween" , "Calculating the value");
		return (value.compareTo(start) > 0 && end.compareTo(value) >=0);
	}

	private void insertIntoDB(String key, String value)
	{
		Log.v("db", "Entering Insert into DB");
		ContentValues mContentValues = new ContentValues();
		mContentValues.put("key",key);
		mContentValues.put("value",value);

		Log.v("db", "About to get writable database");
		SQLiteDatabase sqlDB = myDB.getWritableDatabase();
		Log.v("db", "got writable database");

		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables("Tb_KeyPair");

		long result = sqlDB.insertWithOnConflict("Tb_KeyPair",null,mContentValues,SQLiteDatabase.CONFLICT_REPLACE);
		Log.d("InsertIntoDB" , "Wrote key " + key + " value : " + value);

	}

	@Override
	public boolean onCreate() {

		// TODO Auto-generated method stub

		try{

			clearAndSetMap();
			Log.d("OnCreate","The system has started coding");
			myDB = new DBHandler(getContext(),null,null,1);
			Log.d("OnCreate","Line1");

			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

			Log.d("OnCreate","Line2 : portstr " + portStr);

			myHash = this.genHash(portStr);
			Log.d("OnCreate","Line3 : Myhash " + myHash);

			myPort = String.valueOf((Integer.parseInt(portStr) * 2));
			Log.d("OnCreate","Line4 : myport " + myPort);

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		}

		catch (NoSuchAlgorithmException e){
			Log.e(TAG, "Pratibha Alert No Such Exception");
		}
		catch (IOException e) {
			Log.e("On Create", "IOException Catch in the code");
		}
		catch (Exception e){
			Log.e("On Create", "Can't create a ServerSocket");
		}

		return false;
	}

	private void clearAndSetMap()
	{
		hashInfo.clear();
		hashInfo.put("11108","33d6357cfaaf0f72991b0ecd8c56da066613c089");
		hashInfo.put("11112","208f7f72b198dadd244e61801abe1ec3a4857bc9");
		hashInfo.put("11116","abf0fd8db03e5ecb199a9b82929e9db79b909643");
		hashInfo.put("11120","c25ddd596aa7c81fa12378fa725f706d54325d12");
		hashInfo.put("11124","177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
		return;
	}

	class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try
			{
				Log.d("ClientTask", "Starting the code");

				String msgType = msgs[0];

				Log.d("ClientTask", "Message Type " + msgType);


				if(msgType.equals("ReplicateAndForward")) {

					String message="";
					String replicate_ports[] = getReplicatePorts(msgs[3]);

					for(int i=0;i<replicate_ports.length;i++)
					{
						if(replicate_ports[i].equals(myPort))
						{
							insertIntoDB(msgs[1],msgs[2]);
							continue;
						}

						if(i==0)
							message = "ReplicateAndForward###" + replicate_ports[1] + "###" + replicate_ports[2] + "###" +
										msgs[1] + "###" + msgs[2] + "###" + myPort;

						else
							message = "Replicate###" + msgs[1] + "###" + msgs[2] + "###" + myPort;

						SendDataToPort(replicate_ports[i],message);
					}
				}
				else if(msgType.equals("Forward")){

					Log.d("Client Task" , "Forward Function");

					String key = msgs[1];
					String value = msgs[2];
					String ports[] = new String[]{msgs[3],msgs[4]};
					String message = "Replicate###" + key + "###" + value + "###" + myPort;

					Log.d("Insert" , "key : " + key);
					Log.d("Insert" , "value : " + value);
					Log.d("Insert" , "ports : " + ports[0] + "  " + ports[1]);
					Log.d("Insert" , "Message : " + message);

					for(String port : ports){
						SendDataToPort(port, message);
					}
				}
			}
			catch (Exception e) {
				Log.e("Client Task", "Pratibha Alert ClientTask Common Exception");
			}

			return null;
		}
	}

	private String[] getReplicatePorts(String ownerPort){

		Log.d("getReplicatePorts","Owner port " + ownerPort);

		String replicatePorts[] = new String[3];
		replicatePorts[0] = ownerPort;

		for(int i=0; i<REMOTE_PORTS.length;i++)
			if(REMOTE_PORTS[i].equals(ownerPort))
			{
				Log.d("getReplicatePorts","Port matched: " + REMOTE_PORTS[i]);
				Log.d("getReplicatePorts","i: " + i);
				Log.d("getReplicatePorts","i+1: " + (i+1)%5);
				Log.d("getReplicatePorts","i+2: " + (i+2)%5);

				replicatePorts[1] = REMOTE_PORTS[(i+1)%5];
				replicatePorts[2] = REMOTE_PORTS[(i+2)%5];
				break;
			}

		return replicatePorts;
	}

	private String[] getPreviousPorts(String ownerPort){

		Log.d("getPreviousPorts","Owner port " + ownerPort);

		String previousPorts[] = new String[2];

		if(REMOTE_PORTS[0].equals(ownerPort)){

			Log.d("getReplicatePorts","Port matched: " + REMOTE_PORTS[0]);
			previousPorts[0] = REMOTE_PORTS[4];
			previousPorts[1] = REMOTE_PORTS[3];
		}
		else if(REMOTE_PORTS[1].equals(ownerPort)){

			Log.d("getReplicatePorts","Port matched: " + REMOTE_PORTS[1]);
			previousPorts[0] = REMOTE_PORTS[0];
			previousPorts[1] = REMOTE_PORTS[4];
		}
		else{

			for(int i=2; i<REMOTE_PORTS.length;i++)
				if(REMOTE_PORTS[i].equals(ownerPort))
				{
					Log.d("getReplicatePorts","Port matched: " + REMOTE_PORTS[i]);
					Log.d("getReplicatePorts","i: " + i);

					previousPorts[0] = REMOTE_PORTS[i-1];
					previousPorts[1] = REMOTE_PORTS[i-2];
					break;
				}
		}

		return previousPorts;
	}

	private String SendDataToPort(String port,String message)
	{
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));

			socket.setSoTimeout(200);

			PrintWriter out =
					new PrintWriter(socket.getOutputStream(), true);

			Log.d(TAG, "Client: PrintWriter Created");
			out.println(message);
			out.flush();

			BufferedReader in = new BufferedReader(
					new InputStreamReader(socket.getInputStream()));

			String line = in.readLine();
			out.close();
			socket.close();
			return line;
		}
		catch (UnknownHostException e) {
			Log.e("Client Task", "Pratibha Alert ClientTask UnknownHostException");
		} catch (SocketTimeoutException e) {
			Log.e("Client Task", "Pratibha Alert ClientTask socket time out");
		} catch (IOException e) {
			Log.e("Client Task", "Pratibha Alert ClientTask socket IOException");
		}
		return "error";
	}


	class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Socket socket = null;

			try {

				deleteMyData("*");
				recoverMyData();

				while(true) {

					Log.d("ServerTask", "Inside while true");
					Log.d(TAG, "doInBackground: In try");
					//Server will accept the connection from the client
					Log.d("ServerTask","Accepting..");

					socket = serverSocket.accept();

					Log.d(TAG, "doInBackground: Accepted");

					// This will read the message sent on the InputStream
					BufferedReader in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));

					// Read the message line by line
					String line = in.readLine();

					if(line!=null)
					{
						Log.d(TAG, "doInBackground: Line is not null");
						String lines[] = line.split("###");

						if(lines[0].equals("ReplicateAndForward")){

							String port1 = lines[1];
							String port2 = lines[2];
							String key = lines[3];
							String value = lines[4];
							String sender_port = lines[5];

							insertIntoDB(key,value);
							PrintWriter out =
									new PrintWriter(socket.getOutputStream(), true);
							out.println("Done");
							out.flush();

							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Forward",key,value,port1,port2);
						}
						else if(lines[0].equals("Replicate")){
							insertIntoDB(lines[1],lines[2]);
							PrintWriter out =
									new PrintWriter(socket.getOutputStream(), true);
							out.println("Done");
							out.flush();
						}
						else if(lines[0].equals("Request")){

							String parameter = lines[1];
							String result="";

							Cursor cursor = getMyData(parameter);

							if(cursor==null || cursor.getCount()==0){
								result = "NoData";
							}
							else
								result = "Reply" + convertCursorToString(cursor);

							PrintWriter out =
									new PrintWriter(socket.getOutputStream(), true);
							out.println(result);
							out.flush();
						}
						else if(lines[0].equals("Delete")){

							String parameter = lines[1];
							deleteMyData(parameter);
							PrintWriter out =
									new PrintWriter(socket.getOutputStream(), true);
							out.println("Done");
							out.flush();
						}
					}
					// Log.d("ServerTask", "Line read " + line);
				}
			} catch (SocketTimeoutException e) {
				Log.e("Server Task", "Alert Time Out Exception Catch in the code");
			} catch (IOException e) {
				Log.e("Server Task", "Alert IOException Catch in the code");
			}
			catch (Exception e) {
				e.printStackTrace();
				Log.e("Server Task", "Alert Exception Catch in the code");
			}
			return null;
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub query method

		Log.d("Query" , "Entered Query");

		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables("Tb_KeyPair");

		Log.d("Query" , "Selection is " + selection);

		try{
			if(!(selection.equals("@") || selection.equals("*"))){

				Log.d("query", "Only single key is needed");
				String hashKey = genHash(selection);
				String owner_port = getOwnerPort(hashKey);
				String port_list[] = getReplicatePorts(owner_port);

				for(int i=port_list.length-1;i>=0;i--)
				{

					if(port_list[i].equals(myPort))
					{
						Cursor cursor = getMyData(selection);
						if(cursor!=null && cursor.getCount()>0)
							return cursor;

						continue;
					}

					String reply = SendDataToPort(port_list[i],"Request###" + selection);

					if(reply!=null && !reply.equals("error")){

						String lines[] = reply.split("###");

						if(lines[0].equals("Reply")){

							MatrixCursor temp_cursor=new MatrixCursor(new String[] {"key","value"});

							Log.d("Query", "Reply got");
							String values[] = lines[1].split("\\$\\$\\$");

							Log.d("Query", "key  : "+ values[0]);
							Log.d("Query", "value  : "+ values[1]);

							temp_cursor.addRow(new Object[]{values[0],values[1]});
							return temp_cursor;
						}
					}
				}
			}

			else{
				Log.d("Query" , "Query type is * or @");
				Cursor cursor = getMyData("*");

				if(selection.equals("@"))
					return cursor;

				Log.d("Query" , "Query type is * , proceeding");
				MatrixCursor mcursor = sendQueryRequest(selection);

				Log.d("Query" , "matrix cursor returned");

				if(mcursor==null || mcursor.getCount()==0)
					return cursor;

				for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
					mcursor.addRow(new Object[] {cursor.getString(0),cursor.getString(1)});
				}

				return mcursor;
			}
		}
		catch (Exception e){

		}
		return null;
	}

	private Cursor getMyData(String parameter)
	{
		Log.d("getMyData","Inside getMyData with " + parameter);

		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables("Tb_KeyPair");
		Cursor cursor=null;

		if(parameter.equals("*")){

			cursor = queryBuilder.query(myDB.getReadableDatabase(),
					new String[]{"key","value"}, null, null, null, null,null);
			Log.d("getMyData","count " + cursor.getCount());

		}
		else{
			cursor = queryBuilder.query(myDB.getReadableDatabase(),
					new String[]{"key","value"}, "key =?", new String[]{parameter}, null, null,null);
			Log.d("getMyData","count " + cursor.getCount());
		}

		return cursor;
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private String convertCursorToString(Cursor cursor)
	{
		if(cursor==null || cursor.getCount()==0)
			return "";

		String result = "";

		for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {

			result = result + "###";
			result = result + cursor.getString(0) + "$$$" + cursor.getString(1);
		}
		return result;
	}

	private MatrixCursor sendQueryRequest(String selection){

		Log.d("SendQueryRequest" , "Selection : " + selection);
		MatrixCursor temp_cursor=new MatrixCursor(new String[] {"key","value"});

		String message = "Request###" + selection;

		for(String port : REMOTE_PORTS)
		{
			if(port.equals(myPort))
				continue;

			String reply = SendDataToPort(port,message);

			if(reply!=null && !reply.equals("error"))
			{
				Log.d("SendQueryRequest", "reply for *");
				Log.d("SendQueryRequest", "reply is " + reply);

				String lines[] = reply.split("###");

				if(lines[0].equals("Reply")) {

					Log.d("SendQueryRequest", "Data was sent");

					for(int i=1;i<lines.length;i++){

						String values[] = lines[i].split("\\$\\$\\$");
						temp_cursor.addRow(new Object[]{values[0],values[1]});
					}
				}
			}
		}

		if(temp_cursor.getCount()==0)
			return null;

		Log.d("SendQueryRequest", "cursor has values. sending back cursor");
		return  temp_cursor;
	}

	private int deleteMyData(String parameter){

		Log.d("deleteMyData","Entering the code");

		SQLiteDatabase sqlDB = myDB.getWritableDatabase();


		if(parameter.equals("*")){
			Log.d("deleteMyData","Deleting all");

			return sqlDB.delete("Tb_KeyPair",null, null);
		}
		else{
			Log.d("deleteMyData","Deleting " + parameter);
			return sqlDB.delete("Tb_KeyPair","key = ?", new String[]{parameter});
		}
	}

	private void sendDeleteRequest(){

		for(int i=0;i<REMOTE_PORTS.length;i++)
		{
			if(REMOTE_PORTS[i].equals(myPort))
				continue;

			SendDataToPort(REMOTE_PORTS[i],"Delete###*");
		}
	}

	private void recoverMyData()
	{
		HashMap<String,String> data = new HashMap<String, String>();

		Log.d("recoverMyData" , "Entering the code");

		/*Cursor mycursor = getMyData("*");
		for (mycursor.moveToFirst(); !mycursor.isAfterLast(); mycursor.moveToNext()) {
			data.put(mycursor.getString(0),mycursor.getString(1));
		}*/

		MatrixCursor cursor = sendQueryRequest("*");

		Log.d("recoverMyData" , "matrix cursor formed");

		if(cursor!=null) {

			Log.d("recoverMyData", "before adding data to HashMap");

			for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
				data.put(cursor.getString(0),cursor.getString(1));
			}

			Log.d("recoverMyData", "Added data");

			for (Map.Entry<String,String> entry : data.entrySet()) {
				Log.d("recoverMyData" , "Inside loop");
				String key = entry.getKey();
				String value = entry.getValue();

				Log.d("recoverMyData" , "key : " + key);
				Log.d("recoverMyData" , "value : " + value);

				if(belongsToMe(key))
					insertIntoDB(key,value);
			}
		}
	}

	private Boolean belongsToMe(String key){
		try{
			Log.d("belongsToMe","Starting the code");

			String hashKey = genHash(key);
			String owner = getOwnerPort(hashKey);

			Log.d("belongsToMe","hashKey " + hashKey);
			Log.d("belongsToMe","owner " + owner);
			Log.d("belongsToMe","myPort " + myPort);


			if(owner.equals(myPort))
				return true;

			Log.d("belongsToMe","About to get previous port ");

			String previous_port[] = getPreviousPorts(myPort);

			Log.d("belongsToMe","p1 " + previous_port[0]);
			Log.d("belongsToMe","p2 " + previous_port[1]);

			for(String port : previous_port){
				if(port.equals(owner))
					return true;
			}
			Log.d("belongsToMe","Returning false");
			return false;

		}
		catch (NoSuchAlgorithmException e){
			return false;
		}
		catch (Exception e){
			return false;
		}
	}
}
