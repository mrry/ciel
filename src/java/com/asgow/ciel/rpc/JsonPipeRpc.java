package com.asgow.ciel.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.net.URL;
import java.net.URLClassLoader;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.tasks.TaskInformation;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

public class JsonPipeRpc implements WorkerRpc {

	private final DataOutputStream toWorkerPipe;
	private final DataInputStream fromWorkerPipe;
	private final Gson gson;
	private final JsonParser jsonParser;
	
	private class UserObjectInputStream extends ObjectInputStream {
		
		public UserObjectInputStream(InputStream in) throws IOException {
			super(in);
		}
		
		@Override
		protected Class resolveClass(ObjectStreamClass desc) throws IOException,
			ClassNotFoundException {
			
			try {
				return Ciel.CLASSLOADER.loadClass(desc.getName());
			} catch (Exception e) {
				;
			}
			
			return super.resolveClass(desc);
		}
		
	}
	
	public JsonPipeRpc(String toWorkerPipeName, String fromWorkerPipeName) {
		try {
			this.toWorkerPipe = new DataOutputStream(new FileOutputStream(toWorkerPipeName));
			this.fromWorkerPipe = new DataInputStream(new FileInputStream(fromWorkerPipeName));
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		this.gson = new Gson();
		this.jsonParser = new JsonParser();
	}
	
	private void sendMessage(JsonPrimitive method, JsonElement args) {
		try {
			JsonArray message = new JsonArray();
			message.add(method);
			message.add(args);
			byte[] messageString = this.gson.toJson(message).getBytes();
			System.err.println("Writing " + messageString.length + " bytes");
			this.toWorkerPipe.writeInt(messageString.length);
			this.toWorkerPipe.write(messageString);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
	private JsonElement receiveMessage() {
		try {
			int responseLength = this.fromWorkerPipe.readInt();
			System.err.println("Reading " + responseLength + " bytes");
			byte[] responseBuffer = new byte[responseLength];
			this.fromWorkerPipe.readFully(responseBuffer);
			String responseString = new String(responseBuffer);
			System.out.println(responseString);
			return this.jsonParser.parse(responseString);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
	private JsonElement sendReceiveMessage(JsonPrimitive method, JsonElement args) {
		this.sendMessage(method, args);
		return this.receiveMessage();
	}
	
	public static final JsonPrimitive OPEN_REF = new JsonPrimitive("open_ref");
	public static final JsonPrimitive ALLOCATE_OUTPUT = new JsonPrimitive("allocate_output");
	public static final JsonPrimitive OPEN_OUTPUT = new JsonPrimitive("open_output");
	public static final JsonPrimitive EXIT = new JsonPrimitive("exit");
	public static final JsonPrimitive SPAWN = new JsonPrimitive("spawn");
	public static final JsonPrimitive TAIL_SPAWN = new JsonPrimitive("tail_spawn");
	public static final JsonPrimitive CLOSE_REF = new JsonPrimitive("close_ref");
	public static final JsonPrimitive CLOSE_OUTPUT = new JsonPrimitive("close_output");
	public static final JsonPrimitive BLOCK = new JsonPrimitive("block");
	public static final JsonPrimitive ERROR = new JsonPrimitive("error");
	
	@SuppressWarnings("unchecked")
	public FirstClassJavaTask getTask() throws ShutdownException {
		JsonArray initCommand = this.receiveMessage().getAsJsonArray();

		assert initCommand.size() == 2;
		String command_string = initCommand.get(0).getAsString();
		
		if(command_string.equals("die")) {
			String reason = initCommand.get(1).getAsJsonObject().get("reason").getAsString();
			throw new ShutdownException(reason);
		}
		
		assert command_string.equals("start_task");
		
		JsonObject task = initCommand.get(1).getAsJsonObject();
		
		try {
			JsonArray jarLib = task.get("jar_lib").getAsJsonArray();
			URL[] urls = new URL[jarLib.size()];
			Reference[] refJarLib = new Reference[jarLib.size()];
			
			for (int i = 0; i < urls.length; ++i) {
				refJarLib[i] = Reference.fromJson(jarLib.get(i).getAsJsonObject());
				urls[i] = new URL("file://" + this.getFilenameForReference(refJarLib[i]));
			}
	
			URLClassLoader urlcl = new URLClassLoader(urls);
						
			Ciel.jarLib = refJarLib;
			Ciel.CLASSLOADER = urlcl;
			
			FirstClassJavaTask ret;
			
			if (task.has("args")) {
				JsonArray jsonArgs = task.get("args").getAsJsonArray();
				Ciel.args = new String[jsonArgs.size()];
				for (int i = 0; i < Ciel.args.length; ++i) {
					Ciel.args[i] = jsonArgs.get(i).getAsString();
				}
			} else {
				Ciel.args = new String[0];
			}
			
			if (task.has("object_ref")) {
				// This is an internally-created task.
				String contObjectFilename = this.getFilenameForReference(Reference.fromJson(task.get("object_ref").getAsJsonObject()));
				ObjectInputStream ois = new UserObjectInputStream(new FileInputStream(contObjectFilename));
				ret = (FirstClassJavaTask) ois.readObject();
			} else {
				assert task.has("class_name");
				// This is an externally-created task.
				String taskClassName = task.get("class_name").getAsString();
				Class taskClass = urlcl.loadClass(taskClassName);
				ret = (FirstClassJavaTask) taskClass.newInstance();
			}
			
			return ret;
		
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
	}
	
	public void getFixedContinuationTask() {
		
		JsonArray initCommand = this.receiveMessage().getAsJsonArray();

		assert initCommand.size() == 2;
		String command_string = initCommand.get(0).getAsString();
		assert command_string.equals("start_task");
	
	}

	@Override
	public Reference closeNewObject(WritableReference wref) {
		JsonObject args = new JsonObject();
		args.add("filename", new JsonPrimitive(wref.getFilename()));
		JsonObject response = this.sendReceiveMessage(CLOSE_OUTPUT, args).getAsJsonObject();
		return Reference.fromJson(response.getAsJsonObject("ref"));
	}

	@Override
	public Reference closeOutput(int index) {
		JsonObject args = new JsonObject();
		args.add("index", new JsonPrimitive(index));
		JsonObject response = this.sendReceiveMessage(CLOSE_OUTPUT, args).getAsJsonArray().get(1).getAsJsonObject();
		return Reference.fromJson(response.getAsJsonObject("ref"));
	}

	@Override
	public void error(String errorMessage) {
		JsonObject args = new JsonObject();
		args.add("report", new JsonPrimitive(errorMessage));
		this.sendMessage(ERROR, args);
	}

	@Override
	public void exit(boolean fixed) {
		JsonObject args = new JsonObject();
		if(fixed) {
			args.add("keep_process", new JsonPrimitive("must_keep"));
		}
		else {
			args.add("keep_process", new JsonPrimitive("may_keep"));
		}
		this.sendMessage(EXIT, args);
	}

	@Override
	public String getFilenameForReference(Reference ref) {
		JsonObject args = new JsonObject();
		args.add("ref", ref.toJson());
		JsonObject response = this.sendReceiveMessage(OPEN_REF, args).getAsJsonArray().get(1).getAsJsonObject();
		if (response.has("filename")) {
			return response.get("filename").getAsString();
		} else {
			throw new ReferenceUnavailableException(ref);
		}
	}
	
	@Override
	public WritableReference getNewObjectFilename(String refPrefix) {
		JsonObject args = new JsonObject();
		args.add("prefix", new JsonPrimitive(refPrefix));
		
		JsonObject response = this.sendReceiveMessage(ALLOCATE_OUTPUT, args).getAsJsonArray().get(1).getAsJsonObject();
		int index = response.get("index").getAsInt();
		
		return this.getOutputFilename(index);
	}

	@Override
	public WritableReference getOutputFilename(int index) {
		JsonObject args = new JsonObject();
		args.add("index", new JsonPrimitive(index));
		JsonObject response = this.sendReceiveMessage(OPEN_OUTPUT, args).getAsJsonArray().get(1).getAsJsonObject();
		return new WritableReference(response.get("filename").getAsString(), index);
	}

	@Override
	public Reference[] spawnTask(TaskInformation taskInfo) {
		JsonObject args = taskInfo.toJson();
		JsonArray response = this.sendReceiveMessage(SPAWN, args).getAsJsonArray().get(1).getAsJsonArray();
		Reference[] ret = new Reference[response.size()];
		int i = 0;
		for (JsonElement elem : response) {
			ret[i++] = Reference.fromJson(elem.getAsJsonObject());
		}
		return ret;
	}
	
	@Override
	public void tailSpawnTask(TaskInformation taskInfo) {
		JsonObject args = taskInfo.toJson();
		this.sendMessage(TAIL_SPAWN, args);
	}
	
	public void tailSpawnRaw(JsonElement args) {
		this.sendMessage(TAIL_SPAWN, args);
	}
	
}
