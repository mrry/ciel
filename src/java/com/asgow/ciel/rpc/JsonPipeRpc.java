package com.asgow.ciel.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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
	public static final JsonPrimitive CREATE_REF = new JsonPrimitive("create_ref");
	public static final JsonPrimitive OPEN_OUTPUT = new JsonPrimitive("open_output");
	public static final JsonPrimitive EXIT = new JsonPrimitive("exit");
	public static final JsonPrimitive SPAWN = new JsonPrimitive("spawn");
	public static final JsonPrimitive CLOSE_REF = new JsonPrimitive("close_ref");
	public static final JsonPrimitive CLOSE_OUTPUT = new JsonPrimitive("close_output");
	public static final JsonPrimitive BLOCK = new JsonPrimitive("block");
	public static final JsonPrimitive ERROR = new JsonPrimitive("error");
	
	@SuppressWarnings("unchecked")
	public FirstClassJavaTask getTask() {
		JsonArray initCommand = this.receiveMessage().getAsJsonArray();

		assert initCommand.size() == 2 && initCommand.get(0).getAsString().equals("start_task");
		
		JsonObject task = initCommand.get(1).getAsJsonObject();
		
		try {
			JsonArray jarLib = task.get("jar_lib").getAsJsonArray();
			URL[] urls = new URL[jarLib.size()];
			
			for (int i = 0; i < urls.length; ++i) {
				Reference jarRef = Reference.fromJson(jarLib.get(i).getAsJsonObject());
				urls[i] = new URL("file://" + this.getFilenameForReference(jarRef));
			}
	
			URLClassLoader urlcl = new URLClassLoader(urls);
			
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
				String contObjectFilename = this.getFilenameForReference(Reference.fromJson(task.get("cont_object").getAsJsonObject()));
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(contObjectFilename));
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
	
	@Override
	public Reference[] blockOn(Reference... refs) {
		JsonArray args = new JsonArray();
		for (Reference ref : refs) {
			args.add(ref.toJson());
		}
		JsonArray response = this.sendReceiveMessage(BLOCK, args).getAsJsonArray();
		Reference[] ret = new Reference[response.size()];
		int i = 0;
		for (JsonElement elem : response) {
			ret[i++] = Reference.fromJson(elem.getAsJsonObject());
		}
		return ret;
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
	public void exit() {
		JsonObject args = new JsonObject();
		args.add("keep_process", new JsonPrimitive(false));
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
	public WritableReference getNewObjectFilename() {
		JsonObject args = new JsonObject();
		JsonObject response = this.sendReceiveMessage(CREATE_REF, args).getAsJsonObject();
		return new WritableReference(response.get("filename").getAsString());
	}

	@Override
	public WritableReference getOutputFilename(int index) {
		JsonObject args = new JsonObject();
		args.add("index", new JsonPrimitive(index));
		JsonObject response = this.sendReceiveMessage(OPEN_OUTPUT, args).getAsJsonArray().get(1).getAsJsonObject();
		return new WritableReference(response.get("filename").getAsString());
	}

	@Override
	public Reference[] spawnTask(TaskInformation taskInfo) {
		JsonObject args = taskInfo.toJson();
		JsonArray response = this.sendReceiveMessage(SPAWN, args).getAsJsonArray();
		Reference[] ret = new Reference[response.size()];
		int i = 0;
		for (JsonElement elem : response) {
			ret[i++] = Reference.fromJson(elem.getAsJsonObject());
		}
		return ret;
	}

}
