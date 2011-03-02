package com.asgow.ciel.references;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.asgow.ciel.protocol.CielProtos.Reference.Builder;
import com.asgow.ciel.protocol.CielProtos.Reference.ReferenceType;

public class ValueReference extends Reference {

	private final byte[] value;
	
	public ValueReference(String id, byte[] value) {
		super(id);
		this.value = value;
	}
	
	public ValueReference(com.asgow.ciel.protocol.CielProtos.Reference ref) {
		this(ref.getId(), ref.getValue().getBytes());
	}
	
	public <T> T getValueAsObject(Class<T> clazz) {
		Object ret = null;
		try {
			ret = new ObjectInputStream(new ByteArrayInputStream(this.value)).readObject();
			return clazz.cast(ret);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isConsumable() {
		return true;
	}

	@Override
	public Builder buildProtoBuf(Builder builder) {
		return builder.setType(ReferenceType.VALUE).setValue(new String(this.value));
	}

	
}
