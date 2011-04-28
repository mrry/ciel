package com.asgow.ciel.references;

public enum JsonReferenceType {
	CONCRETE(ConcreteReference.IDENTIFIER.getAsString()),
	FUTURE(FutureReference.IDENTIFIER.getAsString()),
	STREAM(StreamReference.IDENTIFIER.getAsString()),
	SWEETHEART(SweetheartReference.IDENTIFIER.getAsString()),
	VALUE(ValueReference.IDENTIFIER.getAsString()),
	COMPLETED(CompletedReference.IDENTIFIER.getAsString());

	private final String representation;
	
	JsonReferenceType(String representation) {
		this.representation = representation;
	}
	
	public static JsonReferenceType fromString(String representation) {
		if (representation != null) {
			for (JsonReferenceType t : JsonReferenceType.values()) {
				if (representation.equals(t.representation)) {
					return t;
				}
			}
		}
		return null;
	}
	
}
