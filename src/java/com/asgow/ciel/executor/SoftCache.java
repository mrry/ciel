package com.asgow.ciel.executor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.asgow.ciel.references.Reference;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

class RefComparator implements Comparator<Reference> {

	@Override
	public int compare(Reference arg0, Reference arg1) {
		return arg0.getId().compareTo(arg1.getId());
	}
	
}

class CacheEntry {
	
	public Object value;
	public Date lastUse;
	public String tag;
	public Reference[] refs;
	
	public CacheEntry(Object val, String tag, Reference... refs) {
		value = val;
		lastUse = new Date();
		this.tag = tag;
		this.refs = refs;
	}
	
}

public class SoftCache {
	
	private HashMap<String, CacheEntry> cache;
	private RefComparator refComp;
	
	public SoftCache() {
		
		cache = new HashMap<String, CacheEntry>();
		refComp = new RefComparator();
		
	}
	
	private String keyOfSortedTagAndRefs(String tag, Reference[] refs) {
		
		Arrays.sort(refs, refComp);
		StringBuilder build = new StringBuilder();
		build.append(tag);
		for(Reference ref : refs) {
			build.append(ref.getId());
		}
		return build.toString();
		
	}
	
	private String keyOfTagAndRefs(String tag, Reference[] refs) {
		Arrays.sort(refs, refComp);
		return keyOfSortedTagAndRefs(tag, refs);
	}
	
	public Object tryGetCache(String tag, Reference... refs) {
		
		String key = keyOfTagAndRefs(tag, refs);
		CacheEntry hit = cache.get(key);
		if(hit == null) {
			System.err.println("&&&&&&&&&&&&&& Cached miss: " + refs[0].getId());
			return null;
		}
		else {
			System.err.println("&&&&&&&&&&&&&& Cached hit: " + refs[0].getId());
			hit.lastUse = new Date();
			return hit.value;
		}
		
	}

	public void putCache(Object obj, String tag, Reference... refs) {
		
		Arrays.sort(refs, refComp);
		String key = keyOfSortedTagAndRefs(tag, refs);
		CacheEntry newEntry = new CacheEntry(obj, tag, refs);
		cache.put(key, newEntry);
		System.err.println("&&&&&&&&&&&&&& Cached ref: " + refs[0].getId());
	}
	
	public void sweepCache() {
		
		long now = System.currentTimeMillis();

		Set<Map.Entry<String, CacheEntry>> kv_pairs_set = cache.entrySet();
		Iterator<Map.Entry<String, CacheEntry>> kv_pairs = kv_pairs_set.iterator();

		int old_length = kv_pairs_set.size();
		int swept = 0;		
		
		while(kv_pairs.hasNext()) {
			CacheEntry entry = kv_pairs.next().getValue();
			if(entry.lastUse.getTime() + 60000 < now) {
				kv_pairs.remove();
				swept++;
			}
		}
		
		System.out.println("Java2: Swept soft cache, collected " + swept + "/" + old_length + ".");
		
	}
	
	public JsonElement getKeysAsJson() {
		
		JsonArray result = new JsonArray();
		Iterator<CacheEntry> val_iter = cache.values().iterator();
		
		while(val_iter.hasNext()) {
			CacheEntry entry = val_iter.next();
			JsonArray entry_json = new JsonArray();
			JsonArray refs_json = new JsonArray();
			for(Reference ref : entry.refs) {
				refs_json.add(new JsonPrimitive(ref.getId()));
			}
			entry_json.add(refs_json);
			entry_json.add(new JsonPrimitive(entry.tag));
			result.add(entry_json);
		}
		
		return result;
		
	}

}
