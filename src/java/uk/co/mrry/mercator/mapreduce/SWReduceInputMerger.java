package uk.co.mrry.mercator.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.io.WritableComparator;

public class SWReduceInputMerger<K extends WritableComparable, V extends Writable> {
  
  public class SWReduceIterator<K extends WritableComparable, V extends Writable> implements Iterator<V> {

    private SWReduceInputMerger<K, V> merger;
    private K key;
    
    public SWReduceIterator(SWReduceInputMerger<K, V> inputMerger, K key) {
      /* uses an SWReduceInputMerger that takes care of fetching elements from the input streams
       * and deserializes them 
       */
      merger = inputMerger;
      this.key = key;
    }
    
    @Override
    public boolean hasNext() {
      // iterate over all head keys and check if there is one matching the current key
      for (int i = 0; i < streamHeadsKeys.size(); ++i) {
        if (streamHeadsKeys.get(i) == key) return true;
      }
      
      return false;
    }

    @Override
    public V next() {
      // Fetch the next element - can be null if there is none for the active key
      return merger.getNextElement();
    }

    @Override
    public void remove() {
      // fails
      throw new RuntimeException("method not implemented");
    }
    
    
    
    
  }

  
  
  private FileInputStream[] inputs;
  private ArrayList<K> streamHeadsKeys;
  private ArrayList<V> streamHeadsValues;
  
  private K currentKey;
  private int nextStreamID;
  
  // TODO does this have to be writable?
  private WritableSerialization serialization;
  
  private final WritableComparator keyComparator;
  
  public SWReduceInputMerger(FileInputStream[] fis) {
    
    inputs = fis;
    
    streamHeadsKeys = new ArrayList<K>(fis.length);
    streamHeadsValues = new ArrayList<V>(fis.length);
    
    serialization = new WritableSerialization();
    
    // populate the head arrays
    for (int i = 0; i < fis.length; ++i) {
      try {
        fetchFromStream(i);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    
    currentKey = streamHeadsKeys.get(0);
    nextStreamID = 0;
    
    keyComparator = WritableComparator.get(currentKey.getClass());
    
  }
  
  
  public V getNextElement() {
    
    // Fetch the next value for the currently active key (null if there is none) 
    for (int i = nextStreamID; i < streamHeadsKeys.size(); ++i) {
      if (streamHeadsKeys.get(i) == currentKey) {
        V val = streamHeadsValues.get(i);
        nextStreamID = i;
        
        // replace key and value in head arrays
        try {
          fetchFromStream(i);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        
        // return the associated value
        return val;
      } else continue;
    }
    
    // if we can't find a next element for the key, return null
    nextStreamID = getLeastKeyStreamID();
    currentKey = streamHeadsKeys.get(nextStreamID);
    return null;
  }
  
  
  private int getLeastKeyStreamID() {
    
    // Iterate over keys at the head of the input streams and select the least one as the next key
    int currentBestID = 0;
    K currentBest = streamHeadsKeys.get(0);
    
    for (int i = 1; i < inputs.length; ++i) {
      if (currentBest.compareTo(streamHeadsKeys.get(i)) < 0) {
        currentBest = streamHeadsKeys.get(i);
        currentBestID = i;
      }
    }
    
    return currentBestID;
    
  }
  
  private void fetchFromStream(int streamID) throws IOException {
    
    // do the deserialization dance
    Deserializer<Writable> deserializer = serialization.getDeserializer(null);
    deserializer.open(inputs[streamID]);
    
    K key = (K)deserializer.deserialize(null);
    V value = (V)deserializer.deserialize(null);
    
    // update head arrays with next entry from stream
    streamHeadsKeys.set(streamID, key);
    streamHeadsValues.set(streamID, value);
    
  }
  
  
  public void setKey(K key) {
    currentKey = key;
  }
  
  
}
