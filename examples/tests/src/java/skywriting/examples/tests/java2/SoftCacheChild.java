package skywriting.examples.tests.java2;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class SoftCacheChild implements FirstClassJavaTask {

	private Reference ref_in;
	
	public SoftCacheChild(Reference ref_in) {
		this.ref_in = ref_in;
	}
	
	@Override
	public Reference[] getDependencies() {
		Reference[] refs = new Reference[1];
		refs[0] = this.ref_in;
		return refs;
	}
	
	@Override
	public void invoke() throws Exception {
		
		Integer n_bytes;
		Object cached = Ciel.softCache.tryGetCache("bytecount", this.ref_in);
		if(cached == null) {
			System.out.println("SoftCacheChild: calculating afresh");
			String s = Ciel.stringOfRef(this.ref_in);
			n_bytes = s.length();
			Ciel.softCache.putCache(n_bytes, "bytecount", this.ref_in);
		}
		else {
			System.out.println("SoftCacheChild: using cached value");
			n_bytes = (Integer)cached;
		}
		Ciel.returnPlainString(String.format("%d bytes", n_bytes));
		
	}

	@Override
	public void setup() {
		;
	}

}
