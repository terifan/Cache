package org.terifan.util.cache;

import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class CacheNGTest
{
	public CacheNGTest()
	{
	}


	@Test
	public void testPutAndOverflow()
	{
		Cache<String,String> cache = new Cache<>(10);
		cache.put("a", "A", 4);
		cache.put("b", "B", 3);
		cache.put("c", "C", 1);
		cache.put("d", "D", 5);

		assertEquals(cache.keys().toString(), "[b, c, d]");
	}
}
