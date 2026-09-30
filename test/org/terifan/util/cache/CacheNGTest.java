package org.terifan.util.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;
import org.terifan.util.cache.RemovalListener.Cause;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class CacheNGTest
{
	@Test
	public void defaultsAndConfiguration()
	{
		Cache<String, String> cache = new Cache<>();

		assertTrue(cache.isEmpty());
		assertEquals(cache.size(), 0);
		assertEquals(cache.getWeight(), 0L);
		assertEquals(cache.getMaximumCount(), Long.MAX_VALUE);
		assertEquals(cache.getMaximumWeight(), Long.MAX_VALUE);
		assertEquals(cache.getExpireAfterAccess(), Long.MAX_VALUE);
		assertEquals(cache.getExpireAfterWrite(), Long.MAX_VALUE);
		assertNull(cache.getRemovalListener());

		RemovalListener<String, String> listener = (key, value, cause) -> {};
		assertSame(cache.setRemovalListener(listener), cache);
		assertSame(cache.setExpireAfterAccess(100), cache);
		assertSame(cache.setExpireAfterWrite(200), cache);
		assertSame(cache.setMaximumCount(3), cache);
		assertSame(cache.setMaximumWeight(4), cache);
		assertSame(cache.getRemovalListener(), listener);
		assertEquals(cache.getExpireAfterAccess(), 100L);
		assertEquals(cache.getExpireAfterWrite(), 200L);
		assertEquals(cache.getMaximumCount(), 3L);
		assertEquals(cache.getMaximumWeight(), 4L);
	}


	@Test
	public void putReplaceAndWeighEntries()
	{
		Cache<String, String> cache = new Cache<>(3);
		List<Cause> causes = new ArrayList<>();
		cache.setRemovalListener((key, value, cause) -> causes.add(cause));
		cache.setWeighter((key, value) -> (long)value.length());

		assertNull(cache.put("a", "aa"));
		assertNull(cache.put("b", "bb"));
		assertEquals(cache.keys(), List.of("b"));
		assertEquals(cache.getWeight(), 2L);
		assertEquals(causes, List.of(Cause.WEIGHT));

		assertEquals(cache.put("b", "xyz"), "bb");
		assertEquals(cache.peek("b"), "xyz");
		assertEquals(cache.getWeight(), 3L);
		assertEquals(causes, List.of(Cause.WEIGHT, Cause.REPLACED));
		assertEquals(cache.getWeighter().apply("key", "abc"), Long.valueOf(3));
	}


	@Test
	public void entryStoresItsValueAndWeight()
	{
		Cache.Entry<Integer, String> entry = new Cache.Entry<>();
		entry.set("value", 7);

		assertEquals(entry.value, "value");
		assertEquals(entry.weight, 7L);
		assertEquals(entry.accessed, entry.written);
		assertEquals(entry.toString(), "value");
	}


	@Test
	public void readsAndAccessOrder()
	{
		Cache<Integer, String> cache = new Cache<>();
		cache.put(1, "one");
		cache.put(2, "two");

		assertEquals(cache.peek(1), "one");
		assertTrue(cache.containsKey(1));
		assertEquals(cache.keys(), List.of(1, 2));
		assertEquals(cache.get(1), "one");
		assertEquals(cache.keys(), List.of(2, 1));
		assertFalse(cache.containsKey(3));
		assertEquals(cache.toString(), "{2=two, 1=one}");
	}


	@Test
	public void computeAndGetOrDefaultPopulateMissingEntries()
	{
		Cache<String, String> cache = new Cache<>();
		AtomicInteger providerCalls = new AtomicInteger();

		assertEquals(cache.computeIfAbsent("first", key -> {
			providerCalls.incrementAndGet();
			return "value";
		}), "value");
		assertEquals(cache.computeIfAbsent("first", key -> {
			providerCalls.incrementAndGet();
			return "other";
		}), "value");
		assertEquals(providerCalls.get(), 1);

		assertEquals(cache.getOrDefault("second", key -> "default"), "default");
		assertEquals(cache.peek("second"), "default");
		assertEquals(cache.getOrDefault("second", key -> "unused"), "default");
	}


	@Test
	public void countLimitEvictsEldestAndCanBeChanged()
	{
		Cache<Integer, String> cache = new Cache<>();
		List<Cause> causes = new ArrayList<>();
		cache.setRemovalListener((key, value, cause) -> causes.add(cause));
		cache.put(1, "one");
		cache.put(2, "two");
		cache.put(3, "three");

		cache.setMaximumCount(2);
		assertEquals(cache.keys(), List.of(2, 3));
		assertEquals(causes, List.of(Cause.COUNT));
		assertEquals(cache.setMaximumCount(1).keys(), List.of(3));
		assertEquals(cache.setMaximumCount(3).getMaximumCount(), 3L);
	}


	@Test
	public void explicitWeightLimitEvictsEldest()
	{
		Cache<Integer, String> cache = new Cache<>();
		List<Cause> causes = new ArrayList<>();
		cache.setRemovalListener((key, value, cause) -> causes.add(cause));
		cache.put(1, "one", 2);
		cache.put(2, "two", 2);

		cache.setMaximumWeight(3);
		assertEquals(cache.keys(), List.of(2));
		assertEquals(cache.getWeight(), 2L);
		assertEquals(causes, List.of(Cause.WEIGHT));
	}


	@Test
	public void removeManyClearAndQuietClear()
	{
		Cache<Integer, String> cache = new Cache<>();
		List<Cause> causes = new ArrayList<>();
		cache.setRemovalListener((key, value, cause) -> causes.add(cause));
		cache.put(1, "one", 2);
		cache.put(2, "two", 3);
		cache.put(3, "three", 4);

		assertEquals(cache.remove(2), "two");
		cache.removeMany(List.of(1, 3));
		assertTrue(cache.isEmpty());
		assertEquals(cache.getWeight(), 0L);
		assertEquals(causes, List.of(Cause.REMOVED, Cause.REMOVED, Cause.REMOVED));

		cache.put(4, "four", 5);
		cache.clear();
		assertEquals(causes.get(3), Cause.REMOVED);
		assertEquals(cache.getWeight(), 0L);

		cache.put(5, "five", 6);
		cache.clearQuietly();
		assertTrue(cache.isEmpty());
		assertEquals(cache.getWeight(), 0L);
		assertEquals(causes.size(), 4);
	}


	@Test
	public void removeEldestAndTrim()
	{
		Cache<Integer, String> cache = new Cache<>();
		cache.put(1, "one");
		cache.put(2, "two");
		cache.put(3, "three");

		assertSame(cache.trim(), cache);
		cache.removeEldest();
		assertEquals(cache.keys(), List.of(2, 3));
		assertEquals(cache.remove(2), "two");
		assertNull(cache.remove(2));
		assertEquals(cache.size(), 1);
	}


	@Test
	public void expiresByAccessAndWriteTime()
	{
		List<Cause> causes = new ArrayList<>();
		Cache<Integer, String> accessCache = new Cache<>();
		accessCache.setRemovalListener((key, value, cause) -> causes.add(cause));
		accessCache.put(1, "access");
		accessCache.setExpireAfterAccess(0);
		accessCache.trim();
		assertTrue(accessCache.isEmpty());
		assertEquals(causes, List.of(Cause.EXPIRED));

		Cache<Integer, String> writeCache = new Cache<>();
		writeCache.put(1, "old");
		writeCache.getEldestEntry().getValue().written = 0;
		writeCache.put(2, "new");
		writeCache.get(1);
		writeCache.setExpireAfterWrite(10_000);
		writeCache.trim();
		assertEquals(writeCache.keys(), List.of(2));
	}


	@Test
	public void backgroundWorkerCanStopAndRestart() throws InterruptedException
	{
		Cache<Integer, String> cache = new Cache<>();
		AtomicInteger removalCount = new AtomicInteger();
		CountDownLatch firstRemoval = new CountDownLatch(1);
		CountDownLatch secondRemoval = new CountDownLatch(1);
		cache.setRemovalListener((key, value, cause) -> {
			if (removalCount.incrementAndGet() == 1)
			{
				firstRemoval.countDown();
			}
			else
			{
				secondRemoval.countDown();
			}
		});

		try
		{
			cache.put(1, "first");
			cache.setExpireAfterAccess(0);
			cache.startBackgroundWorker(10);
			assertTrue(firstRemoval.await(2, TimeUnit.SECONDS));
			cache.stopBackgroundWorker();

			cache.setExpireAfterAccess(Long.MAX_VALUE);
			cache.put(2, "second");
			cache.setExpireAfterAccess(0);
			cache.startBackgroundWorker(10);
			assertTrue(secondRemoval.await(2, TimeUnit.SECONDS));
		}
		finally
		{
			cache.stopBackgroundWorker();
		}
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void rejectsNegativeConstructorWeight()
	{
		new Cache<String, String>(-1);
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void rejectsNegativeCount()
	{
		new Cache<>().setMaximumCount(-1);
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void rejectsNegativeExplicitWeight()
	{
		new Cache<>().put("key", "value", -1);
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void rejectsNegativeExpiration()
	{
		new Cache<>().setExpireAfterWrite(-1);
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void rejectsNegativeAccessExpiration()
	{
		new Cache<>().setExpireAfterAccess(-1);
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void rejectsNegativeMaximumWeight()
	{
		new Cache<>().setMaximumWeight(-1);
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void rejectsNonpositiveBackgroundRate()
	{
		new Cache<>().startBackgroundWorker(0);
	}
}