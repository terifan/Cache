package org.terifan.util.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.terifan.util.cache.RemovalListener.Cause;


/**
 * The class implements a cache with keys ordered in access order. The cache can have a weight, count and time limit per element. Adding one
 * item may result in multiple items being removed. A background worker can be started to automatically trim the cache from old elements.
 *
 * @param <K> the key type used by this cache
 * @param <V> the value type used by this cache
 */
public class Cache<K, V>
{
	private static Timer GLOBAL_TIMER;

	private final ReentrantReadWriteLock mLock;
	private final LinkedHashMap<K, Entry<K, V>> mMap;
	private RemovalListener<K, V> mRemovalListener;
	private BiFunction<K, V, Long> mWeighter;
	private long mExpireAfterAccess;
	private long mExpireAfterWrite;
	private long mMaximumWeight;
	private long mMaximumCount;
	private long mWeight;
	private TimerTask mTimerTask;


	/**
	 * Constructs a Cache with unlimited size, weight or time.
	 */
	public Cache()
	{
		this(Long.MAX_VALUE);
	}


	/**
	 * Constructs a Cache with a certain weight limit.
	 *
	 * @param aMaximumWeight the maximum weight of this Cache.
	 */
	public Cache(long aMaximumWeight)
	{
		mLock = new ReentrantReadWriteLock();
		mMap = new LinkedHashMap<>();

		mExpireAfterAccess = Long.MAX_VALUE;
		mExpireAfterWrite = Long.MAX_VALUE;
		mMaximumCount = Long.MAX_VALUE;
		mMaximumWeight = Long.MAX_VALUE;
		mWeighter = (K, V) -> 1L;

		mMaximumWeight = aMaximumWeight;
	}


	/**
	 * Start removing old elements every five seconds.
	 */
	public Cache<K, V> startBackgroundWorker(int aRate)
	{
		if (aRate <= 0)
		{
			throw new IllegalArgumentException();
		}

		synchronized (Cache.class)
		{
			if (GLOBAL_TIMER == null)
			{
				GLOBAL_TIMER = new Timer(true);
			}
		}
		mLock.writeLock().lock();
		try
		{
			if (mTimerTask == null)
			{
				mTimerTask = new TimerTask()
				{
					@Override
					public void run()
					{
						trim();
					}
				};
				GLOBAL_TIMER.schedule(mTimerTask, aRate, aRate);
			}
		}
		finally
		{
			mLock.writeLock().unlock();
		}
		return this;
	}


	/**
	 * Stops removing old elements.
	 */
	public Cache<K, V> stopBackgroundWorker()
	{
		mLock.writeLock().lock();
		try
		{
			if (mTimerTask != null)
			{
				mTimerTask.cancel();
			}
		}
		finally
		{
			mLock.writeLock().unlock();
		}
		return this;
	}


	/**
	 * Gets the RemovalListener or null if one isn't defined.
	 */
	public RemovalListener<K, V> getRemovalListener()
	{
		mLock.readLock().lock();
		try
		{
			return mRemovalListener;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * Sets a RemovalListener receiving events when items are removed from indirect operations.
	 */
	public Cache<K, V> setRemovalListener(RemovalListener<K, V> aRemovalListener)
	{
		mLock.writeLock().lock();
		try
		{
			mRemovalListener = aRemovalListener;
			return this;
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * @return the access expiration time of elements, default is Long.MAX_VALUE.
	 */
	public long getExpireAfterAccess()
	{
		mLock.readLock().lock();
		try
		{
			return mExpireAfterAccess;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * @param aExpireAfterAccess the maximum time an element may exist in the cache after accessed.
	 */
	public Cache<K, V> setExpireAfterAccess(long aExpireAfterAccess)
	{
		mLock.writeLock().lock();
		try
		{
			mExpireAfterAccess = aExpireAfterAccess;
			return this;
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * @return the write expiration time of elements, default is Long.MAX_VALUE.
	 */
	public long getExpireAfterWrite()
	{
		mLock.readLock().lock();
		try
		{
			return mExpireAfterWrite;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * @param aExpireAfterWrite the maximum time an element may exist in the cache after written.
	 */
	public Cache<K, V> setExpireAfterWrite(long aExpireAfterWrite)
	{
		mLock.writeLock().lock();
		try
		{
			mExpireAfterWrite = aExpireAfterWrite;
			return this;
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * @return the maximum number of elements in the map, default is Long.MAX_VALUE.
	 */
	public long getMaximumCount()
	{
		mLock.readLock().lock();
		try
		{
			return mMaximumCount;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * Sets the count limit of this Cache. Changing this value may cause elements to be evicted.
	 *
	 * @param aMaximumCount maximum number of elements in the map.
	 */
	public Cache<K, V> setMaximumCount(long aMaximumCount)
	{
		mLock.writeLock().lock();
		try
		{
			mMaximumCount = aMaximumCount;
			return this;
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * @return the maximum weight of elements in the map, default is Long.MAX_VALUE.
	 */
	public long getMaximumWeight()
	{
		mLock.readLock().lock();
		try
		{
			return mMaximumWeight;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * Sets the weight limit of this Cache. Changing this value may cause elements to be evicted.
	 *
	 * @param aMaximumWeight maximum weight of this map.
	 */
	public Cache<K, V> setMaximumWeight(long aMaximumWeight)
	{
		mLock.writeLock().lock();
		try
		{
			mMaximumWeight = aMaximumWeight;
			trimInternal();
		}
		finally
		{
			mLock.writeLock().unlock();
		}
		return this;
	}


	public BiFunction<K, V, Long> getWeighter()
	{
		return mWeighter;
	}


	public Cache<K, V> setWeighter(BiFunction<K, V, Long> aWeighter)
	{
		mWeighter = aWeighter;
		return this;
	}


	/**
	 * Associates the specified value with the specified key in this map.
	 * <p>
	 * This method invokes the weighter to get the weight of an object if a maximum weight has been specified. Otherwise all items assumes
	 * to have weight one.
	 *
	 * @param aKey Key with which the specified value is to be associated. Must not be null or zero length.
	 * @param aValue Value to be associated with the specified key.
	 * @return the previous value associated with specified key, or null if there was no mapping for key. A null return can also indicate
	 * that the map previously associated null with the specified key.
	 */
	public V put(K aKey, V aValue)
	{
		if (mMaximumWeight < Long.MAX_VALUE)
		{
			return put(aKey, aValue, mWeighter.apply(aKey, aValue));
		}
		return put(aKey, aValue, 1);
	}


	/**
	 * Associates the specified value with the specified key in this map.
	 *
	 * @param aKey Key with which the specified value is to be associated. Must not be null or zero length.
	 * @param aValue Value to be associated with the specified key.
	 * @param aWeight the size of the item.
	 * @return the previous value associated with specified key, or null if there was no mapping for key. A null return can also indicate
	 * that the map previously associated null with the specified key.
	 */
	public V put(K aKey, V aValue, long aWeight)
	{
		mLock.writeLock().lock();
		try
		{
			Entry<K, V> entry = mMap.get(aKey);
			V prevValue;

			if (entry != null)
			{
				mWeight += aWeight - entry.weight;
				prevValue = entry.value;
				if (mRemovalListener != null)
				{
					mRemovalListener.elementRemoved(aKey, prevValue, Cause.REPLACED);
				}
			}
			else
			{
				entry = new Entry();
				prevValue = null;
				mWeight += aWeight;
			}

			entry.set(aValue, aWeight);

			mMap.putLast(aKey, entry);

			trimInternal();

			return prevValue;
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	public V computeIfAbsent(K aKey, Function<K, V> aFunction)
	{
		V v = get(aKey);
		if (v == null)
		{
			v = aFunction.apply(aKey);
			put(aKey, v);
		}
		return v;
	}


	/**
	 * Returns the value to which the specified key is mapped in this identity hash map, or null if the map contains no mapping for this
	 * key. A return value of null does not necessarily indicate that the map contains no mapping for the key; it is also possible that the
	 * map explicitly maps the key to null. The containsKey method may be used to distinguish these two cases.<p>
	 *
	 * Getting a value will cause a reorder of items. The retrieved key will be moved to the top of the cache.
	 *
	 * @param aKey Key whose associated value is to be returned. Must not be null or zero length.
	 * @return The value to which this map maps the specified key, or null if the map contains no mapping for this key.
	 */
	public V get(K aKey)
	{
		return getOrDefault(aKey, null);
	}


	public V getOrDefault(K aKey, V aDefaultValue)
	{
		mLock.writeLock().lock();
		try
		{
			Entry<K, V> entry = mMap.get(aKey);

			if (entry != null)
			{
				entry.accessed = System.currentTimeMillis();
				mMap.putLast(aKey, entry);
				return entry.value;
			}

			return aDefaultValue;
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * Returns the value to which the specified key is mapped in this identity hash map, or null if the map contains no mapping for this
	 * key. A return value of null does not necessarily indicate that the map contains no mapping for the key; it is also possible that the
	 * map explicitly maps the key to null. The containsKey method may be used to distinguish these two cases.<p>
	 * <p>
	 * This method will not effect the order of items.
	 *
	 * @return the value to which this map maps the specified key, or null if the map contains no mapping for this key.
	 */
	public V peek(K aKey)
	{
		mLock.readLock().lock();
		try
		{
			Entry<K, V> entry = mMap.get(aKey);
			if (entry == null)
			{
				return null;
			}
			return entry.value;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * @return if this map contains a mapping for the specified key.
	 */
	public boolean containsKey(K aKey)
	{
		mLock.readLock().lock();
		try
		{
			boolean b = mMap.containsKey(aKey);
			if (b)
			{
				mMap.put(aKey, mMap.remove(aKey));
			}
			return b;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * @return the previous value associated with specified key, or null if there was no mapping for key.
	 */
	public V remove(K aKey)
	{
		mLock.writeLock().lock();
		try
		{
			return removeImpl(aKey, Cause.REMOVED);
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * @param aKeys removes many elements in a single transaction.
	 */
	public void removeMany(Collection<K> aKeys)
	{
		mLock.writeLock().lock();
		try
		{
			for (K key : aKeys)
			{
				removeImpl(key, Cause.REMOVED);
			}
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * @return the number of elements in this map.
	 */
	public int size()
	{
		mLock.readLock().lock();
		try
		{
			return mMap.size();
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * Performs trimming of the cache removing elements that has expired, are too large or too many.
	 */
	public Cache<K, V> trim()
	{
		mLock.writeLock().lock();
		try
		{
			trimInternal();
		}
		finally
		{
			mLock.writeLock().unlock();
		}
		return this;
	}


	/**
	 * @return the total weight of this map.
	 */
	public long getWeight()
	{
		mLock.readLock().lock();
		try
		{
			return mWeight;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * Removes all mappings from this map. This method iterates the contents of this cache removing items from the end.
	 */
	public void clear()
	{
		mLock.writeLock().lock();
		try
		{
			while (!mMap.isEmpty())
			{
				remove(getEldestEntry().getKey());
			}

			mWeight = 0;
			mMap.clear();
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * Removes all mappings from this map without calling the RemovalListener.
	 */
	public void clearQuietly()
	{
		mLock.writeLock().lock();
		try
		{
			mWeight = 0;
			mMap.clear();
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * Removes the eldest entry in the cache.
	 */
	public void removeEldest()
	{
		mLock.writeLock().lock();
		try
		{
			removeImpl(getEldestEntry().getKey(), Cause.REMOVED);
		}
		finally
		{
			mLock.writeLock().unlock();
		}
	}


	/**
	 * @return if this map is empty.
	 */
	public boolean isEmpty()
	{
		mLock.readLock().lock();
		try
		{
			return mMap.isEmpty();
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * @return an array of all keys in this map.
	 */
	public List<K> keys()
	{
		mLock.readLock().lock();
		try
		{
			return new ArrayList<>(mMap.keySet());
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	@Override
	public String toString()
	{
		return mMap.toString();
	}


	private V removeImpl(K aKey, Cause aCause)
	{
		Entry<K, V> entry = mMap.remove(aKey);

		if (entry == null)
		{
			return null;
		}

		mWeight -= entry.weight;

		if (mRemovalListener != null)
		{
			mRemovalListener.elementRemoved(aKey, entry.value, aCause);
		}

		return entry.value;
	}


	private void trimInternal()
	{
		while (mMap.size() > mMaximumCount)
		{
			removeImpl(getEldestEntry().getKey(), Cause.COUNT);
		}

		while (mWeight > mMaximumWeight)
		{
			removeImpl(getEldestEntry().getKey(), Cause.WEIGHT);
		}

		if (mExpireAfterAccess < Integer.MAX_VALUE)
		{
			long threshold = System.currentTimeMillis() - mExpireAfterAccess;

			while (!mMap.isEmpty())
			{
				Map.Entry<K, Entry<K, V>> last = getEldestEntry();
				if (last.getValue().accessed > threshold)
				{
					break;
				}
				removeImpl(last.getKey(), Cause.EXPIRED);
			}
		}

		if (mExpireAfterWrite < Integer.MAX_VALUE)
		{
			long threshold = System.currentTimeMillis() - mExpireAfterWrite;

			while (!mMap.isEmpty())
			{
				Map.Entry<K, Entry<K, V>> last = getEldestEntry();
				if (last.getValue().written > threshold)
				{
					break;
				}
				removeImpl(last.getKey(), Cause.EXPIRED);
			}
		}
	}


	/**
	 * @return the eldest entry
	 */
	protected Map.Entry<K, Entry<K, V>> getEldestEntry()
	{
		return mMap.firstEntry();
	}


	public static class Entry<K, V>
	{
		protected V value;
		protected long written;
		protected long accessed;
		protected long weight;


		public Entry()
		{
		}


		public void set(V aValue, long aWeight)
		{
			value = aValue;
			weight = aWeight;
			written = accessed = System.currentTimeMillis();
		}


		@Override
		public String toString()
		{
			return String.valueOf(value);
		}
	}


	public static void main(String... args)
	{
		try
		{
			Cache<Integer, String> cache = new Cache<>();
			cache
				.startBackgroundWorker(4000)
				.setMaximumCount(15)
				.setMaximumWeight(5)
				.setExpireAfterAccess(3000)
				.setExpireAfterWrite(5000)
				.setWeighter((k, v) -> 2L)
				//				.setElementProvider(new ElementProvider<Integer, String>()
				//				{
				//					@Override
				//					public Entry<Integer, String> provide(Integer aKey)
				//					{
				//						return new Entry<>("#" + aKey, 1);
				//					}
				//				})
				//				.setRefreshListener(new RefreshListener<Integer, String>()
				//				{
				//					@Override
				//					public Entry<Integer, String> refresh(Integer aKey)
				//					{
				//						return new Entry<>("#"+aKey, 1);
				//					}
				//				})
				.setRemovalListener(new RemovalListener<Integer, String>()
				{
					@Override
					public void elementRemoved(Integer aKey, String aValue, Cause aCause)
					{
						System.out.println("removed " + aCause + " " + aKey + "=" + aValue + ", " + cache.size() + ", " + cache.getWeight());
					}
				});
			cache.put(1, "-0", 1);
			cache.put(2, "-1", 1);
			cache.put(3, "-2", 1);
			cache.put(4, "-3", 1);
			cache.put(5, "-4", 1);
			cache.put(6, "-5", 1);

			cache.put(2, "b", 4);
			cache.get(1);
			cache.get(4);
			cache.put(1, "A", 3);
			System.out.println(cache);
			Thread.sleep(3_000);
			cache.put(3, "c", 2);
			cache.put(4, "d", 3);
			System.out.println(cache.put(3, "C", 2));
			cache.put(5, "e", 4);
			Thread.sleep(3_000);
			cache.put(6, "f", 3);
			cache.put(7, "g", 2);
			Thread.sleep(3_000);
			cache.put(8, "h", 4);
			cache.put(9, "i", 1);
			cache.remove(8);

			Thread.sleep(10_000);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
