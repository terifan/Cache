package org.terifan.util.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.terifan.util.cache.RemovalListener.Cause;


/**
 * The class implements a cache with keys ordered in access order. The cache can have a weight, count and time limit per element.
 * Adding one item may result in multiple items being removed. A background worker can be started to automatically trim the cache from old
 * elements.
 *
 * @param <K> the key type used by this cache
 * @param <V> the value type used by this cache
 */
public class Cache<K, V>
{
	private static Timer mSharedTimer;

	private final ReentrantReadWriteLock mLock;
	private final LinkedHashMap<K, Entry<K, V>> mMap;
	private RemovalListener<K, V> mRemovalListener;
//	private ElementProvider<K, V> mElementProvider;
	private long mTimeLimit;
	private long mWeightLimit;
	private long mCountLimit;
	private long mWeight;
	private boolean mReduceOverTime;
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
	 * @param aWeightLimit the maximum weight of this Cache.
	 */
	public Cache(long aWeightLimit)
	{
		mLock = new ReentrantReadWriteLock();
		mMap = new LinkedHashMap<>();

		mTimeLimit = Long.MAX_VALUE;
		mCountLimit = Long.MAX_VALUE;
		mWeightLimit = Long.MAX_VALUE;

		mWeightLimit = aWeightLimit;
	}


	/**
	 * @return if items are removed over time.
	 */
	public boolean isReduceOverTimeEnabled()
	{
		return mReduceOverTime;
	}


	/**
	 * Enable the automatic removal of eldest element each time the background worker runs.
	 *
	 * @param aEnabled if the reducer should be enabled.
	 */
	public Cache<K, V> setReduceOverTimeEnabled(boolean aEnabled)
	{
		mReduceOverTime = aEnabled;
		return this;
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
			if (mSharedTimer == null)
			{
				mSharedTimer = new Timer(true);
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
						if (shrinkImpl() == 0 && mReduceOverTime)
						{
							mLock.writeLock().lock();
							try
							{
								if (!mMap.isEmpty())
								{
									removeImpl(mMap.keySet().iterator().next(), Cause.REDUCED);
								}
							}
							finally
							{
								mLock.writeLock().unlock();
							}
						}
					}
				};
				mSharedTimer.schedule(mTimerTask, aRate, aRate);
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


//	public ElementProvider<K, V> getElementProvider()
//	{
//		return mElementProvider;
//	}
//
//
//	public Cache<K, V> setElementProvider(ElementProvider<K, V> aElementProvider)
//	{
//		mElementProvider = aElementProvider;
//		return this;
//	}


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
	 * @return the time limit of elements, default is Long.MAX_VALUE.
	 */
	public long getTimeLimit()
	{
		mLock.readLock().lock();
		try
		{
			return mTimeLimit;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * @param aTimeLimit the maximum time an element may exist in the cache.
	 */
	public Cache<K, V> setTimeLimit(long aTimeLimit)
	{
		mLock.writeLock().lock();
		try
		{
			mTimeLimit = aTimeLimit;
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
	public long getCountLimit()
	{
		mLock.readLock().lock();
		try
		{
			return mCountLimit;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * Sets the count limit of this Cache. Changing this value may cause elements to be evicted.
	 *
	 * @param aCountLimit maximum number of elements in the map.
	 */
	public Cache<K, V> setCountLimit(long aCountLimit)
	{
		mLock.writeLock().lock();
		try
		{
			mCountLimit = aCountLimit;
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
	public long getWeightLimit()
	{
		mLock.readLock().lock();
		try
		{
			return mWeightLimit;
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	/**
	 * Sets the weight limit of this Cache. Changing this value may cause elements to be evicted.
	 *
	 * @param aWeightLimit maximum weight of this map.
	 */
	public Cache<K, V> setWeightLimit(long aWeightLimit)
	{
		mLock.writeLock().lock();
		try
		{
			mWeightLimit = aWeightLimit;
			shrinkImpl();
		}
		finally
		{
			mLock.writeLock().unlock();
		}
		return this;
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
			V prevValue;

			if (mMap.containsKey(aKey))
			{
				Entry<K, V> entry = mMap.remove(aKey);
				mMap.put(aKey, entry);

				prevValue = entry.value;

				mWeight += aWeight - entry.weight;

				entry.value = aValue;
				entry.weight = aWeight;
				entry.added = System.currentTimeMillis();
			}
			else
			{
				prevValue = null;

				Entry entry = new Entry(aValue, aWeight);

				mWeight += aWeight;
				mMap.put(aKey, entry);
			}

			shrinkImpl();

			return prevValue;
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
	 *
	 * Getting a value will cause a reorder of items. The retrieved key will be moved to the top of the cache.
	 *
	 * @param aKey Key whose associated value is to be returned. Must not be null or zero length.
	 * @return The value to which this map maps the specified key, or null if the map contains no mapping for this key.
	 */
	public V get(K aKey)
	{
		mLock.readLock().lock();
		try
		{
			return getOrDefault(aKey, null);
		}
		finally
		{
			mLock.readLock().unlock();
		}
	}


	public V getOrDefault(K aKey, V aDefaultValue)
	{
		mLock.readLock().lock();
		try
		{
			Entry<K, V> entry = mMap.remove(aKey);

//			if (entry == null && mElementProvider != null)
//			{
//				entry = mElementProvider.provide(aKey);
//			}

			if (entry != null)
			{
				mMap.put(aKey, entry);
				return entry.value;
			}

			return aDefaultValue;
		}
		finally
		{
			mLock.readLock().unlock();
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
				remove(mMap.keySet().iterator().next());
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
			throw new NoSuchElementException();
		}

		mWeight -= entry.weight;

		if (mRemovalListener != null)
		{
			mRemovalListener.elementRemoved(aKey, entry.value, aCause);
		}

		return entry.value;
	}


	private int shrinkImpl()
	{
		int count = 0;

		while (mMap.size() > mCountLimit)
		{
			removeImpl(mMap.keySet().iterator().next(), Cause.COUNT);
			count++;
		}

		while (mWeight > mWeightLimit)
		{
			removeImpl(mMap.keySet().iterator().next(), Cause.WEIGHT);
			count++;
		}

		if (mTimeLimit < Integer.MAX_VALUE)
		{
			long threshold = System.currentTimeMillis() - mTimeLimit;

			while (!mMap.isEmpty())
			{
				K last = mMap.keySet().iterator().next();
				Entry entry = mMap.get(last);
				if (entry == null || entry.added > threshold)
				{
					break;
				}
				removeImpl(last, Cause.EXPIRED);
				count++;
			}
		}

		return count;
	}


	public static class Entry<K, V>
	{
		protected V value;
		protected long added;
		protected long weight;


		public Entry()
		{
		}


		public Entry(V aValue, long aWeight)
		{
			added = System.currentTimeMillis();
			value = aValue;
			weight = aWeight;
		}


		@Override
		public String toString()
		{
			return "" + value;
		}
	}

	
	public static void main(String... args)
	{
		try
		{
			Cache<Integer, String> cache = new Cache<>();
			cache
				.startBackgroundWorker(4000)
				.setCountLimit(15)
				.setWeightLimit(5)
				.setTimeLimit(3000)
				.setReduceOverTimeEnabled(true)
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
			cache.put(1, "a", 2);
			cache.put(2, "b", 4);
			cache.get(1);
			cache.get(4);
			cache.put(1, "A", 3);
			System.out.println(cache);
			Thread.sleep(3_000);
			cache.put(3, "c", 2);
			cache.put(4, "d", 3);
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
