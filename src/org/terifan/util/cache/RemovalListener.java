package org.terifan.util.cache;


public interface RemovalListener<K, V>
{
	void elementRemoved(K aKey, V aValue, Cause aCause);

	enum Cause
	{
		/**
		 * removed by user code
		 */
		REMOVED,
		/**
		 * background worker reduced the size of cache
		 */
		REDUCED,
		/**
		 * removed because the entry time limit reached
		 */
		EXPIRED,
		/**
		 * removed because the cache exceeded it's count limit
		 */
		COUNT,
		/**
		 * removed because the cache exceeded it's weight limit
		 */
		WEIGHT
	}
}
