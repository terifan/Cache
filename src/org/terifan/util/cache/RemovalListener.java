package org.terifan.util.cache;


public interface RemovalListener<K, V>
{
	void elementRemoved(K aKey, V aValue, Cause aCause);

	enum Cause
	{
		/**
		 * removed by invoking a remove or clear method
		 */
		REMOVED,
		/**
		 * removed because the entry access or write time expired
		 */
		EXPIRED,
		/**
		 * removed because the cache maximum count was exceeded
		 */
		COUNT,
		/**
		 * removed because the cache maximum weight was exceeded
		 */
		WEIGHT,
		/**
		 * replaced by invoking the put method
		 */
		REPLACED
	}
}
