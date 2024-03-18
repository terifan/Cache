package org.terifan.util.cache;

import org.terifan.util.cache.Cache.Entry;


public interface ElementProvider<K, V>
{
	Entry<K, V> provide(K aKey);
}
