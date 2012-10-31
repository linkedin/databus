package com.linkedin.databus.core;

import java.nio.charset.Charset;
import java.util.Arrays;
import org.apache.avro.util.Utf8;

public class DbusEventKey
{
	public enum KeyType
	{
		LONG,
		STRING
	}

	private final KeyType _keyType;
	private final Long _longKey;
	// TODO Remove this in favor of _strinKeyInBytes
	private final String _stringKey;
	private final byte[] _stringKeyInBytes;

	public DbusEventKey(long key)
	{
		_longKey = key;
		_stringKey = null;
		_keyType = KeyType.LONG;
		_stringKeyInBytes = null;
	}

	/**
	 * @deprecated Use the constructor with byte[] instead.
	 * @param key
	 */
	public DbusEventKey(String key)
	{
		_stringKey = key;
		_longKey = null;
		_keyType = KeyType.STRING;
		_stringKeyInBytes = null;
	}

	public DbusEventKey(byte[] key)
	{
		_keyType = KeyType.STRING;
		_longKey = null;
		_stringKey = null;
		_stringKeyInBytes = Arrays.copyOf(key, key.length);
	}

	public DbusEventKey(Object key)
			throws UnsupportedKeyException
	{
		if(key == null)
		{
			throw new IllegalArgumentException("Key cannot be null.");
		}

		if ((key instanceof Long) || (key instanceof Integer))
		{
			_longKey = ((Number)key).longValue();
			_stringKey = null;
			_keyType = KeyType.LONG;
			_stringKeyInBytes = null;
		}
		else if ((key instanceof String))
		{
			_longKey = null;
			_stringKey = (String) key;
			_keyType = KeyType.STRING;
			_stringKeyInBytes = null;
		}
		else if ((key instanceof Utf8))
		{
			_longKey = null;
			_stringKey = ((Utf8)key).toString();
			_keyType = KeyType.STRING;
			_stringKeyInBytes = null;
		}
		else if ((key instanceof byte[]))
		{
			_longKey = null;
			_stringKey = null;
			_keyType = KeyType.STRING;
			_stringKeyInBytes = Arrays.copyOf((byte[])key, ((byte[]) key).length);
		}
		else
		{
			throw new UnsupportedKeyException("Bad key type: " + key.getClass().getName());
		}
	}

	public KeyType getKeyType()
	{
		return _keyType;
	}
	public Long getLongKey()
	{
		return _longKey;
	}

	/**
	 * @deprecated Use getStringKeyInBytes() instead.
	 * For now, use this API only if DbusEventKey is constructed with a string.
	 * @return
	 */
	public String getStringKey()
	{
		if (_stringKey == null)
		{
			throw new RuntimeException("DbusEventKey was not constructed with a String");
		}
		return _stringKey;
	}

	/**
	 * Returns the key in a byte array.
	 * If the (deprecated) String-based constructor was used to construct the object
	 * then the UTF-8 representation of the string is returned.
	 */
	public byte[] getStringKeyInBytes()
	{
		if (_stringKeyInBytes != null)
			return Arrays.copyOf(_stringKeyInBytes, _stringKeyInBytes.length);
		if (_stringKey != null)
			return _stringKey.getBytes(Charset.forName("UTF-8"));
		throw new RuntimeException("Unsupported for non-string key types yet. Use getLongKey()");
	}

	@Override
	public String toString() {
		return "DbusEventKey [_keyType=" + _keyType + ", _longKey=" + _longKey
				+ ", _stringKey=" + (_stringKey != null ? _stringKey : new String(_stringKeyInBytes)) + "]";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_keyType == null) ? 0 : _keyType.hashCode());
		result = prime * result + ((_longKey == null) ? 0 : _longKey.hashCode());
		result = prime * result
				+ ((_stringKey != null) ? _stringKey.hashCode() : (_stringKeyInBytes == null) ? 0 :
						Arrays.hashCode(_stringKeyInBytes));
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DbusEventKey other = (DbusEventKey) obj;
		if (_keyType != other._keyType)
			return false;
		if (_longKey == null) {
			if (other._longKey != null)
				return false;
		} else if (!_longKey.equals(other._longKey))
			return false;
		if (_stringKey == null) {
			if (other._stringKey != null)
				return false;
		} else if (!_stringKey.equals(other._stringKey))
			return false;
		if (_stringKeyInBytes == null) {
			if (other._stringKeyInBytes != null)
				return false;
		} else if (!Arrays.equals(_stringKeyInBytes, other._stringKeyInBytes))
			return false;
		return true;
	}
}
