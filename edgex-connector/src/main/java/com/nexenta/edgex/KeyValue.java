package com.nexenta.edgex;

import java.util.ArrayList;
import java.util.List;

public class KeyValue {
	public String key;
	public String value;

	public KeyValue() {
		super();
	}

	public KeyValue(String key, String value) {
		super();
		this.key = key;
		this.value = value;
	}

	public int length() {
		return key.length() + value.length() + 1;
	}

	public static List<KeyValue> toList(String ...arr) {
		ArrayList<KeyValue>res = new ArrayList<KeyValue>();
		for (int i = 0; i < arr.length; i += 2) {
			res.add(new KeyValue(arr[i], arr[i+1]));
		}
		return res;
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(key);
		sb.append(";");
		sb.append(value);
		return sb.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		KeyValue other = (KeyValue) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}


}
