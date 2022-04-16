package no.hvl.dat110.util;

/**
 * project 3
 * @author tdoy
 *
 */

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash { 
	
	private static BigInteger hashint; 
	
	public static BigInteger hashOf(String entity) {		
		
		
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(entity.getBytes());
			
			byte[] digest = md.digest();
			
		    String hash = Hash.toHex(digest);
		    
		    hashint = new BigInteger(hash,16);
		    
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return hashint;
	}
	
	public static BigInteger addressSize() {
		
		Integer bits = Hash.bitSize();
		
		BigInteger addressSize = BigInteger.valueOf((long) Math.pow(2, bits));
		
		return addressSize;
	}
	
	public static int bitSize() {
		
		int digestlen = 0;
		
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
			digestlen = md.getDigestLength();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// find the digest length
		
		return digestlen*8;
	}
	
	public static String toHex(byte[] digest) {
		StringBuilder strbuilder = new StringBuilder();
		for(byte b : digest) {
			strbuilder.append(String.format("%02x", b&0xff));
		}
		return strbuilder.toString();
	}

}
