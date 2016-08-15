//package utils;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.util.BitSet;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.log4j.Logger;
//
//import com.netease.backend.nkv.client.NkvClient.NkvOption;
//import com.netease.backend.nkv.client.Result;
//import com.netease.backend.nkv.client.ResultMap;
//import com.netease.backend.nkv.client.Result.ResultCode;
//import com.netease.backend.nkv.client.error.NkvException;
//import com.netease.backend.nkv.client.impl.DefaultNkvClient;
//
//public class NkvMdb_short {
//	private DefaultNkvClient client = null;
//	// test  online
//	private boolean  Online_Environment = false;
//	// test environment
//	private final String master_test = "10.120.148.135:8200";
//	private final String slave_test = "10.120.148.135:8500";
//	//  online environment
//	private final String master_online = "10.164.131.130:5198";
//	private final String slave_online = "10.164.131.131:5198";
//
////	private final String namespace = "cloudMusic";
//	private final short namespace = 501;
//	private final String group_name = "group_1";
//	private final long timeout = 5000;
//
//	static Logger logger = Logger.getLogger(NkvMdb_short.class.getClass().getName());
//	/**
//	 *
//	 * @param Online_Environment true:online/false:test
//	 */
//	public NkvMdb_short(boolean Online_Environment)
//	{
//		this.Online_Environment = Online_Environment;
//		getClient();
//	}
//	//获取mdb连接
//	private DefaultNkvClient getClient()
//	{
//		if(client == null)
//			return getInitClient();
//		else
//			return client;
//	}
//	//初始化连接
//	private DefaultNkvClient getInitClient()
//	{
//		client = new DefaultNkvClient();
//		String master = "";
//		String slave = "";
//		if(this.Online_Environment)
//		{
//			master = this.master_online;
//			slave = this.slave_online;
//		}
//		else
//		{
//			master = this.master_test;
//			slave = this.slave_test;
//		}
//		client.setMaster(master);
//		client.setSlave(slave);
//		client.setGroup(this.group_name);
//		try {
//			client.init();
//			return client;
//		} catch (NkvException e) {
//			logger.error("", e);
//			return null;
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//			return null;
//		}
//
//	}
//	//关闭连接，不需要关闭
//	public void closeClient()
//	{
//		if(client != null)
//			client.close();
//	}
//	/**
//	 *
//	 * @param o  实现Serializable的对象
//	 * @return byte[]
//	 */
//	private byte[]getBytes(Object o)
//	{
//		byte[] r = null;
//		try
//		{
//			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//	        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//	        objectOutputStream.writeObject(o);
//	        String serStr = byteArrayOutputStream.toString("ISO-8859-1");
//	        serStr = java.net.URLEncoder.encode(serStr, "UTF-8");
//
//	        objectOutputStream.close();
//	        byteArrayOutputStream.close();
//
//	        r=serStr.getBytes();
//		}catch (IOException e)
//		{
//			logger.error("", e);
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return r;
//	}
//	/**
//	 * @param bytes 必须是byte[]getBytes(Object o)生成的byte[]
//	 * @return Serializable对象
//	 */
//	private Object getObject(byte[] bytes)
//	{
//		Object o = null;
//		try
//		{
//			String redStr = java.net.URLDecoder.decode(new String(bytes), "UTF-8");
//			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(redStr.getBytes("ISO-8859-1"));
//	        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
//
//	        o = objectInputStream.readObject();
//
//	        objectInputStream.close();
//	        byteArrayInputStream.close();
//		}catch (IOException e)
//		{
//			logger.error("", e);
//		} catch (ClassNotFoundException e) {
//			logger.error("", e);
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return o;
//	}
//	/**
//	 *
//	 * @param bitSet
//	 * @return byte[]
//	 */
//	private byte[] bitSet2ByteArray(BitSet bitSet) {
//        byte[] bytes = new byte[bitSet.size() / 8];
//        for (int i = 0; i < bitSet.size(); i++) {
//            int index = i / 8;
//            int offset = 7 - i % 8;
//            bytes[index] |= (bitSet.get(i) ? 1 : 0) << offset;
//        }
//        return bytes;
//    }
//	/**
//	 *
//	 * @param bytes
//	 * @return BitSet
//	 */
//    private BitSet byteArray2BitSet(byte[] bytes) {
//        BitSet bitSet = new BitSet(bytes.length * 8);
//        int index = 0;
//        for (int i = 0; i < bytes.length; i++) {
//            for (int j = 7; j >= 0; j--) {
//                bitSet.set(index++, (bytes[i] & (1 << j)) >> j == 1 ? true
//                        : false);
//            }
//        }
//        return bitSet;
//    }
//    /**
//     * 存储字符串
//     * @param key
//     * @param value String
//     * @param expire mdb中数据的有效期,单位为秒
//     * @return
//     */
//	public boolean put(String key, String value, int expire)
//	{
//		boolean status = false;
//		NkvOption opt = new NkvOption(timeout);
//		if(expire > 0)
//			opt.setExpireTime(expire);
//
//		try {
//			Result<Void> r = client.put(this.namespace, key.getBytes(), value.getBytes(), opt);
//			if (r.getCode() == ResultCode.OK)
//				status = true;
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return status;
//	}
//	/**
//	 * 获取字符串
//	 * @param key
//	 * @return String
//	 */
//	public String get(String key)
//	{
//		String value = null;
//		try {
//			Result<byte[]> r = client.get(namespace, key.getBytes("utf-8"), new NkvOption(5000));
//			if(r.getCode() == ResultCode.OK)
//				value = new String(r.getResult(), "utf-8");
//		} catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return value;
//	}
//	/**
//     * 存储整数int
//     * @param key
//     * @param value int
//     * @param expire mdb中数据的有效期,单位为秒
//     * @return
//     */
//	public boolean putInt(String key, int value, int expire)
//	{
//		boolean status = false;
//		NkvOption opt = new NkvOption(timeout);
//		if(expire > 0)
//			opt.setExpireTime(expire);
//
//		try {
//
//			Result<Void> r = client.put(this.namespace, key.getBytes(), String.valueOf(value).getBytes(), opt);
//			if (r.getCode() == ResultCode.OK)
//				status = true;
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return status;
//	}
//	/**
//	 * 获取存储的整数int
//	 * @param key
//	 * @return int
//	 */
//	public Integer getInt(String key)
//	{
//		Integer value = null;
//		try {
//			Result<byte[]> r = client.get(namespace, key.getBytes(), new NkvOption(timeout));
//			if(r.getCode() == ResultCode.OK)
//				value = Integer.valueOf(new String(r.getResult()));
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return value;
//	}
//	/**
//	 * 存储BitSet
//	 * @param key
//	 * @param value BitSet
//	 * @param expire mdb中数据的有效期,单位为秒
//	 * @return
//	 */
//	public boolean put_BitSet(String key, BitSet value, int expire)
//	{
//		boolean status = false;
//		NkvOption opt = new NkvOption(timeout);
//		if(expire > 0)
//			opt.setExpireTime(expire);
//
//		try {
//			Result<Void> r = client.put(this.namespace, key.getBytes(), bitSet2ByteArray(value), opt);
//			if (r.getCode() == ResultCode.OK)
//				status = true;
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return status;
//	}
//	/**
//	 * 获取存储的bitset
//	 * @param key
//	 * @return BitSet
//	 */
//	public BitSet get_BitSet(String key)
//	{
//		BitSet value = null;
//		try {
//			Result<byte[]> r = client.get(namespace, key.getBytes(), new NkvOption(timeout));
//			if(r.getCode() == ResultCode.OK)
//				value = byteArray2BitSet(r.getResult());
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return value;
//	}
//	/**
//	 * 存储复杂对象；不建议存储String,Integer,BitSet，这些对象可调用其他函数
//	 * @param key
//	 * @param value Serializable对象
//	 * @param expire mdb中数据的有效期,单位为秒
//	 * @return
//	 */
//	public boolean put_ComplexDataType(String key, Object value, int expire)
//	{
//		boolean status = false;
//		NkvOption opt = new NkvOption(timeout);
//		if(expire > 0)
//			opt.setExpireTime(expire);
//
//		try {
//			Result<Void> r = client.put(this.namespace, key.getBytes(), getBytes(value), opt);
//			if (r.getCode() == ResultCode.OK)
//				status = true;
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return status;
//	}
//
//	/**
//	 * 获取复杂对象，存储方式必须为put_ComplexDataType
//	 * @param key
//	 * @return Serializable对象
//	 */
//	public Object get_ComplexDataType(String key)
//	{
//		Object value = null;
//		try {
//			Result<byte[]> r = client.get(namespace, key.getBytes(), new NkvOption(timeout));
//			if(r.getCode() == ResultCode.OK)
//				value = getObject(r.getResult());
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return value;
//	}
//
//	/**
//     * 存储字符串
//     * @param key
//     * @param value String
//     * @param expire mdb中数据的有效期,单位为秒
//     * @return
//     */
//	public boolean batchPut(Map<String, String> kv, int expire)
//	{
//		boolean status = false;
//		NkvOption opt = new NkvOption(timeout);
//		if(expire > 0)
//			opt.setExpireTime(expire);
//
//		Map<byte[], byte[]> putKV = new HashMap <byte[], byte[]>();
//		for(Map.Entry<String, String> e : kv.entrySet())
//		{
//			putKV.put(e.getKey().getBytes(), e.getValue().getBytes());
//		}
//
//		try {
//			ResultMap<byte[], Result<Void> > r = client.batchPut(this.namespace, putKV, opt);
//
//			if (r.getCode() == ResultCode.OK)
//				status = true;
//		}catch (Exception e) {
//			e.printStackTrace();
//			logger.error("error", e);
//		}
//
//		return status;
//	}
//
//}
