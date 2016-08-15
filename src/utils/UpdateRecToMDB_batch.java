//package utils;
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.InputStreamReader;
//import java.util.Calendar;
//import java.util.HashMap;
//import java.util.HashSet;
//
//import org.apache.commons.cli.CommandLine;
//import org.apache.commons.cli.CommandLineParser;
//import org.apache.commons.cli.Options;
//import org.apache.commons.cli.ParseException;
//import org.apache.commons.cli.PosixParser;
//
//
//public class UpdateRecToMDB_batch {
//
//
//	public static void main(String[]args) throws Exception
//	{
//		Options options = new Options();
//		options.addOption("data", true, "input");
//		options.addOption("suffix", true, "mdb Suffix");
//		options.addOption("environment", true, "test or online");
//		options.addOption("activeuser", true, "activeuser");
//		options.addOption("day", true, "day");
//
//		CommandLineParser parser = new PosixParser();
//		CommandLine commandline = parser.parse(options, args);
//
//		if (!commandline.hasOption("suffix"))
//		{
//			ParseException e = new ParseException("incorrect argument");
//			throw e;
//		}
//
//		String data = commandline.getOptionValue("data");
//		String suffix = commandline.getOptionValue("suffix");
//		String environment = commandline.getOptionValue("environment");
//		String activeuser = commandline.getOptionValue("activeuser");
//
//		int day = 1;
//		if(commandline.hasOption("day"))
//			day = Integer.parseInt(commandline.getOptionValue("day"));
//
//		HashSet<String> activeuserSet = null;
//		if(!activeuser.equals("-1"))
//		{
//			activeuserSet = new HashSet<String>();
//			BufferedReader dataReader = new BufferedReader(
//					new InputStreamReader(new FileInputStream(activeuser), "utf-8"));
//
//			String line = "";
//			while((line = dataReader.readLine()) != null)
//			{
//				activeuserSet.add(line.trim());
//			}
//			dataReader.close();
//
//		}
//
//		NkvMdb_short NkvMdb_short = null;
//		if(environment.equals("online"))
//			NkvMdb_short = new NkvMdb_short(true);
//		else
//			NkvMdb_short = new NkvMdb_short(false);
//
//
//		int start = (int) (System.currentTimeMillis() / 1000);
//		BufferedReader dataReader = new BufferedReader(
//				new InputStreamReader(new FileInputStream(data), "utf-8"));
//
//		String tag = "";
//		String list = "";
//		String line = "";
//		String uid= "";
//		String [] infos = null;
//		int count = 0;
//		//int expiretime =  2 * 24 * 60 * 60;
//		int expiretime = getTimeExpire(day);
//		int update_num = 50;
//
//		HashMap<String, String> updateMap = new HashMap<String, String> ();
//		while((line = dataReader.readLine()) != null)
//		{
//			infos = line.trim().split("\t");
//			uid = infos[0];
//			if(activeuserSet != null && !activeuserSet.contains(uid) )
//				continue;
//			tag = uid + suffix;
//			list = infos[1];
//			//NkvMdb_short.put(tag, list, expiretime);
//
//			updateMap.put(tag, list);
//
//			count ++;
//			if(count % update_num == 0)
//			{
//				NkvMdb_short.batchPut(updateMap, expiretime);
//				updateMap.clear();
//			}
//
//
//			if(  count % 10000 == 0)
//			{
//				//Thread.sleep(500);
//				if(count % 100000 == 0)
//					System.out.println(count);
//			}
//
//
//		}
//
//		NkvMdb_short.batchPut(updateMap, expiretime);
//
//		dataReader.close();
//
//		NkvMdb_short.closeClient();
//		int end = (int) (System.currentTimeMillis() / 1000);
//		System.out.println("update mdb data " + suffix + " end, lineNum:" + count + ", time: " + (end - start) + " s");
//
//	}
//	//当天时间
//	private static int getTimeExpire(int addDay)
//	{
//		Calendar calendar = Calendar.getInstance();
//		int hour = calendar.get(Calendar.HOUR_OF_DAY);
//		//int addDay = 1;
//
//		calendar.set(Calendar.MINUTE, 0);
//		calendar.set(Calendar.SECOND, 0);
//
//		calendar.set(Calendar.HOUR_OF_DAY, 1);
//		calendar.add(Calendar.DAY_OF_MONTH, addDay);
//
//		long currentTime = System.currentTimeMillis();
//
//		return (int) ((calendar.getTimeInMillis() - currentTime) / 1000);
//	}
//}
