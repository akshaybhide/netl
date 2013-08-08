package com.digilant.mobile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.digilant.fastetl.FastUtil;
import com.digilant.fastetl.FileManager;
import com.digilant.fastetl.FastUtil.MyLogType;
import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.BidLogFormatException;
import com.adnetik.shared.Util.ExcName;

public class AggregationEngine {
	FileManager _fileMan;
	QAggregator _quatAgg;
	List<String> brokenfiles = new Vector<String>();
	public String machine;
	public String db;
	public String dest_table;
	public String info_table;
	ArrayList<String> mobilefiles = new ArrayList<String>();
	private String date;
	public static void main(String[] args) throws Exception
	{
		if(args.length < 3){
			System.out.println( "you need to pass date (as 2012-05-29) and the number of look back date \n");
			System.exit(1);
		}
		String configpath = args[0];
		String date = args[1];
		int lookback = Integer.parseInt(args[2]);
		AggregationEngine aggEng = new AggregationEngine("", "", "", "", date, lookback,false, configpath);
		
		
	}	
	public AggregationEngine(String machine, String db, String info_table, String dest_table, String date, int lookback, boolean onlylast15,  String path){
		_fileMan = new FileManager(path);
		_quatAgg = new QAggregator(machine, db, info_table, dest_table, _fileMan);
		this.machine = machine;
		this.db = db;
		this.dest_table = dest_table;
		this.info_table = info_table;
		this.date = date;

		try {
			GeneralDimension.init(machine,db,info_table);
			loadFromSaveData(date, lookback, onlylast15);
			startUp(date , lookback);
//			System.out.println("writing to Db\n");
//			_quatAgg.batchreport();
//			_fileMan.flushCleanList();
//			MoveToCurrent();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("problem with inserting ");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BidLogFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void cleanfraud(String date){
		_quatAgg.cleanfraud(date);
	}
	public void wrapup() throws SQLException{
		GeneralDimension.wrapup();
	}
	void loadFromSaveData(String date, int lookback, boolean onlylast15) throws  SQLException, IOException
	{
		
		
		Set<String> savedata = _fileMan.getAggPathSet(true);
		System.out.printf("Found %d save data paths\n", savedata.size());
		_fileMan.reloadFromSaveDir();
		boolean dateproc = _quatAgg.isDateAlreadyProcessed(date);
		//boolean dateproc2 = _quatAgg.isDateAlreadyProcessed(date);
		if(dateproc)
		{
			_fileMan.removeDateFromCleanList(date);
			HAggregator.RemoveData4Date(date);
		}
		//_quatAgg.reloadFromDB(date, lookback, onlylast15);
		
	}
	void MoveToCurrent() throws Exception
	{
		{
			String jnkpath = _fileMan.getJunkPath();
			FileUtils.createDirForPath(jnkpath);
			FileUtils.createDirForPath(jnkpath);
			File jnk = new File(jnkpath);
			File cur = new File(_fileMan.getBaseDir(true));
			File stg = new File(_fileMan.getBaseDir(false));
			
			// TODO: delete junk directories
			
			// Should be very fast operation
			boolean cur_renamed = cur.renameTo(jnk);
			if(!cur_renamed)
				Util.pf("problem rename to junk path : %s \n", jnk.getAbsoluteFile());
			boolean stg_renamed = stg.renameTo(cur);
			if(!stg_renamed)
				Util.pf("problem rename to current path\n");
			if(stg_renamed && cur_renamed)
				Util.pf("renamed directory %s --> %s\n", stg, cur);			
		}
	}	

	void startUp(String date, int lookback) throws Exception{
		MyLogType[] mlt = new MyLogType[]{MyLogType.imp, MyLogType.conversion, MyLogType.bid_all};
		//MyLogType[] mlt = new MyLogType[]{MyLogType.click};
		ExcName[] Exchanges = new ExcName[]{ExcName.admeld, ExcName.rtb, ExcName.nexage, ExcName.openx};
		for(ExcName e: Exchanges){
			ExcName[] en = new ExcName[]{e};
			for(MyLogType m : mlt){
				MyLogType[] mar = new MyLogType[]{m}; 
				Set<String> pathset = _fileMan.newFilesLookBack(date, lookback, mar, en);
				List<String> pathlist = new Vector<String>(pathset);
				//Collections.shuffle(pathlist);
				System.out.printf("Found %d new paths\n", pathset.size());
				startUp(pathlist, false, date, lookback);
				
			}
		}	
		mlt = new MyLogType[]{MyLogType.click};	
		_quatAgg.loadfraud(date);
		for(ExcName e: Exchanges){
			ExcName[] en = new ExcName[]{e};
			Set<String> pathset = _fileMan.newFilesLookBack(date, lookback, mlt, en);
			List<String> pathlist = new Vector<String>(pathset);
			//Collections.shuffle(pathlist);
			System.out.printf("Found %d new paths\n", pathset.size());
			startUp(pathlist, true, date, lookback);
		}
	}	
	void startUp(List<String> pathlist, boolean isclick, String date, int lookback) throws Exception
	{
		//MyLogType[] mlt = new MyLogType[]{MyLogType.click};
			//Collections.shuffle(pathlist);
			if(isclick)
				_quatAgg.setclick();
			else
				_quatAgg.unsetclick();
				
			for(int i = 0;i < pathlist.size() ; i++)
			{
				String onepath  = pathlist.get(i);
				processFile(onepath, 1);
				if(!isclick)
					_quatAgg.clearFraudData();
				if((i % 10) == 0)
				{
					System.out.printf("Done with file %d/%d\n", i, pathlist.size());
				}
				
				if(i > 5 && AggregationWrapper.mode.equals("test"))
					{ 
					//System.out.println("went over 5 files for test mode\n");
					break; 
					}
				if(_quatAgg._memMap.size() > _quatAgg.MaxMemSize){
					System.out.println("Data got too big ,writing to Db\n");
					_quatAgg.batchreport();
					_fileMan.flushCleanList();
/*					if(isclick){
						_quatAgg.savefraud(date);
					}*/
					MoveToCurrent();
					loadFromSaveData(date, lookback, false);
					
				}
			}
			Object[] copyofbrokenfiles = brokenfiles.toArray();
			for(int i = 0;i < copyofbrokenfiles.length ; i++){
				String onepath  = (String)copyofbrokenfiles[i];
				Util.pf("last attempt for file : %s , if doesn't work it is ignored till next 15 minutes\n", onepath);
				processFile(onepath, 11);
				
			}
			brokenfiles.clear();
			System.out.println("writing to Db\n");
			_quatAgg.batchreport();
			_fileMan.flushCleanList();
//			FileManager.writeFileLines(mobilefiles, "/home/armita/mobilefiles.log");
			if(isclick){
				_quatAgg.savefraud(date);
			}
			MoveToCurrent();
			//loadFromSaveData(date, lookback, false);
	}
	void processFile(String filepath, int tryno) throws BidLogFormatException, SQLException
	{
		
		// TODO: this is kind of ugly, shouldn't need to read the file twice.
		// Util.pf("Running for file %s\n", filepath);
		System.out.printf(".");
		
		try { 
			BufferedReader bread = FastUtil.getGzipReader(filepath);
			PathInfo pinfo = new PathInfo(filepath);
			if(IccAggregate(bread, pinfo))
				mobilefiles.add(filepath);
				
			bread.close();
			_fileMan.reportFinished(filepath);
			
		} catch (IOException ioex) { 
			//Sleep 1 second and try 10 times
			if (tryno > 10){
				brokenfiles.add(filepath);
				return;

			}
			else{
				try {
					Thread.sleep(5000);
					System.out.printf("Waiting 5 sec for file %s\n", filepath);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				processFile(filepath, ++tryno);
				
			}
		}		
		
	}
	private boolean IccAggregate(BufferedReader bread, PathInfo pinfo) throws IOException, BidLogFormatException
	{
		
		boolean filehasmobile = false;
		int lines = 0;
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			lines++;
			
			try {
				//this is basically a hack I am passing date because I know that the lookback values are always set in aggwrapper, if not we wouldn't be able to pass the date like that
				filehasmobile|=_quatAgg.processLogLine(oneline, pinfo, date);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}//usefullineitem++;
		}
		return filehasmobile;
		
		//Util.pf("useful lines for cookie :%d\n" , usefulcooki);
		//Util.pf("useful lines for lineitem :%d\n" , usefullineitem);
		
	}
	


}
