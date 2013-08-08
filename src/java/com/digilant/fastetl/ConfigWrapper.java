package com.digilant.fastetl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.adnetik.shared.Util;

public class ConfigWrapper {
	public static Properties configFile = new Properties();
	public ConfigWrapper(String path) {
		try{
//			configFile.load(new FileInputStream("/home/armita/myworkspace/FastETLPrj/config.properties"));
			configFile.load(new FileInputStream(path));
		}catch(IOException e){
			Util.pf("Config file not found\n");
			System.exit(1);
		}
	}
	public String getICCSrc(){
		String my_value = configFile.getProperty("ICC_SRC");
		return my_value;
	}
	public String getPixelSrc(){
		String my_value = configFile.getProperty("PIXEL_SRC");
		return my_value;
	}
	public String getDest(){
		String my_value = configFile.getProperty("DEST");
		return my_value;
		
	}
	public static void main(String[] args) throws IOException{
		ConfigWrapper cw = new ConfigWrapper("/home/armita/myworkspace/FastETLPrj/config.properties");
		Util.pf(cw.getPixelSrc());
	}
}
