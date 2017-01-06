package com.pujjr.antifraud.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * @author tom
 *
 */
public class PropertyReadTest {

	/**
	 * tom 2017年1月3日
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties pops = new Properties();
		String path;
		path = PropertyReadTest.class.getClassLoader().getResource("").getPath();
		try {
			pops.load(new FileInputStream(new File(path+"//"+"sys.properties")));
			System.out.println(pops.get("userName"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(path);
	}

}
