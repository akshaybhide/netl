
package com.adnetik.pricing;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;

public class BasicLinReg
{
	List<ImpFeature> feats = Util.vector();
	
	List<Double> waits = Util.vector();
	
	public double getBase(BidLogEntry ble)
	{
		return Util.getWinnerPriceCpm(ble);
	}
	
	public double getResidual(BidLogEntry ble)
	{
		double res = Util.getWinnerPriceCpm(ble);
		
		for(int i = 0; i < feats.size(); i++)
		{
			res -= feats.get(i).evali(ble) * waits.get(i);
		}

		return res;
	}
	
	void addFeature(ImpFeature impf, double w)
	{
		feats.add(impf);
		waits.add(w);
	}

}
