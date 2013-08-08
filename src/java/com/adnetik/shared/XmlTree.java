
package com.adnetik.shared;

import java.io.*;
import java.util.*;

// SAX classes.
import org.xml.sax.*;
import org.xml.sax.helpers.*;
//JAXP 1.1
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.stream.*;
import javax.xml.transform.sax.*;


// Taken from DCB's personal library!! Use with caution
public class XmlTree
{
	String code;
	String text;
	
	XmlTree parent;
	
	Map<String, String> atts = new TreeMap<String, String>();
	
	private TreeMap<Integer, XmlTree> _kidMap = Util.treemap();
	
	public XmlTree()
	{
		this("rootdoc");	
	}
	
	public XmlTree(String c)
	{
		code = c;
	}
	
	public XmlTree(String c, String t)
	{
		code = c;
		text = t;
	}
	
	public void addChild(XmlTree t)
	{
		// Kids or text
		if(text != null)
		{
			System.out.printf("\nText %s present on addChild for node %s",
				text, code);
			
		}
		
		Util.massert(text == null);
		
		int newid = _kidMap.isEmpty() ? 0 : _kidMap.lastKey()+1;
		_kidMap.put(newid, t);
		
		Util.massert(t.parent == null, "Child already has a parent");
		t.parent = this;
	}
	
	public void adoptChild(XmlTree child)
	{
		XmlTree prevpar = child.parent;
		Util.massert(prevpar != null);
		
		prevpar.removeChild(child);
		addChild(child);
	}
	
	public void removeChild(XmlTree t)
	{
		t.parent = null;
		
		for(int kidid : _kidMap.keySet())
		{
			if(_kidMap.get(kidid) == t)
			{ 
				_kidMap.remove(kidid); 
				return;
			}
		}
		
		Util.massert(false, "Could not find child to remove in list of children, NB you must use exact reference!");
	}
	
	public Set<String> getAttSet()
	{
		return atts.keySet();
		
	}
	
	public String getAtt(Enum k)
	{
		return getAtt(k.toString());
	}
	
	public String getAtt(String k)
	{
		return atts.get(k);	
	}
	
	public void setAtt(Enum k, String v)
	{
		setAtt(k.toString(), v);
	}
	
	public void setAtt(String k, String v)
	{
		atts.put(k, v);
	}
	
	public Map<String, String> getAttMap()
	{
		return Collections.unmodifiableMap(atts);
	}
	
	public String getCode() { return code; }
	public String getText() { return text; }
	
	public void setText(String t)
	{
		String toset = t.trim();
		if(toset.length() == 0)
			{ return; }
		
		// Can have either text or kids, not both
		Util.massert(_kidMap.isEmpty(), "Can have either text or kids but not both, text is %s", toset);
		text = toset;
	}
	
	public List<XmlTree> getChildren()
	{
		return new Vector<XmlTree>(_kidMap.values());
	}
	
	public String get(String k)
	{
		return atts.get(k);
		
	}
	
	public int numLeafs()
	{
		if(_kidMap.size() == 0)
			{ return 1; } // myself
		
		int lcount = 0;
		
		for(XmlTree kid : _kidMap.values())
			{ lcount += kid.numLeafs(); }
		
		return lcount;
	}
	
	public List<XmlTree> getLeafs()
	{
		LinkedList<XmlTree> res = Util.linkedlist();
		
		if(_kidMap.isEmpty())
			{ res.add(this); }
		
		for(XmlTree kid : _kidMap.values())
			{ res.addAll(kid.getLeafs()); }
		
		return res;
	}
	
	public String toJson()
	{
		List<Pair<Integer, String>> jsonlist = Util.vector();
		popJsonDepthList(this, jsonlist, 0);
		
		StringBuffer sb = new StringBuffer();
		for(Pair<Integer, String> onepair : jsonlist)
		{
			sb.append(getTabStr(onepair._1));
			sb.append(onepair._2);
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	private static void popJsonDepthList(XmlTree xtree, List<Pair<Integer, String>> jsonlist, int depth)
	{
		jsonlist.add(Pair.build(depth, "{"));
		
		jsonlist.add(Pair.build(depth+1, Util.sprintf("nodecode: \"%s\",", xtree.getCode())));
		
		List<String> attlist = new Vector<String>(xtree.getAttSet());
		for(int i : Util.range(attlist.size()))
		{
			String att = attlist.get(i);
			String dispval = escapeQuoteString(xtree.getAtt(att));
			String kv = Util.sprintf("%s : \"%s\",", att, dispval);
			jsonlist.add(Pair.build(depth+1, kv));
		}
		
		jsonlist.add(Pair.build(depth+1, "kids: ["));
		
		List<XmlTree> kids = xtree.getChildren();
		
		for(int i : Util.range(kids.size()))
		{
			popJsonDepthList(kids.get(i), jsonlist, depth+1);
			
			if(i < kids.size()-1)
				{ jsonlist.add(Pair.build(depth+1, ",")); }
		}
		jsonlist.add(Pair.build(depth+1, "]"));
		jsonlist.add(Pair.build(depth, "}"));
	}
	
	private static String getTabStr(int depth)
	{
		String tabstr = "";
		while(tabstr.length() < depth)
			{ tabstr = tabstr + "\t"; }
		return tabstr;		
	}
	
	public int height()
	{
		int d = 0;
		
		for(XmlTree xt : _kidMap.values())
		{
			int kheight = xt.height();			
			d = (d < kheight+1 ? kheight+1 : d);
		}
		
		return d;
	}
	
	public int totalSize()
	{
		int t = 1;
		
		for(XmlTree xt : _kidMap.values())
		{
			t += xt.totalSize();
		}
		
		return t;
	}
	
	public XmlTree getParent() { return parent; }
	
	public void writeXml(String fpath)
	{
		try {
			FileOutputStream fos = new FileOutputStream(new File(fpath));
			writeXml(fos);
			fos.close();
		} catch (Exception ex) {
			
			throw new RuntimeException(ex);
		}
	}
	
	public Double getD(String att)
	{
		String s = get(att);
		return (s == null ? null : Double.valueOf(s));
	}
	
	public Integer getI(String att)
	{
		String s = get(att);
		return (s == null ? null : Integer.valueOf(s));
	}
	
	public void setI(String att, Integer value)
	{
		setAtt(att, ""+value);
	}
	
	public static <K, V> XmlTree fromMap(Map<K, V> targmap)
	{
		XmlTree root = new XmlTree("maproot");
		
		for(K k : targmap.keySet())
		{
			XmlTree entry = new XmlTree("entry");
			entry.setAtt("key", k.toString());
			entry.setAtt("val", targmap.get(k).toString());
			root.addChild(entry);
		}
		
		return root;
	}
	
	public void writeXml(OutputStream outA) throws Exception
	{
		SAXTransformerFactory tf = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
		tf.setAttribute("indent-number", new Integer(4));
		
		// SAX2.0 ContentHandler.
		TransformerHandler hd = tf.newTransformerHandler();
		Transformer serializer = hd.getTransformer();
		serializer.setOutputProperty(OutputKeys.ENCODING,"UTF-8");
		//serializer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM,"users.dtd");
		serializer.setOutputProperty(OutputKeys.INDENT,"yes");
		
		// Random shit necessary to 
		serializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
		
		StreamResult streamResult = new StreamResult(outA);
		hd.setResult(streamResult);
		hd.startDocument();
		
		recWriteNode(hd);
		
		hd.endDocument();	
	}
	
	private void recWriteNode(TransformerHandler hd) throws SAXException
	{
		
		AttributesImpl gdata = new AttributesImpl();
		
		for(String k : atts.keySet())
		{
			gdata.addAttribute("", "", k, "CDATA", atts.get(k));
		}
		
		hd.startElement("","", code, gdata);
		
		Util.massert(_kidMap.isEmpty() || text == null);
		
		if(text != null)
		{
			char[] sdata = text.toCharArray();
			hd.characters(sdata, 0, sdata.length);
		}
		
		for(XmlTree xt : _kidMap.values())
		{
			xt.recWriteNode(hd);
		}
		
		hd.endElement("","", code);
	}
	
	public int numKids()
	{
		return _kidMap.size();	
	}
	
	
	public boolean smartEquals(XmlTree that, boolean checkCode, boolean checkText, boolean checkAtts)
	{
		if(checkCode)
		{
			if(!this.code.equals(that.code))
				return false;
		}
		
		if(checkText)
		{
			if((this.text == null) != (that.text == null))
				{ return false; }
			
			if(this.text != null && !this.text.equals(that.text))
				{ return false; }
		}
		
		if(checkAtts)
		{
			if(this.atts.size() != that.atts.size())
				return false;
			
			for(String k : this.atts.keySet())
			{
				if(!that.atts.containsKey(k))
					return false;
				
				if(!atts.get(k).equals(that.atts.get(k)))
					return false;
			}
		}
		
		
		if(this.numKids() != that.numKids())
			return false;
		
		{
			List<XmlTree> thislist = this.getChildren();
			List<XmlTree> thatlist = that.getChildren();
			
			for(int i = 0; i < thislist.size(); i++)
			{
				XmlTree a = thislist.get(i);
				XmlTree b = thatlist.get(i);
				
				if(!a.smartEquals(b, checkCode, checkText, checkAtts))
					return false;
			}			
			
		}
		
		
		return true;
	}	
	
	public boolean equals(XmlTree that)
	{
		// Check everything
		return smartEquals(that, true, true, true);
	}	
	
	
	
	
	public static XmlTree loadFromXml(String fpath)
	{
		try {
			FileInputStream fis = new FileInputStream(fpath);
			XmlTree xt = loadFromXml(fis);
			fis.close();
			
			return xt;
			
		} catch(IOException ioe) {
			
			throw new RuntimeException(ioe);
		}
	}
	
	public static XmlTree loadFromXml(InputStream is)
	{
		try {
			
			boolean validating = false;
			//...
			SAXParserFactory factory = SAXParserFactory.newInstance();
			factory.setValidating(validating);
			SAXParser parser = factory.newSAXParser();
			//...
			
			XmlTreeHandler xth = new XmlTreeHandler();
			
			parser.parse(is, xth);
			
			return xth.getResult();
			
		} catch(Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	
	public static class XmlTreeHandler extends DefaultHandler
	{
		XmlTree curnode;
		
		
		boolean complete = false;
		
		public XmlTreeHandler()
		{
		}
		
		public void characters(char[] cdata, int start, int length)
		{
			String t = new String(cdata, start, length);
			
			if(curnode.text == null && t.trim().length() == 0)
				return;
			
			String nt = curnode.text == null ? t : curnode.text + t;
			
			curnode.setText(nt);
		}
		
		public XmlTree getResult()
		{
			Util.massert(complete);
			return curnode;
		}
		
		@Override
		public void startElement(String uri, String localName, String qName, Attributes att) 
		{
			Util.massert(!complete);
			
			if(curnode == null) {
				curnode = new XmlTree(qName);
			} else {
				
				XmlTree kid = new XmlTree(qName);
				curnode.addChild(kid);
				curnode = kid;
			}
			
			for(int i = 0; i < att.getLength(); i++)
			{
				curnode.setAtt(att.getQName(i), att.getValue(i));
			}
		}
		
		@Override
		public void endElement(String uri, String localName, String qName) 
		{
			Util.massert(curnode.code.equals(qName));
			
			if(curnode.getParent() == null)
			{
				complete = true;
			} else {
				curnode = curnode.getParent();
			}			
		}
	}	
	
	public static String escapeQuoteString(String s)
	{
		StringBuffer sb = new StringBuffer();
		
		for(int i = 0; i < s.length(); i++)
		{
			char c = s.charAt(i);
			
			if(c == '"')
				{ sb.append("\\\""); }
			else
				{ sb.append(c); }
		}
		return sb.toString();
	}
	
	public static void main(String[] args) throws Exception
	{
		String strwithquote = "\"Dan burfoot is cool\"";
		
		Util.pf("%s\n", strwithquote);
		
		String strwoquote = escapeQuoteString(strwithquote);
		
		Util.pf("%s\n", strwoquote);
	}
}
