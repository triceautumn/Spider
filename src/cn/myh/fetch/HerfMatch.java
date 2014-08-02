package cn.myh.fetch;

import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.util.ArrayList;

public class HerfMatch
{
   public static void main(String[] args)
   {
	   String urlString = "http://www.sohu.com";
	   /*ArrayList<String> urls = new ArrayList<String>();
	   urls = FindURL(urlString);
	   for(int i=0;i<urls.size();i++){
		   System.out.println(urls.get(i));
	   }     */
	   try {
		String s=getContent(urlString);
		System.out.println(s);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   
   public static String getContent(String urlString) throws IOException {
	   URL url=new URL(urlString);
	   if(url==null)
		   return null;
	   InputStreamReader in=new InputStreamReader(url.openStream());
	   StringBuffer input=new StringBuffer();
	   int ch;
	   while ((ch = in.read()) != -1) 
		   input.append((char)ch);
	   return input.toString();
   }
   
   //获取页面上的url
   public static ArrayList<String> FindURL(String urlString){
	   ArrayList<String> result = new ArrayList<String>();

	   try
	      {
		   			URL url=new URL(urlString);
		   			if(url==null)
		   					return null;
	         InputStreamReader in = new InputStreamReader(url.openStream());
	         StringBuffer input = new StringBuffer();
	         int ch;
	         while ((ch = in.read()) != -1) input.append((char)ch);
	         String patternString 
	    = "<a\\s+href\\s*=\\s*(\"[^\"]*\"|[^\\s>]|\"[^\"]*\"\\s*+[^>\"]*\\s*=\\s*(\"[^\"]*\"|[^\\s>]))\\s*>";
	            Pattern pattern = Pattern.compile(patternString,
	            Pattern.CASE_INSENSITIVE);
	         Matcher matcher = pattern.matcher(input);
	         while (matcher.find())
	         {
	            int start = matcher.start();
	            int end = matcher.end();
	            String match = input.substring(start, end);
	            if(match.indexOf("http") != -1){
	            	match = match.substring(match.indexOf("http"));
	            	match = match.substring(0 ,match.indexOf("\""));
	            }else{
	            	match = match.substring(match.indexOf("\"")+1);
	            	match = match.substring(0, match.indexOf("\"") );
	            	String tmp = match;
	            	match = urlString;
	            	match = match.concat(tmp);
	            }
	            result.add(match);	         }
	      }
	      catch (IOException exception)
	      {
	         exception.printStackTrace();
	      }
	      catch (PatternSyntaxException exception)
	      {
	         exception.printStackTrace();
	      }
	return result;
   }
   
}
