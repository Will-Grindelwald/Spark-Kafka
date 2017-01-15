package ljg;

import java.util.ArrayList;
import java.util.List;

public class SensorFactory {
     private static List<Thread> list =new ArrayList<>() ;
     
     public static void   getSensors( int sensorNumber,long timeStamp ){
    	 for(int i =0;i<sensorNumber;i++){
         int sensorId = i;
    	 Thread sensor  = new Thread(new SendMessage(sensorId ,timeStamp));
    	 list.add(sensor);
    	 }
     }
     
     public static void  start(){
    	 if (list.size()!=0){
    		 for(Thread thread :list){
    			 
    			 thread.start();
    		 }
    		 
    	 }
    	 
     }
     
     
}
			 
