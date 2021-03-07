package com.app;

import java.text.SimpleDateFormat;


import com.phidget22.*;
public class RFID0 {
	
	 	private static RFID rfid;
	    private static LCD lcd0;
	    public static RFIDTagLostListener onTagLost =
			new RFIDTagLostListener() {
			@Override
			public void onTagLost(RFIDTagLostEvent e) {
				 try {
					rfid.close();
				} catch (PhidgetException e1) {
					e1.printStackTrace();
				}
			}
		};
	    
		public static AttachListener onAttach =
				new AttachListener() 
		{
				@Override
				public void onAttach(AttachEvent e) {
			            System.out.println(e.toString());
				}
		};

	    public static DetachListener onDetach =
		new DetachListener() 
	    {
	    	@Override
	    	public void onDetach(DetachEvent e) {
			            System.out.println(e.toString());
			}
		};
		
	    public static ErrorListener onError =
			new ErrorListener() {
			@Override
			public void onError(ErrorEvent e) {
				System.out.println("in error");
				System.out.println("Code: " + e.getCode().name());
				System.out.println("Description: " + e.getDescription());

			}
		};
public static void main(String[] args) throws Exception {
	 try{
	        //Create the object for RFID and attach listeners
	            rfid = new RFID();
	            rfid.setDeviceSerialNumber(63468);
	            rfid.addAttachListener(onAttach);
	            rfid.addDetachListener(onDetach);
	            rfid.addTagListener(onTag);
	            rfid.addErrorListener(onError);
	            Thread.sleep(PhidgetBase.DEFAULT_TIMEOUT);
	       
	       //Create the object for LCD and attach listeners    
	            lcd0=new LCD();
	            lcd0.setDeviceSerialNumber(30686);
	            lcd0.addAttachListener(onAttach);
	            lcd0.addDetachListener(onDetach);
	       
	       //Open RFID with default timout     
	            rfid.open(PhidgetBase.DEFAULT_TIMEOUT);
	            Thread.sleep(PhidgetBase.DEFAULT_TIMEOUT);

	       //Open LCD with default timout	       
	            lcd0.open(PhidgetBase.DEFAULT_TIMEOUT);
	            Thread.sleep(PhidgetBase.DEFAULT_TIMEOUT);
	                
	       // Close all objects and exit
	            System.in.read();
	                rfid.close();
	                lcd0.close();
	        }
	        catch(PhidgetException e){
	            System.out.println(e.toString());
	        }

}

public static RFIDTagListener onTag =
new RFIDTagListener() {
@Override
public void onTag(RFIDTagEvent e) {
            try{

            	// clear the previous text
            	lcd0.clear();
//            	String timeStamp = new SimpleDateFormat("yyyy/MM/dd_HH:mm:ss").format(Calendar.getInstance().getTime());
            	
            	
            	// if a tag is produced/touched
            	if (rfid.getTagPresent()) {

            		// get the tag value
            		lcd0.setBacklight(1);
            		String tag_value=e.getTag();
            		String navigate=null;
            		
            		//compare the tag value with pre-evaluated codes and print accordingly 
            		if (tag_value.equalsIgnoreCase("01058ed1ac")) {
            			navigate="Home";
            		}
            		else if (tag_value.equalsIgnoreCase("010693444f")) {
            			navigate="Restaurant";
            		}
            		lcd0.writeText(com.phidget22.LCDFont.DIMENSIONS_6X12, 0, 0,"Navigate to :");
            		lcd0.writeText(com.phidget22.LCDFont.DIMENSIONS_6X12, 0, 1,navigate);
            		System.out.println(e.getTag());
            		
            		//flush the buffer to lcd
            		lcd0.flush();
            		
            		Thread.sleep(2000);
            	}
            	
            	// wait for sometime before another action
                Thread.sleep(5000);
               
            }
            catch(Exception err)
            {
                System.out.println(err.toString());
            }
}
};

}
