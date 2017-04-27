package test;

public class TestUtils {

	
	public static void main(String[] args) {
		
		String aa = "{gpsspeed=21, bearing=70, lon=117.56496, gpsreliable=1, deviceid=P011002100002011, offsetx=561, carspeed=21, offsety=-137, roadlevel=2, id=39584118, gpstime=1492422124145, updatetime=1492422163272, user=P011002100002011, lat=34.39868, direction=4}";
		
		int lastIndexOf = aa.indexOf("deviceid=") + "deviceid=".length();
		//int length = "P011002100002011".length();
		int offsetx =  aa.indexOf(", offsetx");
		
		String deviceId = aa.substring(lastIndexOf, offsetx);
		System.out.println("deviceId:"+deviceId);
		
	}
}
