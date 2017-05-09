package com.pateo.df.utils;

import java.util.HashMap;
import java.util.Map;

public class DistanceUtils {
	  
	private static double EARTH_RADIUS = 6378.137; 
	   
    private static double rad(double d) { 
        return d * Math.PI / 180.0; 
    }
     
    /**
     * 根据两个位置的经纬度，来计算两地的距离（单位为KM）
     * 参数为String类型
     * @param lat1 用户经度
     * @param lng1 用户纬度
     * @param lat2 商家经度
     * @param lng2 商家纬度
     * @return
     */
    public static String getDistance(String lat1Str, String lng1Str, String lat2Str, String lng2Str) {
        Double lat1 = Double.parseDouble(lat1Str);
        Double lng1 = Double.parseDouble(lng1Str);
        Double lat2 = Double.parseDouble(lat2Str);
        Double lng2 = Double.parseDouble(lng2Str);
         
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double difference = radLat1 - radLat2;
        double mdifference = rad(lng1) - rad(lng2);
        double distance = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(difference / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2)
                * Math.pow(Math.sin(mdifference / 2), 2)));
        distance = distance * EARTH_RADIUS;
        distance = Math.round(distance * 10000) / 10000;
        String distanceStr = distance+"";
        distanceStr = distanceStr.
            substring(0, distanceStr.indexOf("."));
         
        return distanceStr;
    }
     
    /**
     * 获取当前用户一定距离以内的经纬度值
     * 单位米 return minLat
     * 最小经度 minLng
     * 最小纬度 maxLat
     * 最大经度 maxLng
     * 最大纬度 minLat
     */
    public static Map<String, String> getAround(String latStr, String lngStr, String raidus) {
        Map<String, String> map = new HashMap<String, String>();
         
        Double latitude = Double.parseDouble(latStr);// 传值给经度
        Double longitude = Double.parseDouble(lngStr);// 传值给纬度
 
        Double degree = (24901 * 1609) / 360.0; // 获取每度
        double raidusMile = Double.parseDouble(raidus);
         
        Double mpdLng = Double.parseDouble((degree * Math.cos(latitude * (Math.PI / 180))+"").replace("-", ""));
        Double dpmLng = 1 / mpdLng;
        Double radiusLng = dpmLng * raidusMile;
        //获取最小经度
        Double minLat = longitude - radiusLng;
        // 获取最大经度
        Double maxLat = longitude + radiusLng;
         
        Double dpmLat = 1 / degree;
        Double radiusLat = dpmLat * raidusMile;
        // 获取最小纬度
        Double minLng = latitude - radiusLat;
        // 获取最大纬度
        Double maxLng = latitude + radiusLat;
         
        map.put("minLat", minLat+"");
        map.put("maxLat", maxLat+"");
        map.put("minLng", minLng+"");
        map.put("maxLng", maxLng+"");
         
        return map;
    }
     
    public static void main(String[] args) {
        //济南国际会展中心经纬度：117.11811  36.68484
        //趵突泉：117.00999000000002  36.66123
        //System.out.println(getDistance("117.11811","36.68484","117.00999000000002","36.66123"));
         
        System.out.println(getAround("117.11811", "36.68484", "13000"));
        //117.01028712333508(Double), 117.22593287666493(Double),
        //36.44829619896034(Double), 36.92138380103966(Double)
         
    }
    
    private double radLat;  // latitude in radians
    private double radLon;  // longitude in radians

    private double degLat;  // latitude in degrees
    private double degLon;  // longitude in degrees

    private static final double EARTHS_RADIUS_KM = 6371.01;
    private static final double  EARTHS_RADIUS_M = 3958.762079;

    public static final int UNIT_KM = 0;
    public static final int UNIT_M = 1;

    private static final double MIN_LAT = Math.toRadians(-90d);  // -PI/2
    private static final double MAX_LAT = Math.toRadians(90d);   //  PI/2
    private static final double MIN_LON = Math.toRadians(-180d); // -PI
    private static final double MAX_LON = Math.toRadians(180d);  //  PI

    private DistanceUtils () {
    }

    /**
     * @param latitude the latitude, in degrees.
     * @param longitude the longitude, in degrees.
     */
    public static DistanceUtils fromDegrees(double latitude, double longitude) {
        DistanceUtils result = new DistanceUtils();
        result.radLat = Math.toRadians(latitude);
        result.radLon = Math.toRadians(longitude);
        result.degLat = latitude;
        result.degLon = longitude;
        result.checkBounds();
        return result;
    }

    /**
     *
     * @param unit 0 km || kilometers; 1 m || miles
     * @return
     */
    protected double getEarthsRadius(int unit) {
        return unit == UNIT_KM?EARTHS_RADIUS_KM:EARTHS_RADIUS_M;
    }

    /**
     * @param latitude the latitude, in radians.
     * @param longitude the longitude, in radians.
     */
    public static DistanceUtils fromRadians(double latitude, double longitude) {
        DistanceUtils result = new DistanceUtils();
        result.radLat = latitude;
        result.radLon = longitude;
        result.degLat = Math.toDegrees(latitude);
        result.degLon = Math.toDegrees(longitude);
        result.checkBounds();
        return result;
    }

    private void checkBounds() {
        if (radLat < MIN_LAT || radLat > MAX_LAT ||
                radLon < MIN_LON || radLon > MAX_LON)
            throw new IllegalArgumentException();
    }

    /**
     * @return the latitude, in degrees.
     */
    public double getLatitudeInDegrees() {
        return degLat;
    }

    /**
     * @return the longitude, in degrees.
     */
    public double getLongitudeInDegrees() {
        return degLon;
    }

    /**
     * @return the latitude, in radians.
     */
    public double getLatitudeInRadians() {
        return radLat;
    }

    /**
     * @return the longitude, in radians.
     */
    public double getLongitudeInRadians() {
        return radLon;
    }

    @Override
    public String toString() {
        return "(" + degLat + "\u00B0, " + degLon + "\u00B0) = (" +
                radLat + " rad, " + radLon + " rad)";
    }

    /**
     * Computes the great circle distance between this DistanceUtils instance
     * and the location argument.
     * @param radius the radius of the sphere, e.g. the average radius for a
     * spherical approximation of the figure of the Earth is approximately
     * 6371.01 kilometers.
     * @return the distance, measured in the same unit as the radius
     * argument.
     */
    public double distanceTo(DistanceUtils location, double radius) {
        return Math.acos(Math.sin(radLat) * Math.sin(location.radLat) +
                Math.cos(radLat) * Math.cos(location.radLat) *
                        Math.cos(radLon - location.radLon)) * radius;
    }

    /**
     *
     * @param location
     * @param unit
     * @return
     */
    public double distanceToByUnit(DistanceUtils location, int unit) {
        return distanceTo(location, getEarthsRadius(unit));
    }

    /**
     * <p>Computes the bounding coordinates of all points on the surface
     * of a sphere that have a great circle distance to the point represented
     * by this DistanceUtils instance that is less or equal to the distance
     * argument.</p>
     * <p>For more information about the formulae used in this method visit
     * <a href="http://JanMatuschek.de/LatitudeLongitudeBoundingCoordinates">
     * http://JanMatuschek.de/LatitudeLongitudeBoundingCoordinates</a>.</p>
     * @param distance the distance from the point represented by this
     * DistanceUtils instance. Must me measured in the same unit as the radius
     * argument.
     * @param radius the radius of the sphere, e.g. the average radius for a
     * spherical approximation of the figure of the Earth is approximately
     * 6371.01 kilometers.
     * @return an array of two DistanceUtils objects such that:<ul>
     * <li>The latitude of any point within the specified distance is greater
     * or equal to the latitude of the first array element and smaller or
     * equal to the latitude of the second array element.</li>
     * <li>If the longitude of the first array element is smaller or equal to
     * the longitude of the second element, then
     * the longitude of any point within the specified distance is greater
     * or equal to the longitude of the first array element and smaller or
     * equal to the longitude of the second array element.</li>
     * <li>If the longitude of the first array element is greater than the
     * longitude of the second element (this is the case if the 180th
     * meridian is within the distance), then
     * the longitude of any point within the specified distance is greater
     * or equal to the longitude of the first array element
     * <strong>or</strong> smaller or equal to the longitude of the second
     * array element.</li>
     * </ul>
     */
    public DistanceUtils[] boundingCoordinates(double distance, double radius) {

        if (radius < 0d || distance < 0d)
            throw new IllegalArgumentException();

        // angular distance in radians on a great circle
        double radDist = distance / radius;

        double minLat = radLat - radDist;
        double maxLat = radLat + radDist;

        double minLon, maxLon;
        if (minLat > MIN_LAT && maxLat < MAX_LAT) {
            double deltaLon = Math.asin(Math.sin(radDist) /
                    Math.cos(radLat));
            minLon = radLon - deltaLon;
            if (minLon < MIN_LON) minLon += 2d * Math.PI;
            maxLon = radLon + deltaLon;
            if (maxLon > MAX_LON) maxLon -= 2d * Math.PI;
        } else {
            // a pole is within the distance
            minLat = Math.max(minLat, MIN_LAT);
            maxLat = Math.min(maxLat, MAX_LAT);
            minLon = MIN_LON;
            maxLon = MAX_LON;
        }

        return new DistanceUtils[]{fromRadians(minLat, minLon),
                fromRadians(maxLat, maxLon)};
    }

}
