package uq.spatial.transformation;

import java.io.Serializable;

import uq.spatial.GeoInterface;

/**
 * Convert latitude and longitude coordinates
 * to cartesian coordinates.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class ProjectionTransformation implements Serializable, GeoInterface {
	// referential longitude and latitude (origin - North East)
	private static final double LON_0 = 70.0;
	private static final double LAT_0 = 20.0;
	// scaling by MIN MAX of the map area
	private static final double SCALE_X = (MAX_X / 100);
	private static final double SCALE_Y = (MAX_Y / 100);
	
	/**
	 * Get the Mercator projection of this Longitude and Latitude
	 * coordinates on a map. The map scale is given by the application
	 * paramenters.
	 * 
	 * @return Return the projected coordinates as a double vector 
	 * with x = vec[0] and y = vec[1]
	 */
	public static double[] getMercatorProjection(double lon, double lat){
		double lonRad = (lon - LON_0) * (PI/180);
		double latRad = (lat - LAT_0) * (PI/180);
		
		double x = EARTH_RADIUS * lonRad;
		double y = EARTH_RADIUS * Math.log(Math.tan((PI/4)+(latRad/2)));
		
		double[] res = new double[2];
        res[0] = x / SCALE_X;
		res[1] = y / SCALE_Y;
		
		return res;
	}

	/**
	 * Get the projection of this Longitude and Latitude
	 * coordinates into Cartesian coodinates (x,y,z). 
	 * </br></br>
	 * The x-axis goes through long,lat (0,0), so longitude 0 meets the equator.
	 * </br>
	 * The y-axis goes through (0,90);
	 * </br>
	 * The z-axis goes through the poles.
	 * 
	 * @return Return the cartesian coordinates as a double vector 
	 * with x = vec[0], y = vec[1] and z = vec[2]
	 */
	public static double[] getCartesianProjection(double lon, double lat){
		double x = EARTH_RADIUS * Math.cos(Math.toRadians(lat)) * Math.cos(Math.toRadians(lon));
		double y = EARTH_RADIUS * Math.cos(Math.toRadians(lat)) * Math.sin(Math.toRadians(lon));
	    double z = EARTH_RADIUS * Math.sin(Math.toRadians(lat));

		double[] res = new double[3];
        res[0] = x; res[1] = y;  res[2] = z;

		return res;
	}
	
	public static void main(String [] a){
		double[] 
		b = getMercatorProjection(105.80204, 0.12783);
		System.out.println(b[0]  + "  " + b[1]);
		
		b = getMercatorProjection(107.22343, 27.89323);
		System.out.println(b[0]  + "  " + b[1]);
		
		b = getMercatorProjection(108.97939, 31.62847);
		System.out.println(b[0]  + "  " + b[1]);
		
		b = getMercatorProjection(110.78873, 32.90354);
		System.out.println(b[0]  + "  " + b[1]);
		
		b = getMercatorProjection(111.28009, 34.0);
		System.out.println(b[0]  + "  " + b[1]);
		
		b = getMercatorProjection(113.68132, 35.05894);
		System.out.println(b[0]  + "  " + b[1]);
		
		b = getMercatorProjection(117.24706, 38.0);
		System.out.println(b[0]  + "  " + b[1]);
		
		b = getMercatorProjection(119.58125, 40.0);
		System.out.println(b[0]  + "  " + b[1]);
		
		b = getMercatorProjection(120.93483, 50.0);
		System.out.println(b[0]  + "  " + b[1]);
	}
}
