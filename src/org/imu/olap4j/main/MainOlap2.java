package org.imu.olap4j.main;

import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.olap4j.Axis;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.layout.CellSetFormatter;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.query.Query;
import org.olap4j.query.QueryDimension;

public class MainOlap2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("ERROR Class : " + e.getMessage());
			return;
		}
		OlapConnection connection = null;
		try {
			connection = (OlapConnection) DriverManager.getConnection(
					// This is the SQL Server service end point.
					"jdbc:xmla:Server=http://localhost/OLAP/msmdpump.dll"

					// Tells the XMLA driver to use a SOAP request cache layer.
					// We will use an in-memory static cache.
					+ ";Cache=org.olap4j.driver.xmla.cache.XmlaOlap4jNamedMemoryCache"

					// Sets the cache name to use. This allows cross-connection
					// cache sharing. Don't give the driver a cache name and it
					// disables sharing.
					+ ";Cache.Name=MyNiftyConnection"

					// Some cache performance tweaks.
					// Look at the javadoc for details.
					+ ";Cache.Mode=LFU;Cache.Timeout=600;Cache.Size=100");
		} catch (SQLException e) {
			System.out.println("ERROR Connection : " + e.getMessage());
			return;
		}
		
		try {
			Query myQuery = new Query("AdventureWorksCube", connection.getOlapSchema().getCubes().get(0));
			
			QueryDimension rowDimension = myQuery.getDimension("Geography");
			myQuery.getAxis(Axis.ROWS).addDimension(rowDimension);
			
			QueryDimension measuresDimension = myQuery.getDimension("Measures");
			measuresDimension.include(IdentifierNode.ofNames("Reseller Sales Amount").getSegmentList());
			measuresDimension.include(IdentifierNode.ofNames("Sales Amount").getSegmentList());
			myQuery.getAxis(Axis.COLUMNS).addDimension(measuresDimension);
			
			myQuery.validate();
			
			System.out.println(myQuery.getSelect().toString());
			
			CellSet set = myQuery.execute();
			
			CellSetFormatter formatter = new RectangularCellSetFormatter(true);
			formatter.format(set, new PrintWriter(System.out, true));
			set.close();
			connection.close();
		} catch (OlapException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
