package org.imu.olap4j.main;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.Position;

public class MainOlap4 {

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
			PrintWriter out = new PrintWriter("C:\\Users\\12023227\\Desktop\\AdventureWorks.txt");
			
			String mdx="SELECT NON EMPTY { [Measures].[Reseller Sales Amount],[Measures].[Reseller Tax Amount] } ON COLUMNS, NON EMPTY { ([Geography].[Geography].[Postal Code].ALLMEMBERS ) } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS FROM [Adventure Works]";
			CellSet cellSet = connection.createStatement().executeOlapQuery(mdx);
		
			for (Position rowPos : cellSet.getAxes().get(Axis.ROWS.axisOrdinal()).getPositions()) {
				
				
				for (Position colPos : cellSet.getAxes().get(Axis.COLUMNS.axisOrdinal()).getPositions()) {
					Cell cell=cellSet.getCell(colPos,rowPos);
					out.println("value: " + cell.getFormattedValue());
				
				}
			}
			out.flush();
			out.close();
			cellSet.close();
			connection.close();
		} catch (OlapException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

}
