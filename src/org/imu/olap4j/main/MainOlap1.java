package org.imu.olap4j.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.layout.CellSetFormatter;
import org.olap4j.layout.RectangularCellSetFormatter;

public class MainOlap1 {

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
			PrintWriter pw = new PrintWriter(new File("C:\\Users\\12023227\\Desktop\\test.txt"));
			
			String mdx="SELECT NON EMPTY { [Measures].[Reseller Sales Amount] } ON COLUMNS, NON EMPTY { ([Geography].[Geography].[City].ALLMEMBERS ) } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS FROM [Adventure Works] CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS";
			mdx="SELECT {[Geography].[Country].Members} * {[Product].[Product Categories].Members}  ON ROWS,{[Measures].[Reseller Sales Amount]} ON COLUMNS FROM [Adventure Works] WHERE ([Date].[Calendar].[Calendar Year].&[2007])";
			//mdx="SELECT {KPIValue(\"Revenue\"), KPIGoal(\"Revenue\"), KPIStatus(\"Revenue\"), KPITrend(\"Revenue\")} ON COLUMNS,{[Date].[Fiscal].[Fiscal Year].Members} ON ROWS FROM [Adventure Works]";
			mdx="WITH MEMBER [Measures].[WashCustCountSum] AS SUM([Customer].[Customer Geography].[State-Province].[Washington].Children, [Measures].[Customer Count]) SELECT {[Measures].[WashCustCountSum]} ON COLUMNS FROM [Adventure Works]";
			CellSet set = connection.createStatement().executeOlapQuery(mdx);
			CellSetFormatter formatter = new RectangularCellSetFormatter(true);
			formatter.format(set, pw);
			set.close();
			connection.close();
			pw.close();
		} catch (OlapException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
						
	}

}
