package org.imu.olap4j.main;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;

public class MainOlap3 {

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
					+ ";Cache.Mode=LFU;Cache.Timeout=600000;Cache.Size=100");
		} catch (SQLException e) {
			System.out.println("ERROR Connection : " + e.getMessage());
			return;
		}
		PrintWriter out = null;
		try {
			String cubeName = "Adventure Works";
			Cube cube= connection.getOlapSchema().getCubes().get(cubeName);
			out = new PrintWriter("C:\\Users\\12023227\\Desktop\\" + cubeName + ".txt");
			
			List<Measure> mes = cube.getMeasures();
			for (Measure measure : mes) {
				out.println("Measure : " + measure.getName());
			}
			
			NamedList<Dimension> dims=cube.getDimensions();
			for (Dimension dimension : dims) {
				if (dimension.getName().equals("Measures")) {
					continue;
				}
				out.println("Dimension : " + dimension.getName());
				
				NamedList<Hierarchy> hirs=dims.get(dimension.getName()).getHierarchies();
				for (Hierarchy hierarchy : hirs) {
					out.println("\tHierarchy (" + dimension.getName() + ") : " + hierarchy.getName());
					
					for (Level level : hierarchy.getLevels()) {
//						if (level.getName().equals("(All)")) {
//							continue;
//						}
						out.println("\t\tLevel (" + hierarchy.getName() + ") : " + level.getName());
//						for(Member member : level.getMembers()) {
//							out.println("\t\t\tMember (" + level.getName() + ") : " + member.getName());
//						}
					}
				}
			}
			
			String mdx="SELECT NON EMPTY { [Measures].[Reseller Sales Amount],[Measures].[Reseller Tax Amount] } ON COLUMNS, NON EMPTY { ([Geography].[Country].[Country].ALLMEMBERS ) } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS FROM [Adventure Works]";
			final CellSet cellSet = connection.createStatement().executeOlapQuery(mdx);
			final CellSetAxis rowsAxis = cellSet.getAxes().get(Axis.ROWS.axisOrdinal());
			final CellSetAxis columnsAxis = cellSet.getAxes().get(Axis.COLUMNS.axisOrdinal());
			
			for (Position rowPos : rowsAxis) {
				
				for (Position colPos : columnsAxis) {
					
					System.out.println("Name Member : " + rowPos.getMembers().get(0).getName());
					
					for(Member member : colPos.getMembers()) {
						System.out.println("Name Cell : " + member.getName());
					}
					Cell currentCell = cellSet.getCell(colPos, rowPos);
					
					System.out.println("value = " + currentCell.getFormattedValue());
					
				}
			}
			
			
			cellSet.close();
			connection.close();
		} catch (OlapException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally {
			out.close();
		}
		
	}

}
