package org.imu.olap4j.main;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.olap4j.Axis;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.metadata.Member;

import com.google.gson.Gson;

public class MainOlap6 {

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
			//mdx=" SELECT NON EMPTY { [Measures].[Reseller Sales Amount], [Measures].[Reseller Tax Amount] } ON COLUMNS, NON EMPTY { ([Geography].[Geography].[Postal Code].ALLMEMBERS * [Product].[Product Model Lines].[Model].ALLMEMBERS ) } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS FROM [Adventure Works]";
			CellSet cellSet = connection.createStatement().executeOlapQuery(mdx);
			final CellSetAxis rowsAxis = cellSet.getAxes().get(Axis.ROWS.axisOrdinal());
			final CellSetAxis columnsAxis = cellSet.getAxes().get(Axis.COLUMNS.axisOrdinal());
			
			List<List<Object>> mapData = new ArrayList<List<Object>>();
			
			getHeader(out,mapData,rowsAxis,columnsAxis);
			
			for (int i=0;i<rowsAxis.getPositionCount();i++) {
				Position rowPos = rowsAxis.getPositions().get(i);
				
				List<Member> members = rowPos.getMembers();
				List<Object> objData = new ArrayList<Object>();
				for (Member member : members) {
					
					Member[] memberLength = new Member[member.getDepth()];
					
					Member memberTemp=member;
					memberLength[0] = memberTemp;
					
					for(int j=1;j<memberLength.length;j++) {
						memberLength[j]=memberTemp.getParentMember();
						memberTemp=memberLength[j];
					}
					
					for(int j=memberLength.length-1;j>=0;j--) {
						objData.add(memberLength[j].getName());
						
						out.print(memberLength[j].getName() + " , ");
					}
					
				}
				
				for (Position colPos : columnsAxis) {
					Object val = cellSet.getCell(colPos,rowPos).getValue();
					objData.add(val);
					out.print( val + " , ");
				}
				mapData.add(objData);
				out.println();
			}
			out.println();
			out.println(new Gson().toJson(mapData));
            out.flush();
            out.close();
            mapData.clear();
		} catch (OlapException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	private static void getHeader(PrintWriter out, List<List<Object>> data,CellSetAxis rowAxis,CellSetAxis colAxis) {
		List<Object> listObj = new ArrayList<Object>();
		for (Member member : rowAxis.getPositions().get(0).getMembers()) {
			
			Member[] memberLength = new Member[member.getDepth()];
			
			Member memberTemp=member;
			memberLength[0] = memberTemp;
			
			for(int j=1;j<memberLength.length;j++) {
				memberLength[j]=memberTemp.getParentMember();
				memberTemp=memberLength[j];
			}
			
			for(int j=memberLength.length-1;j>=0;j--) {
				Member memberRow = memberLength[j];
				String val=memberRow.getLevel().getName();
				listObj.add(val);
				out.print(val + " , ");
			}
		}
		
		for (Position colPos : colAxis) {
			for (Member memberCol : colPos.getMembers()) {
				String val=memberCol.getName();
				listObj.add(val);
				out.print(val + " , ");
			}
		}

		data.add(listObj);
		
		
	}
}
