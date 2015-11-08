package org.imu.olap4j.main;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;

public class MainOlap5 {

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
			
			Cube eachCube = connection.getOlapSchema().getCubes().get(0);
			
			for (Measure measure : eachCube.getMeasures()) {

				
                out.println("Measures " + measure.getName());
                out.println("Measure Levels...."
                        + measure.getLevel().getCaption());

            }

            for (Hierarchy hierarchy : eachCube.getHierarchies()) {

                out.println("hierarchy " + hierarchy.getName());
                NamedList<Level> levels = hierarchy.getLevels();

                for (Level l : levels) {

                    out.println("Hierarchy levels " + l.getName());
                    List<Member> members = l.getMembers();

                    for(Member member:members){
                        out.println("Member name " +member.getName());
                    }

                }
            }
            out.flush();
            out.close();
		} catch (OlapException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

}
