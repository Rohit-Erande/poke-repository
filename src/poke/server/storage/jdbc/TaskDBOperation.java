package poke.server.storage.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.JobProposal;
import eye.Comm.NameValueSet;
import eye.Comm.NameValueSet.Builder;
import eye.Comm.NameValueSet.NodeType;

public class TaskDBOperation {
	protected static Logger logger = LoggerFactory.getLogger("server");
	private Builder setValue;
	private Connection conn;
	NameValueSet.Builder ns = NameValueSet.newBuilder();
	NameValueSet.Builder ns1 = NameValueSet.newBuilder();

	public NameValueSet signUp(JobProposal req)
	{
		logger.info("inside db signup");
		conn =  MyDBConnection.getConnection(); // Getting connection from myDBConnection class
		int i = 0;

		try {
			if(!conn.isClosed() || conn != null){
				PreparedStatement ps = conn.prepareStatement(SqlQueries.str_insertUser);
				logger.info("Got the connection in Sign Up!");
				logger.info("req.getOptions().getNode(0).getValue() : "+req.getOptions().getNode(0).getValue());
				logger.info("req.getOptions().getNode(1).getValue() : "+req.getOptions().getNode(1).getValue());
				ps.setString(1, req.getOptions().getNode(0).getValue());
				ps.setString(2, req.getOptions().getNode(1).getValue());
				ps.setString(3, req.getOptions().getNode(2).getValue());
				ps.setString(4, req.getOptions().getNode(3).getValue());
				int rs = ps.executeUpdate();
				logger.info("inserted the value for signup :"+rs);
				if(rs >0){
					ns.setNodeType(NodeType.NODE);
					ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("SUCCESS");
					ns.addNode(0,ns1.build());  				
				}else{
					ns.setNodeType(NodeType.NODE);
					ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("FAILURE");
					ns.addNode(0,ns1.build());  
				}
				conn.close();
				if(ns != null){
					return ns.build();
				}
			}
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}	
		catch(Exception e)
		{

			System.out.println("error is " + e);
			e.printStackTrace();
		}
		return null;
	}

	public NameValueSet login(JobProposal req)
	{
		Connection conn = null;
		String UserName = null;
		String Password = null;
		int val = 0;

		try
		{
			UserName = req.getOptions().getNode(0).getValue();
			Password = req.getOptions().getNode(1).getValue();

			logger.info("Username is : "+UserName+" Pass : "+Password);
			if(!UserName.equals(null)){
				conn = MyDBConnection.getConnection(); // Getting connection from myDBConnection class
				if(!conn.isClosed() || conn != null)
				{	
					System.out.println("Successfully Connected"+ "MySQL server using TCP/IP" );
					PreparedStatement ps = conn.prepareStatement(SqlQueries.str_validateLogin);

					ps.setString(1,UserName);
					ps.setString(2, Password);
					ResultSet rs = ps.executeQuery();
					logger.info("Inside login !");
					if(rs.next())
					{	 
						val = rs.getInt(1);
						logger.info("value inside login is "+val);
					}
					if(val>0){
						ns.setNodeType(NodeType.NODE);
						ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("SUCCESS");
						ns.addNode(0,ns1.build());  				
						return ns.build();
					}
					conn.close();
				}
			}
			ns.setNodeType(NodeType.NODE);
			ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("FAILURE");
			ns.addNode(0,ns1.build());  				
			return ns.build();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		ns.setNodeType(NodeType.NODE);
		ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("FAILURE");
		ns.addNode(0,ns1.build());  				
		return ns.build();

	}



	public NameValueSet listCourses()
	{
		conn =  MyDBConnection.getConnection(); // Getting connection from myDBConnection class
		int i = 0;

		try {
			PreparedStatement ps = conn.prepareStatement(SqlQueries.str_getCourseList);
			logger.info("Got the connection in listCourses");
			ResultSet rs = ps.executeQuery();
			ns.setNodeType(NodeType.NODE);
			while(rs.next())
			{	 		
				Integer course_id = rs.getInt("Course_Id");
				ns1.setNodeType(NodeType.VALUE).setName("Course ID").setValue(course_id.toString());
				ns.addNode(i++,ns1.build());  
				ns1.setNodeType(NodeType.VALUE).setName("Course Name").setValue(rs.getString("course_name"));
				ns.addNode(i++,ns1.build());  
			}
			conn.close();
			if(ns != null){
				return ns.build();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
		return null;

	}

	public NameValueSet viewMyCourse(JobProposal req)
	{
		conn =  MyDBConnection.getConnection(); // Getting connection from myDBConnection class
		try {
			if(!conn.isClosed() || conn != null){
				PreparedStatement ps = conn.prepareStatement(SqlQueries.str_insertUser);
				logger.info("Got the connection in Sign Up!");
				ps.setString(1, req.getOptions().getNode(0).getValue());
				ps.setString(2, req.getOptions().getNode(1).getValue());
				int rs = ps.executeUpdate();
				logger.info("inserted the value for signup :"+rs);
				if(rs >0){
					ns.setNodeType(NodeType.NODE);
					ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("SUCCESS");
					ns.addNode(0,ns1.build());  				
				}else{
					ns.setNodeType(NodeType.NODE);
					ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("FAILURE");
					ns.addNode(0,ns1.build());  
				}
				conn.close();
				if(ns != null){
					return ns.build();
				}
			}
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}	
		catch(Exception e)
		{

			System.out.println("error is " + e);
			e.printStackTrace();
		}
		return null;
	}
	public NameValueSet Enroll(JobProposal req)
	{

		conn =  MyDBConnection.getConnection(); // Getting connection from myDBConnection class
		int i = 0;

		try {
			if(!conn.isClosed() || conn != null){
				PreparedStatement ps = conn.prepareStatement(SqlQueries.str_insertEnroll);
				logger.info("Got the connection in Enroll!");

				PreparedStatement ps1 = conn.prepareStatement(SqlQueries.str_getUserId);
				ps1.setString(1,req.getOptions().getNode(0).getValue());
				ResultSet rs1 = ps1.executeQuery();
				if(rs1.next()){
					int userId = rs1.getInt("User_Id");
					ps.setInt(1,userId);
					ps.setInt(2, Integer.parseInt(req.getOptions().getNode(1).getValue()));
					int rs = ps.executeUpdate();
					logger.info("inserted the value for enrollment :"+rs);
					if(rs >0){
						ns.setNodeType(NodeType.NODE);
						ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("SUCCESS");
						ns.addNode(0,ns1.build());  				
					}else{
						ns.setNodeType(NodeType.NODE);
						ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("FAILURE");
						ns.addNode(0,ns1.build());  
					}
				}else{
					logger.info("User id not present!");
					ns.setNodeType(NodeType.NODE);
					ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("FAILURE");
					ns.addNode(0,ns1.build()); 
				}
				conn.close();
				if(ns != null){
					return ns.build();
				}
			}
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}	
		catch(Exception e)
		{

			System.out.println("error is " + e);
			e.printStackTrace();
		}
		return null;
	}

	public int getJobWeight(String namespace) {
		int jobWeight=0;
		try {
			conn=MyDBConnection.getConnection();
			PreparedStatement ps = conn.prepareStatement(SqlQueries.str_getWeight);
			logger.info("Inside getting jobweight 1... !");
			ps.setString(1, namespace);
			logger.info("Inside getting jobweight 2..!");
			ResultSet rs = ps.executeQuery();
			logger.info("Inside getting jobweight !");
			if(rs.next()){
				logger.info("Inside getting jobweight 3....!");
				jobWeight=rs.getInt("Job_Weight");
			}
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
		return jobWeight;
	}

	public String getJobId(String namespace) {
		String jobId=null;
		try {
			conn=MyDBConnection.getConnection();
			if(conn != null){
				logger.info("Connection esstablished!");
				PreparedStatement ps = conn.prepareStatement(SqlQueries.str_getJobId);
				ps.setString(1, namespace);
				ResultSet rs = ps.executeQuery();
				if(rs.next())
					jobId=rs.getString("Job_Id");
				conn.close();
			}else{
				logger.info("Connection not established!");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
		return jobId;
	}

	// Added by nikita
	public NameValueSet viewCourseDescription(JobProposal req)
	{
		conn = MyDBConnection.getConnection();
		int i = 0;

		try {
			PreparedStatement ps = conn.prepareStatement(SqlQueries.str_getCourseDetails);
			logger.info("Got the connection in view course description  = "+Integer.parseInt(req.getOptions().getNode(0).getValue()));
			
			ps.setInt(1,Integer.parseInt(req.getOptions().getNode(0).getValue()));
			ResultSet rs = ps.executeQuery();
			ns.setNodeType(NodeType.NODE);
			while(rs.next())
			{	 		
				//Integer course_id = rs.getInt("Course_Id");
				//String coursename =rs.getString("course_name");
				
				ns1.setNodeType(NodeType.VALUE).setName("Course Name").setValue(rs.getString("course_name"));				
				ns.addNode(i++,ns1.build());  
				ns1.setNodeType(NodeType.VALUE).setName("Course Description").setValue(rs.getString("course_description"));
				ns.addNode(i++,ns1.build());  
				logger.info("Course name and course description is: "+ns.build());
			}
			conn.close();
			if(ns != null){
				return ns.build();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
		return null;

	}

	public NameValueSet viewEnrolledCourses(JobProposal req)
	{

		conn =  MyDBConnection.getConnection(); // Getting connection from myDBConnection class
		int i = 0;

		try {
			if(!conn.isClosed() || conn != null){


				/*PreparedStatement ps2 = conn.prepareStatement(SqlQueries.str_getUserId);
				ps2.setString(1,req.getOptions().getNode(0).getValue());*/
				/*ResultSet rs2=ps2.executeQuery();*/
				/*if(rs2.next())
				{
					int user_id =rs2.getInt("User_Id");*/
				PreparedStatement ps1 = conn.prepareStatement(SqlQueries.str_getEnrolledCourses);
				logger.info("Got the connection in list enrolled courses for a given user");
				ps1.setString(1,req.getOptions().getNode(0).getValue());
				ResultSet rs =ps1.executeQuery();
				ns.setNodeType(NodeType.NODE);
				while(rs.next())
				{
					Integer course_id = rs.getInt("Course_Id");
					ns1.setNodeType(NodeType.VALUE).setName("Course ID").setValue(course_id.toString());
					ns.addNode(i++,ns1.build());
					ns1.setNodeType(NodeType.VALUE).setName("Course Name").setValue(rs.getString("course_name"));
					ns.addNode(i++,ns1.build());
				}
				logger.info("Course name and course Id to which a user has enrolled is: "+ns.build());
			//}
			conn.close();
			if(ns != null){
				return ns.build();
			}
		}else{
			logger.info("User hasn't enrolled in any course!");
			ns.setNodeType(NodeType.NODE);
			ns1.setNodeType(NodeType.VALUE).setName("Result").setValue("FAILURE");
			ns.addNode(1,ns1.build()); 
		}
		conn.close();
		if(ns != null){
			return ns.build();
		}

	} 
	catch (SQLException e) {
		// TODO Auto-generated catch block
		System.out.println(e);
		e.printStackTrace();
	}	
	catch(Exception e)
	{
		System.out.println("error is " + e);
		e.printStackTrace();
	}
	return null;
}
//End Nikita


}

