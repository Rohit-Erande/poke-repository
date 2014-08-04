package poke.server.storage.jdbc;


public class SqlQueries {

	public static final String str_getWeight = "SELECT Job_Weight FROM table_job_desc WHERE Job_Desc =?";

	public static final String str_validateLogin = "SELECT COUNT(*) FROM table_user_info WHERE UserName = ? AND pwd = ?";

	public static final String str_insertUser = "INSERT INTO table_user_info (FirstName, LastName, UserName, pwd) values (?,?,?,?)";

	public static final String str_getUserId = "SELECT User_Id FROM table_user_info WHERE UserName = ?";

	public static final String str_insertTaskStatus = "INSERT INTO table_job_status (Job_Id,Job_Status) values (?,?)";
	
	public static final String str_updateTaskStatus = "UPDATE table_job_status SET Node_Id = ? AND Job_Status=? where Job_Id= ?";
	
	public static final String str_getDescription="SELECT Job_desc FROM table_job_desc WHERE Job_Id = ?";

	public static final String str_getCourseList="SELECT Course_Id,course_name FROM table_courses";

	public static final String str_insertEnroll="INSERT INTO table_enroll(User_Id, Course_Id) values (?,?)";

	public static final String str_insertJobStatus = "INSERT INTO table_job_status(Job_Id,Node_Id,Job_Status) VALUES (?,?,?)";
	//  
	//public static final String str_getUserId="SELECT User_Id FROM table_user_info";

	public static final String str_getCourseDetails="SELECT course_name,course_description FROM table_courses WHERE Course_Id = ?";
	
	public static final String str_getJobId = "SELECT Job_Id FROM table_job_desc WHERE Job_Desc =?";
	
	public static final String str_getEnrolledCourses="SELECT  table_enroll.Course_Id, table_courses.course_name FROM table_courses, table_enroll, table_user_info WHERE "
			+ "table_courses.Course_Id=table_enroll.Course_Id AND table_enroll.User_Id= table_user_info.User_Id AND table_user_info.UserName = ?";
}
