package COMS363;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;

/**
 * Created by taylorsdugger on 2/20/17.
 */
public class P3 {

    //Infos
    private String dbUrl = "jdbc:mysql://mysql.cs.iastate.edu:3306/db363tdugger";
    private String user = "dbu363tdugger";
    private String password = "cuzEkYKk";
    private Connection conn1;


    /**
     * Open yer database
     */
    private void openDB() {
        try {
            // Load the driver (registers itself)
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception E) {
            System.err.println("Unable to load driver.");
            E.printStackTrace();
        }

        try {
            // Connect to the database
            conn1 = DriverManager.getConnection(dbUrl, user, password);
            System.out.println("*** Connected to the database ***");

        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

    /**
     * Main.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        P3 project3 = new P3();

        project3.openDB();
        //Part A
        project3.getStudentGrades();
        //Part B
        project3.topFiveStudents();
    }

    /**
     * Part A. I did this probably a little different. It said to not use
     * arrays wastefully. So I decided to use no array, and update to the db
     * dynamically as i process the data. Less storage used. Probably less time
     * as well? A lot of data base accesses, but only one loop through data...
     * So maybe this is good.
     */
    private void getStudentGrades(){

        try{
            Statement statement = conn1.createStatement();
            ResultSet rs;

            //THE SQL statement
            rs = statement.executeQuery(
                    "SELECT t1.StudentID, t2.Classification, t2.CreditHours, t2.GPA,t1.Grade FROM\n" +
                            "(SELECT  *\n" +
                            "FROM Enrollment e) AS t1\n" +
                            "\n" +
                            "LEFT JOIN\n" +
                            "\n" +
                            "  (SELECT *\n" +
                            "  FROM Student s) AS t2\n" +
                            "  ON t1.StudentID = t2.StudentID\n" +
                            "ORDER BY t1.StudentID;");

            //some variables
            int curStudentID = 0;
            int nextStudentID = 0;
            String classification = "";
            int creditHours = 0;
            float GPA = 0;
            float nGPA = 0;
            float grade = 0;

            //Now go through it
            while(rs.next()){
                //I get the current student and the next a little later to check
                //if its the same student still (different class)
                curStudentID = rs.getInt(1);

                //If a different student, then this student has
                // no more class so then update the db
                if(curStudentID != nextStudentID && nextStudentID != 0){
                    classification = setClassification(creditHours);

                    //update
                    PreparedStatement ps = conn1.prepareStatement("UPDATE Student SET Classification = ?, GPA = ?, CreditHours = ? WHERE StudentID = ?");
                    ps.setString(1,classification);
                    ps.setDouble(2, (Math.round(nGPA*100)/100.0)  );
                    ps.setInt(3,creditHours);
                    ps.setInt(4,nextStudentID);

                    ps.executeUpdate();
                    ps.close();

                    creditHours = 0;
                }

                //If one the same student still, get the data
                classification = rs.getString(2);
                creditHours += rs.getInt(3) + 3;
                GPA = rs.getFloat(4);
                grade = gradeConvert(rs.getString(5));

                //calculate
                nGPA = (((GPA * (creditHours-3)) + (3 * grade)) / (creditHours));

                //now peek at the next student
                rs.next();
                nextStudentID = rs.getInt(1);

                //If the same one, we only need gpa from next class.
                if(nextStudentID == curStudentID){

                    nGPA = (((GPA * creditHours) + (3 * grade)) / (creditHours + 3));
                    creditHours += 3;
                }
            }

            statement.close();
            rs.close();
            conn1.close();

        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

    /**
     * Helper class. Self explanatory
     * @param g Letter grade
     * @return Grade as a float
     */
    private float gradeConvert(String g){

        switch (g.trim()){
            case ("A"):
                return 4.00f;
            case ("A-"):
                return 3.66f;
            case ("B+"):
                return 3.33f;
            case ("B"):
                return 3.00f;
            case ("B-"):
                return 2.66f;
            case ("C+"):
                return 2.33f;
            case ("C"):
                return 2.00f;
            case ("C-"):
                return 1.66f;
            case ("D+"):
                return 1.33f;
            case ("D"):
                return 1.00f;
            case ("F"):
                return 0.00f;
            default:
                return 0.00f;
        }
    }

    /**
     * Helper class. Self explanatory
     * @param c Credit hours
     * @return The classification as a string
     */
    private String setClassification(int c){

        if(c > 89){
            return "Senior";
        }else if(c > 59 && c < 90){
            return "Junior";
        }else if(c > 29 && c < 60){
            return "Sophomore";
        }else{
            return "Freshman";
        }
    }

    /**
     * Part B.
     */
    private void topFiveStudents(){
        try{
            Statement statement = conn1.createStatement();
            ResultSet rs;

            //SQL
            rs = statement.executeQuery(
                    "SELECT t1.Name, t1.GPA, t2.Name FROM\n" +
                    "(SELECT p.Name, s.GPA, s.MentorID\n" +
                    "FROM Student s, Person p\n" +
                    "WHERE s.StudentID = p.ID\n" +
                    "      AND s.Classification = 'Senior'\n" +
                    "ORDER BY s.GPA DESC\n" +
                    "LIMIT 5) AS t1\n" +
                    "\n" +
                    "INNER JOIN\n" +
                    "(SELECT p.Name, i.InstructorID\n" +
                    " FROM Person p, Instructor i\n" +
                    " WHERE p.ID = i.InstructorID) AS t2\n" +
                    "ON t1.MentorID = t2.InstructorID;");

            PrintWriter writer = null;

            //print out
            try{
                writer = new PrintWriter("P3Output.txt","UTF-8");
                while(rs.next()){
                    writer.println("Student Name: " + rs.getString(1) + ", GPA: "
                            + rs.getDouble(2) + ". Mentor: " + rs.getString(3));

                }

            }catch (IOException e){

            }

            writer.close();

            statement.close();
            rs.close();
            conn1.close();

        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

}
