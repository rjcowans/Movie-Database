import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Properties;
import java.util.Scanner;

public class Modeling_Data {
	public ArrayList<String> toPrint = new ArrayList<>();
	/** The name of the MySQL account to use (or empty for anonymous) */
	private final String userName = "root";

	/** The password for the MySQL account (or empty for anonymous) */
	private final String password = "password";

	/** The name of the computer running MySQL */
	private final String serverName = "localhost";

	/** The port of the MySQL server (default is 3306) */
	private final int portNumber = 3306;

	private final String dbName = "moviesearch";

	public boolean tableExisit = false;
	public boolean databasexist = false;
	public boolean datainserted = false;
	public boolean inputgiven = false;
	public int goodInput = 0;
	public String movieinput = "";
	public int takeaway;

	public Connection getConnection() throws SQLException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", this.userName);
		connectionProps.put("password", this.password);

		conn = DriverManager.getConnection("jdbc:mysql://" + this.serverName + ":" + this.portNumber, connectionProps);

		return conn;
	}

	public void executeUpdate(Connection conn, String command) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			takeaway = stmt.executeUpdate(command); // This will throw a SQLException if it fails
		} finally {

			// This will run whether we throw an exception or not
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	public void q4(Connection conn) {
		System.out.println("Everything Looks Good Running search");
		try {
			takeBack(conn, "SELECT M.name,firstconnection.COUNT\r\n" + 
					"FROM movies M INNER JOIN (\r\n" + 
					"  SELECT COUNT(g.id) AS COUNT, g.movieid\r\n" + 
					"  FROM actor g INNER JOIN (\r\n" + 
					"    SELECT T.id\r\n" + 
					"    FROM actor T\r\n" + 
					"    WHERE T.movieid="+this.goodInput+") AS secondconnection ON g.id = secondconnection.id GROUP BY\r\n" + 
					"          g.movieid) AS firstconnection ON firstconnection.movieid = M.movieid and m.movieid!="+this.goodInput+" ORDER BY\r\n" + 
					"        firstconnection.COUNT DESC, M.vote_count DESC, m.pop DESC LIMIT 5;", 3);
		} catch (SQLException e) {
			return;
		}
		if (toPrint.size() != 0) {
			System.out.println("Table of Movies with Actors Match of Movie: "+ this.movieinput);
			System.out.printf("%50s\n", "Movie Name");
			String pad = String.format("%" + 77 + "s", "").replace(" ", "-");
			System.out.println(pad);
			for (int i = 0; i < toPrint.size(); i++) {
				System.out.print(toPrint.get(i));
			}
		} else
			System.out.println("No Output: no Directors match is not in Database");
		toPrint.clear();
	}

	public void printQuery(ResultSet answer, int type) throws SQLException {
		while (answer.next()) {
			switch (type) {
			case 0:
				this.goodInput = (Integer) answer.getInt(1);
			case 1:
				String temp1 = (String) answer.getString(1);
				String go = String.format("%50s|\n", temp1);
				toPrint.add(go);
				break;
			case 2:
				String temp4 = (String) answer.getString(1);
				String go1 = String.format("%50s|\n", temp4);
				toPrint.add(go1);
				break;
			case 3:
				String temp6 = (String) answer.getString(1);
				String go2 = String.format("%50s|\n", temp6);
				toPrint.add(go2);
				break;
			case 4:
				String temp8 = (String) answer.getString(1);
				String go3 = String.format("%50s|\n", temp8);
				toPrint.add(go3);
				break;
			}
		}
		return;
	}

	public void takeBack(Connection conn, String command, int type) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(command); // This will throw a SQLException if it fails
			printQuery(rs, type);
		} finally {

			// This will run whether we throw an exception or not
			if (stmt != null) {
				stmt.close();
			}
		}

	}

	public void insert_data(String filename, Connection conn, int type) throws IOException, SQLException {
		int[] play = { 4, 6, 5, 4, 6 };
		System.out.println("Inserting Data from file " + filename);
		BufferedReader intake = new BufferedReader(new FileReader(filename));
		String line = "";
		String insert = "";
		String name = filename.replace("csv", "").replace("_", "").replace("v2", "").replace(".", "");
		String inside = "";
		for (int x = 0; x < play[type]-2; x++)
			inside += "?,";
		inside += "?";
		System.out.println(inside);
		insert = "INSERT INTO " + name + " VALUES (" + inside + ")";
		conn.setAutoCommit(false);
		PreparedStatement sql = conn.prepareStatement(insert);
		while ((line = intake.readLine()) != null) {
			String temp = null;
			String[] turn = null;
			turn = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);;
			//System.out.println(line +" " + turn.length);
			for (int i = 0; i <= turn.length-1; i++) {
				if (turn[i].equals("")) {
					turn[i] = "null";
					sql.setString(i + 1, null);
				} else {
					temp = turn[i].trim().replace("\"", "");
					if (temp.matches("\\d+"))
						sql.setInt(i + 1, Integer.parseInt(temp.trim()));
					else if (temp.matches("\\d+\\.\\d+")) 
						sql.setFloat(i + 1, Float.parseFloat(temp.trim()));
					else 
						sql.setString(i + 1, temp.trim());
					//System.out.print(temp +  " ");
				}
				
			}
			//System.out.println();
			sql.addBatch();
		}
		sql.executeBatch();
		conn.commit();
		sql.close();
		conn.setAutoCommit(true);
		intake.close();
	}

	public void create_table(Connection conn) {
		String keywords = "create table keywords(id INT, name VARCHAR(250), movie INT, primary key (id,movie));";
		String genres = "create table genres(id INT, name VARCHAR(250), movie INT, primary key (id,movie));";
		String movies = "create table movies(movieid INT,name VARCHAR(250) ,pop real, vote_average real, vote_count INT, primary key (movieid));";
		String director = "create table directors(name VARCHAR(250), id INT, gender INT, movieid INT,  primary key (id,movieid));";
		String actors = "create table actor(id INT, actor_name VARCHAR(250), character_name VARCHAR(500), gender INT, movieid INT, primary key (id,movieid,character_name));";
		try {
			this.executeUpdate(conn, keywords);
			System.out.println("Made Keywords");
			this.executeUpdate(conn, genres);
			System.out.println("Made Genres");
			this.executeUpdate(conn, movies);
			System.out.println("Made Movies");
			this.executeUpdate(conn, director);
			System.out.println("Made Director");
			this.executeUpdate(conn, actors);
			System.out.println("Tables\nKeywords\nGenres\nMovies\nDirector\nActors\n Going back to menu.");
		} catch (SQLException e) {
			System.out.println("Tables Do not Exist");
			return;
		}

	}

	public void q1(Connection conn) {
		System.out.println("Everything Looks Good Running search");
		try {
			takeBack(conn, "SELECT M.name,firstconnection.COUNT\r\n" + 
					"FROM movies M INNER JOIN (\r\n" + 
					"  SELECT COUNT(g.id) AS COUNT, g.movie\r\n" + 
					"  FROM genres g INNER JOIN (\r\n" + 
					"    SELECT T.id\r\n" + 
					"    FROM genres T\r\n" + 
					"    WHERE T.movie="+this.goodInput+") AS secondconnection ON g.id = secondconnection.id GROUP BY\r\n" + 
					"          g.movie) AS firstconnection ON firstconnection.movie = M.movieid and m.movieid!="+this.goodInput+" ORDER BY\r\n" + 
					"        firstconnection.COUNT DESC, M.vote_count DESC, m.pop DESC LIMIT 5;"
					, 1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (toPrint.size() != 0) {
			System.out.println("Table of Genre Matches of Movie:"+ this.movieinput +"Below:");
			System.out.printf("%50s\n", "Movie Name");
			String pad = String.format("%" + 103 + "s", "").replace(" ", "-");
			System.out.println(pad);
			for (int i = 0; i < toPrint.size(); i++) {
				System.out.print(toPrint.get(i));
			}
		} else
			System.out.println("No Genre Matches the Movie " + this.movieinput);
		toPrint.clear();
	}

	
	public void q2(Connection conn){  
		  System.out.println("Everything Looks Good Running search");
			try {
				takeBack(conn, "SELECT M.name,firstconnection.COUNT\r\n" + 
						"FROM movies M INNER JOIN (\r\n" + 
						"  SELECT COUNT(g.id) AS COUNT, g.movie\r\n" + 
						"  FROM keywords g INNER JOIN (\r\n" + 
						"    SELECT T.id\r\n" + 
						"    FROM keywords T\r\n" + 
						"    WHERE T.movie="+this.goodInput+") AS secondconnection ON g.id = secondconnection.id GROUP BY\r\n" + 
						"          g.movie) AS firstconnection ON firstconnection.movie = M.movieid and m.movieid!="+this.goodInput+" ORDER BY\r\n" + 
						"        firstconnection.COUNT DESC, M.vote_count DESC, m.pop DESC LIMIT 5;"
						, 1);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			if (toPrint.size() != 0) {
				System.out.println("Table of Keyword Matches of Movie:"+ this.movieinput +"Below:");
				System.out.printf("%50s\n", "Movies Name");
				String pad = String.format("%" + 103 + "s", "").replace(" ", "-");
				System.out.println(pad);
				for (int i = 0; i < toPrint.size(); i++) {
					System.out.print(toPrint.get(i));
				}
			} else
				System.out.println("No Keyword Matches the Movie " + this.movieinput);
			toPrint.clear();
	  }
	
	public void run() {
		Scanner input = new Scanner(System.in);
		String value = "";
		// Connect to MySQL
		Connection conn = null;
		while (true) {
			System.out.println("Enter Command 1-11 shown below");
			System.out.println("1.  Create Database "+ this.dbName +" and Tables");
			System.out.println("2.  Insert Data");
			System.out.println("3   Input Movie Name :)");
			System.out.println("4.  Show Recommendation based off of Genres");
			System.out.println("5.  Show Recommendation based off of Keywords");
			System.out.println("6.  Show Recommendation based off of Actors");
			System.out.println("7.  Show Recommendation based off of Directors");
			System.out.println("8.  Drop Database " + this.dbName);
			System.out.println("9.  Drop Database " + this.dbName + " and Exit program");
			System.out.println("10. Reset Input");
			System.out.println("11. Reset Input and Enter a New One");
			System.out.println("12. Attempted Search of True ALL MIGHTY QUERY");
			try {
				int opt = input.nextInt();
				input.nextLine();
				switch (opt) {
				case 1:
					try {
						conn = this.getConnection();
						conn.setAutoCommit(true);
						
						this.executeUpdate(conn, "drop database if exists " + this.dbName);
						this.executeUpdate(conn, "CREATE database " + this.dbName);
						this.executeUpdate(conn, "USE " + this.dbName);
						System.out.println("Connected to database");
						databasexist = true;
						create_table(conn);
						tableExisit = true;
					} catch (SQLException e) {
						System.out.println("Error: database is still there");
						e.printStackTrace();
						return;
					}

					break;
				case 2:
					if (databasexist && tableExisit) {
						try {
							this.insert_data("genres.csv", conn, 0);
							this.insert_data("actor.csv", conn, 1);
							this.insert_data("directors.csv", conn, 2);
							this.insert_data("keywords.csv", conn, 3);
							this.insert_data("movies.csv", conn, 4);
							System.out.println("Data Inserted");
						} catch (IOException e) {
							System.out.println("Error: File not found");
							e.printStackTrace();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						datainserted = true;
					} else
						checkPrint();
					break;
				case 3:
					System.out.println("OK Enter Movie and Press Enter\n or Enter Quit");
					value = input.nextLine();
					String tester = value.toUpperCase(); 
					if (tester.equals("QUIT")) {
						break;
					}else {
						if (this.queryStringCheck(conn, value)) {
							System.out.println("\nThank you Input is Now Stored");
							inputgiven = true;
							toPrint.clear();
							break;
						}
						value = "";
					}
						
					break;
				case 4:
					if (check()) {
						System.out.println("I WAS SUPPOSE TO RUN");
						 q1(conn);
					} else {
						checkPrint();
					}
					break;
				case 5:
					if (check()) {
						System.out.println("I WAS SUPPOSE TO RUN2");
						q2(conn);
					} else {
						checkPrint();
					}
					break;
				case 6:
					if (check()) {
						System.out.println("I WAS SUPPOSE TO RUN3");
						q4(conn);
					} else {
						checkPrint();
					}
					break;
				case 7:
					if (check()) {
						System.out.println("I WAS SUPPOSE TO RUN4");
						q3(conn);
					} else {
						checkPrint();
					}
					break;
				case 8:
					try {
						String dropString = "DROP DATABASE IF EXISTS " + this.dbName;
						this.executeUpdate(conn, dropString);
						System.out.println("Dropped the Database");
					} catch (SQLException e) {
						System.out.println("ERROR: Could not drop the Database");
						e.printStackTrace();
						return;
					}
					break;
				case 9:
					try {
						String dropString = "DROP DATABASE " + this.dbName;
						this.executeUpdate(conn, dropString);
						System.out.println("Dropped the Database and Exiting Program");
						input.close();
						System.exit(0);
					} catch (SQLException e) {
						System.out.println("ERROR: Could not drop the Database");
						e.printStackTrace();
						return;
					}
					break;
				case 10:
					inputgiven = false;
					value = "";
					this.goodInput = 0;
					System.out.println("Value Reset! :) Returning");
					break;
				case 11:
					System.out.println("OK Enter Movie and Press Enter or Enter Quit");
					value = input.nextLine();
					String tester2 = value.toUpperCase(); 
					if (tester2.equals("QUIT")) {
						break;
					}else {
						if (this.queryStringCheck(conn, value)) {
							System.out.println("\nThank you Input is Now Stored");
							inputgiven = true;
							toPrint.clear();
							break;
						}
						value = "";
					}
					break;
				case 12:
					System.out.println("Running Almight Top Recommendation");
					q5(conn);
					break;
				case 1000:
					System.exit(0);
					break;
				case 1001:
					try {
						conn = this.getConnection();
						datainserted = true;
						databasexist = true;
						tableExisit = true;
						if (check2()) {
							this.executeUpdate(conn, "USE " + this.dbName);
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
					break;
				default:
					System.out.println("Wrong Type of Input try again");
					break;
				}
			} catch (InputMismatchException e) {
				System.out.println("Not a int try again");
				run();
			}
		}
	}

	private boolean queryStringCheck (Connection conn,String value) {
		try {
			this.takeBack(conn,"SELECT movieid from movies where name='" + value + "' LIMIT 1;",0);
		}catch(SQLException e) {
			e.printStackTrace();
			System.out.println("O Looks Like The Database is Not Connected");
			return false;
		}
		if (toPrint.size() != 0) {
			return true;
		}else {
			System.out.println("Sorry Movie Does Not Exist In Data");
			this.movieinput = "";
			return false;
		}
	}
	
	private void q3(Connection conn) {
		System.out.println("Everything Looks Good Running search");
		try {
			takeBack(conn, "SELECT M.name,firstconnection.COUNT\r\n" + 
					"FROM movies M INNER JOIN (\r\n" + 
					"  SELECT COUNT(g.id) AS COUNT, g.movieid\r\n" + 
					"  FROM directors g INNER JOIN (\r\n" + 
					"    SELECT T.id\r\n" + 
					"    FROM directors T\r\n" + 
					"    WHERE T.movieid="+this.goodInput+") AS secondconnection ON g.id = secondconnection.id GROUP BY\r\n" + 
					"          g.movieid) AS firstconnection ON firstconnection.movieid = M.movieid and m.movieid!="+this.goodInput+" ORDER BY\r\n" + 
					"        firstconnection.COUNT DESC, M.vote_count DESC, m.pop DESC LIMIT 5;"
					, 4);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (toPrint.size() != 0) {
			System.out.println("Table of Directors Matches of Movie:"+ this.movieinput +"Below:");
			System.out.printf("%50s\n", "Movie Name");
			String pad = String.format("%" + 103 + "s", "").replace(" ", "-");
			System.out.println(pad);
			for (int i = 0; i < toPrint.size(); i++) {
				System.out.print(toPrint.get(i));
			}
		} else
			System.out.println("No Actors Matches the Movie " + this.movieinput);
		toPrint.clear();

	}
	
	private void q5(Connection conn) {
		System.out.println("Everything Looks Good Running search");
		try {
			takeBack(conn, "SELECT DISTINCT X.name from movies X left JOIN (SELECT M.movieid,firstconnection.COUNT\r\n" + 
					"FROM movies M INNER JOIN (\r\n" + 
					"  SELECT COUNT(g.id) AS COUNT, g.movie\r\n" + 
					"  FROM genres g INNER JOIN (\r\n" + 
					"    SELECT T.id\r\n" + 
					"    FROM genres T\r\n" + 
					"    WHERE T.movie="+this.goodInput+") AS secondconnection ON g.id = secondconnection.id GROUP BY\r\n" + 
					"          g.movie) AS firstconnection ON firstconnection.movie = M.movieid and m.movieid!="+this.goodInput+") as q1 on X.movieid=q1.movieid\r\n" + 
					"left JOIN\r\n" + 
					"  (SELECT M.movieid,firstconnection.COUNT\r\n" + 
					"FROM movies M INNER JOIN (\r\n" + 
					"  SELECT COUNT(g.id) AS COUNT, g.movie\r\n" + 
					"  FROM keywords g INNER JOIN (\r\n" + 
					"    SELECT T.id\r\n" + 
					"    FROM keywords T\r\n" + 
					"    WHERE T.movie="+this.goodInput+") AS secondconnection ON g.id = secondconnection.id GROUP BY\r\n" + 
					"          g.movie) AS firstconnection ON firstconnection.movie = M.movieid and m.movieid!="+this.goodInput+" ) as q2 on q1.movieid=q2.movieid and X.movieid=q2.movieid\r\n" + 
					"left JOIN\r\n" + 
					"  (SELECT M.movieid,firstconnection.COUNT\r\n" + 
					"FROM movies M INNER JOIN (\r\n" + 
					"  SELECT COUNT(g.id) AS COUNT, g.movieid\r\n" + 
					"  FROM directors g INNER JOIN (\r\n" + 
					"    SELECT T.id\r\n" + 
					"    FROM directors T\r\n" + 
					"    WHERE T.movieid="+this.goodInput+") AS secondconnection ON g.id = secondconnection.id GROUP BY\r\n" + 
					"          g.movieid) AS firstconnection ON firstconnection.movieid = M.movieid and m.movieid!="+this.goodInput+") as q3 on q2.movieid=q3.movieid and X.movieid=q3.movieid\r\n" + 
					"left JOIN\r\n" + 
					"(SELECT M.name,M.movieid,firstconnection.COUNT\r\n" + 
					"FROM movies M INNER JOIN (\r\n" + 
					"  SELECT COUNT(g.id) AS COUNT, g.movieid\r\n" + 
					"  FROM actor g INNER JOIN (\r\n" + 
					"    SELECT T.id\r\n" + 
					"    FROM actor T\r\n" + 
					"    WHERE T.movieid="+this.goodInput+") AS secondconnection ON g.id = secondconnection.id GROUP BY\r\n" + 
					"          g.movieid) AS firstconnection ON firstconnection.movieid = M.movieid and m.movieid!="+this.goodInput+" ) as q4 on q4.movieid=q3.movieid and X.movieid=q4.movieid\r\n" + 
					"order by q1.COUNT desc,q2.COUNT desc,q3.COUNT desc,q4.COUNT desc,X.vote_count desc,X.pop desc,X.vote_average desc limit 5;"
					, 4);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (toPrint.size() != 0) {
			System.out.println("Table of ALL TIME TOP RECOMMENDATIONS:"+ this.movieinput +"Below:");
			System.out.printf("%50s\n", "Movie Name");
			String pad = String.format("%" + 103 + "s", "").replace(" ", "-");
			System.out.println(pad);
			for (int i = 0; i < toPrint.size(); i++) {
				System.out.print(toPrint.get(i));
			}
		} else
			System.out.println("No Actors Matches the Movie " + this.movieinput);
		toPrint.clear();

	}

	public boolean check() {
		boolean value = true;
		if (!databasexist)
			value = false;
		if (!tableExisit)
			value = false;
		if (!datainserted)
			value = false;
		if (!inputgiven)
			value = false;
		return value;
	}
	
	public boolean check2() {
		boolean value = true;
		if (!databasexist)
			value = false;
		if (!tableExisit)
			value = false;
		if (!datainserted)
			value = false;
		return value;
	}

	public void checkPrint() {
		if (!databasexist)
			System.out.println(" Error: Database does not Exist");
		if (!tableExisit)
			System.out.println("Error: Tables does not Exist");
		if (!datainserted)
			System.out.println("Error: Data Not Insert");
		if (!inputgiven)
			System.out.println("Error: Input not given");
	}

	public static void main(String[] args) {
		Modeling_Data app = new Modeling_Data() ;
		app.run();
	}
}