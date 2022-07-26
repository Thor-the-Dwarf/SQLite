import java.sql.*;
import java.util.ArrayList;

public class SQLite {

    private Connection  con;
    private Statement   stmt;
    private String      activeDB;

       /** oeffnet Datenbank
     * */
    public void openOrCreateDatabase(String database) {
        this.activeDB = database;
        try {
            System.out.println("Treiber wird geladen...");
            Class.forName("org.sqlite.JDBC");
            System.out.println("Treiber wurde ordnungsgemäß geladen...");
            con = DriverManager.getConnection("jdbc:sqlite:" + activeDB);
            stmt = con.createStatement();
            System.out.println("Verbindung zur Datenbank hergestellt...");
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /** schliesst datenbank
     * */
    public void close() {
        try {
            this.stmt.close();
            con.close();
            System.out.println("Datenbankverbindung getrennt...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** Formuliere ein SQL-Statement und es wird ausgefuehrt
     * */
    public boolean command(String select_statement) {
    try {
        stmt.executeUpdate(select_statement);
        if(select_statement.length() > 31)
            System.out.println("command(): " + select_statement.substring(0, 30) + "... Wurde ausgeführt");
        else
            System.out.println("command(): " + select_statement + " Wurde ausgeführt");
    } catch (SQLException e) {
        e.printStackTrace();
    }
    return true;
    }

    /** Returniert ein SQL-SELECT-Statement in einem String[][]
     * */
    public String[][] getTable(String select_statement) {
        String[][] allRows = null;
        ResultSet rs = null;

        if(isSelectStatement(select_statement))
            try {
                stmt = con.createStatement();
                rs = stmt.executeQuery(select_statement);
                int rows = resultSize(select_statement);
                int columns = rs.getMetaData().getColumnCount();

                allRows = new String[rows][columns];
                for (int i = 0; i < rows; i++) {
                    rs.next();
                    String[] currentRow = new String[columns];

                    for (int j = 0; j < columns; j++)
                        currentRow[j] = rs.getString(j+1);

                    allRows[i] = currentRow;
                }
                stmt.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        return allRows;
    }

    /** Returniert ein SQL-SELECT-Statement in einer ArrayList<String[]>
     * */
    public ArrayList<String[]> getTableList(String select_statement) {
        ArrayList<String[]> allRows = null;
        ResultSet rs = null;

        if(isSelectStatement(select_statement))
            try {
                stmt = con.createStatement();
                rs = stmt.executeQuery(select_statement);
                int columns = rs.getMetaData().getColumnCount();

                allRows = new ArrayList<>();
                while (rs.next()) {
                    String[] currentRow = new String[columns];

                    for (int j = 0; j < columns; j++)
                        currentRow[j] = rs.getString(j + 1);

                    allRows.add(currentRow);
                }
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        return allRows;
    }

    /** Returniert die erste Reihe eines SELECT-Statements in einem String[]
     * */
    public String[] getSingleRow(String select_statement) {
        String[] row = null;
        ResultSet rs = null;

        if(isSelectStatement(select_statement))
            try {
                stmt = con.createStatement();
                rs = stmt.executeQuery(select_statement);
                int columns = rs.getMetaData().getColumnCount();
                row = new String[columns];

                for (int i = 0; i < columns; i++) {
                    rs.next();
                    row[i] = rs.getString(i+1);
                }
                stmt.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        return row;
    }
    /** Formuliere eine SQL-Select-Abfrage die einen int returniert und erhalte einen int zurueck
     * */
    public int getInt(String select_statemant) {
        if(isSelectStatement(select_statemant))
            try {
                stmt = null;
                stmt = con.createStatement();
                ResultSet resultSet = stmt.executeQuery(select_statemant);
                int x =  resultSet.getInt(1);
                stmt.close();
                return x;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        return 0;
    }

    /** Formuliere eine SQL-Select-Abfrage erhalte Objekt-Resultset zurueck
     * */
    public ResultSet getResultset(String select_statemant) {
        ResultSet rs = null;
        if(isSelectStatement(select_statemant))
            try {
            rs = stmt.executeQuery(select_statemant);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return rs;
    }

    /** wandelt !COUNT() - Statement in COUNT() - Statement returniert int
     * */
    public int resultSize(String s) {
        int fromIndex = s.toUpperCase().indexOf(" FROM");
        int kommaIndex = s.indexOf(",");
        int lowerIndex = ((kommaIndex > 0) & (kommaIndex < fromIndex))? kommaIndex : fromIndex;
        String firstTable = s.substring(7, lowerIndex);
        String done = "SELECT COUNT(" + firstTable + ")" + s.substring(fromIndex);

        return this.getInt(done);
    }

    /** stellt sicher das bei this.get-Methoden keine Daten verändert werden
     * */
    public boolean isSelectStatement(String select_statement) {
        String flag = select_statement.substring(0,6).toUpperCase();
        return(flag.equals("SELECT"));
    }

    public static void main(String[] args) {
        SQLite sqLite = new SQLite();
        sqLite.openOrCreateDatabase("Test1.db");

        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<command() - Test>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        sqLite.command("CREATE TABLE IF NOT EXISTS table_name (" +
                "    column1 TEXT," +
                "    column2 int," +
                "    column3 int" +
                ");" +
                "INSERT INTO table_name (column1, column2, column3) VALUES" +
                "(\"column1\", 16, 106)," +
                "(\"Test\", 18, NULL)," +
                "(\"10+10\", NULL, 20);"
        );
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<command() - Test>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<getTable() - Test>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        System.out.println("\ngetTable()");
        String[][] table = sqLite.getTable("SELECT column1, column2, column3 FROM table_name");
        for (String[] value : table) {
            for (String s : value) {
                System.out.printf("%10s", s);
            }
            System.out.println();
        }
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<getTable() - Test>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<getArrayList() - Test>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        System.out.println("\n getTableList()");
        ArrayList<String[]> list = sqLite.getTableList("SELECT * FROM table_name");
        for (String[] strings : list) {
            for (String string : strings) {
                System.out.printf("%10s", string);
            }
            System.out.println();
        }
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<getArrayList() - Test>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        System.out.println("\ngetSingleRow()");
        String[] singleRow = sqLite.getSingleRow("SELECT * FROM table_name WHERE column1 = \"column1\"");
        for (String s : singleRow) {
            System.out.printf("%10s", s);
        }
        System.out.println();
        sqLite.close();
    }
}