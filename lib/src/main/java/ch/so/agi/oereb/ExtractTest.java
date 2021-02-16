package ch.so.agi.oereb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ExtractTest extends BaseTest {

    @ParameterizedTest(name = "#{index} - Test with municipality : {0}")
    @MethodSource("municipalitiesWithPLR")
    public void xmlExtract(String arg) {
        assertNotNull(arg);
    }
    
    private static String[] municipalitiesWithPLR() throws Exception {
        List<String> municipalityList = new ArrayList<String>();
        try (Connection conn = DriverManager.getConnection(dbUrl); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT CAST(MUNICIPALITY AS TEXT) FROM ANNEX_MUNICIPALITYWITHPLRC;");
            while(rs.next()) {
                municipalityList.add(rs.getString(1));
            }
            String[] municipalityArray = new String[municipalityList.size()];
            municipalityArray = municipalityList.toArray(municipalityArray);
            return municipalityArray;
        } catch (SQLException e) {
            throw new Exception(e.getMessage());
        }
    } 
}
