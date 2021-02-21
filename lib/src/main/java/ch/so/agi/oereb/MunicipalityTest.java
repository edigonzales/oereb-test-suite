package ch.so.agi.oereb;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MunicipalityTest extends BaseTest {

    @Test
    public void annexMatchesMunicipalities() throws Exception {
        try (var conn = DriverManager.getConnection(dbUrl); var stmt = conn.createStatement()) {
//            ResultSet rs = stmt.executeQuery(
//                    "SELECT \n" +
//                            "ANNEX.T_ID, \n" +
//                            "ANNEX.MUNICIPALITY \n" +
//                            "FROM \n" +
//                            ANNEX_MUNICIPALITYWITHPLRC + " AS ANNEX \n" +
//                            "LEFT JOIN "+HOHEITSGRENZEN_GEMEINDEGRENZE+" AS GEMEINDE \n" +
//                            "ON GEMEINDE.BFS_GEMEINDENUMMER = ANNEX.MUNICIPALITY \n" +
//                            "WHERE \n" +
//                            "GEMEINDE.BFS_GEMEINDENUMMER IS NULL\n" +
//                            ";"
//            );
            
            var sql = """
            SELECT 
                annex.T_ID,
                annex.MUNICIPALITY
            FROM 
                ANNEX_MUNICIPALITYWITHPLRC AS ANNEX 
                LEFT JOIN HOHEITSGRENZEN_GEMEINDEGRENZE AS GEMEINDE
                ON GEMEINDE.BFS_GEMEINDENUMMER = ANNEX.MUNICIPALITY 
            WHERE 
                gemeinde.BFS_GEMEINDENUMMER IS NULL
            """;
            
            var rs = stmt.executeQuery(sql);

            assertFalse(rs.next(), "Annex data has superfluous municipalities.");

        } catch (SQLException e) {
            throw new Exception(e.getMessage());
        }
    }

    @Test
    public void municipalitiesMatchesAnnex() throws Exception {
        try (var conn = DriverManager.getConnection(dbUrl); var stmt = conn.createStatement()) {
            var rs = stmt.executeQuery(
                    "SELECT \n" +
                            "GEMEINDE.T_ID, \n" +
                            "GEMEINDE.BFS_GEMEINDENUMMER \n" +
                            "FROM \n" +
                            ANNEX_MUNICIPALITYWITHPLRC + " AS ANNEX \n" +
                            "RIGHT JOIN "+HOHEITSGRENZEN_GEMEINDEGRENZE+" AS GEMEINDE \n" +
                            "ON GEMEINDE.BFS_GEMEINDENUMMER = ANNEX.MUNICIPALITY \n" +
                            "WHERE \n" +
                            "ANNEX.MUNICIPALITY IS NULL\n" +
                            ";"
            );

            assertFalse(rs.next(), "Annex data has missing municipalities.");

        } catch (SQLException e) {
            throw new Exception(e.getMessage());
        }
    }
}
