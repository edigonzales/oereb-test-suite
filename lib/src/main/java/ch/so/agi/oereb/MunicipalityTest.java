package ch.so.agi.oereb;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MunicipalityTest extends BaseTest {

    /**
     * 
     *  @Description Der Test vergleicht die Gemeinden aus dem Annex-Modell mit den Gemeinden aus dem offiziellen 
     *  Gemeindegrenzdatensatz des Kantons. Der Test findet Gemeinden, die im Annex-Modell vorhanden sind, im 
     *  Gemeindegrenzdatensatz jedoch nicht.
     *  
     *  @throws Exception
     */
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

    /**
     * 
     *  @Description Der Test vergleicht die Gemeinden aus dem offiziellen Gemeindegrenzdatensatz mit den
     *  Gemeinden aus dem  dem Annex-Modell. Der Test findet Gemeinden, die im Gemeindegrenzdatensatz
     *  vorhanden sind, im Annex-Modell jedoch nicht.
     *  
     *  @throws Exception
     */
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
