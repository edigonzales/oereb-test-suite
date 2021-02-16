package ch.so.agi.oereb;

import static org.junit.jupiter.api.Assertions.assertFalse;

import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.gui.Config;
import ch.ehi.ili2h2gis.H2gisMain;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MunicipalityTest extends BaseTest {

    @Test
    public void annexMatchesMunicipalities() throws Exception {
        try (Connection conn = DriverManager.getConnection(dbUrl); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT \n" +
                            "ANNEX.T_ID, \n" +
                            "ANNEX.MUNICIPALITY \n" +
                            "FROM \n" +
                            ANNEX_MUNICIPALITYWITHPLRC + " AS ANNEX \n" +
                            "LEFT JOIN "+HOHEITSGRENZEN_GEMEINDEGRENZE+" AS GEMEINDE \n" +
                            "ON GEMEINDE.BFS_GEMEINDENUMMER = ANNEX.MUNICIPALITY \n" +
                            "WHERE \n" +
                            "GEMEINDE.BFS_GEMEINDENUMMER IS NULL\n" +
                            ";"
            );

            assertFalse(rs.next(), "Annex data has superfluous municipalities.");

        } catch (SQLException e) {
            throw new Exception(e.getMessage());
        }
    }

    @Test
    public void municipalitiesMatchesAnnex() throws Exception {
        try (Connection conn = DriverManager.getConnection(dbUrl); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
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
