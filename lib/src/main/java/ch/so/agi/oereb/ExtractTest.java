package ch.so.agi.oereb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.xml.transform.stream.StreamSource;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.locationtech.jts.geom.Point;

import ch.ehi.oereb.schemas.oereb._1_0.extract.GetEGRIDResponse;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetEGRIDResponseType;
import jakarta.xml.bind.JAXBElement;

public class ExtractTest extends BaseTest {

    @Order(1)
    @ParameterizedTest(name = "#{index} - Test with municipality : {0}")
    @MethodSource("municipalitiesWithPLR")
    public void xmlGetEgrid(Map.Entry<String, Point> arg) throws Exception {
        assertNotNull(arg);
        
        var fosnr = arg.getKey();
        var point = arg.getValue();
        
        var url = new URL(OEREB_SERVICE_BASE_URL + "getegrid/xml/?XY="+point.getX()+","+point.getY());
        var connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(4000);
        connection.setReadTimeout(20000);
        connection.setRequestMethod("GET");

        var getEgridFile = Paths.get(tempDir.getAbsolutePath(), fosnr+"_"+String.valueOf(point.getX()) + "_" +String.valueOf(point.getY())+ ".xml").toFile();
        var getEgridInputStream = connection.getInputStream();
        Files.copy(getEgridInputStream, getEgridFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        getEgridInputStream.close();
        
        var content = new String(Files.readAllBytes(Paths.get(getEgridFile.getAbsolutePath())));
        System.out.println(content);
        

        
        var xmlSource = new StreamSource(getEgridFile);
        var obj = (GetEGRIDResponse) unmarshaller.unmarshal(xmlSource);
        var egridResponseType = obj.getValue();
        var egridXmlList = egridResponseType.getEgridAndNumberAndIdentDN();

        for (int i=0; i<egridXmlList.size(); i++) {
            
            System.out.println(egridXmlList.get(i).getValue());
        }

//        for (int i=0; i<egridXmlList.size(); i=i+3) {
//            Egrid egridObj = new Egrid();
//            egridObj.setEgrid(egridXmlList.get(i).getValue());
//            egridObj.setNumber(egridXmlList.get(i+1).getValue());
//            egridObj.setIdentDN(egridXmlList.get(i+2).getValue());
//            egridObj.setOerebServiceBaseUrl(oerebBaseUrl);
//            logger.debug("E-GRID: " + egridObj.getEgrid());
//            egridList.add(egridObj);
//        }

        
    }
    
    @Order(2)
    @Test
    public void fubar() {
        System.out.println("fubar");
    }
    
    
    private static Stream<Map.Entry<String, Point>> municipalitiesWithPLR() throws Exception {
        Map<String, Point> municipalityMap = new HashMap<String, Point>();
        try (Connection conn = DriverManager.getConnection(dbUrl); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT \n"
                    + "    CAST(MUNICIPALITY AS TEXT) AS bfsnr, \n"
                    + "    ST_PointOnSurface(GEOMETRIE) AS coord  \n"
                    + "FROM \n"
                    + "    "+ANNEX_MUNICIPALITYWITHPLRC+" AS plr \n"
                    + "    LEFT JOIN "+HOHEITSGRENZEN_GEMEINDEGRENZE+" AS gemeinde \n"
                    + "    ON plr.MUNICIPALITY = gemeinde.BFS_GEMEINDENUMMER ");
            while(rs.next()) {
                municipalityMap.put(rs.getString(1), (Point) rs.getObject(2));
                
                break;
                
                
            }            
            Stream<Map.Entry<String, Point>> stream = mapToStream(municipalityMap);
            return stream;
        } catch (SQLException e) {
            throw new Exception(e.getMessage());
        }
    } 
    
    private static <K,V> Stream<Map.Entry<K,V>> mapToStream (Map<K,V> map) {
        return map.entrySet().stream();
    }
}
