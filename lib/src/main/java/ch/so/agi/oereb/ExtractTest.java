package ch.so.agi.oereb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.locationtech.jts.geom.Point;
import org.xml.sax.SAXException;

import ch.ehi.oereb.schemas.oereb._1_0.extract.GetEGRIDResponse;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetExtractByIdResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(ExtractTest.class);

    private static SchemaFactory factory;
    private static Schema schema;
    private static Validator validator;
    
    @BeforeAll
    public static void setupValidators() throws Exception {
        factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        schema = factory.newSchema(new File("src/main/xsd/OeREB/1.0/Extract.xsd"));
        validator = schema.newValidator();
    }
    
    /**
     * 
     *  @Description Der Test versucht mit einer beliebigen Koordinate innerhalb der Gemeinde einen 
     *  XML-Auszug anzufordern. Geprüft wird folgendes: <br><br>
     *  
     *  - Koordinate darf nicht NULL sein. Falls die Koordinate NULL ist, stimmen Annex-Modell und 
     *  Gemeindegrenzdatensatz nicht überein.
     *  <br>
     *  - Mit der Koordinate wird ein GetEgrid-Request ausgeführt. Der zurückgelieferte Status-Code
     *  muss 200 sein. Das XML wird auf Schemakonformität geprüft.
     *  <br>
     *  - Mit dem so eruierten E-GRID wird ein (reduced) Extract-Request gemacht. Respektive mehrere, falls 
     *  mehrere Grundstücke betroffen sind. Der zurückgelieferte Status-Code muss 200 sein und das
     *  XML wird auf Schemakonformität geprüft.
     *  <br>
     *  - Falls im Auszug Eigentumsbeschränkungen vorhanden sind, müssen auch WMS-Requests vorhanden sind. 
     *  Es wird geprüft, ob die WMS-Requests den Status-Code 200 zurücklieferen.
     *    
     *  @throws Exception
     */
    @ParameterizedTest(name = "#{index} - Test with municipality : {0}")
    @MethodSource("municipalitiesWithPLR")
    public void extract_Ok(Map.Entry<String, Point> arg) throws Exception {
        assertNotNull(arg);
        
        var fosnr = arg.getKey();
        var point = arg.getValue();
        
        assertNotNull(fosnr, "FOS nr must not be null.");
        assertNotNull(point, "Coordinate must not be null.");
        
        var egridUrl = OEREB_SERVICE_BASE_URL + "getegrid/xml/?XY="+point.getX()+","+point.getY();
        var egridClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        var egridRequest = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(egridUrl))
                .timeout(Duration.ofSeconds(30))
                .header("Accept", "application/xml")
                .build();

        var getEgridPath = Paths.get(tempDir.getAbsolutePath(), fosnr+"_"+String.valueOf(point.getX()) + "_" +String.valueOf(point.getY())+ ".xml");
        var egridResponse = egridClient.send(egridRequest, BodyHandlers.ofFile(getEgridPath));

        assertEquals(200, egridResponse.statusCode(), "Wrong GetEgrid response code.");
        
        var content = new String(Files.readAllBytes(getEgridPath));
        
        var xsdCompliant = true;
        try {
            validator.validate(new StreamSource(getEgridPath.toFile()));   
        } catch (SAXException e) {
            e.printStackTrace();
            xsdCompliant = false;
        }
        
        assertTrue(xsdCompliant, "GetEgrid response is not xsd compliant: \n " + content);

        var xmlSource = new StreamSource(getEgridPath.toFile());
        var obj = (GetEGRIDResponse) unmarshaller.unmarshal(xmlSource);
        var egridResponseType = obj.getValue();
        var egridXmlList = egridResponseType.getEgridAndNumberAndIdentDN();

        var egrids = new ArrayList<String>();
        for (int i=0; i<egridXmlList.size(); i+=3) {
            var egrid = egridXmlList.get(i).getValue();
            var identdn = egridXmlList.get(i+2).getValue();
            
            assertEquals(14, egrid.length(), "Not valid egrid.");
            assertEquals(12, identdn.length(), "Not valid identdn.");
            
            egrids.add(egrid);
        }
        
        for (var egrid : egrids) {
            var extractUrl = OEREB_SERVICE_BASE_URL + "extract/reduced/xml/geometry/"+egrid;
            
            var extractClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
            var extractRequest = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create(extractUrl))
                    .timeout(Duration.ofSeconds(30))
                    .header("Accept", "application/xml")
                    .build();

            var getExtractPath = Paths.get(tempDir.getAbsolutePath(), fosnr+"_"+egrid+ ".xml");
            var extractResponse = extractClient.send(extractRequest, BodyHandlers.ofFile(getExtractPath));

            assertEquals(200, extractResponse.statusCode(), "Wrong Extract response status code for " + extractUrl.toString());
            
            var extractXsdCompliant = true;
            try {
                validator.validate(new StreamSource(getExtractPath.toFile()));   
            } catch (SAXException e) {
                e.printStackTrace();
                extractXsdCompliant = false;
            }

            assertTrue(extractXsdCompliant, "Extract response is not xsd compliant.");
            
            var extractXmlSource = new StreamSource(getExtractPath.toFile());
            var extractObj = (GetExtractByIdResponse) unmarshaller.unmarshal(extractXmlSource);
            var xmlExtract = extractObj.getValue().getExtract().getValue();
            
            var concernedThemes = xmlExtract.getConcernedTheme();
            if (concernedThemes.size() > 0) {
                xmlExtract.getRealEstate().getRestrictionOnLandownership().stream().forEach(r -> {
                    var wmsUrl = r.getMap().getReferenceWMS();
                    var wmsRequest = HttpRequest.newBuilder()
                            .GET()
                            .uri(URI.create(wmsUrl))
                            .timeout(Duration.ofSeconds(30))
                            .build();
                    
                    try {
                        log.info(wmsUrl);
                        var wmsResponse = extractClient.send(wmsRequest, BodyHandlers.discarding());
                        assertEquals(200, wmsResponse.statusCode(), "Wrong response status code for WMS request.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }   
        }
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
                 //break;
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
