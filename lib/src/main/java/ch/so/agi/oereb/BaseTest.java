package ch.so.agi.oereb;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.gui.Config;
import ch.ehi.ili2h2gis.H2gisMain;
import net.lingala.zip4j.ZipFile;

public class BaseTest {
    static String DB_FILE_NAME = "hoheitsgrenzen_annex";
    static String MUNICIPALITY_MODEL = "SO_Hoheitsgrenzen_Publikation_20170626";
    static String MUNICIPALITY_FILE_NAME = "ch.so.agi.hoheitsgrenzen.xtf";
    static String ANNEX_MODEL = "OeREB_ExtractAnnex_V1_0";
    static String ANNEX_FILE_NAME = "ch.so.agi.OeREB_extractAnnex.oereb.xtf";
    static String ANNEX_FILE_URL = "https://s3.eu-central-1.amazonaws.com/ch.so.agi.geodata/ch.so.agi.OeREB_extractAnnex.oereb_xtf.zip";

    static String ANNEX_MUNICIPALITYWITHPLRC = "ANNEX_MUNICIPALITYWITHPLRC";
    static String HOHEITSGRENZEN_GEMEINDEGRENZE = "HOHEITSGRENZEN_GEMEINDEGRENZE";

    static Logger log = LoggerFactory.getLogger(MunicipalityTest.class);

    static String dbUrl;

    @TempDir
    static File tempDir;

    @BeforeAll
    public static void setupDatabase() throws Exception {
        Config settings = new Config();
        new H2gisMain().initConfig(settings);
        settings.setFunction(Config.FC_SCHEMAIMPORT);
        settings.setModels(MUNICIPALITY_MODEL+";"+ANNEX_MODEL);
        settings.setItfTransferfile(false);

        settings.setDoImplicitSchemaImport(true);
        settings.setDefaultSrsCode("2056");
        settings.setNameOptimization(settings.NAME_OPTIMIZATION_TOPIC);
        Config.setStrokeArcs(settings, Config.STROKE_ARCS_ENABLE);
        settings.setValidation(false);

        String h2FileName = Paths.get(tempDir.getAbsolutePath(), DB_FILE_NAME).toFile().getAbsolutePath();
        settings.setDbfile(h2FileName);
        dbUrl = "jdbc:h2:file:" + h2FileName;
        settings.setDburl(dbUrl);
        Ili2db.run(settings, null);

        InputStream municipalityInputStream = MunicipalityTest.class.getClassLoader().getResourceAsStream(MUNICIPALITY_FILE_NAME);
        File municipalityFile = Paths.get(tempDir.getAbsolutePath(), MUNICIPALITY_FILE_NAME).toFile();
        Files.copy(municipalityInputStream, municipalityFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        IOUtils.closeQuietly(municipalityInputStream);

        settings.setFunction(Config.FC_IMPORT);
        settings.setModels(MUNICIPALITY_MODEL);
        settings.setConfigReadFromDb(true);
        settings.setDoImplicitSchemaImport(false);
        settings.setXtffile(municipalityFile.getAbsolutePath());
        Ili2db.run(settings, null);

        URL url = new URL(ANNEX_FILE_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(4000);
        connection.setReadTimeout(20000);
        connection.setRequestMethod("GET");

        File annexZipFile = Paths.get(tempDir.getAbsolutePath(), ANNEX_FILE_NAME + ".zip").toFile();
        InputStream annexInputStream = connection.getInputStream();
        Files.copy(annexInputStream, annexZipFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        annexInputStream.close();

        new ZipFile(annexZipFile.getAbsolutePath()).extractAll(tempDir.getAbsolutePath());

        settings.setModels(ANNEX_MODEL);
        settings.setXtffile(Paths.get(tempDir.getAbsolutePath(), ANNEX_FILE_NAME).toFile().getAbsolutePath());
        Ili2db.run(settings, null);

//        Files.copy(Paths.get(tempDir.getAbsolutePath(), DB_FILE_NAME +".mv.db"),
//                Paths.get("/Users/stefan/tmp/", DB_FILE_NAME + ".mv.db"),
//                StandardCopyOption.REPLACE_EXISTING);

    }
}
