package database;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RemoteFileHandler {

    /**
     * ref: https://cloud.google.com/storage/docs
     */

    public static final String DEFAULT_DATABASE_ROOT_PATH = "Database";
    public static final String GOOGLE_BUCKET_NAME = "csci5408_dbms_remote";
    public static final String GOOGLE_PROJECT_ID = "csci5408-w21";

    private final String directoryName;
    private final String fileName;


    public RemoteFileHandler(String directoryName, String fileName) {
        this.fileName = fileName;
        this.directoryName = directoryName;
    }

    void uploadObject() throws IOException {
        String filePath;
        String googleFilePath;
        if (directoryName.equalsIgnoreCase(DEFAULT_DATABASE_ROOT_PATH)) {
            filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + fileName + ".txt";
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH +  "/" + fileName;
        } else {
            filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName + ".txt";
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName;
        }
        StorageOptions storageOptions = StorageOptions.newBuilder()
                .setProjectId(GOOGLE_PROJECT_ID)
                .setCredentials(GoogleCredentials.fromStream(new
                        FileInputStream("key.json"))).build();
        Storage storage = storageOptions.getService();
        BlobId blobId = BlobId.of(GOOGLE_BUCKET_NAME,
                googleFilePath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        Blob blob = storage.get(blobId);
        if (blob != null) {
            WritableByteChannel channel = blob.writer();
            channel.write(ByteBuffer.wrap(Files.readAllBytes(Paths.get(filePath))));
            channel.close();
        } else {
            storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
        }
        //storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
    }

    public void deleteObject() throws IOException {
        String googleFilePath;
        if (directoryName.equalsIgnoreCase(DEFAULT_DATABASE_ROOT_PATH)) {
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + fileName;
        } else {
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName;
        }

        StorageOptions storageOptions = StorageOptions.newBuilder()
                .setProjectId(GOOGLE_PROJECT_ID)
                .setCredentials(GoogleCredentials.fromStream(new
                        FileInputStream("key.json"))).build();
        Storage storage = storageOptions.getService();
        storage.delete(GOOGLE_BUCKET_NAME, googleFilePath);
    }

    public void downloadObject() {
        String filePath;
        String googleFilePath;
        if (directoryName.equalsIgnoreCase(DEFAULT_DATABASE_ROOT_PATH)) {
            filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + fileName + ".txt";
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH +  "/" + fileName;
        } else {
            filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName + ".txt";
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName;
        }
        Storage storage = StorageOptions.newBuilder().setProjectId(GOOGLE_PROJECT_ID).build().getService();
        try{
        Blob blob = storage.get(BlobId.of(GOOGLE_BUCKET_NAME, googleFilePath));
        blob.downloadTo(Paths.get(filePath));}
        catch(Exception e){
            //e.getStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            RemoteFileHandler remoteFileHandler = new RemoteFileHandler("news", "j");
            remoteFileHandler.uploadObject();
            remoteFileHandler.downloadObject();
            String filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + "news" + "/" + "j.txt";
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            while((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
