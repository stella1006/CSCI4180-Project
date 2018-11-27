import java.io.*;
import java.util.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

class Chunk {
    byte btArr[];

    Chunk (byte arr[]) {
        btArr = arr;
    }
}

class Indexing {
    HashMap<String, Integer> checkSum;
    HashMap<String, ArrayList<Chunk> > fileRecipe;

    Indexing () {
        checkSum = new HashMap<String, Integer>();
        fileRecipe = new HashMap<String, ArrayList<Chunk> >();
    }
    Integer checkChunkExist(String str) {

        return 0;
    }

    ArrayList<Chunk> getChunks(String filename) {
        return fileRecipe.get(filename);
    }
}

public class MyDedup {
    public static final String storageConnectionString = "";

    static {
      System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
      System.setProperty("https.proxyPort", "8000");
      System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
      System.setProperty("http.proxyPort", "8000");
    }

    public static void usage() {
      System.out.println("Usage: ");
      System.out.println("    java -cp .:./lib/* MyDedup upload [min_chunk] [avg_chunk] [max_chunk] [d] [file_to_upload] [local|azure]");
      System.out.println("    java -cp .:./lib/* MyDedup download [file_to_download] [local_file_name] [local|azure]");
      System.out.println("    java -cp .:./lib/* MyDedup delete [file_to_delete] [local|azure]");
    }

    public static String getSHA(byte hashBytes[]) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < hashBytes.length; i++) {
            sb.append(Integer.toString((hashBytes[i] & 0xff) + 0x100, 16).substring(1));
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            usage();
            System.exit(1);
        }

        String operation = args[0];
        try {
            if (operation.equals("upload")) {
                if (args.length < 7) {
                    usage();
                    System.exit(1);
                }
            }
            int minChSize = Integer.parseInt(args[1]);
            int modulus = Integer.parseInt(args[2]);
            int maxChSize = Integer.parseInt(args[3]);
            String fileToUpload = args[4];
            String mode = args[5];

        } catch (Exception e) {
          e.printStackTrace();
        }
    }
}
