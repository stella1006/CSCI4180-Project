import java.io.*;
import java.util.*;
import java.security.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

// class Chunk {
//     byte btArr[];
//
//     Chunk (byte arr[]) {
//         btArr = arr;
//     }
//
//     byte[] getBytes() {
//         return btArr;
//     }
// }

class ByteArrayEntry {
    public byte btArr[];

    public ByteArrayEntry(byte[] data)
    {
        if (data == null)
        {
            throw new NullPointerException();
        }
        this.btArr = data;
    }

    public String toString() {
        return new String(this.btArr);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ByteArrayEntry))
        {
            return false;
        }
        return Arrays.equals(this.btArr, ((ByteArrayEntry)other).btArr);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(btArr);
    }
}

class Indexing {
    public int NumOfCheckSum;
    public int NumOfFile;
    public HashMap<ByteArrayEntry, Integer> checkSumMap;
    public HashMap<String, ArrayList<ByteArrayEntry> > fileRecipe;

    Indexing () {
        NumOfCheckSum = 0;
        NumOfFile = 0;
        checkSumMap = new HashMap<ByteArrayEntry, Integer>();
        fileRecipe = new HashMap<String, ArrayList<ByteArrayEntry> >();
    }

    Integer checkChunkExist(String str) {

        return 0;
    }

    void update(ByteArrayEntry chSum) {
        if (!checkSumMap.containsKey(chSum)) {
            checkSumMap.put(chSum,1);
        } else {
            int num = checkSumMap.get(chSum) + 1;
            checkSumMap.put(chSum, num);
        }
    }

    int loadIndexing(String filename) {
        String  thisLine = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            //get NumOfCheckSum
            if ((thisLine = br.readLine()) != null) {
                this.NumOfCheckSum = Integer.parseInt(thisLine);
            }
            //get NumOfFile
            if ((thisLine = br.readLine()) != null) {
                this.NumOfFile = Integer.parseInt(thisLine);
            }
            //get checkSum
            if ((thisLine = br.readLine()) != null) {
                StringTokenizer itr = new StringTokenizer(thisLine);
                while (itr.hasMoreTokens()) {
                    checkSumMap.put(new ByteArrayEntry(itr.nextToken().getBytes()), Integer.parseInt(itr.nextToken()));
                }
            }
            //get fileRecipe
            while ((thisLine = br.readLine()) != null) {
                StringTokenizer itr = new StringTokenizer(thisLine);
                String name = itr.nextToken();
                ArrayList<ByteArrayEntry> temp = new ArrayList<ByteArrayEntry>();
                while (itr.hasMoreTokens()) {
                    byte[] byt = itr.nextToken().getBytes();
                    ByteArrayEntry chk = new ByteArrayEntry(byt);
                    temp.add(chk);
                }
                fileRecipe.put(name, temp);
            }
        } catch (FileNotFoundException e) {
            return -1;
        } catch (IOException e) {
        	e.printStackTrace();
        }
        return 0;
    }

    void saveIndexing(String filename) throws IOException {
        FileWriter fw = new FileWriter(filename);
        fw.write(Integer.toString(NumOfCheckSum));
        fw.write("\n");
        fw.write(Integer.toString(NumOfFile));
        fw.write("\n");

        Iterator iter = checkSumMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            fw.write(((ByteArrayEntry)entry.getKey()).toString() + " " + String.valueOf(entry.getValue()) + "\n");
            fw.flush();
        }

        Iterator iter2 = fileRecipe.entrySet().iterator();
        StringBuffer sb = new StringBuffer();
        while (iter2.hasNext()) {
            Map.Entry entry = (Map.Entry)iter2.next();
            ArrayList<ByteArrayEntry> list = (ArrayList<ByteArrayEntry>)entry.getValue();

            sb.append(String.valueOf(entry.getKey()));
            for (ByteArrayEntry chk: list) {
                sb.append(" ");
                sb.append(new String(chk.btArr));
            }
            sb.append("\n");

            fw.write(sb.toString());
            fw.flush();
        }

    	fw.close();
    }

    ArrayList<ByteArrayEntry> getChunks(String filename) {
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

    // public static byte[] getCheckSum(byte data[], int len) {
    //     try {
    //         MessageDigest md = MessageDigest.getInstance("SHA-256");
    //         md.update(data, 0, len);
    //         byte[] checksumBytes = md.digest();
    //         return checksumBytes;
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    //
    //
    //     // StringBuffer sb = new StringBuffer();
    //     // for (int i = 0; i < hashBytes.length; i++) {
    //     //     sb.append(Integer.toString((hashBytes[i] & 0xff) + 0x100, 16).substring(1));
    //     // }
    //     //
    //     // return sb.toString();
    //
    // }

    static int power(int x, int y, int p) {
        int res = 1;
        x = x % p;

        while (y > 0)
        {
            if((y & 1)==1)
                res = (res * x) % p;
            y = y >> 1;
            x = (x * x) % p;
        }
        return res;
    }

    public static int RFPAlgoritm(byte bytes[], int index, int prev, int minChSize, int base, int modulus) {
        int result = 0;
        if (index == 0) {
            int sum = 0;
            for (int i = 0; i < minChSize; i++) {
                sum += ((bytes[i] % modulus) * (power(base, minChSize-i-1, modulus))) % modulus;
            }
            sum %= modulus;
            result = sum;
        } else {
            result = ((((base % modulus) * (prev % modulus)) % modulus)
                        - ((power(base,minChSize,modulus) * (bytes[0] % modulus)) % modulus)
                        + (bytes[minChSize-1] % modulus)) % modulus;
        }

        return result;
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
            int base = Integer.parseInt(args[4]);
            String fileToUpload = args[5];
            String mode = args[6];

            //get mydedup.index, or create a new one
            Indexing indexing = new Indexing();
            indexing.loadIndexing("mydedup.index");
            //indexing.saveIndexing("./savefile.index");

            try {
                InputStream is = new FileInputStream(fileToUpload);
                byte[] bytes = new byte[maxChSize];
                byte[] windows = new byte[minChSize];
                StringBuffer sb = new StringBuffer();
                int len = -1, pos = 0, prev = 0, RFPValue = 0, bound = 0;
                ArrayList<ByteArrayEntry> list = new ArrayList<ByteArrayEntry>();
                MessageDigest md = MessageDigest.getInstance("SHA-256");



                while((len = is.read(bytes))!=-1)
    			{
    				//sb.append(new String(bytes,0,len));
                    int fir = 0, sec = fir + minChSize - 1;

                    while (sec < len) {
                        System.arraycopy(bytes, fir, windows, 0, minChSize);
                        RFPValue = RFPAlgoritm(windows, pos, prev, minChSize, base, modulus);

                        if ((RFPValue & 0xFF) == 0) {
                            pos = 0;
                            //chunking
                            byte[] bytesChk = new byte[sec-bound+1];
                            System.arraycopy(bytes, bound, bytesChk, 0, sec-bound+1);
                            ByteArrayEntry chk = new ByteArrayEntry(bytesChk);
                            list.add(chk);
                            //get SHA value
                            md.update(bytesChk, 0, sec-bound+1);
                            byte[] checksumBytes = md.digest();
                            //updage checkSum in indexing
                            ByteArrayEntry chS = new ByteArrayEntry(checksumBytes);
                            indexing.update(chS);
                        } else {
                            pos++;
                        }
                        fir++;
                        sec++;
                    }
                    indexing.fileRecipe.put(fileToUpload, list);

                    //sb.setLength(0);
    			}

    			is.close();

            } catch (FileNotFoundException e) {
    			e.printStackTrace();
    		} catch (IOException e) {
    			e.printStackTrace();
    		}

        } catch (Exception e) {
          e.printStackTrace();
        }
    }
}
