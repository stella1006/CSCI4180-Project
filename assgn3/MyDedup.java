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
    public String shaString;
    public int index;

    public ByteArrayEntry(byte[] data, String str, int index)
    {
        if (data == null)
        {
            throw new NullPointerException();
        }
        this.btArr = data;
        this.shaString = str;
        this.index = index;
    }

    public ByteArrayEntry(String str, int index)
    {
        this.btArr = null;
        this.shaString = str;
        this.index = index;
    }

    public void setString(String str) {
        this.shaString = str;
    }

    @Override
    public int hashCode() {
        return this.shaString.hashCode();
    }
}

class Indexing {
    public int NumOfCheckSum;
    public int NumOfZeroChunks;
    public HashMap<String, Integer> checkSumMap;
    public HashMap<String, Integer> zeroChunks;

    public int NumOfFile;
    public HashMap<String, ArrayList<ByteArrayEntry> > fileRecipe;

    Indexing () {
        NumOfCheckSum = 0;
        NumOfZeroChunks = 0;
        checkSumMap = new HashMap<String, Integer>();
        zeroChunks = new HashMap<String, Integer>();
        NumOfFile = 0;
        fileRecipe = new HashMap<String, ArrayList<ByteArrayEntry> >();
    }

    int updateCheckSumMap(String chSum, int numOfZero) {
        if (numOfZero == 0) {
            if (!this.checkSumMap.containsKey(chSum)) {
                this.checkSumMap.put(chSum,1);
                this.NumOfCheckSum++;
                return 1;
            }
        } else {
            if (!this.zeroChunks.containsKey(chSum)) {
                this.zeroChunks.put(chSum,numOfZero);
                this.NumOfZeroChunks++;
            }
        }

        return 0;
    }

    int loadIndexing(String filename) {
        String  thisLine = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            //get NumOfCheckSum
            if ((thisLine = br.readLine()) != null) {
                this.NumOfCheckSum = Integer.parseInt(thisLine);
            }

            //get NumOfZeroChunks
            if ((thisLine = br.readLine()) != null) {
                this.NumOfZeroChunks = Integer.parseInt(thisLine);
            }

            //get NumOfFile
            if ((thisLine = br.readLine()) != null) {
                this.NumOfFile = Integer.parseInt(thisLine);
            }

            //get checkSum
            for (int i = 0; i < this.NumOfCheckSum; i++) {
                thisLine = br.readLine();
                this.checkSumMap.put(thisLine, 1);
            }

            //get zeroChunks
            for (int i = 0; i < this.NumOfZeroChunks; i++) {
                thisLine = br.readLine();
                String[] spl = thisLine.split(" ");
                this.zeroChunks.put(spl[0], Integer.parseInt(spl[1]));
            }

            //get fileRecipe
            for (int i = 0; i < this.NumOfFile; i++) {
                String name = br.readLine();
                thisLine = br.readLine();
                //System.out.println("num: " + thisLine);
                Integer num = Integer.parseInt(thisLine);
                ArrayList<ByteArrayEntry> list = new ArrayList<ByteArrayEntry>();
                for (int j = 0; j < num; j++) {
                    thisLine = br.readLine();
                    ByteArrayEntry arrTemp = new ByteArrayEntry(thisLine, j);
                    list.add(arrTemp);
                }
                this.fileRecipe.put(name, list);
            }
            while ((thisLine = br.readLine()) != null) {
                // StringTokenizer itr = new StringTokenizer(thisLine);
                //while (itr.hasMoreTokens()) {
                this.checkSumMap.put(thisLine, 1);
                //}
            }

            br.close();
        } catch (FileNotFoundException e) {
            return -1;
        } catch (IOException e) {
        	e.printStackTrace();
        }
        return 0;
    }

    void saveIndexing(String filename) throws IOException {
        FileWriter els = new FileWriter("./test_1.txt");
        els.write("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
        els.close();

        FileWriter fw = new FileWriter(filename);
        fw.write(Integer.toString(NumOfCheckSum));
        fw.write("\n");
        fw.write(Integer.toString(NumOfZeroChunks));
        fw.write("\n");
        fw.write(Integer.toString(NumOfFile));
        fw.write("\n");

        //write checkSumMap
        Iterator iter = this.checkSumMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            fw.write((entry.getKey()).toString() + "\n");
            fw.flush();
        }

        //write zeroChunks
        Iterator iter_zero = this.zeroChunks.entrySet().iterator();
        while (iter_zero.hasNext()) {
            Map.Entry entry = (Map.Entry)iter_zero.next();
            fw.write((entry.getKey()).toString() + " " + (entry.getValue()).toString() + "\n");
            fw.flush();
        }

        //write fileRecipe
        Iterator iter2 = this.fileRecipe.entrySet().iterator();
        while (iter2.hasNext()) {
            Map.Entry entry = (Map.Entry)iter2.next();
            ArrayList<ByteArrayEntry> list = (ArrayList<ByteArrayEntry>)entry.getValue();

            fw.write((entry.getKey()).toString() + "\n");
            fw.write(Integer.toString(list.size()) + "\n");
            fw.flush();

            for (ByteArrayEntry byArr :list) {
                fw.write(byArr.shaString + "\n");
            }
            fw.flush();

        }
    	fw.close();
    }
    void updateFileRecipe(String str, ArrayList<ByteArrayEntry> list) {
        if (!fileRecipe.containsKey(str)) {
            NumOfFile++;
        }
        fileRecipe.put(str, list);

    }

    void reconstructFile(String path, String filename, String output_name) throws IOException {
        // Collections.sort(list,com);
        ArrayList<ByteArrayEntry> list = this.fileRecipe.get(filename);
        //System.out.println(list.size());
        StringBuffer sb = new StringBuffer();
        for (ByteArrayEntry chk: list) {

            if (this.zeroChunks.containsKey(chk.shaString)) {
                int numOfZero = zeroChunks.get(chk.shaString)
            } else {
                String input_name = path + chk.shaString;
                File file = new File(input_name);
    			InputStream is = new FileInputStream(file);
    			byte[] bytes = new byte[(int)file.length()];
                int len = is.read(bytes);
    			is.close();
                if (len>0 && bytes != null) sb.append(new String(bytes));
            }
        }

        FileWriter fw = new FileWriter(output_name);
        fw.write(sb.toString());
        fw.close();
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

    public static boolean byteArrayCheck(final byte[] array, int fir, int sec) {
        int sum = 0;
        for (int i = fir; i <= sec; i++) {
            sum |= array[i];
        }
        return (sum == 0);
    }

    public static int getNonZeroByte(final byte[] array, int pos) {
        int sum = 0;
        int i = pos;
        for (; i < array.length; i++) {
            sum |= array[i];
            if (sum != 0) break;
        }
        return i;
    }

    public static ByteArrayEntry chunking(MessageDigest md, byte[] bytes, int bound, int sec, int index) {

        byte[] bytesChk = new byte[sec-bound+1];
        System.arraycopy(bytes, bound, bytesChk, 0, sec-bound+1);

        // ByteArrayEntry res = new ByteArrayEntry(bytesChk, index);
        //list.add(chk);
        //get SHA value
        md.update(bytesChk, 0, sec-bound+1);
        byte[] checksumBytes = md.digest();
        //updage checkSum in indexing
        StringBuffer sb2 = new StringBuffer();
        for (int i = 0; i < checksumBytes.length; i++) {
            sb2.append(Integer.toString((checksumBytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        ByteArrayEntry res = new ByteArrayEntry(bytesChk, sb2.toString(), index);
        // res.setString(sb2.toString());
        //res.add(new ByteArrayEntry(checksumBytes));
        //indexing.update(chS);
        return res;
    }

    public static void uploadChunk(String path, ByteArrayEntry byteEntry) throws IOException {
        String output_name = path +  byteEntry.shaString;
        FileWriter fw = new FileWriter(output_name);
        fw.write(new String(byteEntry.btArr));
        fw.close();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            usage();
            System.exit(1);
        }

        String operation = args[0];
        String indexFileName = "mydedup.index";
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
            indexing.loadIndexing(indexFileName);
            indexing.saveIndexing("./mid.index");
            //FileRecipe fr = new FileRecipe();
            //fr.loadFileChunks("./data/");
            //fr.reconstructFile("./mid/", fileToUpload);
            //indexing.saveIndexing("./savefile.index");

            try {
                InputStream is = new FileInputStream(fileToUpload);
                byte[] bufferMax = new byte[maxChSize];
                byte[] windows = new byte[minChSize];
                byte[] sByte = new byte[1];
                int rLen = -1, pos = 0, prev = 0, RFPValue = 0, index = 0;
                int allLen = -1, fir = 0, winSize = minChSize;
                boolean lastChunk = true, lastZero = false, isEnd = false;
                StringBuffer sbZeros = new StringBuffer();
                ArrayList<ByteArrayEntry> list = new ArrayList<ByteArrayEntry>();
                MessageDigest md = MessageDigest.getInstance("SHA-256");

                while (true) {
                    // System.out.println("ZZ");
                    if (lastChunk == true) {
                        // System.out.println("ZZ1");
                        Arrays.fill(bufferMax, (byte)0);
                        Arrays.fill(windows, (byte)0);
                        allLen = winSize;
                        fir = 0;
                        if (!lastZero) {
                            if((rLen = is.read(windows))!=-1) {
                                winSize = rLen;
                                System.arraycopy(windows, 0, bufferMax, 0, winSize);
                                allLen = rLen;
                                fir = 0;
                                lastChunk = false;
                            } else break;
                        } else {
                            lastZero = false;
                            allLen = 1;
                            windows[0] = sByte[0];
                            bufferMax[0] = sByte[0];
                            byte[] temp = new byte[winSize-1];
                            if((rLen = is.read(temp))!=-1) {
                                lastChunk = false;
                                System.arraycopy(temp, 0, bufferMax, 1, rLen);
                                System.arraycopy(temp, 0, windows, 1, rLen);
                                winSize = rLen + 1;
                                allLen = rLen + 1;
                                fir = 0;
                            } else {
                                // System.out.println("zeros_end");
                                //if (bufferMax != null) {
                                    ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1, index++);
                                    list.add(res);
                                    int temStatus = indexing.updateCheckSumMap(res.shaString);
                                    if (temStatus == 1) {
                                        uploadChunk("./data/", res);
                                    }
                                //}
                                break;
                            }
                        }
                    } else {
                        // System.out.println("ZZ2");
                        if (lastZero == true) {
                            // System.out.println("YY");
                            lastZero = false;
                            sbZeros.append((new String(bufferMax)).substring(0,allLen));
                            int tpp=0;
                            while ((rLen = is.read(sByte))!=-1) {
                                if (sByte[0] == '\0') {
                                    // System.out.println(Integer.toString(++tpp));
                                    sbZeros.append(new String(sByte));
                                } else break;
                            }
                            //chunking
                            lastChunk = true;
                            byte[] zeros = sbZeros.toString().getBytes();
                            // System.out.println(Integer.toString(zeros.length));
                            ByteArrayEntry res = chunking(md, zeros, 0, zeros.length-1, index++);
                            list.add(res);
                            int temStatus = indexing.updateCheckSumMap(res.shaString);
                            if (temStatus == 1) {
                                uploadChunk("./data/", res);
                            }
                            //read EOF
                            if (rLen == -1) break;
                        } else {
                            if((rLen = is.read(sByte))!=-1) {
                                bufferMax[allLen++] = sByte[0];
                                System.arraycopy(bufferMax, ++fir, windows, 0, winSize);
                            } else {
                                // System.out.println(Integer.toString(22));
                                ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1, index++);
                                list.add(res);
                                int temStatus = indexing.updateCheckSumMap(res.shaString);
                                if (temStatus == 1) {
                                    uploadChunk("./data/", res);
                                }
                                break;
                            }
                        }


                    }

                    // //zero chunks
                    // if (byteArrayCheck(windows) == true) {
                    //     System.out.println(Integer.toString(1));
                    //     lastZero = true;
                    // } else {

                    RFPValue = RFPAlgoritm(windows, pos, prev, winSize, base, modulus);
                    prev = RFPValue;

                    if ((RFPValue & 0xFF) == 0 || allLen == maxChSize) {
                        //zero Chunks
                        if (byteArrayCheck(bufferMax, 0, allLen-1)) {
                            lastZero = true;
                            // System.out.println("XX");
                        } else {
                            pos = 0;
                            lastChunk = true;
                            // System.out.println("normal");
                            ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1, index++);
                            list.add(res);
                            //update indexing
                            int temStatus = indexing.updateCheckSumMap(res.shaString);
                            //upload
                            if (temStatus == 1) {
                                uploadChunk("./data/", res);
                            }
                        }
                    } else {
                        lastChunk = false;
                        pos++;
                    }
                    // }
                }
                is.close();
                indexing.updateFileRecipe(fileToUpload, list);
                //fr.saveFileChunks("./data/", fileToUpload);
                indexing.saveIndexing(indexFileName);
                indexing.reconstructFile("./data/", fileToUpload, "./construction.txt");
            }
            catch (FileNotFoundException e) {
    			e.printStackTrace();
    		} catch (IOException e) {
    			e.printStackTrace();
    		}

        } catch (Exception e) {
          e.printStackTrace();
        }
    }
}
