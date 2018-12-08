import java.io.*;
import java.util.*;
import java.security.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

class ByteArrayEntry {
    public byte btArr[];
    public String shaString;
    public int byteLength;

    public ByteArrayEntry(byte[] data, String str)
    {
        if (data == null)
        {
            throw new NullPointerException();
        }
        this.btArr = data;
        this.shaString = str;
        this.byteLength = data.length;
    }

    public ByteArrayEntry(String str, int byteLength)
    {
        this.btArr = null;
        this.shaString = str;
        this.byteLength = byteLength;
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
    int NumOfCheckSum;
    int NumOfZeroChunks;
    int NumOfFile;
    int NumOfPysical;
    int WithDepu;
    int WithoutDepu;
    public HashMap<String, Integer> checkSumMap;
    public HashMap<String, ArrayList<Integer> > zeroChunks;
    public HashMap<String, ArrayList<ByteArrayEntry> > fileRecipe;

    Indexing () {
        this.NumOfCheckSum = 0;
        this.NumOfZeroChunks = 0;
        this.NumOfPysical = 0;
        this.WithDepu = 0;
        this.WithoutDepu = 0;
        this.checkSumMap = new HashMap<String, Integer>();
        this.zeroChunks = new HashMap<String, ArrayList<Integer>>();
        this.NumOfFile = 0;
        this.fileRecipe = new HashMap<String, ArrayList<ByteArrayEntry> >();
    }

    int getTotalLogical() {
        return (this.NumOfCheckSum + this.NumOfZeroChunks);
    }
    int getTotalPysical() {
        return this.NumOfPysical;
    }
    int getWithDepu() {
        return this.WithDepu;
    }
    int getWithoutDepu() {
        return this.WithoutDepu;
    }

    void updateCheckSumMap(ByteArrayEntry res, int numOfZero)  throws IOException {
        if (numOfZero == 0) {
            if (!this.checkSumMap.containsKey(res.shaString)) {
                this.checkSumMap.put(res.shaString,1);
                this.NumOfCheckSum++;
                uploadChunk("./data/", res);
            } else {
                int no = this.checkSumMap.get(res.shaString)+1;
                this.checkSumMap.put(res.shaString,no);

            }
        } else {
            if (!this.zeroChunks.containsKey(res.shaString)) {
                ArrayList<Integer> list = new ArrayList<Integer>();
                list.add(numOfZero);
                list.add(1);
                this.zeroChunks.put(res.shaString,list);
                this.NumOfZeroChunks++;
            } else {
                ArrayList<Integer> list = this.zeroChunks.get(res.shaString);
                int tmp = list.get(1)+1;
                list.set(1, tmp);
                this.zeroChunks.put(res.shaString,list);

            }
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

            //get NumOfZeroChunks
            if ((thisLine = br.readLine()) != null) {
                this.NumOfZeroChunks = Integer.parseInt(thisLine);
            }

            //get NumOfFile
            if ((thisLine = br.readLine()) != null) {
                this.NumOfFile = Integer.parseInt(thisLine);
            }

            //get NumOfPysical
            if ((thisLine = br.readLine()) != null) {
                this.NumOfPysical = Integer.parseInt(thisLine);
            }

            //get WithDepu
            if ((thisLine = br.readLine()) != null) {
                this.WithDepu = Integer.parseInt(thisLine);
            }

            //get WithoutDepu
            if ((thisLine = br.readLine()) != null) {
                this.WithoutDepu = Integer.parseInt(thisLine);
            }

            //get checkSum
            for (int i = 0; i < this.NumOfCheckSum; i++) {
                thisLine = br.readLine();
                String[] spl = thisLine.split(" ");
                this.checkSumMap.put(spl[0], Integer.parseInt(spl[1]));
            }

            //get zeroChunks
            for (int i = 0; i < this.NumOfZeroChunks; i++) {
                thisLine = br.readLine();
                String[] spl = thisLine.split(" ");
                ArrayList<Integer> list = new ArrayList<Integer>();
                list.add(Integer.parseInt(spl[1]));
                list.add(Integer.parseInt(spl[2]));
                this.zeroChunks.put(spl[0], list);
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
                    String[] spl = thisLine.split(" ");
                    ByteArrayEntry arrTemp = new ByteArrayEntry(spl[0], Integer.parseInt(spl[1]));
                    list.add(arrTemp);
                }
                this.fileRecipe.put(name, list);
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
        // FileWriter els = new FileWriter("./test_1.txt");
        // els.write("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
        // els.close();

        FileWriter fw = new FileWriter(filename);
        fw.write(Integer.toString(this.NumOfCheckSum));
        fw.write("\n");
        fw.write(Integer.toString(this.NumOfZeroChunks));
        fw.write("\n");
        fw.write(Integer.toString(this.NumOfFile));
        fw.write("\n");
        fw.write(Integer.toString(this.NumOfPysical));
        fw.write("\n");
        fw.write(Integer.toString(this.WithDepu));
        fw.write("\n");
        fw.write(Integer.toString(this.WithoutDepu));
        fw.write("\n");

        //write checkSumMap
        Iterator iter = this.checkSumMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            fw.write((entry.getKey()).toString() + " " + (entry.getValue()).toString() + "\n");
            fw.flush();
        }

        //write zeroChunks
        Iterator iter_zero = this.zeroChunks.entrySet().iterator();
        while (iter_zero.hasNext()) {
            Map.Entry entry = (Map.Entry)iter_zero.next();
            ArrayList<Integer> list = (ArrayList<Integer>)entry.getValue();
            int zeroLength = list.get(0);
            int count = list.get(1);
            fw.write((entry.getKey()).toString() + " " + Integer.toString(zeroLength) + " " + Integer.toString(count) + "\n");
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
                fw.write(byArr.shaString + " " + Integer.toString(byArr.byteLength) + "\n");
            }
            fw.flush();

        }
    	fw.close();
    }
    void updateFileRecipe(String str, ArrayList<ByteArrayEntry> list) {
        for (ByteArrayEntry chk : list) {
            this.WithoutDepu += chk.byteLength;
        }

        if (!this.fileRecipe.containsKey(str)) {
            NumOfFile++;
        }
        this.fileRecipe.put(str, list);

    }

    void reconstructFile(String path, String filename, String output_name) throws IOException {
        if (!this.fileRecipe.containsKey(filename)) {
            System.out.println(filename + "has not been uploaded");
        } else {
            ArrayList<ByteArrayEntry> list = this.fileRecipe.get(filename);
            StringBuffer sb = new StringBuffer();
            for (ByteArrayEntry chk: list) {
                if (this.zeroChunks.containsKey(chk.shaString)) {
                    int numOfZero = ((ArrayList<Integer>)zeroChunks.get(chk.shaString)).get(0);
                    for (int i = 0; i < numOfZero; i++) {
                        sb.append('\0');
                    }
                } else {
                    String input_name = path + chk.shaString;
                    File file = new File(input_name);
        			InputStream is = new BufferedInputStream(new FileInputStream(file));
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

    void uploadChunk(String path, ByteArrayEntry byteEntry) throws IOException {
        this.NumOfPysical++;
        this.WithDepu += byteEntry.byteLength;
        String output_name = path +  byteEntry.shaString;
        FileWriter fw = new FileWriter(output_name);
        fw.write(new String(byteEntry.btArr));
        fw.close();
    }

    void deleteFile(String path, String filename) {
        if (!this.fileRecipe.containsKey(filename)) {
            System.out.println(filename + "has not been uploaded");
        } else {
            ArrayList<ByteArrayEntry> list = this.fileRecipe.get(filename);
            for (ByteArrayEntry chk: list) {
                this.WithoutDepu -= chk.byteLength;
                if (this.zeroChunks.containsKey(chk.shaString)) {
                    ArrayList<Integer> tmp = (ArrayList<Integer>)(this.zeroChunks.get(chk.shaString));
                    int count = tmp.get(1) - 1;
                    tmp.set(1, count);
                    this.zeroChunks.put(chk.shaString, tmp);

                    if (count == 0) {
                        this.zeroChunks.remove(chk.shaString);
                        this.NumOfZeroChunks--;
                    }
                } else {
                    int tmp = this.checkSumMap.get(chk.shaString);
                    int count = tmp - 1;
                    this.checkSumMap.put(chk.shaString, count);

                    if (count == 0) {
                        this.checkSumMap.remove(chk.shaString);
                        this.NumOfCheckSum--;
                        this.NumOfPysical--;
                        this.WithDepu -= chk.byteLength;
                        String file_name = path +  chk.shaString;
                        File file = new File(file_name);
                        file.delete();
                    }
                }
            }
            //this.NumOfPysical -= list.size();
            this.fileRecipe.remove(filename);
            this.NumOfFile--;
        }

    }

    void getReport() {
        double saving = 1 - ((1.0 * this.getWithDepu()) / this.getWithoutDepu());
        System.out.println("Total number of logical chunks in storage: "
                            + Integer.toString(this.getTotalLogical()));
        System.out.println("Number of unique physical chunks in storage: "
                            + Integer.toString(this.getTotalPysical()));
        System.out.println("Number of bytes in storage with deduplication: "
                            + Integer.toString(this.getWithDepu()));
        System.out.println("Number of bytes in storage without deduplication: "
                            + Integer.toString(this.getWithoutDepu()));
        System.out.println("Space saving: " + Double.toString(saving));
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

    public static ByteArrayEntry chunking(MessageDigest md, byte[] bytes, int bound, int sec) {

        byte[] bytesChk = new byte[sec-bound+1];
        System.arraycopy(bytes, bound, bytesChk, 0, sec-bound+1);

        //get SHA value
        md.update(bytesChk, 0, sec-bound+1);
        byte[] checksumBytes = md.digest();
        //updage checkSum in indexing
        StringBuffer sb2 = new StringBuffer();
        for (int i = 0; i < checksumBytes.length; i++) {
            sb2.append(Integer.toString((checksumBytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        ByteArrayEntry res = new ByteArrayEntry(bytesChk, sb2.toString());
        return res;
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

                int minChSize = Integer.parseInt(args[1]);
                int modulus = Integer.parseInt(args[2]);
                int maxChSize = Integer.parseInt(args[3]);
                int base = Integer.parseInt(args[4]);
                String fileToUpload = args[5];
                String mode = args[6];

                try {
                    //get mydedup.index, or create a new one
                    Indexing indexing = new Indexing();
                    indexing.loadIndexing(indexFileName);
                    File file = new File(fileToUpload);
                    //BufferedReader is = new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF16"));
                    InputStream is = new BufferedInputStream(new FileInputStream(file));
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
                                    //System.out.println(new String(windows));
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
                                    ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1);
                                    list.add(res);
                                    indexing.updateCheckSumMap(res, 0);
                                    break;
                                }
                            }
                        } else {
                            if (lastZero == true) {
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
                                ByteArrayEntry res = chunking(md, zeros, 0, zeros.length-1);
                                list.add(res);
                                indexing.updateCheckSumMap(res, zeros.length);
                                //read EOF
                                if (rLen == -1) break;
                            } else {
                                if((rLen = is.read(sByte))!=-1) {
                                    bufferMax[allLen++] = sByte[0];
                                    System.arraycopy(bufferMax, ++fir, windows, 0, winSize);
                                } else {
                                    // System.out.println(Integer.toString(22));
                                    ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1);
                                    list.add(res);
                                    indexing.updateCheckSumMap(res, 0);
                                    break;
                                }
                            }


                        }

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
                                ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1);
                                list.add(res);
                                //update indexing
                                indexing.updateCheckSumMap(res, 0);
                            }
                        } else {
                            lastChunk = false;
                            pos++;
                        }
                        // }
                    }
                    is.close();
                    System.out.println("saving chunkings");
                    indexing.updateFileRecipe(fileToUpload, list);
                    //fr.saveFileChunks("./data/", fileToUpload);
                    System.out.println("indexing");
                    indexing.saveIndexing(indexFileName);
                    //indexing.reconstructFile("./data/", fileToUpload, "./construction.txt");

                    //Report output:
                    System.out.println(fileToUpload);
                    indexing.getReport();
                } catch (FileNotFoundException e) {
        			e.printStackTrace();
        		} catch (IOException e) {
        			e.printStackTrace();
        		}
            } else if (operation.equals("download")) {
                if (args.length < 4) {
                    usage();
                    System.exit(1);
                }

                String fileToDownload = args[1];
                String output_name = args[2];
                String mode = args[3];

                try {
                    Indexing indexing = new Indexing();
                    indexing.loadIndexing(indexFileName);
                    indexing.reconstructFile("./data/", fileToDownload, output_name);
                } catch (FileNotFoundException e) {
        			e.printStackTrace();
        		} catch (IOException e) {
        			e.printStackTrace();
        		}

            } else if (operation.equals("delete")) {
                if (args.length < 3) {
                    usage();
                    System.exit(1);
                }

                String fileToDelete = args[1];
                String mode = args[2];

                try {
                    Indexing indexing = new Indexing();
                    indexing.loadIndexing(indexFileName);
                    indexing.deleteFile("./data/", fileToDelete);
                    indexing.saveIndexing(indexFileName);
                    indexing.getReport();

                } catch (FileNotFoundException e) {
        			e.printStackTrace();
        		} catch (IOException e) {
        			e.printStackTrace();
        		}
            }
        } catch (Exception e) {
          e.printStackTrace();
        }
    }
}
