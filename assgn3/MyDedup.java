import java.io.*;
import java.util.*;
import java.security.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

class ByteArrayEntry {
    public byte btArr[];
    public String shaString;
    public int byteLength;
    public boolean zeroChk;

    public ByteArrayEntry(byte[] data, int len, String str)
    {
        if (data == null)
        {
            throw new NullPointerException();
        }
        this.btArr = data;
        this.shaString = str;
        this.byteLength = len;
        this.zeroChk = false;
    }

    public ByteArrayEntry(int byteLength, String str, boolean zeroChk)
    {
        this.btArr = null;
        this.shaString = str;
        this.byteLength = byteLength;
        this.zeroChk = zeroChk;
    }

    public ByteArrayEntry(String str, int byteLength)
    {
        this.btArr = null;
        this.shaString = str;
        this.byteLength = byteLength;
        this.zeroChk = false;
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
    long WithDepu;
    long WithoutDepu;
    public HashMap<String, Integer> checkSumMap;
    public HashMap<String, Integer > zeroChunks;
    public HashMap<String, ArrayList<ByteArrayEntry> > fileRecipe;

    Indexing () {
        this.NumOfCheckSum = 0;
        this.NumOfZeroChunks = 0;
        this.NumOfPysical = 0;
        this.WithDepu = 0L;
        this.WithoutDepu = 0L;
        this.checkSumMap = new HashMap<String, Integer>();
        this.zeroChunks = new HashMap<String, Integer>();
        this.NumOfFile = 0;
        this.fileRecipe = new HashMap<String, ArrayList<ByteArrayEntry> >();
    }

    int getTotalLogical() {
        return (this.NumOfCheckSum + this.zeroChunks.size());
    }
    int getTotalPysical() {
        return this.NumOfPysical;
    }
    long getWithDepu() {
        return this.WithDepu;
    }
    long getWithoutDepu() {
        return this.WithoutDepu;
    }

    void updateCheckSumMap(String storageConnectionString, ByteArrayEntry res, int numOfZero, String mode)  throws IOException {
        if (numOfZero == 0) {
            if (!this.checkSumMap.containsKey(res.shaString)) {
                this.checkSumMap.put(res.shaString,1);
                this.NumOfCheckSum++;
                uploadChunk(storageConnectionString, "./data/", res, mode);
            } else {
                int no = this.checkSumMap.get(res.shaString)+1;
                this.checkSumMap.put(res.shaString,no);

            }
        } else {
            String zero_string = Integer.toString(numOfZero);
            if (!this.zeroChunks.containsKey(numOfZero)) {
                this.zeroChunks.put(zero_string,1);
                this.NumOfZeroChunks++;
            } else {
                int tmp = this.zeroChunks.get(zero_string)+1;
                this.zeroChunks.put(zero_string,tmp);

            }
            zero_string=null;
        }
    }

    int loadIndexing(String path, String filename) {
        String  thisLine = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(path+filename));
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
                this.WithDepu = Long.parseLong(thisLine);
            }

            //get WithoutDepu
            if ((thisLine = br.readLine()) != null) {
                this.WithoutDepu = Long.parseLong(thisLine);
            }

            //get checkSum
            for (int i = 0; i < this.NumOfCheckSum; i++) {
                thisLine = br.readLine();
                String[] spl = thisLine.split(" ");
                this.checkSumMap.put(spl[0], Integer.parseInt(spl[1]));
                if (spl != null) spl=null;
            }

            //get zeroChunks
            for (int i = 0; i < this.NumOfZeroChunks; i++) {
                thisLine = br.readLine();
                String[] spl = thisLine.split(" ");
                this.zeroChunks.put(spl[0], Integer.parseInt(spl[1]));
                if (spl != null) spl=null;
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
                    if (spl!=null) spl=null;
                }
                this.fileRecipe.put(name, list);
                list = null;
            }
            thisLine = null;
            br.close();
            if (br != null) {
                br = null;
            }
        } catch (FileNotFoundException e) {
            return -1;
        } catch (IOException e) {
        	e.printStackTrace();
        }
        return 0;
    }

    void saveIndexing(String path, String filename) throws IOException {

        FileWriter fw = new FileWriter(path+filename);
        fw.write(Integer.toString(this.NumOfCheckSum));
        fw.write("\n");
        fw.write(Integer.toString(this.zeroChunks.size()));
        fw.write("\n");
        fw.write(Integer.toString(this.NumOfFile));
        fw.write("\n");
        fw.write(Integer.toString(this.NumOfPysical));
        fw.write("\n");
        fw.write(Long.toString(this.WithDepu));
        fw.write("\n");
        fw.write(Long.toString(this.WithoutDepu));
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
                fw.write(byArr.shaString + " " + Integer.toString(byArr.byteLength) + "\n");
            }
            fw.flush();

        }
    	fw.close();
        if (fw != null) fw=null;
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

    void reconstructFile(String storageConnectionString, String path, String filename, String output_name, String mode) throws IOException {
        if (!this.fileRecipe.containsKey(filename)) {
            System.out.println(filename + " has not been uploaded");
        } else {
            ArrayList<ByteArrayEntry> list = this.fileRecipe.get(filename);
            FileOutputStream fw = new FileOutputStream(output_name);
            //System.out.println(output_name);

            if (mode.equals("azure")) {
                try {
                    CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
                    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
                    CloudBlobContainer container = blobClient.getContainerReference("mycontainer");
                    container.createIfNotExists();
                    for (ByteArrayEntry chk: list) {
                        if (this.zeroChunks.containsKey(chk.shaString)) {
                            int numOfZero = Integer.parseInt(chk.shaString);
                            for (int i = 0; i < numOfZero; i++) {
                                fw.write('\0');
                            }
                        } else {
                            // String input_name = path + chk.shaString;
                            CloudBlockBlob blob = container.getBlockBlobReference(chk.shaString);
                            // blob.download(new FileOutputStream(input_name));
                            // BlobInputStream blobInputStream = blob.openInputStream();
                            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                            //System.output(chk.shaString);
                        	blob.download(outStream);
                        	byte[] byteData = outStream.toByteArray();
                            fw.write(byteData);
                            if (byteData != null) byteData=null;
                            // blobInputStream.close();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else {
                for (ByteArrayEntry chk: list) {
                    //System.out.println(chk.shaString);
                    if (this.zeroChunks.containsKey(chk.shaString)) {

                        int numOfZero = Integer.parseInt(chk.shaString);
                        //System.out.println(Integer.toString(numOfZero));
                        for (int i = 0; i < numOfZero; i++) {
                            fw.write('\0');
                        }
                    } else {
                        String input_name = path + chk.shaString;
                        try {
                            File file = new File(input_name);
                			InputStream is = new BufferedInputStream(new FileInputStream(file));
                			byte[] bytes = new byte[(int)file.length()];
                            int len = is.read(bytes);
                			is.close();
                            if (len>0 && bytes != null) fw.write(bytes);
                            if (bytes!=null) bytes=null;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        input_name=null;
                    }
                }
            }
            fw.close();
            if (fw != null) fw=null;
        }
    }

    void uploadChunk(String storageConnectionString, String path, ByteArrayEntry byteEntry, String mode) throws IOException {
        this.NumOfPysical++;
        this.WithDepu += (long)byteEntry.byteLength;
        //System.out.println(mode);
        if (mode.equals("azure")) {
            try {
                //System.out.println(storageConnectionString);
                // Retrieve storage account from connection-string.
                CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
                CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
                CloudBlobContainer container = blobClient.getContainerReference("mycontainer");
                container.createIfNotExists();
                // Create or overwrite the remoteFileName blob with contents from a local file.
                CloudBlockBlob blob = container.getBlockBlobReference(byteEntry.shaString);

                BlobOutputStream blobOutputStream = blob.openOutputStream();
                blobOutputStream.write(byteEntry.btArr, 0, byteEntry.byteLength);
                blobOutputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            //check directory exist
            File directory = new File(path);
            if (! directory.exists()){
                directory.mkdir();
            }
            if (directory != null) directory = null;

            String output_name = path +  byteEntry.shaString;
            FileOutputStream fileOuputStream = new FileOutputStream(output_name);
            fileOuputStream.write(byteEntry.btArr, 0, byteEntry.byteLength);
            fileOuputStream.close();
            if (fileOuputStream != null) fileOuputStream = null;
            output_name=null;
        }

    }

    void deleteFile(String storageConnectionString, String path, String filename, String mode) {
        if (!this.fileRecipe.containsKey(filename)) {
            System.out.println(filename + " has not been uploaded");
        } else {
            ArrayList<ByteArrayEntry> list = this.fileRecipe.get(filename);
            for (ByteArrayEntry chk: list) {
                this.WithoutDepu -= chk.byteLength;
                if (this.zeroChunks.containsKey(chk.shaString)) {
                    // ArrayList<Integer> tmp = (ArrayList<Integer>)(this.zeroChunks.get(chk.shaString));
                    int count = (int)(this.zeroChunks.get(chk.shaString)) - 1;
                    //tmp.set(1, count);
                    this.zeroChunks.put(chk.shaString, count);

                    if (count == 0) {
                        this.zeroChunks.remove(chk.shaString);
                        this.NumOfZeroChunks--;
                    }
                } else {
                    if (!this.checkSumMap.containsKey(chk.shaString)) continue;
                    int tmp = this.checkSumMap.get(chk.shaString);
                    int count = tmp - 1;
                    this.checkSumMap.put(chk.shaString, count);

                    if (count == 0) {
                        this.checkSumMap.remove(chk.shaString);
                        this.NumOfCheckSum--;
                        this.NumOfPysical--;
                        this.WithDepu -= chk.byteLength;
                        if (mode.equals("azure")) {
                            // Retrieve storage account from connection-string.
                            try {
                                CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
                                CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
                                CloudBlobContainer container = blobClient.getContainerReference("mycontainer");
                                container.createIfNotExists();

                                CloudBlockBlob blob = container.getBlockBlobReference(chk.shaString);
                                blob.deleteIfExists();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        } else {
                            String file_name = path +  chk.shaString;
                            File file = new File(file_name);
                            file.delete();
                            file_name=null;
                            if (file != null) file = null;
                        }
                    }
                }
            }
            //this.NumOfPysical -= list.size();
            this.fileRecipe.remove(filename);
            this.NumOfFile--;
        }

    }
    boolean chechFileExist(String filename) {
        if (this.fileRecipe.containsKey(filename)) {
            System.out.println(filename + " has been uploaded. Please do not upload again");
            return true;
        }
        return false;
    }

    void getReport() {
        double saving = 1 - ((1.0 * this.getWithDepu()) / this.getWithoutDepu());
        System.out.println("Total number of logical chunks in storage: "
                            + Integer.toString(this.getTotalLogical()));
        System.out.println("Number of unique physical chunks in storage: "
                            + Integer.toString(this.getTotalPysical()));
        System.out.println("Number of bytes in storage with deduplication: "
                            + Long.toString(this.getWithDepu()));
        System.out.println("Number of bytes in storage without deduplication: "
                            + Long.toString(this.getWithoutDepu()));
        System.out.println("Space saving: " + Double.toString(saving));
    }
}

public class MyDedup {
    public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=csci4180group8;AccountKey=5N448nyJbNBCvK9rsxG/luyt6kA7qIzYfWeH652qdP0KEm/ptPefpwsTTYVYJekpVmdkoM0EDWrrMco5QPsx+Q==;EndpointSuffix=core.windows.net";
    public static HashMap<Integer, Integer>  myPowerCom = new HashMap<Integer, Integer>();

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
    public static void getPowerCom(int minChSize, int base, int modulus) {
        //HashMap<String, Integer> res = new HashMap<String, Integer>();
        for (int i = 0; i <= minChSize; i++) {
            int temp = power(base, minChSize-i, modulus);
            //String str = Integer.toString(base)+" "+Integer.toString(minChSize-i)+" "+Integer.toString(modulus);
            //System.out.println(str+" "+Integer.toString(temp));
            myPowerCom.put(minChSize-i, temp);
            //str=null;
        }
        //return res;
    }

    public static int getMod(int a, int m) {
        int res = a % m;
        if (res < 0) res += m;
        return res;
    }

    public static void toUnsignedInt(byte bytes[], int len, int res[]) {
        for (int i = 0; i < len; i++) {
            res[i] = bytes[i] & 0xFF;
        }
    }

    public static int RFPAlgoritm(byte bytes[], int index, int prev, int minChSize, int base, int modulus) {
        int result = 0;
        // int[] conv = new int[minChSize];
        // toUnsignedInt(bytes, minChSize, conv);
        // for (int i = 0; i < minChSize; i++) {
        //     bytes[i] = bytes[i] & 0xFF;
        // }
        if (index == 0) {
            int sum = 0;
            for (int i = 0; i < minChSize; i++) {
                //String str = Integer.toString(base)+" "+Integer.toString(minChSize-i-1)+" "+Integer.toString(modulus);
                int tmp = myPowerCom.get(minChSize-i-1);
                //System.out.println(str);
                int tmm1 = getMod((getMod((bytes[i] & 0xFF), modulus) * (tmp)), modulus);
                if (tmm1 < 0) tmm1 += modulus;
                sum += tmm1;
                //str = null;
                //System.out.println(Integer.toString(index)+" "+Integer.toString((int)conv[i])+" "+Integer.toString(tmp)+" "+Integer.toString(sum));
            }
            sum = getMod(sum,modulus);
            result = sum;
        } else {
            //String str = new String(Integer.toString(base)+" "+Integer.toString(minChSize)+" "+Integer.toString(modulus));
            int tmp = (int)myPowerCom.get(minChSize);
            int first = getMod((getMod(base, modulus) * getMod(prev, modulus)), modulus);
            int sec = getMod((tmp * getMod((bytes[0] & 0xFF), modulus)), modulus);
            int third = getMod((bytes[minChSize-1]&0xFF), modulus);
            result = getMod((first - sec + third), modulus);
            //str = null;
            //System.out.println(Integer.toString(index)+" "+Integer.toString((int)conv[0])+" "+Integer.toString((int)conv[minChSize-1])+" "+Integer.toString((int)conv[minChSize-1])+" "+Integer.toString(first)+" "+Integer.toString(sec)+" "+Integer.toString(third)+" "+Integer.toString(tmp)+" "+Integer.toString(result));
        }
        //if (conv != null) conv = null;
        return result;
    }

    public static boolean byteArrayCheck(final byte[] array, int fir, int sec) {
        int sum = 0;
        for (int i = fir; i <= sec; i++) {
            sum |= array[i];
        }
        return (sum == 0);
    }
    //
    // public static int getNonZeroByte(final byte[] array, int pos) {
    //     int sum = 0;
    //     int i = pos;
    //     for (; i < array.length; i++) {
    //         sum |= array[i];
    //         if (sum != 0) break;
    //     }
    //     return i;
    // }

    public static ByteArrayEntry chunking(MessageDigest md, byte[] bytes, int bound, int sec, int numOfZero) {
        if (numOfZero == 0) {
            //System.out.println(Integer.toString(sec-bound+1));
            // byte[] bytesChk = new byte[sec-bound+1];
            // System.arraycopy(bytes, bound, bytesChk, 0, sec-bound+1);

            //get SHA value
            md.update(bytes, bound, sec-bound+1);
            byte[] checksumBytes = md.digest();
            //updage checkSum in indexing
            StringBuffer sb2 = new StringBuffer();
            for (int i = 0; i < checksumBytes.length; i++) {
                sb2.append(Integer.toString((checksumBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            //byte[] bytesChk = Arrays.copyOfRange(bytes, bound, sec-bound+1);
            ByteArrayEntry res = new ByteArrayEntry(bytes, sec-bound+1, sb2.toString());
            sb2 = null;
            //bytesChk=null;
            checksumBytes=null;
            return res;
        } else {
            ByteArrayEntry res = new ByteArrayEntry(numOfZero, Integer.toString(numOfZero), true);
            return res;
        }

    }

    public static void OperationUpload(int minChSize, int modulus, int maxChSize, int base, String fileToUpload, String mode, String indexFileName) throws Exception{
        //get mydedup.index, or create a new one
        Indexing indexing = new Indexing();
        indexing.loadIndexing("./data/",indexFileName);
        if (indexing.chechFileExist(fileToUpload)) System.exit(1);
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
        getPowerCom(winSize, base, modulus);

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
                        if (temp != null) temp=null;
                    } else {
                        ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1, 0);
                        list.add(res);
                        indexing.updateCheckSumMap(storageConnectionString, res, 0, mode);
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
                    int zero_len = sbZeros.length();
                    ByteArrayEntry res = chunking(md, sByte, 0, zero_len-1, zero_len);
                    list.add(res);
                    indexing.updateCheckSumMap(storageConnectionString, res, zero_len, mode);
                    sbZeros.setLength(0);
                    //read EOF
                    if (rLen == -1) break;
                } else {
                    if((rLen = is.read(sByte))!=-1) {
                        bufferMax[allLen++] = sByte[0];
                        System.arraycopy(bufferMax, ++fir, windows, 0, winSize);
                    } else {
                        // System.out.println(Integer.toString(22));
                        ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1, 0);
                        list.add(res);
                        indexing.updateCheckSumMap(storageConnectionString, res, 0, mode);
                        break;
                    }
                }
            }
            RFPValue = RFPAlgoritm(windows, pos, prev, winSize, base, modulus);
            //System.out.println(RFPValue);
            // if (RFPValue == 0) {
            //     //System.out.println(RFPValue);
            //     for (int i = 0; i < 16; i++) {
            //         System.out.print(Integer.toString((int)windows[i]));
            //         System.out.print(" ");
            //     }
            //     System.out.print("\n");
            // }

            prev = RFPValue;
            //oxFF
            if ((RFPValue & 0xFF) == 0 || allLen == maxChSize) {
                //zero Chunks
                if (byteArrayCheck(bufferMax, 0, allLen-1)) {
                    lastZero = true;
                    // System.out.println("XX");
                } else {
                    pos = 0;
                    lastChunk = true;
                    // System.out.println("normal");
                    ByteArrayEntry res = chunking(md, bufferMax, 0, allLen-1, 0);
                    list.add(res);
                    //update indexing
                    indexing.updateCheckSumMap(storageConnectionString, res, 0, mode);
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
        indexing.saveIndexing("./data/",indexFileName);
        //indexing.reconstructFile("./data/", fileToUpload, "./construction.txt");

        //Report output:
        System.out.println(fileToUpload);
        indexing.getReport();
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
                    OperationUpload(minChSize, modulus, maxChSize, base, fileToUpload, mode, indexFileName);

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
                    indexing.loadIndexing("./data/",indexFileName);
                    indexing.reconstructFile(storageConnectionString, "./data/", fileToDownload, output_name, mode);
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
                    indexing.loadIndexing("./data/", indexFileName);
                    indexing.deleteFile(storageConnectionString, "./data/", fileToDelete, mode);
                    indexing.saveIndexing("./data/", indexFileName);
                    //indexing.getReport();

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
