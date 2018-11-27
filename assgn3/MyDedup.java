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

    public ByteArrayEntry(byte[] data, int index)
    {
        if (data == null)
        {
            throw new NullPointerException();
        }
        this.btArr = data;
        this.shaString = "";
        this.index = index;
    }

    public void setString(String str) {
        this.shaString = str;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(btArr);
    }
}
class FileRecipe {
    public int NumOfFile;
    public HashMap<String, ArrayList<ByteArrayEntry> > fileRecipe;

    FileRecipe () {
        NumOfFile = 0;
        fileRecipe = new HashMap<String, ArrayList<ByteArrayEntry> >();
    }

    void updateFileRecipe(String str, ArrayList<ByteArrayEntry> list) {
        fileRecipe.put(str, list);
        NumOfFile++;
    }

    ArrayList<ByteArrayEntry> getChunks(String filename) {
        return fileRecipe.get(filename);
    }

    int loadFileChunks(String path) {
        //get fileRecipe
        String  thisLine = null;
        try {
            File aDirectory = new File(path);
            String[] filesInDir = aDirectory.list();

            for (int i = 0; i < filesInDir.length; i++) {
                String[] spl = filesInDir[i].split("_");

                File file = new File(filesInDir[i]);
    			InputStream is = new FileInputStream(file);
    			byte[] bytes = new byte[(int)file.length()] ;
                ArrayList<ByteArrayEntry> list;
    			is.close();

                if (!fileRecipe.containsKey(spl[0])) {
                    list = new ArrayList<ByteArrayEntry>();
                } else {
                    list = fileRecipe.get(spl[0]);
                }

                ByteArrayEntry byteEntry = new ByteArrayEntry(bytes, Integer.parseInt(spl[1]));
                byteEntry.setString(spl[2]);
                list.add(byteEntry);
                fileRecipe.put(spl[0], list);
            }
            // BufferedReader br = new BufferedReader(new FileReader(filename));
            // while ((thisLine = br.readLine()) != null) {
            //     StringTokenizer itr = new StringTokenizer(thisLine);
            //     String name = itr.nextToken();
            //     ArrayList<ByteArrayEntry> temp = new ArrayList<ByteArrayEntry>();
            //     while (itr.hasMoreTokens()) {
            //         byte[] byt = itr.nextToken().getBytes();
            //         ByteArrayEntry chk = new ByteArrayEntry(byt);
            //         temp.add(chk);
            //     }
            //     fileRecipe.put(name, temp);
            // }
        } catch (FileNotFoundException e) {
            return -1;
        } catch (IOException e) {
        	e.printStackTrace();
        }
        return 0;
    }

    void reconstructFile(String path, String filename) throws IOException {
        ArrayList<ByteArrayEntry> list = fileRecipe.get(filename);

        Comparator<ByteArrayEntry> com = new Comparator<ByteArrayEntry>() {
			@Override
			public int compare(ByteArrayEntry o1, ByteArrayEntry o2) {
				// TODO Auto-generated method stub
				if(o1.index < o2.index) {
                    return 1;
                } else {
                    return -1;
                }
			}
		};
        Collections.sort(list,com);

        String output_name = path + filename;
        StringBuffer sb = new StringBuffer();
        for (ByteArrayEntry chk: list) {
            sb.append(new String(chk.btArr));
        }
        FileWriter fw = new FileWriter(output_name);
        fw.write(sb.toString());
        fw.close();
    }

    void saveFileChunks(String path, String filename) throws IOException {
        // FileWriter fw = new FileWriter(filename);

        Iterator iter2 = fileRecipe.entrySet().iterator();
        //StringBuffer sb = new StringBuffer();
        while (iter2.hasNext()) {


            Map.Entry entry = (Map.Entry)iter2.next();
            ArrayList<ByteArrayEntry> list = (ArrayList<ByteArrayEntry>)entry.getValue();

            //sb.append(String.valueOf(entry.getKey()));
            for (ByteArrayEntry chk: list) {
                String output_name = path + filename + "_" + chk.index + "_" + chk.shaString;
                FileWriter fw = new FileWriter(output_name);
                fw.write(new String(chk.btArr));
                fw.close();
                // sb.append(" ");
                // sb.append(chk.shaString);
            }
            // sb.append("\n");


        }

    }
}
class Indexing {
    public int NumOfCheckSum;
    public HashMap<String, Integer> checkSumMap;

    Indexing () {
        NumOfCheckSum = 0;
        checkSumMap = new HashMap<String, Integer>();
    }

    void updateCheckSumMap(String chSum) {
        if (!this.checkSumMap.containsKey(chSum)) {
            this.checkSumMap.put(chSum,1);
            this.NumOfCheckSum++;
        }
        // else {
        //     int num = checkSumMap.get(chSum) + 1;
        //     checkSumMap.put(chSum, num);
        // }
    }

    int loadIndexing(String filename) {
        String  thisLine = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            //get NumOfCheckSum
            if ((thisLine = br.readLine()) != null) {
                this.NumOfCheckSum = Integer.parseInt(thisLine);
            }
            // //get NumOfFile
            // if ((thisLine = br.readLine()) != null) {
            //     this.NumOfFile = Integer.parseInt(thisLine);
            // }
            //get checkSum
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
        FileWriter fw = new FileWriter(filename);
        fw.write(Integer.toString(NumOfCheckSum));
        fw.write("\n");
        // fw.write(Integer.toString(NumOfFile));
        // fw.write("\n");

        Iterator iter = this.checkSumMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            fw.write((entry.getKey()).toString() + "\n");
            fw.flush();
        }

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

    public static boolean byteArrayCheck(final byte[] array) {
        int sum = 0;
        for (byte b : array) {
            sum |= b;
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

        ByteArrayEntry res = new ByteArrayEntry(bytesChk, index);
        //list.add(chk);
        //get SHA value
        md.update(bytesChk, 0, sec-bound+1);
        byte[] checksumBytes = md.digest();
        //updage checkSum in indexing
        StringBuffer sb2 = new StringBuffer();
        for (int i = 0; i < checksumBytes.length; i++) {
            sb2.append(Integer.toString((checksumBytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        res.setString(sb2.toString());
        //res.add(new ByteArrayEntry(checksumBytes));
        //indexing.update(chS);
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
            FileRecipe fr = new FileRecipe();
            //fr.loadFileChunks("./data/");
            //fr.reconstructFile("./mid/", fileToUpload);
            //indexing.saveIndexing("./savefile.index");

            try {
                InputStream is = new FileInputStream(fileToUpload);
                byte[] bytes = new byte[maxChSize];
                byte[] windows = new byte[minChSize];
                //StringBuffer sb = new StringBuffer();
                int len = -1, pos = 0, prev = 0, RFPValue = 0, index = 0;
                ArrayList<ByteArrayEntry> list = new ArrayList<ByteArrayEntry>();
                MessageDigest md = MessageDigest.getInstance("SHA-256");

                while((len = is.read(bytes))!=-1)
    			{
    				//sb.append(new String(bytes,0,len));
                    //System.out.print(Integer.toString(len));
                    //System.out.println(new String(bytes,0,len));
                    int fir = 0;
                    int sec = fir + minChSize - 1;
                    int bound = 0;
                    int winSize = minChSize;

                    if (sec >= len) {
                        sec = len - 1;
                        winSize = len;
                    }

                    while (sec < len) {
                        Arrays.fill(windows, (byte)0);
                        System.arraycopy(bytes, fir, windows, 0, winSize);
                        //System.out.print(Integer.toString(winSize));
                        //System.out.println(new String(windows));

                        if (byteArrayCheck(windows) == true) {
                            int posNonZero = getNonZeroByte(bytes, sec);
                            //if (posNonZero == maxChSize) {
                            sec = len;
                            fir = len - winSize + 1;
                            if (posNonZero == len) {
                                posNonZero--;
                            }
                            ByteArrayEntry res = chunking(md, bytes, fir, posNonZero, index++);
                            list.add(res);
                            indexing.updateCheckSumMap(res.shaString);
                            bound = posNonZero;
                            continue;
                            //}
                        }

                        RFPValue = RFPAlgoritm(windows, pos, prev, winSize, base, modulus);
                        prev = RFPValue;
                        if ((RFPValue & 0xFF) == 0) {
                            pos = 0;
                            //chunking
                            ByteArrayEntry res = chunking(md, bytes, bound, sec, index++);
                            list.add(res);
                            indexing.updateCheckSumMap(res.shaString);
                            bound = sec;
                        } else {
                            pos++;
                        }
                        fir++;
                        sec++;
                    }
                    if (bound+1 < len) {
                        if (bound == 0) {
                            bound = -1;
                        }
                        ByteArrayEntry res = chunking(md, bytes, bound+1, len-1, index++);
                        list.add(res);
                        indexing.updateCheckSumMap(res.shaString);
                    }
                    //sb.setLength(0);
    			}
    			is.close();
                fr.updateFileRecipe(fileToUpload, list);
                fr.saveFileChunks("./data/", fileToUpload);
                indexing.saveIndexing(indexFileName);

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
