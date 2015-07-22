
package com.redislabs.solution.lsu.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * POCUtil
 * @author itamar
 *
 */
public final class POCUtil {

    private static final String padding = "T";

    /**
     * nbEncode
     * @param s
     * @return
     */
    public static String nbEncode(String s) {
        // expects a string of ACGT, length has to be multiple of 4 so complete bytes are used
        String r = "";
        int b;
        int n, l;

        b = 0;
        l = s.length();

        if (l % 4 != 0)
            return "";

        n = 0;

        for (int i = 0; i < l; i++) {
            char c = s.charAt(i);
            int e = 0;

            switch (c) {
                case 'A': e = 0;
                    break;
                case 'G': e = 1;
                    break;
                case 'C': e = 2;
                    break;
                case 'T': e = 3;
                    break;
                default:
                    System.out.println("nbEncode: Invalid character: <"+ c + ">. The key is:<" + s + ">" );
                    break;
            }

            b = ((b << 2) | e);

            n = n + 1;
            if (n == 4) {
                r += (char) b;
                n = 0;
            }
        }

        return (r);
    }

    /**
     * nbDecode
     * @param s
     * @return
     */
    public static String nbDecode(String s) {
        String r = "";
        int l;

        l = s.length();

        for (int i = 0; i < l; i++) {
            char c = s.charAt(i);
            for (int j = 0; j < 4; j++) {
                int d;

                d = (c << j * 2) >> 6 & 3; // the mask is because the sign carries
                switch (d) {
                    case 0:
                        r += 'A';
                        break;
                    case 1:
                        r += 'G';
                        break;
                    case 2:
                        r += 'C';
                        break;
                    case 3:
                        r += 'T';
                        break;
                    default:
                        System.out.println("nbDecode: Invalid character: <"+ d + ">. The key is:<" + s + ">" );
                        break;
                }
            }
        }

        return (r);
    }

    public static String hashKey(String k) {
        //Convert string to bytes
        byte b[] = k.getBytes();

        Checksum c = new CRC32();
        c.update(b, 0, b.length);
        int ic = (int) c.getValue();

        // "ignore" 2 LSB by always setting to 1
        ic = ic | 3;

        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(ic);
        return (new String(buffer.array()));
    }

    /**
     * encodeKey
     * @param s
     * @return
     */
    public static String encodeKey(String s) {
        // add an extra character to the 31 nb sequence to get 64 encoding
        String newKey = s + padding;

        return nbEncode(newKey);
    }

    /**
     * encodeValue
     * @param s
     * @return
     */
    public static String encodeValue(String s) {
        // add extra characters to get complete byte encoding
        int n = 4 - (s.length() % 4);
        if (n == 4)
            n = 0;

        String newKey = s;
        for (int i = 0; i < n; i++)
            newKey += padding;

        return nbEncode(newKey);
    }
    public static <T>List<List<T>> chopIntoParts( final List<T> ls, final int iParts )
    {
        final List<List<T>> lsParts = new ArrayList<List<T>>();
        final int iChunkSize = ls.size() / iParts;
        int iLeftOver = ls.size() % iParts;
        int iTake = iChunkSize;

        for( int i = 0, iT = ls.size(); i < iT; i += iTake )
        {
            if( iLeftOver > 0 )
            {
                iLeftOver--;

                iTake = iChunkSize + 1;
            }
            else
            {
                iTake = iChunkSize;
            }

            lsParts.add( new ArrayList<T>( ls.subList( i, Math.min( iT, i + iTake ) ) ) );
        }

        return lsParts;
    }
}
