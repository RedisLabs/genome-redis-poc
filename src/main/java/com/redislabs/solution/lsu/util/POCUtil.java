
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
    public static byte[] nbEncode(String s) {
        // expects a string of ACGT, length has to be multiple of 4 so complete bytes are used
        int b;
        int n, l;

        b = 0;
        l = s.length();
        byte[] r = new byte[l / 4];

        if (l % 4 != 0)
            return null;

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
                    System.exit(1);
                    break;
            }

            b = ((b << 2) | e);

            n = n + 1;
            if (n == 4) {

                r[i/4] = (byte) b;
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
    public static String nbDecode(byte[] s) {
        String r = "";
        int l;

        l = s.length;

        for (int i = 0; i < l; i++) {
            byte c = s[i];
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
                        System.exit(1);
                        break;
                }
            }
        }

        return (r);
    }

    public static byte[] hashKey(String k, long m) {
        byte[] b = k.getBytes();
        int l = k.length();
        long r;
        int ir;

        Checksum c1 = new CRC32();
        Checksum c2 = new CRC32();
        c1.update(b, 0, l/2);
        c2.update(b, l/2, l/2 + l%2);

        r = c1.getValue() << 32 | c2.getValue() & 0xFFFFFFFF;
        r %= m;
        ir = (int) Math.abs(r);

        if (ir == 0xFFFFFFFF)
            System.out.println(Long.toBinaryString(r));
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(ir);
        return (buffer.array());
    }

    /**
     * encodeKey
     * @param s
     * @return
     */
    public static byte[] encodeKey(String s) {
        // add an extra character to the 31 nb sequence to get 64 encoding
        String newKey = s + padding;

        return nbEncode(newKey);
    }

    /**
     * encodeValue
     * @param s
     * @return
     */
    public static byte[] encodeValue(String s) {
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
