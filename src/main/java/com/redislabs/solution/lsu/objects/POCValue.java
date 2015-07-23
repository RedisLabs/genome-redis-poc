/**
 * Created by foo on 7/21/15.
 */
package com.redislabs.solution.lsu.objects;

import com.redislabs.solution.lsu.util.POCUtil;

/**
 * POCValue
 * @author itamar
 *
 */
public class POCValue {

    private int freq;
    private String ie;
    private String oe;

    public POCValue(int freq, String ie, String oe) {

        this.freq = freq;
        this.ie = ie.replaceAll(",", "");
        this.oe = oe.replaceAll(",", "");
    }

    public POCValue(String r) {
        long record = 0;
        for (int i = 0; i < 7; i++) {
            record = record | ((long) r.charAt(i) << 8 * i);
        }

        this.freq = (int) (record & 0xFFFFFFFF);

        int ieLength = (int) ((record >> 32) & 0xF);
        byte[] ieTmpValue = {(byte) ((record >> 40) & 0xFF)};
        this.ie = POCUtil.nbDecode(ieTmpValue);
        this.ie = this.ie.substring(0, ieLength);

        System.out.println(">" + ieTmpValue);

        int oeLength = (int) ((record >> 36) & 0xF);
        byte[] oeTmpValue = {(byte) ((record >> 48) & 0xFF)};
        this.oe = POCUtil.nbDecode(oeTmpValue);
        this.oe = this.oe.substring(0, oeLength);
    }

    public int getFreq() {
        return freq;
    }

    public String getIe() {
        return ie;
    }

    public String getOe() {
        return oe;
    }

    public byte[] getRecord () {
        long result = 0;

        byte[] charResult = new byte[7];

        try {
            byte[] eIe = POCUtil.encodeValue(ie);
            byte[] eOe = POCUtil.encodeValue(oe);
            byte bIe = 0;
            byte bOe = 0;

            if (eIe.length != 0)
                bIe = eIe[0];
            if (eOe.length != 0)
                bOe = eOe[0];

            result = freq |
                    ((long) ie.length() << 32) |
                    ((long) oe.length() << 36) |
                    ((long) bIe << 40) |
                    ((long) bOe << 48);

            for (int i=0; i< 7 ; i++){
                charResult[i] = (byte)((result >> (8*i)) & 0xFF);
            }
        } catch (Exception e) {

            e.printStackTrace();
        }

        return(charResult);
    }

}