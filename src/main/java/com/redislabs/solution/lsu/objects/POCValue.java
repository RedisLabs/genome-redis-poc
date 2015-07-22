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
        char ieTmpValue = (char) ((record >> 40) & 0xFF);
        this.ie = getValue(POCUtil.nbDecode(String.valueOf(ieTmpValue)), ieLength);

        System.out.println(">" + ieTmpValue);

        int oeLength = (int) ((record >> 36) & 0xF);
        char oeTmpValue = (char) ((record >> 48) & 0xFF);
        this.oe = getValue(POCUtil.nbDecode(String.valueOf(oeTmpValue)), oeLength);
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

    public String getRecord () {
        long result = 0;

        char[] charResult = new char [7];

        try {
            String eIe = POCUtil.encodeValue(ie);
            String eOe = POCUtil.encodeValue(oe);
            byte bIe = 0;
            byte bOe = 0;

            if (eIe.length() != 0)
                bIe = eIe.getBytes()[0];
            if (eOe.length() != 0)
                bOe = eOe.getBytes()[0];

            result = freq |
                    ((long) ie.length() << 32) |
                    ((long) oe.length() << 36) |
                    ((long) bIe << 40) |
                    ((long) bOe << 48);

            for (int i=0; i< 7 ; i++){
                charResult[i] = (char) ((result >> (8*i)) & 0xFF);
            }
        } catch (Exception e) {

            e.printStackTrace();
        }

        return(String.valueOf(charResult));
    }


    /**
     * getValue
     * @param elementValue
     * @param length
     * @return
     */
    private String getValue(String elementValue, int length ){
        String value = "";

        if (length > 0 && length < 5){
            value = elementValue.substring(0, length);
        }

        return value;
    }

}