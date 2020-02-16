package com.github.dfs.namenode;

/**
 * @author wangsz
 * @create 2020-02-16
 **/
public class ByteUtils {

    public static int bytes2Int(byte[] bytes ) {
        int int1=bytes[3]&0xff;
        int int2=(bytes[2]&0xff)<<8;
        int int3=(bytes[1]&0xff)<<16;
        int int4=(bytes[0]&0xff)<<24;
        return int1|int2|int3|int4;
    }

    public static byte[] int2Bytes(int integer) {
        byte[] bytes = new byte[4];
        bytes[3] = (byte) (integer>>24);
        bytes[2] = (byte) (integer>>16);
        bytes[1] = (byte) (integer>>8);
        bytes[0] = (byte) integer;
        return bytes;
    }
}
