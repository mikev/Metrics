using System;
using System.Text;
using NBitcoin.DataEncoders;

public class PublicKey
{
    private byte[] binaryData;

    public PublicKey(byte[] binaryData)
    {
        this.binaryData = binaryData;
    }

    public override string ToString()
    {
        byte[] data = new byte[this.binaryData.Length + 1];
        Array.Copy(this.binaryData, 0, data, 1, this.binaryData.Length);

        string encoded = Encoders.Base58Check.EncodeData(data);
        return encoded;
    }

    public static string PubKeyToB58(byte[] binPubKey)
    {
        return new PublicKey(binPubKey).ToString();
    }

    public static byte[] B58ToRawBinary(string b58Str)
    {
        byte[] decoded = Encoders.Base58Check.DecodeData(b58Str);
        byte[] result = new byte[decoded.Length - 1];
        Array.Copy(decoded, 1, result, 0, result.Length);
        return result;
    }
}