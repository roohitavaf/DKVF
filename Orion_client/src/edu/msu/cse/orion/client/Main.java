package edu.msu.cse.orion.client;

import com.google.protobuf.ByteString;
import edu.msu.cse.dkvf.config.ConfigReader;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String args[]) {
        System.out.println("Starting Client....");
        ConfigReader cnfReader = new ConfigReader(args[0]);
        OrionClient client = new OrionClient(cnfReader);
        System.out.println(client.runAll());
        System.out.println("Client started...");
        int numKeys = 100;
        Map<String, String> inputKeyValue = new HashMap<>(numKeys);
        for (int i = 0; i < numKeys; i++) {
            inputKeyValue.put("k" + i, "value" + i );
            client.put("k" + i, ("value" + i ).getBytes());
        }
        try {
            for (int i = 0; i < numKeys; i++)
                assert(new String(client.get("k" + i), "UTF-8").equals("value" + i));

            Map<String, ByteString> rotKeyValues = client.rot(inputKeyValue.keySet());
            System.out.println(rotKeyValues.size());
            for (Map.Entry<String, ByteString> entry : rotKeyValues.entrySet()) {
                assert(entry.getValue() != null);
                System.out.println(entry.getKey() + "  " + entry.getValue().toStringUtf8());
                assert(entry.getValue().toStringUtf8().equals(inputKeyValue.get(entry.getKey())));
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


    }
}
