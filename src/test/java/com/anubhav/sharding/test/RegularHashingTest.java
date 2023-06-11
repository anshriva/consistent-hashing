package com.anubhav.sharding.test;

import org.junit.Test;

import java.util.*;

public class RegularHashingTest {
    @Test
    public void checkNumberOfReBalances(){
        List<String> serverIds = new ArrayList<>(Arrays.asList("server1", "server2", "server3"));
        Map<Integer,String> InputToServerIdMapping = new HashMap<>();
        for(int i =0;i< 1000;i++){
            int index = i% serverIds.size();
            String serverName = serverIds.get(index);
            InputToServerIdMapping.put(i,serverName);
        }

        serverIds.add("server4");

        int rebalances  = 0;
        for(int i =0;i< 1000;i++){
            int index = i% serverIds.size();
            String serverName = serverIds.get(index);
            if(!InputToServerIdMapping.get(i).equalsIgnoreCase(serverName)){
                rebalances ++;
            }
        }
        System.out.println("re-balances = "+ rebalances);
    }
}
