package com.anubhav.sharding.hashing;

public class HashingHelper {
    public int hash(String key){
        return Integer.parseInt(key);
    }

    public int getServerEndingRangeFromGivenHash(int hashIndex, int[] serverMappings){
        int length = serverMappings.length;
        if(hashIndex > serverMappings[length-1] || hashIndex <= serverMappings[0]){
            return serverMappings[0];
        }

        int end = serverMappings.length-1;
        int start = 0;

        while (start<= end){
            if(start == end){
                return serverMappings[start];
            }
            int mid = start + (end -start)/2;
            if(serverMappings[mid] ==  hashIndex){
                return serverMappings[mid];
            }
            if(hashIndex <= serverMappings[mid] && hashIndex > serverMappings[mid -1]){
                return serverMappings[mid];
            }
            if(hashIndex > serverMappings[mid]){
                start = mid + 1;
            }
            else {
                end = mid -1;
            }

        }
        return -1;
    }
}
