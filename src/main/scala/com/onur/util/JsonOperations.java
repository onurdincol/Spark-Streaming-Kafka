package com.onur.util;

import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;

/**
 * Created by Onur_Dincol on 10/6/2017.
 */
public class JsonOperations {
    public String makeJsonFlatten(String inputJson)
    {
        return (new JsonFlattener(inputJson).withFlattenMode(FlattenMode.KEEP_ARRAYS).flatten());
    }
}
