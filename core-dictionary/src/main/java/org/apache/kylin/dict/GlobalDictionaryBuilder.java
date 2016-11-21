/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.dict;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;

/**
 * GlobalDictinary based on whole cube, to ensure one value has same dict id in different segments.
 * GlobalDictinary mainly used for count distinct measure to support rollup among segments.
 * Created by sunyerui on 16/5/24.
 */
public class GlobalDictionaryBuilder implements IDictionaryBuilder {

    @Override
    public Dictionary<String> build(DictionaryInfo dictInfo, IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList<String> returnSamples) throws IOException {
        if (dictInfo == null) {
            throw new IllegalArgumentException("GlobalDictinaryBuilder must used with an existing DictionaryInfo");
        }

        AppendTrieDictionary.Builder<String> builder = AppendTrieDictionary.Builder.getInstance(dictInfo.getResourceDir());
        byte[] value;
        while (valueEnumerator.moveNext()) {
            value = valueEnumerator.current();
            if (value == null) {
                continue;
            }
            String v = Bytes.toString(value);
            builder.addValue(v);
            if (returnSamples.size() < nSamples && returnSamples.contains(v) == false)
                returnSamples.add(v);
        }
        return builder.build(baseId);
    }
}
