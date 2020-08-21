/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DiscretePercentile extends UserDefinedAggregateFunction {


  private StructType inputDataType;

  //dynamic value
  @Override
  public StructType inputSchema() {
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(DataTypes.createStructField("value", DataTypes.IntegerType, true));
    return DataTypes.createStructType(inputFields);
  }

  //dynamic what does this
  @Override
  public StructType bufferSchema() {
//    StructType buffer = new StructType(new StructField[]{
//      new StructField("buffer",
//                      DataTypes.createArrayType(DataTypes.DoubleType), false,
//                      Metadata.empty())});
//
//    return buffer;

    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(DataTypes.createStructField("value_buffer", DataTypes.IntegerType, true));
    return DataTypes.createStructType(inputFields);
  }

  //dynamic
  @Override
  public DataType dataType() {
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(DataTypes.createStructField("aliasDiscPercent", DataTypes.IntegerType, true));
    return DataTypes.createStructType(inputFields);
  }

  @Override
  public boolean deterministic() {
    return true;
  }

  @Override
  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(0, 0);
  }

  @Override
  public void update(MutableAggregationBuffer buffer, Row input) {
    buffer.update(0, input.get(0)); //todo IDK
  }

  @Override
  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

  }

  @Override
  public Object evaluate(Row buffer) {
    return buffer.get(0);
  }
}
