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

'use strict';

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal, cubeConfig, MetaModel, cubesManager) {
  $scope.cubesManager = cubesManager;

  //convert some undefined or null value
  angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns, function (rowkey) {
      if (!rowkey.dictionary) {
        rowkey.dictionary = "false";
      }
    }
  );
  //edit model



  $scope.dictionaryUpdated = function (rowkey_column) {
    if (rowkey_column.dictionary === "true") {
      rowkey_column.length = 0;
    }

  }

  $scope.addNewRowkeyColumn = function () {
    $scope.cubeMetaFrame.rowkey.rowkey_columns.push({
      "column": "",
      "length": 0,
      "dictionary": "true",
      "mandatory": false
    });
  };

  $scope.addNewAggregationGroup = function () {
    $scope.cubeMetaFrame.rowkey.aggregation_groups.push([]);
  };

  $scope.refreshAggregationGroup = function (list, index, aggregation_group) {
    if (aggregation_group) {
      list[index].length = aggregation_group.length;
      for (var i = 0; i < aggregation_group.length; i++) {
        list[index][i] = aggregation_group[i];
      }
    }
  }

  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };


});
