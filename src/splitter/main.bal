import ballerina/io;
import ballerina/math;

function foo(string filePath) returns error? {
    io:ReadableCSVChannel rCsvChannel = checkpanic io:openReadableCsvFile(filePath);
    string [] columnHeaders = [];
    if (rCsvChannel.hasNext()) {
        var titles = check rCsvChannel.getNext();
        if (titles is string[]) {
            io:println(titles);
            columnHeaders = titles;
        }
    }

    string[][] recs = [];
    int rowNum = 0;
    while (rCsvChannel.hasNext()) {
        var records = rCsvChannel.getNext();

        if (records is string[]) {
            recs.push(records);
        } else {
            io:println("error at row num: " + rowNum.toString());
            io:println(records);
        }
        rowNum += 1;
    }

    io:println("-------------- parsed -----------------------");
    io:println("count: " + rowNum.toString());

    (function (string) returns boolean) spliter = p => p.trim() == "1"; //math:random() > .5;

    string[][] a = recs; //.slice(0, 10);
    string[][][]|error splited = partition(a, columnHeaders, "district", spliter);
    if (splited is string[][][]) {
        io:println(splited[0]);
        io:println();
        io:println(splited[1]);
        io:println(splited[0].length());
        io:println(splited[1].length());
    }

    checkpanic rCsvChannel.close();
}

function partition(string[][] recs, 
                    string[] headers, string headerName, 
                    (function (string) returns boolean) spliter) returns string[][][]|error {
    int? columnPos = headers.map(s => s.toLowerAscii()).indexOf("b");
    io:println("Column number: " + columnPos.toString());
    if (columnPos is ()) {
        return error("Missing column");
    }

    int length = recs.length();
    int midPoint = length/2;
    io:println("length:" + length.toString());
    io:println("mid:" + midPoint.toString());

    function(string[]) returns boolean cellFilter = function(string[] row) returns boolean {
        return spliter(row[<int>columnPos]);
    };

    future<string[][][]> f1 = start partitionInternal(recs, cellFilter, 0, midPoint);
    future<string[][][]> f2 = start partitionInternal(recs, cellFilter, midPoint, length);

    string[][][] f1_ = wait f1;
    string[][][] f2_ = wait f2;

    f1_[0].push(...f2_[0]);
    f1_[1].push(...f2_[1]);
    return f1_;
}


function partitionInternal(string[][] recs, 
                        function(string[]) returns boolean cellFilter, 
                        int startPoint, 
                        int end) returns string[][][] {
    string[][] truePartition = [];
    string[][] falsePartition = [];
    int i = startPoint;
    while(i < end) {
        string[] row = recs[i];
        if (cellFilter(row)) {
            truePartition.push(row);
        } else {
            falsePartition.push(row);
        }
        i += 1;
    }

    return [truePartition, falsePartition];
}
