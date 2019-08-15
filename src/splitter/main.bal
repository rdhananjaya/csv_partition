import ballerina/io;
import ballerina/math;

type CsvTab string[][];

function getHeaders(io:ReadableCSVChannel chan) returns string[]|error {
    string [] columnHeaders = [];
    if (chan.hasNext()) {
        var titles = check chan.getNext();
        if (titles is string[]) {
            io:println(titles);
            columnHeaders = titles;
        }
    }
    return <@untainted> columnHeaders;
}

function getCsvContent(io:ReadableCSVChannel chan) returns CsvTab {
    CsvTab tab = [];
    int rowNum = 0;
    while (chan.hasNext()) {
        var records = chan.getNext();

        if (records is string[]) {
            tab.push(records);
        } else {
            io:println("error at row num: " + rowNum.toString());
            io:println(records);
        }
        rowNum += 1;
    }
    return <@untainted> tab;
}

function foo(string filePath) returns error? {
    io:ReadableCSVChannel rCsvChannel = checkpanic io:openReadableCsvFile(filePath);
    string [] columnHeaders = check getHeaders(rCsvChannel);
    CsvTab tab = getCsvContent(rCsvChannel);
    checkpanic rCsvChannel.close();

    (function (string) returns boolean) spliter = p => p.trim() == "1";

    string[][][]|error splited = partition(tab, columnHeaders, "b", spliter);
    if (splited is string[][][]) {
        io:println(splited[0]);
        io:println();
        io:println(splited[1]);
        io:println(splited[0].length());
        io:println(splited[1].length());
    }
}

function partition(string[][] tab, 
                    string[] headers, string headerName, 
                    (function (string) returns boolean) spliter) returns string[][][]|error {
    int? columnPos = headers.map(s => s.toLowerAscii()).indexOf(headerName);
    io:println("Column number: " + columnPos.toString());
    if (columnPos is ()) {
        return error("Column not found:" + headerName);
    }

    int length = tab.length();
    int midPoint = length/2;
    // io:println("length:" + length.toString());
    // io:println("mid:" + midPoint.toString());

    function(string[]) returns boolean cellFilter = function(string[] row) returns boolean {
        return spliter(row[<int>columnPos]);
    };

    future<string[][][]> f1 = start partitionInternal(tab, cellFilter, 0, midPoint);
    future<string[][][]> f2 = start partitionInternal(tab, cellFilter, midPoint, length);

    string[][][] f1_ = wait f1;
    string[][][] f2_ = wait f2;

    f1_[0].push(...f2_[0]);
    f1_[1].push(...f2_[1]);
    return f1_;
}

function partitionInternal(string[][] tab, 
                        function(string[]) returns boolean cellFilter, 
                        int startPoint, 
                        int end) returns string[][][] {
    string[][] truePartition = [];
    string[][] falsePartition = [];
    int i = startPoint;
    while(i < end) {
        string[] row = tab[i];
        if (cellFilter(row)) {
            truePartition.push(row);
        } else {
            falsePartition.push(row);
        }
        i += 1;
    }

    return [truePartition, falsePartition];
}
