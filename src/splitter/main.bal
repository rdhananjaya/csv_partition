import ballerina/io;
import ballerina/math;

type CsvTab string[][];

function getHeaders(io:ReadableCSVChannel chan) returns string[]|error {
    string [] columnHeaders = [];
    if (chan.hasNext()) {
        var titles = check chan.getNext();
        if (titles is string[]) {
            io:println("Headers:");
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

    (function (string) returns boolean) spliter = p => p.trim() == "2";

    CsvTab[]|error splited = partition(tab, columnHeaders, "b", spliter, 3);
    if (splited is CsvTab[]) {
        io:println("Partition for true eval: [size]" + splited[0].length().toString());
        io:println(splited[0]);
        io:println();
        io:println("Partition for false eval:[size]" + splited[0].length().toString());
        io:println(splited[1]);
    }
}

function partition(CsvTab tab, 
                    string[] headers, string headerName, 
                    (function (string) returns boolean) spliter, int parallelism) returns CsvTab[]|error {
    int? columnPos = headers.map(s => s.toLowerAscii()).indexOf(headerName);
    io:println("Column number: " + columnPos.toString());
    if (columnPos is ()) {
        return error("Column not found:" + headerName);
    }

    int length = tab.length();
    int subSectionLen = length/parallelism;

    function(string[]) returns boolean cellFilter = function(string[] row) returns boolean {
        return spliter(row[<int>columnPos]);
    };

    future<CsvTab[]>[] futures = [];
    int startPos = 0;
    int endPos = subSectionLen;
    // start partition parallely.
    while (true) {
        future<CsvTab[]> f = start partitionInternal(tab, cellFilter, startPos, endPos);
        futures.push(f);
        startPos += subSectionLen;
        endPos += subSectionLen;
        if (endPos + 1 == length) {
            endPos = length;
        }
        if (endPos > length) {
            break;
        }
    }

    // merge sub-partitions into single array.
    CsvTab[][] subTabs = futures.map(f => wait f);
    CsvTab t1 = [];
    CsvTab t2 = [];
    CsvTab[] merged = [t1, t2];

    worker w1 {
        foreach int i in 0...(subTabs.length() - 1) {
            merged[0].push(...subTabs[i][0]);
        }
    }

    worker w2 {
        foreach int i in 0...(subTabs.length() - 1) {
            merged[1].push(...subTabs[i][1]);
        }
    }

    wait w1;
    wait w2;

    return merged;
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
