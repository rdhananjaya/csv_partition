import ballerina/test;
import ballerina/io;

@test:Config{}
function testFunction () {
    var e = foo("src\\splitter\\tests\\resources\\crime.csv");
    io:println(e);
}