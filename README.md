# Teradiff

A tool which compares 2 large scale datasets based on Spark

# Runner script

- Locate "run.sh" file under directory "src/scripts/"
- Deploy "run.sh" file to the same level of teradiff jar file
- Create properties file and put it in the same directory as teradiff jar file
  * Example: src/main/resources/teradiff.properties
- Tune parameters of "--num-executors", "--executor-memory", etc based on cluster resources
- Execute ./run.sh



# Properties explained

- LEFT_SCHEMA: Comma seperated list of columns of the left source
- RIGHT_SCHEMA: Comma seperated list of columns of the right source
- LEFT_DELIMITER: Only applies to csv, default is comma
- RIGHT_DELIMITER: Only applies to csv, default is comma
- LEFT_WITH_HEADER: Only applies to csv, whether schema header is included in the file
- RIGHT_WITH_HEADER: Only applies to csv, whether schema header is included in the file
- LEFT_KEY: Comma seperated list of columns to be the unique ID of the left source
- RIGHT_KEY: Comma seperated list of columns to be the unique ID of the right source
- LEFT_VALUES: Comma seperated list of columns to be compared for the left source
- RIGHT_VALUES: Comma seperated list of columns to be compared for the right source
* List of columns between left and right must follow the same order

# Argument List:
src/test/resources/data1.txt src/test/resources/data2.txt csv src/main/resources/teradiff.properties src/main/resources/summaryFile.txt 1