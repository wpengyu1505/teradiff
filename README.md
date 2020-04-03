# Teradiff

A tool which compares 2 large scale datasets based on Spark

# Runner script

- Locate "run.sh" file under directory "src/scripts/"
- Deploy "run.sh" file to the same level of teradiff jar file
- Create properties file and put it in the same directory as teradiff jar file
  * Example: src/main/resources/teradiff.properties
- Tune parameters of "--num-executors", "--executor-memory", etc based on cluster resources
- Execute ./run.sh



# Arguments explained

- left: Left source
- right: Right source
- sourceType1: Left source type (csv/rdbms/json/hive/parquet)
- sourceType2: Right source type (csv/rdbms/json/hive/parquet)
- propertyFile: JDBC property (only applies to rdbms option)
- runMode: local or cluster
- partitions: Level of parallelism (for high volume)
- leftSchema: Comma seperated list of columns of the left source
- rightSchema: Comma seperated list of columns of the right source
- leftDelimiter: Only applies to csv, default is comma
- rightDelimiter: Only applies to csv, default is comma
- leftWithHeader: Only applies to csv, whether schema header is included in the file
- rightWithHeader: Only applies to csv, whether schema header is included in the file
- leftKey: Comma seperated list of columns to be the unique ID of the left source
- rightKey: Comma seperated list of columns to be the unique ID of the right source
- leftValue: Comma seperated list of columns to be compared for the left source
- rightValue: Comma seperated list of columns to be compared for the right source
- leftIgnores: Comma seperated list of columns to be ignored for left
- rightIgnores: Comma seperated list of columns to be ignored for the right
* List of columns between left and right must follow the same order

# Example delimiter options:
Pipe:   --leftDelimiter '|'
Ctrl-A: --leftDelimiter $'\001'