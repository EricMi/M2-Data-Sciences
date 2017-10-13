hdfs dfs -rm -r pivot_output
rm -rf pivot_output

# hdfs dfs -put pivot_input pivot_input
hadoop jar Pivot.jar Pivot pivot_input pivot_output
hdfs dfs -get pivot_output pivot_output
