# Pivot a matrix by using Map Reduce

We can implement the pivot function as a map-reduce procedure.

## 1. map function

The map function reads the document contents, split the elements of the table, create key-value pair like <#col, <#row, val>>, where *#col* is the original column number, *#row* is the original row number, *val* is the value of table element.

In this way, the elements of the same row in the new table will be sorted together after the *shuffling* procedure.

The pseudo code of the map function is as follow:

```java
public void map (Object key, Text value, Context context) {
    // key: document name
    // value: document contents
    // context

    String lines[] = value.trim().split("\\r?\\n");
    Integer row = 0;
    for (String l : lines) {
        String fields[] = l.trim().split(",");
        Integer col = 0;
        for (String f : fields) {
            context.write(Integer.toString(col), Integer.toString(row)+","+fields);
            col++;
        }
        row++;
    }
}
```

## 2. reduce function

The reduce function should retrieve the *#row* and *val*, then sort in ascending order by *#row*,
then write the values one by one.

```java
public void reduce (Text key, Iterable<Text>, values, Context context) {
    Map<Integer, String> fields;
    for (Text f : values) {
        String pair[] = f.split(",");
        fields.put(pair[0], pair[1]);
    }

    Map<Integer, String> sorted = fields.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

    for (Map.Entry<Integer, String> entry : sorted.entrySet()) {
        if (entry.getKey() != 0) {
            context.write(",");
        }
        context.write(entry.getValue());
    }
    context.write("\n");
}
```
