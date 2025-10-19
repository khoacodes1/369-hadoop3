Task 1: Country Request Count

Job 1 (Map-side Join and Count):
- Mapper: loads elements from hostname_country.csv into Hashmap in setup(). For each log entry, it parses the Apache log, extracts the hostname, looks up the country in the HashMap, and emits (country, 1) pairs.
- Reducer: Sum up counts for each country by the 1s values, producing (country, total_count) pairs
- Output: Unsorted country counts written to temp_output_task1

Job 2 (Sort by Count):
- Mapper: reads the temp outpu8ts and create key (count, country) sorting count decescending order.
- Reducer: use the sorted data to output (country, count)
- Output: Final sorted results in output_task1/

Task 2: Country URL Request Count
Job 1 (Map-side Join and Count)
- Mapper: loads elements from hostname_country.csv. Parses each log entry, strips query parameters from URLs, looks up the country for each hostname, and emits ((country, url), 1) as a composite key-value pair.
- Reducer: Sums each counts for unique (country, url) combination.
- Output: Country URL parids with counts writtent o temp2/

Job 2 (Sort by Country, Count, URL)
- Mapper: Read temp output and create key (country, count, url)
- Partition: Ensure all records for the same country go to the same Reducer
- Comparator: Sort by country, count, then URL
- Grouping: Groups record by country for reducer to process by the country
- Reducer: Receives sorted data and outputs (country + "\t" + url, count)
- Output: Final sorted results in output2/

Task 3: URL Country List
Job 1 (Map-side Join)
- Mapper: Loads country mappings. For each log entry, strips query parameters from the URL, looks up the country, and emits (url, country) pairs.
- Reducer: Use a HashMap to collect unique country names for each URL. (url, "country1, country2, ...") duplicates are removed
- Output: URLs with unsorted country lists written to temp3/

Job 2 (Sort by URL):
- Mapper: Identity mapper that passes through (url, country_list) pairs.
- Reducer: Sorted alphabetically by URL
- Output: output3/