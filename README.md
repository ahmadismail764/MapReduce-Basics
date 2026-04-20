# Assignment 1 – Hadoop MapReduce Run Commands (WSL Ubuntu / Linux)

## Build (run once)

```bash
cd "/mnt/g/Uni/Big Data/Assignment1"
mvn -DskipTests package
```

> [!NOTE]
> This produces 5 JARs under `target/`:

> - `InnerJoin.jar`
> - `FullOuterJoin.jar`
> - `SetDifference.jar`
> - `FriendsOfFriends.jar`
> - `HadoopSort.jar`

---

## Q1.1 – Inner Join

```bash
rm -rf q1_inner_output
mvn -DskipTests exec:java -Dexec.mainClass=assignment1.InnerJoin -Dexec.args="input_q1.txt q1_inner_output"
cat q1_inner_output/part-r-00000
```

**Expected Output:**

```text
A    1,x
B    2,x
C    3,x
```

## Q1.2 – Full Outer Join

```bash
rm -rf q1_full_outer_output
mvn -DskipTests exec:java -Dexec.mainClass=assignment1.FullOuterJoin -Dexec.args="input_q1.txt q1_full_outer_output"
cat q1_full_outer_output/part-r-00000
```

**Expected Output:**

```text
A      1,x
B      2,x
C      3,x
null   4,y
null   5,y
null   6,y
null   7,z
null   8,z
```

## Q1.3 – Set Difference `A1[T1] – A1[T2]`

```bash
rm -rf q1_set_difference_output
mvn -DskipTests exec:java -Dexec.mainClass=assignment1.SetDifference -Dexec.args="input_q1.txt q1_set_difference_output"
cat q1_set_difference_output/part-r-00000
```

**Expected Output:** `4 5 6 7 8`

---

## Q2 – Friends of Friends (all pairs)

```bash
rm -rf q2_all_pairs_output q2_all_pairs_output_temp
mvn -DskipTests exec:java -Dexec.mainClass=assignment1.FriendsOfFriends -Dexec.args="input_q2.txt q2_all_pairs_output"
cat q2_all_pairs_output/part-r-00000
```

## Q2 – Friends of Friends (filtered for P1)

```bash
rm -rf q2_p1_output q2_p1_output_temp
mvn -DskipTests exec:java -Dexec.mainClass=assignment1.FriendsOfFriends -Dexec.args="input_q2.txt q2_p1_output P1"
cat q2_p1_output/part-r-00000
```

---

## Q3 – Hadoop Sort

```bash
rm -rf q3_sorted_output
mvn -DskipTests exec:java -Dexec.mainClass=assignment1.HadoopSort -Dexec.args="input_q3.txt q3_sorted_output"
cat q3_sorted_output/part-r-00000
```

> [!TIP]
> Keys are sorted ascending; edit `input_q3.txt` with any `key<TAB>value` pairs.

---

## Notes

- **Delete Outputs:** Always delete the output folder before rerunning the same job.
- **Temp Folders:** Intermediate temp folders (e.g., `q2_all_pairs_output_temp`) are created automatically; delete them too on rerun.
- **Paths:** All paths are relative to the project root.
- **HDFS Usage:** For actual HDFS usage:
  - Replace local paths with `hdfs://` URIs.
  - Remove the `mapreduce.framework.name=local` / `fs.defaultFS=file:///` configuration overrides.
