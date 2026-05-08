# SINGLE 模式 Iceberg 表老化示例

这个示例演示如何用 TaskPlugin Spark 的 `SINGLE` 提交模式老化一张 Iceberg 表：

1. 删除 `event_ts < TIMESTAMP '2026-05-01 00:00:00'` 的旧业务数据。
2. 调用 Iceberg Spark procedure `expire_snapshots` 清理旧快照。

默认老化表：

```text
iceberg_hdfs.demo.user_profile_source
```

默认 HDFS 根路径：

```text
hdfs:///tmp/taskplugin/single-iceberg-aging-task
```

如果要换路径或表名，需要同时修改本 README、`app-config.yaml`、`config.yaml` 和 `sql/sql.yaml` 中对应值。

## 1. 用例目录

```text
single-iceberg-aging-task/
|-- README.md
|-- app-config.yaml
|-- config.yaml
`-- sql/
    `-- sql.yaml
```

文件说明：

| 文件 | 用途 | HDFS 位置 |
|---|---|---|
| `app-config.yaml` | 统一提交入口，声明 `SINGLE` 模式 | `${TASK_HOME}/app-config.yaml` |
| `config.yaml` | 单任务配置，声明 SQL 文件和 Iceberg catalog | `${TASK_HOME}/config.yaml` |
| `sql/sql.yaml` | 实际老化 SQL | `${TASK_HOME}/sql/sql.yaml` |

## 2. 环境前提

Linux 环境需要具备：

1. HDFS 可用。
2. Spark on YARN 可用。
3. `spark-submit` 可提交任务。
4. `spark-sql` 可用于建表和验证。
5. Iceberg Spark runtime 可用。
6. 当前项目已经打出 `task-plugin-spark` jar。

本文默认你已经把 Iceberg runtime 依赖放进 `task-plugin-spark` fat jar，或 Spark 集群已经提供 Iceberg runtime。

如果没有，需要在后面的 `spark-submit` 或 `spark-sql` 命令里追加：

```bash
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:<your-iceberg-version>
```

也可以使用本地 jar：

```bash
--jars /path/to/iceberg-spark-runtime.jar
```

Iceberg runtime 版本需要和你的 Spark / Scala 版本匹配。

## 3. 构建并上传任务 jar

在项目根目录执行：

```bash
mvn -pl task-plugin-spark -am -DskipTests package
```

设置后续命令使用的变量：

```bash
TASK_HOME=hdfs:///tmp/taskplugin/single-iceberg-aging-task
JAR_HOME=hdfs:///tmp/taskplugin/jars
TASK_JAR=${JAR_HOME}/task-plugin-spark-1.0.0-SNAPSHOT.jar
ICEBERG_WAREHOUSE=${TASK_HOME}/warehouse
```

上传 jar：

```bash
hdfs dfs -mkdir -p ${JAR_HOME}
hdfs dfs -put -f task-plugin-spark/target/task-plugin-spark-1.0.0-SNAPSHOT.jar ${TASK_JAR}
```

## 4. 创建 Iceberg 测试表

下面用 `spark-sql` 创建 Hadoop catalog Iceberg 表，并插入 cutoff 前后的测试数据。

启动 `spark-sql`：

```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg_hdfs=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg_hdfs.type=hadoop \
  --conf spark.sql.catalog.iceberg_hdfs.warehouse=${ICEBERG_WAREHOUSE}
```

在 `spark-sql` 中执行：

```sql
CREATE NAMESPACE IF NOT EXISTS iceberg_hdfs.demo;

DROP TABLE IF EXISTS iceberg_hdfs.demo.user_profile_source;

CREATE TABLE iceberg_hdfs.demo.user_profile_source (
  user_id BIGINT,
  user_name STRING,
  event_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
  'format-version' = '2'
);

INSERT INTO iceberg_hdfs.demo.user_profile_source VALUES
  (1, 'alice_old', TIMESTAMP '2026-04-28 10:00:00'),
  (2, 'bob_old', TIMESTAMP '2026-04-30 23:59:59'),
  (3, 'carol_new', TIMESTAMP '2026-05-01 00:00:00'),
  (4, 'david_new', TIMESTAMP '2026-05-02 09:30:00');
```

先确认老化前有 2 条旧数据：

```sql
SELECT COUNT(*) AS old_rows
FROM iceberg_hdfs.demo.user_profile_source
WHERE event_ts < TIMESTAMP '2026-05-01 00:00:00';
```

预期结果：

```text
2
```

说明：`DELETE FROM` 依赖 Iceberg Spark extensions。表属性使用 `format-version=2`，方便支持行级删除能力。

## 5. 上传 SINGLE 配置到 HDFS

在 `examples/spark/single-iceberg-aging-task` 目录下执行：

```bash
hdfs dfs -mkdir -p ${TASK_HOME}/sql
hdfs dfs -rm -f ${TASK_HOME}/app-config.yaml ${TASK_HOME}/config.yaml ${TASK_HOME}/sql/sql.yaml

hdfs dfs -put -f app-config.yaml ${TASK_HOME}/app-config.yaml
hdfs dfs -put -f config.yaml ${TASK_HOME}/config.yaml
hdfs dfs -put -f sql/sql.yaml ${TASK_HOME}/sql/sql.yaml
```

不要在这里执行 `hdfs dfs -rm -r -f ${TASK_HOME}`，因为 Iceberg warehouse 默认就在 `${TASK_HOME}/warehouse`，整目录删除会把刚创建好的测试表也删掉。

上传后的 HDFS 结构应为：

```text
hdfs:///tmp/taskplugin/single-iceberg-aging-task/
|-- app-config.yaml
|-- config.yaml
|-- warehouse/
`-- sql/
    `-- sql.yaml
```

其中：

1. `app-config.yaml` 的 `single.task-path` 必须等于 `${TASK_HOME}`。
2. `config.yaml` 的 `computation.sql.yaml-file` 是相对 `${TASK_HOME}` 的路径，当前值为 `sql/sql.yaml`。
3. `config.yaml` 已经配置 `spark.sql.catalog.iceberg_hdfs.warehouse=${TASK_HOME}/warehouse`。

确认文件已上传：

```bash
hdfs dfs -ls -R ${TASK_HOME}
```

## 6. 启动 SINGLE 老化任务

执行：

```bash
spark-submit \
  --class com.taskplugin.spark.SparkTaskApplication \
  --master yarn \
  --deploy-mode client \
  ${TASK_JAR} \
  --app-config ${TASK_HOME}/app-config.yaml
```

如果你的 fat jar 没有打进 Iceberg runtime，可以追加 `--packages`：

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:<your-iceberg-version> \
  --class com.taskplugin.spark.SparkTaskApplication \
  --master yarn \
  --deploy-mode client \
  ${TASK_JAR} \
  --app-config ${TASK_HOME}/app-config.yaml
```

也可以使用 `--jars`：

```bash
spark-submit \
  --jars /path/to/iceberg-spark-runtime.jar \
  --class com.taskplugin.spark.SparkTaskApplication \
  --master yarn \
  --deploy-mode client \
  ${TASK_JAR} \
  --app-config ${TASK_HOME}/app-config.yaml
```

任务会按顺序执行 `sql/sql.yaml` 中的两条 SQL：

1. `DELETE FROM iceberg_hdfs.demo.user_profile_source ...`
2. `CALL iceberg_hdfs.system.expire_snapshots(...)`

## 7. 确认是否成功

首先确认 `spark-submit` 退出码为 0。随后用同一个 Iceberg catalog 配置进入 `spark-sql`：

```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg_hdfs=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg_hdfs.type=hadoop \
  --conf spark.sql.catalog.iceberg_hdfs.warehouse=${ICEBERG_WAREHOUSE}
```

确认 cutoff 前旧数据已经删除：

```sql
SELECT COUNT(*) AS old_rows
FROM iceberg_hdfs.demo.user_profile_source
WHERE event_ts < TIMESTAMP '2026-05-01 00:00:00';
```

预期结果：

```text
0
```

确认 cutoff 后的数据仍然保留：

```sql
SELECT user_id, user_name, event_ts
FROM iceberg_hdfs.demo.user_profile_source
ORDER BY event_ts;
```

预期只剩：

```text
3 carol_new 2026-05-01 00:00:00
4 david_new 2026-05-02 09:30:00
```

查看 snapshot 元数据：

```sql
SELECT committed_at, snapshot_id, operation
FROM iceberg_hdfs.demo.user_profile_source.snapshots
ORDER BY committed_at;
```

或查看 history：

```sql
SELECT made_current_at, snapshot_id, is_current_ancestor
FROM iceberg_hdfs.demo.user_profile_source.history
ORDER BY made_current_at;
```

需要确认当前表仍有可读 snapshot。若你的测试环境本身存在 `2026-05-01 00:00:00` 之前提交的旧 snapshot，它们会按 `older_than` 和 `retain_last` 规则被清理。

注意：如果按本 README 临时新建 demo 表，所有 snapshot 的提交时间都是当前时间生成，通常不会早于 `2026-05-01 00:00:00`，所以 `expire_snapshots` 可能不会真正删除 snapshot。这是正常结果，业务旧数据删除是否成功应以 `old_rows = 0` 为准。

## 8. 清理

清理 HDFS 示例目录：

```bash
hdfs dfs -rm -r -f ${TASK_HOME}
```

如果还想保留 jar，只清理任务目录即可。否则可以继续清理 jar：

```bash
hdfs dfs -rm -f ${TASK_JAR}
```
