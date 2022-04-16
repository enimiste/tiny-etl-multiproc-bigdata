# Requirements :
- Python 3.7.x
- MySQL 8.x or (Apache Cassandra 3.x + Java 8)
- Git bash to run main.py script
- `pip install mysql-connector-python`
- `pip install concurrent-log-handler`
- `pip install psutil`

- `logs` and `out_dir folders`

# Mysql store :
## DB Schema :
```sql
CREATE TABLE `words` (
	`word` VARCHAR(255) NOT NULL,
	`word_len` INT(10) NOT NULL DEFAULT '0',
	`word_truncated` BIT(1) NOT NULL DEFAULT b'0',
	`file_path` VARCHAR(270) NOT NULL,
	`file_words_count` INT(10) NOT NULL DEFAULT '0'
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB
;
```
## Config :
```python
IN_DIR = '../bdall_test_data/tiny_data/__files'
SAVE_TO_DB = True
DB_HOST = 'localhost'
DB_NAME = 'words'
DB_USER = 'root'
DB_PWD = 'root'
LOAD_BALANCER_PIPES = 4
DB_SQL_QUERIES= ["INSERT INTO words (word, word_len, word_truncated, file_path, file_words_count)  VALUES(%s,%s,%s,%s,%s)" 
                                                        for _ in range(0,LOAD_BALANCER_PIPES)]
CPU_MAX_USAGE = 0.90 #0...1
MONO_PIPELINE = True
```

## Data exploration queries :
```sql
SELECT COUNT(*) FROM words_1;
SELECT COUNT(*) FROM words_2;
SELECT COUNT(*) FROM words_3;
SELECT COUNT(*) FROM words_4;

SELECT COUNT(*) FROM words_1 WHERE word_truncated=b'1';
SELECT COUNT(*) FROM words_2 WHERE word_truncated=b'1';
SELECT COUNT(*) FROM words_3 WHERE word_truncated=b'1';
SELECT COUNT(*) FROM words_4 WHERE word_truncated=b'1';

select sum(file_words_count) FROM (SELECT DISTINCT(w.file_path), w.file_words_count FROM words_1 AS w) AS X;
select sum(file_words_count) FROM (SELECT DISTINCT(w.file_path), w.file_words_count FROM words_2 AS w) AS X;
select sum(file_words_count) FROM (SELECT DISTINCT(w.file_path), w.file_words_count FROM words_3 AS w) AS X;
select sum(file_words_count) FROM (SELECT DISTINCT(w.file_path), w.file_words_count FROM words_4 AS w) AS X;


SELECT count(DISTINCT(file_path)) FROM words_1;
SELECT count(DISTINCT(file_path)) FROM words_2;
SELECT count(DISTINCT(file_path)) FROM words_3;
SELECT count(DISTINCT(file_path)) FROM words_4;

SELECT * FROM words_1 LIMIT 1000;
SELECT * FROM words_2 LIMIT 1000;
SELECT * FROM words_3 LIMIT 1000;
SELECT * FROM words_4 LIMIT 1000;

SELECT concat(word, file_path), COUNT(*) AS x FROM words GROUP BY 1 HAVING X>1; 
```

# Cassandra Store :
## DB Schema :
## Config :
