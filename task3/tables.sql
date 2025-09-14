CREATE DATABASE IF NOT EXISTS test;
USE test;



CREATE TABLE IF NOT EXISTS repositories
(
    name     String,
    owner    String,
    stars    Int32,
    watchers Int32,
    forks    Int32,
    language String,
    updated  DateTime
) ENGINE = ReplacingMergeTree(updated)
      ORDER BY (owner, name);

-- Таблица для хранения коммитов авторов по датам
CREATE TABLE IF NOT EXISTS repositories_authors_commits
(
    date        Date,
    repo        String,
    author      String,
    commits_num Int32
) ENGINE = ReplacingMergeTree
      ORDER BY (date, repo, author);

-- Таблица для хранения позиций репозиториев в топе по датам
CREATE TABLE IF NOT EXISTS repositories_positions
(
    date     Date,
    repo     String,
    position UInt32
) ENGINE = ReplacingMergeTree
      ORDER BY (date, repo);
