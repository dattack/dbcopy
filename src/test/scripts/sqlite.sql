
DROP TABLE IF EXISTS DEPT;
CREATE TABLE DEPT (
    DEPTNO  INTEGER PRIMARY KEY,
    DNAME   TEXT,
    LOC     TEXT
);

DROP TABLE IF EXISTS EMP;
CREATE TABLE EMP (
    EMPNO   INTEGER PRIMARY KEY,
    ENAME   TEXT,
    JOB     TEXT,
    MGR     INTEGER,
    HIREDATE TEXT,
    SAL     REAL,
    COMM    REAL,
    DEPTNO  INTEGER
);

DROP TABLE IF EXISTS BONUS;
CREATE TABLE BONUS (
    ENAME	TEXT,
    JOB     TEXT,
    SAL     INTEGER,
    COMM    INTEGER
);

DROP TABLE IF EXISTS SALGRADE;
CREATE TABLE SALGRADE (
    GRADE	INTEGER,
    LOSAL   INTEGER,
    HISAL   INTEGER
);

INSERT INTO DEPT VALUES (10, 'ACCOUNTING', 'NEW YORK');
INSERT INTO DEPT VALUES (20, 'RESEARCH', 'DALLAS');
INSERT INTO DEPT VALUES (30, 'SALES', 'CHICAGO');
INSERT INTO DEPT VALUES (40, 'OPERATIONS', 'BOSTON');

INSERT INTO EMP VALUES (7369, 'SMITH', 'CLERK', 7902, '17-12-1980', 800, NULL, 20);
INSERT INTO EMP VALUES (7499, 'ALLEN', 'SALESMAN', 7698, '20-2-1981', 1600, 300, 30);
INSERT INTO EMP VALUES (7521, 'WARD', 'SALESMAN', 7698, '22-2-1981', 1250, 500, 30);
INSERT INTO EMP VALUES (7566, 'JONES', 'MANAGER', 7839, '2-4-1981', 2975, NULL, 20);
INSERT INTO EMP VALUES (7654, 'MARTIN', 'SALESMAN', 7698, '28-9-1981', 1250, 1400, 30);
INSERT INTO EMP VALUES (7698, 'BLAKE', 'MANAGER', 7839, '1-5-1981', 2850, NULL, 30);
INSERT INTO EMP VALUES (7782, 'CLARK', 'MANAGER', 7839, '9-6-1981', 2450, NULL, 10);
INSERT INTO EMP VALUES (7788, 'SCOTT', 'ANALYST', 7566, '13-JUL-87', 3000, NULL, 20);
INSERT INTO EMP VALUES (7839, 'KING', 'PRESIDENT', NULL, '17-11-1981', 5000, NULL, 10);
INSERT INTO EMP VALUES (7844, 'TURNER', 'SALESMAN', 7698, '8-9-1981', 1500, 0, 30);
INSERT INTO EMP VALUES (7876, 'ADAMS', 'CLERK', 7788, '13-JUL-87', 1100, NULL, 20);
INSERT INTO EMP VALUES (7900, 'JAMES', 'CLERK', 7698, '3-12-1981', 950, NULL, 30);
INSERT INTO EMP VALUES (7902, 'FORD', 'ANALYST', 7566, '3-12-1981', 3000, NULL, 20);
INSERT INTO EMP VALUES (7934, 'MILLER', 'CLERK', 7782, '23-1-1982', 1300, NULL, 10);

INSERT INTO SALGRADE VALUES (1, 700, 1200);
INSERT INTO SALGRADE VALUES (2, 1201, 1400);
INSERT INTO SALGRADE VALUES (3, 1401, 2000);
INSERT INTO SALGRADE VALUES (4, 2001, 3000);
INSERT INTO SALGRADE VALUES (5, 3001, 9999);
