[![Travis Badge](https://secure.travis-ci.org/dattack/dbcopy.svg?branch=master)](https://travis-ci.org/dattack/dbcopy/builds)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ebbfe656384f4f1993ec46fffd1d8aa3)](https://www.codacy.com/app/dattack/dbping)
[![Codeship Badge](https://codeship.com/projects/f73609f0-6fe2-0134-dcfd-3acc74581569/status?branch=master)](https://app.codeship.com/projects/178133)
[![CircleCI](https://circleci.com/gh/dattack/dbcopy.svg?style=svg)](https://circleci.com/gh/dattack/dbcopy)
[![codecov](https://codecov.io/gh/dattack/dbcopy/branch/master/graph/badge.svg)](https://codecov.io/gh/dattack/dbcopy)
[![license](https://img.shields.io/:license-Apache-blue.svg?style=plastic-square)](LICENSE.md)

dbcopy
=======

dbCopy is a database copy and conversion tool that moves or copies data from one database
to another.

Bugs and Feedback
=========
For bugs and discussions please use the [Github Issues](https://github.com/dattack/dbcopy/issues).
If you have other questions, please contact by [email](mailto:dev@dattack.com) or
[@dattackteam](https://twitter.com/dattackteam) 

## Configuration

SQL statements for the creation of the table for storing the log of executions in the database in which the 
INSERT statements will be executed:

```sql
DROP TABLE dbcopy_log;

CREATE TABLE dbcopy_log (
   task_name      VARCHAR2(100)            NOT NULL,
   execution_id   VARCHAR2(40)             NOT NULL,
   log_time       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   object_name    VARCHAR2(100)            NOT NULL,
   start_time     TIMESTAMP WITH TIME ZONE NOT NULL,  
   end_time       TIMESTAMP WITH TIME ZONE,
   retrieved_rows NUMBER(19,0)             DEFAULT 0,
   processed_rows NUMBER(19,0)             DEFAULT 0,
   err_msg        VARCHAR2(200)            DEFAULT NULL
);

ALTER TABLE dbcopy_log ADD PRIMARY KEY(task_name, execution_id);

COMMIT;
```
Copyright and license
=========

Copyright 2017 Dattack Team

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work 
except in compliance with the License. You may obtain a copy of the License in the LICENSE
file, or at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

This product includes software developed by The Apache Software Foundation (http://www.apache.org/).

