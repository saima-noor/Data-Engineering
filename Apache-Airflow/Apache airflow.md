What is Airflow?
--Apache Airflow is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. Airflow’s extensible Python framework enables you to build workflows connecting with virtually any technology. A web interface helps manage the state of your workflows. Airflow is deployable in many ways, varying from a single process on your laptop to a distributed setup to support even the biggest workflows.

-- airflow is a platform to programmatically author, schedule and monitor workflows or data pipelines

--


DAG - directic acyclic graph : it is the data pipeline in airflow between tasks. Dags are the dependencies in the tasks. Tasks are the nodes. there cannot be a cyclic dependencies in the tasks. If T1 is depended on t2 then t2 cannot be depended on t1. cyclic dependencies will show error.
Operator : It is the task in the DAG
    1. Pythonoperator - executes python function
    2. Bashoperator - executes bash command
    3. Postgresoperator - inserts data into DB
    4. branchpythonoperator - moves from one task to another

