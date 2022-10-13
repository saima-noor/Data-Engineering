Step to install airflow on windows without Docker:

https://www.youtube.com/watch?v=SYOUbiGtGiU

Step 1: Control Panel | Programs and Features | Turn Windows features on or off

Enable : Windows Subsystem for Linux

Step 2: Install Ubuntu from windows store and restart system

Step 3: Install and update PIP

sudo apt-get install software-properties-common
sudo apt-add-repository universe
sudo apt-get update
sudo apt-get install python-pip


sudo apt install python3-pip (this works)

Step 4: Install airflow:

Note:



if above command doesn't work use below
    python3 -m pip install apache-airflow

export SLUGIFY_USES_TEXT_UNIDECODE=yes
    sudo pip install apache-airflow  (this works -- use sudo)

Step 5: Initialize DB: 
    airflow initdb

or 

    airflow db init  (this works)

Step 6: Start airflow server:
    airflow webserver -p 8080

Step 7: URL is ready : http://localhost:8080/

or type in web browser

localhost:8080/admin/

this will ask u to login with user and password

create user: if the user is not created

airflow users create \
    --username saima \
    --firstname saima \
    --lastname noor \
    --role Admin \
    --email saima@fab.org
or,
 FLASK_APP=airflow.www.app flask fab create-admin
    sername [admin]: saima
    User first name [admin]: saima
    User last name [user]: noor
    Email [admin@fab.org]: saima@fab.org
    Password: 123456
    Repeat for confirmation: 123456


Run the webserver again after the creation of user. if it is created then no need to create again.

Step 8: Setup folder for DAG

creae dag folder in the PC

For example:
create folder DAG in C: drive (C:\DAG)
then, dags_folder = /mnt/c/dag (when the airflow.cfg file is edited)

create folder DAG in C: drive (D:\Saima\Data-Engineering\Apache-Airflow\DAG\dags_bat\airflow_proj\dags)
then, dags_folder = /mnt/d/Saima/Data-Engineering/Apache-Airflow/DAG/dags_bat/airflow_proj/dags (when the airflow.cfg file is edited)

Step 9: Add new DAG 

saima@Saima-DA-LPTP:~$ cd ..
saima@Saima-DA-LPTP:/home$ ls
saima
saima@Saima-DA-LPTP:/home$ cd
saima@Saima-DA-LPTP:~$ ls
airflow
saima@Saima-DA-LPTP:~$ cd airflow/
saima@Saima-DA-LPTP:~/airflow$ ls
airflow-webserver.pid  airflow.cfg  airflow.db  logs  webserver_config.py
saima@Saima-DA-LPTP:~/airflow$ nano airflow.cfg
saima@Saima-DA-LPTP:~/airflow$ nano airflow.cfg (the file is edited)
saima@Saima-DA-LPTP:~/airflow$ airflow db init (It will bring all the dags in airflow)

run airflow initdb




Exporting : Imp step after installing airflow
saima@Saima-DA-LPTP:~$ export AIRFLOW_HOME=/d/Saima/Data-Engineering/Apache-Airflow/DAG 
///airflow home is the environment variable to know where is the airflow conf file
saima@Saima-DA-LPTP:~$ nano ~/.bashrc

edit the file and type --- 
export AIRFLOW_HOME=/d/Saima/Data-Engineering/Apache-Airflow/DAG

this is to make the environment variable permanent in that location  in the system.

then close ubuntu and relaunch it. this will send the config file in that location of windows and the file can be edited from there.

Validationn of airflow installation--
saima@Saima-DA-LPTP:~$ airflow version
saima@Saima-DA-LPTP:~$ echo $AIRFLOW_HOME 
iT WILL SHOW THE LOCATION OF THE VARIABLE

then initislaize the meta Db--
    airflow db init
the run airflow webserver -- 
    airflow webserver
then run the airflow scheuler -- 
    airflow scheduler


