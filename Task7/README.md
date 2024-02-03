<h1>Airflow introduction</h1>

<h2>Описание задания</h2>
<p>В данном задании требуется создать и настроить локальное рабочее окружение с необходимыми инструментами:</p>
<ul>
    <li>Apache Airflow;</li>
    <li>Python;</li>
    <li>Pandas;</li>
    <li>MongoDB.</li>
</ul>

<p>Цель задания - разработать Directed Acyclic Graph для обработки данных и их загрузки в MongoDB с использованием Apache Airflow.</p>

<h2>Описание дагов</h2>
<p>Первый даг состоит из следующих задач</p>
<ul>
    <li>check_file = FileSensor</li>
    <li>branching = BranchPythonOperator</li>
    <li>empty_file = BashOperator</li>
    <li>not_empty = EmptyOperator</li>
</ul>
<p>Здесь запускается сенсор, который ожидает появление файла, после чего идет проверка на наличие данных в нем. 
Если данные есть, то они перегоняются в pandas dataframe и выполняются следующие задания:</p>
<ul>
    <li>edit_null = PythonOperator - заменяет NaN на "-"</li>
    <li>sort = PythonOperator - сортирует по дате</li>
    <li>remove = PythonOperator - удаляет смайлики из contents</li>
</ul>
<p>Также этот даг имеет датасет, за счет которого, после выполнения операций над ним, подается сигнал второму дагу. Он состоит из одной задачи, которая выполняет подключение к mongoDB.</p>
<p>В конце в комментариях содержатся запросы для mongo shell</p>

