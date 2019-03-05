# Simple data transfer tool

is able to transfer data from any sources to postgresql

## Usage
* wright sql query and put it into query folder as .sql file
* fill the `config/config.yaml` up for a source db in the source_name section
* fill the `config/config.yaml` up for a sink db in the sink_name section
* run `python etl.py --from your_source_name --to your_sink_name`
* check `logs/info.log` and `logs/errors.log`

## TODO
* add csv support
* add mongodb support

## Introduction
<ul>
<li>Prototype - base class</li>
    <ul>
        <li>Batch loading (based on pandas) uses together the following classes:</li>
            <ul>
            <li>Source class</li>
                <ol>
                    <li> can use all sources available for SQLAlchemy</li>
                    <li> usefull for small data</li>
                    <li> specify date_column field in the congif.yaml if result of query execution contains timestamp fields  </li>
                </ol>
            <li>Sink   class</li>
                <ol>
                <li> can use all sinks   available for SQLAlchemy</li>
                <li> usefull for small data</li>
                </ol>
            </ul>
            </ul>
            <ul>
        <li>Parallel loading (as for sink only postgres is available) uses together the following classes:</li>
            <ul>
                <li>Producer class</li>
                <li>Sink   class</li>
                </ul>
            </ul>
</ul>


### Config description

source_name:
  test_source2:
    type: postgresql+psycopg2
    host: 172.18.151.27
    port: 5432
    dbname: superset
    schema: upload
    user: superset
    psw: DEkde3467XDer4G
    file: query/test.sql
    receive mode: batch

source_name - section name, can't be changed
test_source - source pseudonym, used in the command prompt
type - type db, see also SQLAlchemy
file - file containig query to db
cursor_size - number of rows transmitted to insertion in one commitment (receive_mode = row_by_row or multiprocessing]


### Command promt
receive_mode:
* all_data, will be used pandas as backend
* row_by_row,  will consume small batch from db cursor and load it into sink (specify cursor_size in the source_name section(
* multi,  will consume small batch from db cursor and load it into sink concurrently