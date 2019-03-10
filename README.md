# pysdtt - simple data transfer tool

is able to transfer data from any sources to postgresql

## Usage
* wright sql query and put it into query folder as .sql file
* fill the `config/config.yaml` up for a source db in the source_name section
* fill the `config/config.yaml` up for a sink db in the sink_name section
* run `python etl.py --from your_source_name --to your_sink_name --mode multi`
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

* source_name - section name, can't be changed
* test_source - source pseudonym, used in the command prompt
* type - type db, see also SQLAlchemy
* file - file containig query to db
* cursor_size - number of rows transmitted to insertion in one commitment (receive_mode = row_by_row or multiprocessing]


### Command prompt
receive_mode:
* all_data, will be used pandas as backend
* row_by_row,  will consume small batch from db cursor and load it into sink (specify cursor_size in the source_name section). This allows you to process unlimited or unknown amounts of data with a fixed amount of memory.
* multi, the same as row_by_row, but much faster

### Challenges with writing Custom ETL scripts to move data from MongoDB to PostgreSQL:
   1. Schema detection cannot be done up front
   Unlike a relational database, a MongoDB collection doesn’t have a predefined schema.
   Hence, it is impossible to look at a collection and create a compatible table in PostgreSQL upfront.

   2. Different documents in a single collection can have a different set of fields
   A document in a collection in MongoDB can have a different set of fields. 

     {

        "name": "John Doe",

        "age": 32,

        "gender": "Male"

    }

    {

        "first_name": "John",

        "last_name": "Doe",

        "age": 32,

        "gender": "Male"

    }

   3. Different documents in a single collection can have incompatible field data types
   Hence, the schema of the collection cannot be determined by reading one or a few documents.

   Two documents in a single MongoDB collection can have fields with values of different types.

    {

        "name": "John Doe",

        "age": 32,

        "gender": "Male"

        "mobile": "(424) 226-6998"

    }
   and

    {
        "name": "John Doe",

        "age": 32,

        "gender": "Male",

        "mobile": 4242266998

    }
   The field mobile is a string and a number in the above documents respectively.
   It is a completely valid state in MongoDB. In PostgreSQL, however, both these values
   either will have to be converted to a string or a number before being persisted.

   4. New fields can be added to a document at any point in time
   It is possible to add columns to a document in MongoDB by running a simple update
   to the document. In PostgreSQL, however, the process is harder as you have to construct
   and run ALTER statements each time a new field is detected.

   5. Character lengths of String columns
   MongoDB doesn’t put a limit on the length of the string columns.
   It has a 16MB limit on the size of the entire document.
   However, in PostgreSQL, it is a common practice to restrict string columns
   to a certain maximum length for better space utilization.
   Hence, each time you encounter a longer value than expected,
   you will have to resize the column.

   6. A document can have nested objects and arrays with a dynamic structure
   The most complex of MongoDB ETL problems is handling nested objects and arrays.

    {

        "name": "John Doe",

        "age": 32,

        "gender": "Male",

        "address": {

            "street": "1390 Market St",

            "city": "San Francisco",

            "state": "CA"

        },

        "groups": ["Sports", "Technology"]

    }
   MongoDB allows nesting objects and arrays to a number of levels.
   In a complex real-life scenario is may become a nightmare trying to flatten
   such documents into rows for a PostgreSQL table.

   7. Data Type incompatibility between MongoDB and PostgreSQL
   Not all data types of MongoDB are compatible with PostgreSQL.
   ObjectId, Regular Expression, Javascript are not supported by PostgreSQL.