# Simple data transfer tool

## Usage
* wright sql query and put it into query folder as .sql file
* fill the `config/config.yaml` up for a source db in the source_name section
* fill the `config/config.yaml` up for a sink db in the sink_name section
* run `python etl.py --from your_source_name --to your_sink_name`
* check `logs/info.log` and `logs/errors.log`

## TODO
* add csv support