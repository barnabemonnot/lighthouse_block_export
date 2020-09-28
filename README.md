# lighthouse_block_export

## Requirements

python3.8 or later

## Install

**leveldb** must be installed, on Mac OS, run `brew install leveldb`.

```
pipenv install
pipenv shell
```

## Usage

Run a lighthouse beacon node for some amount of time:

```
$ lighthouse bn --testnet medalla
```

When you've built up enough block history to analyze, stop that process and run the following:

```
$ python3 export.py --datadir $LIGHTHOUSE_DATA_DIR --outdir $OUTPUT_DIR
```

Note that the default location for `$LIGHTHOUSE_DATA_DIR` is `$HOME/.lighthouse`.

### Other options

- `-s --stepsize`: How many blocks each data file covers.
- `-st --startslot`: Start slot (inclusive).
- `-en --endslot`: End slot (exclusive).
