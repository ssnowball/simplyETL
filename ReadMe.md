# Flow Maker

This tool allows a user not used to data extraction to easily create ETL pipelines with there data.
    - *only to CSV for now

## Database Connections

- open source <https://rnacentral.org/help/public-database>

## Workings

The tool queries the datasource and output the results into a KV store named BadgeDB - <https://dgraph.io/docs/badger/get-started/#using-key-value-pairs>

- distrubuted data store
- option to use persistant or memory (*currently using persistant)

## How to Use

You drag a command from the list on the left into the central window. Once in the central view you populate the settings in the right section of the sceen.

There are buttons to help you and to run your final command instructions
- Lock
- Zoom in
- Zoom out
- Zoom reset
- Clear Screen
- Export (run instructions)

### Basic example

1. Drag the 'start' command from the list on the left
2. Drag a 'datasource' command
    1. connect start to datasource
    2. save
3. Drag a 'dataget' command
    1. connect to datasource
    2. save
4. Drag a 'select' command
    1. connect to dataget
    2. save
5. Drag a 'end' command
    1. connect to select
    2. save
6. Click the Export button to run the instructions - as testing still
    - no error or response output to screen, can look into console to see outputs
    - only outputs to CSV file called `outFileName`

## TODO

- [ ] right join / Cross join?
- [ ] Mutation of columns
- [ ] In filter
- [ ] Notie feedback
- [ ] Other channels to output data - add command to list
- [ ] Pivot data?
