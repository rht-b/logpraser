## Setups to run the program:
1. Install go runtime.
2. Install dependencies by running `go mod download`
3. Command to run the program
   `go run main.go -t1="2020-08-09 18:29:00,000" -t2="2020-08-09 18:59:40,000"`
4. The clustered logs will be created in `./mergedlogs` directory.
5. The result of the queries will be printed in terminal based on the input
   files in `./rawlogs` directory.