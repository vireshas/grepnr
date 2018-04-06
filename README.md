# grepnr
Concurrently search s3 access logs for a regex pattern and write the matches to an output file

### Usage:
> grepnr PREFIX REGEX OUTPUTFILE CONCURRENY(default: 20)  
> grepnr "images/2018-04-01" "DELETE.*profile1.png" out 40

Takes about 5m3.202009292s to search 4970 s3-access-logs on a t2.micro.
