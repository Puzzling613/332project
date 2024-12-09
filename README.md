# CSED332 Distributed Sorting Project
Team Black: [조민석](https://github.com/Puzzling613), [백지은](https://github.com/hsprt218), [홍지우](https://github.com/jiwooh)  
Status: WIP

# Instructions
## Prepare
```
git clone https://github.com/Puzzling613/332project.git
cd 332project
wget https://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -xvf gensort-linux-1.5.tar.gz
```
## Build
```
cd DistributedSorting
sbt compile
```
## Run
```
sbt runMain machine.MasterApp
sbt runMain machine.Worker [MASTER_IP]:7777 -I [ABSOLUTE_PATH_OF_INPUT_DIRECTORIES] -O [ABSOLUTE_PATH_OF_OUTPUT_DIRECTORY]
```
## Test
```
./gensort -a -b[start_num] [size] partition_[worker_num]
```
