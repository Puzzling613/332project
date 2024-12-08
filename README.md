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
sbt runMain ds.MasterApp
```
## Test
```
./gensort -b 1000 test.txt
```
