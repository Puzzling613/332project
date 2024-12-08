# CSED332 Distributed Sorting Project
Team Black: [조민석](github.com/Puzzling613), [백지은](github.com/hsprt218), [홍지우](github.com/jiwooh)  
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
