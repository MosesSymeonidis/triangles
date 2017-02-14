#Triangles counting algorithm in Hadoop

This is a benchmark about 3 trianlge counting algorithms in map-reduce infrastracture (hadoop).

The algorithms takes two arguments. The first argument is the input file and the second argument is the output file.

For GraphPartition algorithm (gp) and for TriangleTypePartition algorithm (ttp) we have a third argument which is the number of partitions. (the max number of partitions for gp algorithm is 22 because of the limit of long type in java)

An example of execution:
`hadoop jar ~/ttp.jar {{path_of_input}}/0.edges {{path_of_output}}/outputTTP 20`

For verification, we used the following datasets: 
https://snap.stanford.edu/data/index.html 
and especially 
https://snap.stanford.edu/data/egonets-Facebook.html

In statistics.pdf there are some statistics about the execution time of different configurations. 

The project was part of the **Advanced Data Mining** class of the Master's progra in Aristotle University of Thessaloniki, taught by A. Gounaris and A. N. Papadopoulos.

## Team members

* Moses Symeonidis - [email](msymewnidhs2113@yahoo.gr)
* Dimitrios Giouroukis - [email](dimitrios.giouroukis@gmail.com)
* Konstantinos Arvanitis

##REFERENCES

http://islab.kaist.ac.kr/chungcw/InterConfPapers/km0805-ha-myung.pdf

https://theory.stanford.edu/~sergei/papers/www11-triangles.pdf

https://pdfs.semanticscholar.org/7805/de482edfbec3a736bb6b3d1bb5163435752d.pdf

