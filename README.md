# MapreduceWHoToFollow
Mapreduce Program to create Twitter functionality of Who to follow

**Folders and Files**
 1.FinalCode:Contains the final code for WHo to follow problem
 2.Input:Consists of input file for the problem
 3.IntermediateCodes:can ignore:) has only the intermediate codes used for my reference for second map and reduce
 4.output:consists of final and temporary outputs
 
**Steps**
1.Mention the values of all Strings in main class according to your input and output directory for eg.
 
   1.1inputDir = "/home/nids/WTFocomb/input.txt"; --input file's path
   1.2outputTempDir = "/home/nids/WTFocomb/outputTemp"; --Don't create the output folder,just mention the  directory and its name(for   output of first Reduce function)
   1.3.outputFinalDir = "/home/nids/WTFocomb/outputFinal"; --Don't create the output folder,just mention the directory and its name(for output of final Reduce function)
   1.4Compile in command line using command 'javac filename.java'
2.Create a jar file using command 'jar cvf jar_name.jar class_name*.class'(class name generated after compiling, its actually the same as the filename)

3.Finally, run the file using command 'hadoop jar jar_name.jar file_name'

**Notes**
1.Java Version needed -Java 8
2.Hadoop 2.7.3
