# MapreduceWHoToFollow
Mapreduce Program to create Twitter functionality of Who to follow

#Folders and Files
 -FinalCode:Contains the final code for WHo to follow problem
 -Input:Consists of input file for the problem
 -IntermediateCodes:can ignore:) has only the intermediate codes used for my reference for second map and reduce
 -output:consists of final and temporary outputs
 
#Steps
 -Mention the values of all Strings in main class according to your input and output directory for eg.
   inputDir = "/home/nids/WTFocomb/input.txt"; --input file's path
   outputTempDir = "/home/nids/WTFocomb/outputTemp"; --Don't create the output folder,just mention the directory and its name(for output 
   of first Reduce function)
   outputFinalDir = "/home/nids/WTFocomb/outputFinal"; --Don't create the output folder,just mention the directory and its name(for output 
   of final Reduce function)
 -Compile in command line using command 'javac filename.java'
 -Create a jar file using command 'jar cvf jar_name.jar class_name*.class'(class name generated after compiling, its actually the same 
  as the   filename)
 -Finally, run the file using command 'hadoop jar jar_name.jar file_name'

#Notes
 Java Version needed -Java 8
 Hadoop 2.7.3
 

